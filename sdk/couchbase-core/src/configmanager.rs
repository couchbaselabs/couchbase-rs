use crate::agent::AgentInner;
use crate::cbconfig::TerseConfig;
use crate::configfetcher::ConfigFetcherMemd;
use crate::configparser::ConfigParser;
use crate::configwatcher::{ConfigWatcherMemd, ConfigWatcherMemdConfig, ConfigWatcherMemdOptions};
use crate::kvclientmanager::KvClientManager;
use crate::nmvbhandler::ConfigUpdater;
use crate::parsedconfig::{ParsedConfig, ParsedConfigBucket};
use log::{debug, warn};
use std::cmp::Ordering;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, watch, Notify};

pub(crate) trait ConfigManager: Sized + Send + Sync {
    fn watch(&self) -> watch::Receiver<ParsedConfig>;
    fn reconfigure(&self, config: ConfigManagerMemdConfig) -> crate::error::Result<()>;
    fn out_of_band_version(
        &self,
        rev_id: i64,
        rev_epoch: i64,
    ) -> impl Future<Output = Option<ParsedConfig>> + Send;
    fn out_of_band_config(&self, config: ParsedConfig) -> Option<ParsedConfig>;
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigVersion {
    pub rev_epoch: i64,
    pub rev_id: i64,
}

pub(crate) struct ConfigManagerMemdConfig {
    pub endpoints: Vec<String>,
}

pub(crate) struct ConfigManagerMemdOptions<M: KvClientManager> {
    pub polling_period: Duration,
    pub first_config: ParsedConfig,
    pub kv_client_manager: Arc<M>,
    pub on_shutdown_rx: broadcast::Receiver<()>,
}

pub(crate) struct ConfigManagerMemd<M: KvClientManager> {
    inner: Arc<ConfigManagerMemdInner<M>>,
}

pub(crate) struct ConfigManagerMemdInner<M: KvClientManager> {
    fetcher: Arc<ConfigFetcherMemd<M>>,
    watcher: Arc<ConfigWatcherMemd<M>>,

    out_of_band_notify: Notify,
    performing_out_of_band_fetch: AtomicBool,

    latest_config: Arc<Mutex<ParsedConfig>>,
    latest_version_tx: watch::Sender<ConfigVersion>,
    bucket: Mutex<Option<ParsedConfigBucket>>,

    on_new_config_tx: watch::Sender<ParsedConfig>,

    on_shutdown_rx: broadcast::Receiver<()>,
    watcher_shutdown_tx: broadcast::Sender<()>,
}

impl<M: KvClientManager + 'static> ConfigManagerMemdInner<M> {
    pub fn watch(&self) -> watch::Receiver<ParsedConfig> {
        self.on_new_config_tx.subscribe()
    }

    pub fn reconfigure(&self, config: ConfigManagerMemdConfig) -> crate::error::Result<()> {
        self.watcher.reconfigure(ConfigWatcherMemdConfig {
            endpoints: config.endpoints,
        })
    }

    async fn perform_out_of_band_fetch(&self, rev_id: i64, rev_epoch: i64) -> Option<ParsedConfig> {
        loop {
            let (latest_rev_epoch, latest_rev_id) = {
                let latest_config = self.latest_config.lock().unwrap();
                (latest_config.rev_epoch, latest_config.rev_id)
            };

            if rev_epoch < latest_rev_epoch
                || (rev_epoch == latest_rev_epoch && rev_id <= latest_rev_id)
            {
                debug!(
                    "Skipping out-of-band fetch, already have newer or same version: new: \
                rev_epoch={rev_epoch}, rev_id={rev_id}, old: rev_epoch={latest_rev_epoch}, \
                rev_id={latest_rev_id}"
                );
                // No need to poll, we already have a newer version.
                return None;
            }

            if self.performing_out_of_band_fetch.compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) == Ok(true)
            {
                // Right now we don't reach here because unsolicited packets are sent on a channel
                // so read serially.
                self.out_of_band_notify.notified().await;
                continue;
            } else {
                let endpoints = self.watcher.endpoints();

                for endpoint in endpoints {
                    let parsed_config = match self
                        .fetcher
                        .poll_one(&endpoint, latest_rev_id, latest_rev_epoch, |_c| false)
                        .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("Out-of-band fetch from {endpoint} failed: {e}");
                            continue;
                        }
                    };

                    if let Some(parsed_config) = parsed_config {
                        if let Some(cfg) = Self::handle_config(
                            self.latest_config.lock().unwrap(),
                            parsed_config,
                            self.latest_version_tx.clone(),
                        ) {
                            self.performing_out_of_band_fetch
                                .store(false, std::sync::atomic::Ordering::SeqCst);
                            self.out_of_band_notify.notify_waiters();

                            return Some(cfg);
                        };
                    }
                }

                debug!("No newer config found during out-of-band fetch");

                self.performing_out_of_band_fetch
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                self.out_of_band_notify.notify_waiters();

                return None;
            }
        }
    }

    pub async fn out_of_band_version(&self, rev_id: i64, rev_epoch: i64) -> Option<ParsedConfig> {
        self.perform_out_of_band_fetch(rev_id, rev_epoch).await
    }

    pub fn out_of_band_config(&self, parsed_config: ParsedConfig) -> Option<ParsedConfig> {
        Self::handle_config(
            self.latest_config.lock().unwrap(),
            parsed_config,
            self.latest_version_tx.clone(),
        )
    }

    fn handle_config(
        mut latest_config: MutexGuard<ParsedConfig>,
        parsed_config: ParsedConfig,
        latest_version_tx: watch::Sender<ConfigVersion>,
    ) -> Option<ParsedConfig> {
        if Self::can_update_config(&parsed_config, latest_config.deref()) {
            let new_latest_version = ConfigVersion {
                rev_epoch: parsed_config.rev_epoch,
                rev_id: parsed_config.rev_id,
            };
            *latest_config = parsed_config.clone();
            drop(latest_config);

            if let Err(e) = latest_version_tx.send(new_latest_version) {
                warn!("Failed to update config watcher with latest version: {e}");
            }

            return Some(parsed_config);
        }

        None
    }

    fn shutdown(&self) {
        if let Err(e) = self.watcher_shutdown_tx.send(()) {
            debug!("Failed to send shutdown signal to watcher: {e}");
        }
    }

    fn can_update_config(new_config: &ParsedConfig, old_config: &ParsedConfig) -> bool {
        if new_config.bucket != old_config.bucket {
            debug!(
                "Switching config due to changed bucket type (bucket takeover) old: {:?} new: {:?}",
                old_config.bucket, new_config.bucket
            );
            return true;
        } else if let Some(cmp) = new_config.partial_cmp(old_config) {
            if cmp == Ordering::Less {
                debug!("Skipping config due to new config being an older revision old: rev_epoch={}, rev_id={} new: rev_epoch={}, rev_id={}",
                    old_config.rev_epoch, old_config.rev_id, new_config.rev_epoch, new_config.rev_id);
            } else if cmp == Ordering::Equal {
                debug!("Skipping config due to matching revisions old: rev_epoch={}, rev_id={} new: rev_epoch={}, rev_id={}",
                    old_config.rev_epoch, old_config.rev_id, new_config.rev_epoch, new_config.rev_id);
            } else {
                return true;
            }
        }

        false
    }

    pub fn start_watcher(&self, watcher_shutdown_rx: broadcast::Receiver<()>) {
        let mut rx = self.watcher.watch(watcher_shutdown_rx);
        let latest_version_tx = self.latest_version_tx.clone();
        let guard = self.latest_config.clone();
        let new_config_tx = self.on_new_config_tx.clone();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(cfg) => {
                        if let Some(new_cfg) = Self::handle_config(
                            guard.lock().unwrap(),
                            cfg,
                            latest_version_tx.clone(),
                        ) {
                            new_config_tx.send_replace(new_cfg);
                        }
                    }
                    Err(e) => {
                        if e == RecvError::Closed {
                            debug!("Config watcher channel closed");
                            return;
                        } else {
                            warn!("Config watcher channel error: {e}");
                        }
                    }
                }
            }
        });
    }
}

impl<M: KvClientManager + 'static> ConfigManagerMemd<M> {
    pub fn new(
        config: ConfigManagerMemdConfig,
        opts: ConfigManagerMemdOptions<M>,
    ) -> ConfigManagerMemd<M> {
        let latest_version = ConfigVersion {
            rev_epoch: opts.first_config.rev_epoch,
            rev_id: opts.first_config.rev_id,
        };

        let (latest_version_tx, latest_version_rx) = watch::channel(latest_version.clone());

        let fetcher = Arc::new(ConfigFetcherMemd::new(opts.kv_client_manager));
        let watcher = Arc::new(ConfigWatcherMemd::new(
            ConfigWatcherMemdConfig {
                endpoints: config.endpoints,
            },
            ConfigWatcherMemdOptions {
                polling_period: opts.polling_period,
                config_fetcher: fetcher.clone(),
                latest_version_rx,
            },
        ));

        let (watcher_shutdown_tx, watcher_shutdown_rx) = broadcast::channel(1);
        let bucket = opts.first_config.bucket.clone();

        let (on_new_config_tx, _on_new_config_rx) =
            watch::channel::<ParsedConfig>(opts.first_config.clone());

        let inner = Arc::new(ConfigManagerMemdInner {
            fetcher,
            watcher: watcher.clone(),

            out_of_band_notify: Notify::new(),
            performing_out_of_band_fetch: AtomicBool::new(false),

            latest_config: Arc::new(Mutex::new(opts.first_config)),
            latest_version_tx,
            bucket: Mutex::new(bucket),

            on_new_config_tx,

            on_shutdown_rx: opts.on_shutdown_rx,
            watcher_shutdown_tx,
        });

        inner.start_watcher(watcher_shutdown_rx);

        ConfigManagerMemd { inner }
    }
}

impl<M: KvClientManager + 'static> ConfigManager for ConfigManagerMemd<M> {
    fn watch(&self) -> watch::Receiver<ParsedConfig> {
        self.inner.watch()
    }

    fn reconfigure(&self, config: ConfigManagerMemdConfig) -> crate::error::Result<()> {
        self.inner.reconfigure(config)
    }

    async fn out_of_band_version(&self, rev_id: i64, rev_epoch: i64) -> Option<ParsedConfig> {
        let inner = self.inner.clone();
        inner.out_of_band_version(rev_id, rev_epoch).await
    }

    fn out_of_band_config(&self, config: ParsedConfig) -> Option<ParsedConfig> {
        self.inner.out_of_band_config(config)
    }
}
