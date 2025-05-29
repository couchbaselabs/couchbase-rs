use std::future::Future;
use std::net::SocketAddr;
use std::ops::{Add, Deref};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::auth_mechanism::AuthMechanism;
use crate::authenticator::Authenticator;
use crate::error::Error;
use crate::error::{MemdxError, Result};
use crate::memdx;
use crate::memdx::connection::{ConnectOptions, ConnectionType, TcpConnection, TlsConnection};
use crate::memdx::dispatcher::{Dispatcher, DispatcherOptions, OrphanResponseHandler};
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::op_auth_saslauto::SASLAuthAutoOptions;
use crate::memdx::op_bootstrap::BootstrapOptions;
use crate::memdx::request::{GetErrorMapRequest, HelloRequest, SelectBucketRequest};
use crate::service_type::ServiceType;
use crate::tls_config::TlsConfig;
use crate::util::hostname_from_addr_str;
use futures::future::BoxFuture;
use log::debug;
use tokio::sync::Mutex;
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct KvClientConfig {
    pub address: String,
    pub tls: Option<TlsConfig>,
    pub client_name: String,
    pub authenticator: Arc<Authenticator>,
    pub selected_bucket: Option<String>,
    pub disable_default_features: bool,
    pub disable_error_map: bool,
    pub auth_mechanisms: Vec<AuthMechanism>,

    // disable_bootstrap provides a simple way to validate that all bootstrapping
    // is disabled on the client, mainly used for testing.
    pub disable_bootstrap: bool,
}

impl PartialEq for KvClientConfig {
    fn eq(&self, other: &Self) -> bool {
        // TODO: compare root certs or something somehow.
        self.address == other.address
            && self.client_name == other.client_name
            && self.selected_bucket == other.selected_bucket
            && self.disable_default_features == other.disable_default_features
            && self.disable_error_map == other.disable_error_map
            && self.disable_bootstrap == other.disable_bootstrap
    }
}

pub(crate) type OnKvClientCloseHandler =
    Arc<dyn Fn(String) -> BoxFuture<'static, ()> + Send + Sync>;

pub(crate) type OnErrMapFetchedHandler = Arc<dyn Fn(&[u8]) + Send + Sync>;

pub(crate) struct KvClientOptions {
    pub orphan_handler: OrphanResponseHandler,
    pub on_close: OnKvClientCloseHandler,
    pub on_err_map_fetched: Option<OnErrMapFetchedHandler>,
    pub disable_decompression: bool,
}

pub(crate) trait KvClient: Sized + PartialEq + Send + Sync {
    fn new(
        config: KvClientConfig,
        opts: KvClientOptions,
    ) -> impl Future<Output = Result<Self>> + Send;
    fn reconfigure(&self, config: KvClientConfig) -> impl Future<Output = Result<()>> + Send;
    fn has_feature(&self, feature: HelloFeature) -> bool;
    fn load_factor(&self) -> f64;
    fn remote_hostname(&self) -> &str;
    fn remote_addr(&self) -> SocketAddr;
    fn local_addr(&self) -> SocketAddr;
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
    fn id(&self) -> &str;
}

// TODO: connect timeout
pub(crate) struct StdKvClient<D: Dispatcher> {
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    remote_hostname: String,

    pending_operations: u64,
    cli: D,
    current_config: Mutex<KvClientConfig>,

    supported_features: Vec<HelloFeature>,

    // selected_bucket atomically stores the currently selected bucket,
    // so that we can use it in our errors.  Note that it is set before
    // we send the operation to select the bucket, since things happen
    // asynchronously and we do not support changing selected buckets.
    pub(crate) selected_bucket: std::sync::Mutex<Option<String>>,

    id: String,
}

impl<D> StdKvClient<D>
where
    D: Dispatcher,
{
    pub fn client(&self) -> &D {
        &self.cli
    }
}

impl<D> KvClient for StdKvClient<D>
where
    D: Dispatcher,
{
    async fn new(config: KvClientConfig, opts: KvClientOptions) -> Result<StdKvClient<D>> {
        let requested_features = if config.disable_default_features {
            vec![]
        } else {
            vec![
                HelloFeature::DataType,
                HelloFeature::SeqNo,
                HelloFeature::Xattr,
                HelloFeature::Xerror,
                HelloFeature::Snappy,
                HelloFeature::SnappyEverywhere,
                HelloFeature::Json,
                HelloFeature::UnorderedExec,
                HelloFeature::Durations,
                HelloFeature::SyncReplication,
                HelloFeature::ReplaceBodyWithXattr,
                HelloFeature::SelectBucket,
                HelloFeature::CreateAsDeleted,
                HelloFeature::AltRequests,
                HelloFeature::Collections,
                HelloFeature::ClusterMapKnownVersion,
                HelloFeature::DedupeNotMyVbucketClustermap,
            ]
        };

        let boostrap_hello = if !config.client_name.is_empty() && !requested_features.is_empty() {
            Some(HelloRequest {
                client_name: Vec::from(config.client_name.clone()),
                requested_features,
            })
        } else {
            None
        };

        let bootstrap_get_error_map = if !config.disable_error_map {
            Some(GetErrorMapRequest { version: 2 })
        } else {
            None
        };

        let creds = match config.authenticator.as_ref() {
            // PasswordAuthenticator(auth) => get_credentials(ServiceType::Memd, config.address.to_string())
            Authenticator::PasswordAuthenticator(a) => {
                a.get_credentials(&ServiceType::MEMD, config.address.to_string())?
            }
        };

        let bootstrap_auth = Some(SASLAuthAutoOptions {
            username: creds.username.clone(),
            password: creds.password.clone(),
            enabled_mechs: config
                .auth_mechanisms
                .iter()
                .cloned()
                .map(memdx::auth_mechanism::AuthMechanism::from)
                .collect(),
        });

        let bootstrap_select_bucket =
            config
                .selected_bucket
                .as_ref()
                .map(|bucket_name| SelectBucketRequest {
                    bucket_name: bucket_name.clone(),
                });

        let should_bootstrap = boostrap_hello.is_some()
            || bootstrap_auth.is_some()
            || bootstrap_get_error_map.is_some();

        if should_bootstrap && config.disable_bootstrap {
            return Err(Error::new_invalid_argument_error(
                "bootstrap was disabled but options requiring bootstrap were specified",
                None,
            ));
        }

        let closed = Arc::new(AtomicBool::new(false));
        let closed_clone = closed.clone();
        let id = Uuid::new_v4().to_string();
        let read_id = id.clone();

        let client_id = Uuid::new_v4().to_string();

        debug!(
            "Kvclient {} assigning client id {} for {}",
            &id, &client_id, &config.address
        );

        let on_close = opts.on_close.clone();
        let memdx_client_opts = DispatcherOptions {
            on_connection_close_handler: Arc::new(move || {
                // There's not much to do when the connection closes so just mark us as closed.
                closed_clone.store(true, Ordering::SeqCst);
                let on_close = on_close.clone();
                let read_id = read_id.clone();

                Box::pin(async move {
                    on_close(read_id).await;
                })
            }),
            orphan_handler: opts.orphan_handler,
            disable_decompression: opts.disable_decompression,
            id: client_id,
        };

        let conn = if let Some(tls) = config.tls.clone() {
            let conn = match TlsConnection::connect(
                &config.address,
                tls,
                ConnectOptions {
                    deadline: Instant::now().add(Duration::new(7, 0)),
                },
            )
            .await
            {
                Ok(conn) => conn,
                Err(e) => {
                    return Err(Error::new_contextual_memdx_error(
                        MemdxError::new(e).with_dispatched_to(config.address),
                    ))
                }
            };
            ConnectionType::Tls(conn)
        } else {
            let conn = match TcpConnection::connect(
                &config.address,
                ConnectOptions {
                    deadline: Instant::now().add(Duration::new(7, 0)),
                },
            )
            .await
            {
                Ok(conn) => conn,
                Err(e) => {
                    return Err(Error::new_contextual_memdx_error(
                        MemdxError::new(e).with_dispatched_to(config.address),
                    ))
                }
            };
            ConnectionType::Tcp(conn)
        };

        let remote_addr = *conn.peer_addr();
        let local_addr = *conn.local_addr();
        let remote_hostname = hostname_from_addr_str(&config.address);

        let mut cli = D::new(conn, memdx_client_opts);

        let mut kv_cli = StdKvClient {
            remote_addr,
            local_addr,
            remote_hostname,
            pending_operations: 0,
            cli,
            current_config: Mutex::new(config),
            supported_features: vec![],
            selected_bucket: std::sync::Mutex::new(None),
            id: id.clone(),
        };

        if should_bootstrap {
            if let Some(b) = &bootstrap_select_bucket {
                let mut guard = kv_cli.selected_bucket.lock().unwrap();
                *guard = Some(b.bucket_name.clone());
            };

            let res = match kv_cli
                .bootstrap(BootstrapOptions {
                    hello: boostrap_hello,
                    get_error_map: bootstrap_get_error_map,
                    auth: bootstrap_auth,
                    select_bucket: bootstrap_select_bucket,
                    deadline: Instant::now().add(Duration::from_secs(7)),
                    get_cluster_config: None,
                })
                .await
            {
                Ok(res) => res,
                Err(e) => {
                    kv_cli.close().await.unwrap_or_default();
                    return Err(Error::new_contextual_memdx_error(e));
                }
            };

            if let Some(hello) = res.hello {
                kv_cli.supported_features = hello.enabled_features;
            }

            if let Some(handler) = opts.on_err_map_fetched {
                if let Some(err_map) = res.error_map {
                    handler(&err_map.error_map);
                }
            }
        }

        Ok(kv_cli)
    }

    async fn reconfigure(&self, config: KvClientConfig) -> Result<()> {
        let mut current_config = self.current_config.lock().await;

        // TODO: compare root certs or something somehow.
        if !(current_config.address == config.address
            && current_config.client_name == config.client_name
            && current_config.disable_default_features == config.disable_default_features
            && current_config.disable_error_map == config.disable_error_map
            && current_config.disable_bootstrap == config.disable_bootstrap)
        {
            return Err(Error::new_invalid_argument_error(
                "cannot reconfigure due to conflicting options",
                None,
            ));
        }

        let selected_bucket_name = if current_config.selected_bucket != config.selected_bucket {
            if current_config.selected_bucket.is_some() {
                return Err(Error::new_invalid_argument_error(
                    "cannot reconfigure from one selected bucket to another",
                    None,
                ));
            }

            current_config
                .selected_bucket
                .clone_from(&config.selected_bucket);
            config.selected_bucket.clone()
        } else {
            None
        };

        if *current_config.deref() != config {
            return Err(Error::new_invalid_argument_error(
                "client config after reconfigure did not match new configuration",
                None,
            ));
        }

        if let Some(bucket_name) = selected_bucket_name {
            {
                let mut current_bucket = self.selected_bucket.lock().unwrap();
                *current_bucket = Some(bucket_name.clone());
            }

            match self
                .select_bucket(SelectBucketRequest { bucket_name })
                .await
            {
                Ok(_) => {}
                Err(_e) => {
                    {
                        let mut current_bucket = self.selected_bucket.lock().unwrap();
                        *current_bucket = None;
                    }

                    current_config.selected_bucket = None;
                }
            }
        }

        Ok(())
    }

    fn has_feature(&self, feature: HelloFeature) -> bool {
        self.supported_features.contains(&feature)
    }

    fn load_factor(&self) -> f64 {
        0.0
    }

    fn remote_hostname(&self) -> &str {
        &self.remote_hostname
    }

    fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    async fn close(&self) -> Result<()> {
        self.cli
            .close()
            .await
            .map_err(|e| Error::new_contextual_memdx_error(MemdxError::new(e)))
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl<D> PartialEq for StdKvClient<D>
where
    D: Dispatcher,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
