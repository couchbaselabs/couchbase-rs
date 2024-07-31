use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::cbconfig::TerseConfig;

pub(crate) trait NotMyVbucketConfigHandler {
    async fn not_my_vbucket_config(&self, config: TerseConfig, source_hostname: &str);
}

pub(crate) trait ConfigUpdater: Send + Sync + Sized {
    async fn apply_terse_config(&self, config: TerseConfig, source_hostname: &str);
}

pub(crate) struct StdNotMyVbucketConfigHandler<C> {
    watcher: Mutex<Option<Arc<C>>>,
}

impl<C> StdNotMyVbucketConfigHandler<C>
where
    C: ConfigUpdater,
{
    pub fn new() -> Self {
        Self {
            watcher: Mutex::new(None),
        }
    }

    pub async fn set_watcher(&self, updater: Arc<C>) {
        let mut watcher = self.watcher.lock().await;
        *watcher = Some(updater);
    }
}

impl<C> NotMyVbucketConfigHandler for StdNotMyVbucketConfigHandler<C>
where
    C: ConfigUpdater,
{
    async fn not_my_vbucket_config(&self, config: TerseConfig, source_hostname: &str) {
        if let Some(watcher) = self.watcher.lock().await.deref() {
            watcher.apply_terse_config(config, source_hostname).await;
        }
    }
}
