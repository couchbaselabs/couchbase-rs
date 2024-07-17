use std::future::Future;
use std::net::SocketAddr;
use std::ops::{Add, Deref};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use tokio_rustls::rustls::RootCertStore;
use uuid::Uuid;

use crate::authenticator::Authenticator;
use crate::error::CoreError;
use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::connection::{Connection, ConnectOptions};
use crate::memdx::dispatcher::{Dispatcher, DispatcherOptions};
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::op_auth_saslauto::SASLAuthAutoOptions;
use crate::memdx::op_bootstrap::BootstrapOptions;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::request::{GetErrorMapRequest, HelloRequest, SelectBucketRequest};
use crate::result::CoreResult;
use crate::service_type::ServiceType;

#[derive(Debug, Clone)]
pub(crate) struct KvClientConfig {
    pub address: SocketAddr,
    pub root_certs: Option<RootCertStore>,
    pub accept_all_certs: Option<bool>,
    pub client_name: String,
    pub authenticator: Option<Arc<dyn Authenticator>>,
    pub selected_bucket: Option<String>,
    pub disable_default_features: bool,
    pub disable_error_map: bool,

    // disable_bootstrap provides a simple way to validate that all bootstrapping
    // is disabled on the client, mainly used for testing.
    pub disable_bootstrap: bool,
}

impl PartialEq for KvClientConfig {
    fn eq(&self, other: &Self) -> bool {
        // TODO: compare root certs or something somehow.
        self.address == other.address
            && self.accept_all_certs == other.accept_all_certs
            && self.client_name == other.client_name
            && self.selected_bucket == other.selected_bucket
            && self.disable_default_features == other.disable_default_features
            && self.disable_error_map == other.disable_error_map
            && self.disable_bootstrap == other.disable_bootstrap
    }
}

pub(crate) struct KvClientOptions {
    pub orphan_handler: Arc<UnboundedSender<ResponsePacket>>,
    pub on_close_tx: Option<UnboundedSender<String>>,
}

pub(crate) trait KvClient: Sized + PartialEq + Send + Sync {
    fn new(
        config: KvClientConfig,
        opts: KvClientOptions,
    ) -> impl Future<Output = CoreResult<Self>> + Send;
    fn reconfigure(&self, config: KvClientConfig) -> impl Future<Output = CoreResult<()>> + Send;
    fn has_feature(&self, feature: HelloFeature) -> bool;
    fn load_factor(&self) -> f64;
    fn remote_addr(&self) -> SocketAddr;
    fn local_addr(&self) -> Option<SocketAddr>;
    fn close(&self) -> impl Future<Output = CoreResult<()>> + Send;
    fn id(&self) -> &str;
}

// TODO: connect timeout
pub(crate) struct StdKvClient<D: Dispatcher> {
    remote_addr: SocketAddr,
    local_addr: Option<SocketAddr>,

    pending_operations: u64,
    cli: D,
    current_config: Mutex<KvClientConfig>,

    supported_features: Vec<HelloFeature>,

    // selected_bucket atomically stores the currently selected bucket,
    // so that we can use it in our errors.  Note that it is set before
    // we send the operation to select the bucket, since things happen
    // asynchronously and we do not support changing selected buckets.
    selected_bucket: Mutex<Option<String>>,

    closed: Arc<AtomicBool>,

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
    async fn new(config: KvClientConfig, opts: KvClientOptions) -> CoreResult<StdKvClient<D>> {
        let requested_features = if config.disable_default_features {
            vec![]
        } else {
            vec![
                HelloFeature::DataType,
                HelloFeature::SeqNo,
                HelloFeature::Xattr,
                HelloFeature::Xerror,
                HelloFeature::Snappy,
                HelloFeature::Json,
                HelloFeature::UnorderedExec,
                HelloFeature::Durations,
                HelloFeature::SyncReplication,
                HelloFeature::ReplaceBodyWithXattr,
                HelloFeature::SelectBucket,
                HelloFeature::CreateAsDeleted,
                HelloFeature::AltRequests,
                HelloFeature::Collections,
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

        let bootstrap_auth = if let Some(ref auth) = config.authenticator {
            let creds = auth.get_credentials(ServiceType::Memd, config.address.to_string())?;

            Some(SASLAuthAutoOptions {
                username: creds.username.clone(),
                password: creds.password.clone(),
                enabled_mechs: vec![AuthMechanism::ScramSha512, AuthMechanism::ScramSha256],
            })
        } else {
            None
        };

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
            // TODO: error model needs thought.
            return Err(CoreError::Placeholder(
                "Bootstrap was disabled but options requiring bootstrap were specified".to_string(),
            ));
        }

        let (connection_close_tx, mut connection_close_rx) =
            oneshot::channel::<crate::memdx::client::MemdxResult<()>>();
        let memdx_client_opts = DispatcherOptions {
            on_connection_close_handler: Some(connection_close_tx),
            orphan_handler: opts.orphan_handler,
        };

        let closed = Arc::new(AtomicBool::new(false));
        let closed_clone = closed.clone();

        let conn = Connection::connect(
            config.address,
            ConnectOptions {
                tls_config: None,
                deadline: Instant::now().add(Duration::new(7, 0)),
            },
        )
        .await?;

        let remote_addr = match conn.peer_addr() {
            Some(addr) => *addr,
            None => config.address,
        };

        let local_addr = *conn.local_addr();

        let mut cli = D::new(conn, memdx_client_opts);
        let id = Uuid::new_v4().to_string();

        let mut kv_cli = StdKvClient {
            remote_addr,
            local_addr,
            pending_operations: 0,
            cli,
            current_config: Mutex::new(config),
            supported_features: vec![],
            selected_bucket: Mutex::new(None),
            closed,
            id: id.clone(),
        };

        tokio::spawn(async move {
            // There's not much to do when the connection closes so just mark us as closed.
            if connection_close_rx.await.is_ok() {
                closed_clone.store(true, Ordering::SeqCst);
            };

            if let Some(mut tx) = opts.on_close_tx {
                // TODO: Probably log on failure.
                tx.send(id).unwrap_or_default();
            }
        });

        if should_bootstrap {
            if let Some(b) = &bootstrap_select_bucket {
                let mut guard = kv_cli.selected_bucket.lock().await;
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
                    return Err(e);
                }
            };

            if let Some(hello) = res.hello {
                kv_cli.supported_features = hello.enabled_features;
            }
        }

        Ok(kv_cli)
    }

    async fn reconfigure(&self, config: KvClientConfig) -> CoreResult<()> {
        let mut current_config = self.current_config.lock().await;

        // TODO: compare root certs or something somehow.
        if !(current_config.address == config.address
            && current_config.accept_all_certs == config.accept_all_certs
            && current_config.client_name == config.client_name
            && current_config.disable_default_features == config.disable_default_features
            && current_config.disable_error_map == config.disable_error_map
            && current_config.disable_bootstrap == config.disable_bootstrap)
        {
            return Err(CoreError::Placeholder(
                "Cannot reconfigure due to conflicting options".to_string(),
            ));
        }

        let selected_bucket_name = if current_config.selected_bucket != config.selected_bucket {
            if current_config.selected_bucket.is_some() {
                return Err(CoreError::Placeholder(
                    "Cannot reconfigure from one selected bucket to another".to_string(),
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
            return Err(CoreError::Placeholder(
                "Client config after reconfigure did not match new configuration".to_string(),
            ));
        }

        if let Some(bucket_name) = selected_bucket_name {
            let mut current_bucket = self.selected_bucket.lock().await;
            *current_bucket = Some(bucket_name.clone());
            drop(current_bucket);

            match self
                .select_bucket(SelectBucketRequest { bucket_name })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    let mut current_bucket = self.selected_bucket.lock().await;
                    *current_bucket = None;
                    drop(current_bucket);

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

    fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    async fn close(&self) -> CoreResult<()> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Err(CoreError::Placeholder("Client closed".to_string()));
        }

        Ok(self.cli.close().await?)
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

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::Instant;

    use crate::authenticator::PasswordAuthenticator;
    use crate::kvclient::{KvClient, KvClientConfig, KvClientOptions, StdKvClient};
    use crate::memdx::client::Client;
    use crate::memdx::packet::ResponsePacket;
    use crate::memdx::request::{GetRequest, SetRequest};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roundtrip_a_request() {
        let _ = env_logger::try_init();

        let instant = Instant::now().add(Duration::new(7, 0));

        let (orphan_tx, mut orphan_rx) = unbounded_channel::<ResponsePacket>();

        tokio::spawn(async move {
            loop {
                match orphan_rx.recv().await {
                    Some(resp) => {
                        dbg!("unexpected orphan", resp);
                    }
                    None => {
                        return;
                    }
                }
            }
        });

        let client_config = KvClientConfig {
            address: "192.168.107.128:11210"
                .parse()
                .expect("Failed to parse address"),
            root_certs: None,
            accept_all_certs: None,
            client_name: "myclient".to_string(),
            authenticator: Some(Arc::new(PasswordAuthenticator {
                username: "Administrator".to_string(),
                password: "password".to_string(),
            })),
            selected_bucket: Some("default".to_string()),
            disable_default_features: false,
            disable_error_map: false,
            disable_bootstrap: false,
        };

        let mut client = StdKvClient::<Client>::new(
            client_config,
            KvClientOptions {
                orphan_handler: Arc::new(orphan_tx),
                on_close_tx: None,
            },
        )
        .await
        .unwrap();

        let result = client
            .set(SetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                flags: 0,
                value: "test".as_bytes().into(),
                datatype: 0,
                expiry: None,
                preserve_expiry: None,
                cas: None,
                on_behalf_of: None,
                durability_level: None,
                durability_level_timeout: None,
            })
            .await
            .unwrap();

        dbg!(result);

        let get_result = client
            .get(GetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                on_behalf_of: None,
            })
            .await
            .unwrap();

        dbg!(get_result);

        client.close().await.unwrap();
    }
}
