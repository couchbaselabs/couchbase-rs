use std::net::SocketAddr;
use std::ops::{Add, Deref};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_rustls::rustls::RootCertStore;

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

pub(crate) struct KvClientConfig {
    pub address: SocketAddr,
    pub root_certs: Option<RootCertStore>,
    pub accept_all_certs: Option<bool>,
    pub client_name: String,
    pub authenticator: Option<Box<dyn Authenticator>>,
    pub selected_bucket: Option<String>,
    pub disable_default_features: bool,
    pub disable_error_map: bool,

    // disable_bootstrap provides a simple way to validate that all bootstrapping
    // is disabled on the client, mainly used for testing.
    pub disable_bootstrap: bool,
}

pub(crate) struct KvClientOptions {
    orphan_handler: UnboundedSender<ResponsePacket>,
}

pub(crate) trait KvClient {
    fn reconfigure(&self, config: KvClientConfig, on_complete: oneshot::Sender<CoreResult<()>>);
    fn has_feature(&self, feature: HelloFeature) -> bool;
    fn load_factor(&self) -> f64;
    fn remote_addr(&self) -> SocketAddr;
    fn local_addr(&self) -> SocketAddr;
}

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
    selected_bucket: Arc<Mutex<Option<String>>>,

    closed: Arc<AtomicBool>,
}

impl<D> StdKvClient<D>
where
    D: Dispatcher,
{
    pub async fn new(config: KvClientConfig, opts: KvClientOptions) -> CoreResult<StdKvClient<D>> {
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
            return Err(CoreError {
                msg: "oopsies".to_string(),
            });
        }

        let (connection_close_tx, mut connection_close_rx) =
            oneshot::channel::<crate::memdx::client::Result<()>>();
        let memdx_client_opts = DispatcherOptions {
            on_connection_close_handler: Some(connection_close_tx),
            orphan_handler: opts.orphan_handler,
        };

        let closed = Arc::new(AtomicBool::new(false));
        let closed_clone = closed.clone();

        tokio::spawn(async move {
            // There's not much to do when the connection closes so just mark us as closed.
            if connection_close_rx.await.is_ok() {
                closed_clone.store(true, Ordering::Relaxed)
            };
        });

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

        // let mut cli = Client::new(conn, memdx_client_opts);

        let mut cli = D::new(conn, memdx_client_opts);

        let mut kv_cli = StdKvClient {
            remote_addr,
            local_addr,
            pending_operations: 0,
            cli,
            current_config: Mutex::new(config),
            supported_features: vec![],
            selected_bucket: Arc::new(Mutex::new(None)),
            closed,
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
                    return Err(e);
                }
            };

            if let Some(hello) = res.hello {
                kv_cli.supported_features = hello.enabled_features;
            }
        }

        Ok(kv_cli)
    }

    pub async fn close(self) -> CoreResult<()> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Err(CoreError {
                msg: "closed".to_string(),
            });
        }

        Ok(self.cli.close().await?)
    }

    pub fn client(&self) -> &D {
        &self.cli
    }

    pub fn client_mut(&mut self) -> &mut D {
        &mut self.cli
    }

    pub fn has_feature(&self, feature: HelloFeature) -> bool {
        self.supported_features.contains(&feature)
    }
}
#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::time::Duration;

    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::Instant;

    use crate::authenticator::PasswordAuthenticator;
    use crate::kvclient::{KvClientConfig, KvClientOptions, StdKvClient};
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

        let mut client = StdKvClient::<Client>::new(
            KvClientConfig {
                address: "192.168.107.128:11210"
                    .parse()
                    .expect("Failed to parse address"),
                root_certs: None,
                accept_all_certs: None,
                client_name: "myclient".to_string(),
                authenticator: Some(Box::new(PasswordAuthenticator {
                    username: "Administrator".to_string(),
                    password: "password".to_string(),
                })),
                selected_bucket: Some("default".to_string()),
                disable_default_features: false,
                disable_error_map: false,
                disable_bootstrap: false,
            },
            KvClientOptions {
                orphan_handler: orphan_tx,
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
