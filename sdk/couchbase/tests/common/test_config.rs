use std::env;
use std::future::Future;
use std::io::Write;
use std::ops::Deref;
use std::sync::LazyLock;

use crate::common::default_cluster_options;
use crate::common::node_version::NodeVersion;
use crate::common::test_cluster::TestCluster;
use couchbase::cluster::Cluster;
use couchbase_connstr::ResolvedConnSpec;
use envconfig::Envconfig;
use lazy_static::lazy_static;
use log::LevelFilter;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

lazy_static! {
    pub static ref TEST_CONFIG: RwLock<Option<TestCluster>> = RwLock::new(None);
}

#[derive(Debug, Clone, Envconfig)]
pub struct EnvTestConfig {
    #[envconfig(from = "RCBUSERNAME", default = "Administrator")]
    pub username: String,
    #[envconfig(from = "RCBPASSWORD", default = "password")]
    pub password: String,
    #[envconfig(from = "RCBCONNSTR", default = "couchbases://192.168.107.128")]
    pub conn_string: String,
    #[envconfig(from = "RCBBUCKET", default = "default")]
    pub default_bucket: String,
    #[envconfig(from = "RCBSCOPE", default = "_default")]
    pub default_scope: String,
    #[envconfig(from = "RCBCOLLECTION", default = "_default")]
    pub default_collection: String,
    #[envconfig(from = "RCBSERVER_VERSION", default = "7.6.2")]
    pub server_version: String,
}

#[derive(Debug, Clone)]
pub struct TestSetupConfig {
    pub username: String,
    pub password: String,
    pub conn_str: String,
    pub resolved_conn_spec: ResolvedConnSpec,
    pub default_bucket: String,
    pub default_scope: String,
    pub default_collection: String,
}

impl TestSetupConfig {
    pub async fn setup_cluster(&self) -> Cluster {
        Cluster::connect(
            self.conn_str.clone(),
            default_cluster_options::create_default_options(self.clone()).await,
        )
        .await
        .unwrap()
    }
}

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub fn run_test<T, Fut>(test: T)
where
    T: FnOnce(TestCluster) -> Fut,
    Fut: Future<Output = ()>,
{
    RUNTIME.block_on(async {
        let mut config = TEST_CONFIG.write().await;

        if let Some(cluster) = config.deref() {
            let cluster = cluster.clone();
            drop(config);
            test(cluster).await;
            return;
        }

        env_logger::Builder::new()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{}:{} {} [{}] - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                    record.level(),
                    record.args()
                )
            })
            .filter(Some("rustls"), LevelFilter::Warn)
            .filter_level(
                env::var("RUST_LOG")
                    .unwrap_or("TRACE".to_string())
                    .parse()
                    .unwrap(),
            )
            .init();

        let test_cluster = create_test_cluster().await;

        *config = Some(test_cluster.clone());
        drop(config);

        test(test_cluster).await;
    });
}

pub async fn create_test_cluster() -> TestCluster {
    let test_config = EnvTestConfig::init_from_env().unwrap();

    let conn_spec = couchbase_connstr::parse(&test_config.conn_string).unwrap();

    let test_setup_config = TestSetupConfig {
        default_bucket: test_config.default_bucket,
        default_scope: test_config.default_scope,
        default_collection: test_config.default_collection,
        username: test_config.username,
        password: test_config.password,
        conn_str: test_config.conn_string,
        resolved_conn_spec: couchbase_connstr::resolve(conn_spec).await.unwrap(),
    };

    TestCluster::new(
        NodeVersion::from(test_config.server_version.clone()),
        test_setup_config.clone(),
    )
    .await
}

impl TestCluster {}
