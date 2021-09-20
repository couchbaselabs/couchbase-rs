use super::{ConfigAware, TestConfig};
use crate::util::config::CavesConfig;
use bytes::Buf;
use couchbase::Cluster;
use lazy_static::lazy_static;
use log::debug;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::tcp::ReadHalf;
use tokio::net::{TcpListener, TcpStream};

use crate::util::features::TestFeature;
use uuid::Uuid;

#[cfg(target_os = "windows")]
const CAVES_BINARY: &str = "gocaves-windows.exe";
#[cfg(target_os = "macos")]
const CAVES_BINARY: &str = "gocaves-macos";
#[cfg(target_os = "linux")]
const CAVES_BINARY: &str = "gocaves-linux";

const CAVES_URL: &str = "https://github.com/couchbaselabs/gocaves/releases/download";
const CAVES_VERSION: &str = "v0.0.1-40";

lazy_static! {
    static ref SUPPORTS: Vec<TestFeature> = vec![TestFeature::KeyValue];
}

#[derive(Serialize, Deserialize)]
struct CreateConfig {
    #[serde(rename(serialize = "type"))]
    typ: String,
    id: String,
}

pub struct MockCluster {
    caves: Child,
    config: Arc<TestConfig>,
    _stream: TcpStream,
}

impl MockCluster {
    pub async fn start(c: Option<CavesConfig>) -> Self {
        MockCluster::start_caves(c).await
    }

    // TODO: write caves binary to something like /tmp and check if binary already exists before fetch
    async fn start_caves(c: Option<CavesConfig>) -> Self {
        let mut version = CAVES_VERSION.to_string();
        if let Some(cc) = c {
            version = cc.version();
        }
        let path = std::env::temp_dir().join(Path::new(CAVES_BINARY));
        if path.exists() {
            debug!(
                "Found existing caves binary for {} at {}",
                CAVES_VERSION,
                path.to_string_lossy()
            );
        } else {
            debug!(
                "Fetching caves {} to {}",
                CAVES_VERSION,
                path.to_string_lossy()
            );
            fetch_caves(&path, version).await;
            debug!("Fetched caves");
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind tcp listener");
        let port = listener
            .local_addr()
            .expect("Failed to get local addr from listener")
            .port();

        debug!("Caves starting");
        let caves = start_caves_process(&path, port);
        debug!("Caves started");

        let (mut stream, _) = listener
            .accept()
            .await
            .expect("Failed to accept connection");
        debug!("Caves connected");

        let (reader, mut writer) = stream.split();
        let mut buf_reader = BufReader::new(reader);

        let hello_msg = read_from_stream(&mut buf_reader).await;
        debug!("Received hello {}", hello_msg);

        let data = CreateConfig {
            typ: "createcluster".to_string(),
            id: Uuid::new_v4().to_string(),
        };

        debug!("Sending createcluster request");
        let mut cluster_req_data =
            serde_json::to_vec(&data).expect("Failed to serialize request data to json");
        cluster_req_data.push(0);
        writer
            .write_all(&cluster_req_data)
            .await
            .expect("Failed to write data");
        debug!("Sent createcluster request");

        let create_msg = read_from_stream(&mut buf_reader).await;
        debug!("Received create cluster response {}", &create_msg);

        let addr = parse_create_cluster_response(create_msg);
        debug!("Setting hostnames to {}", &addr);

        let cluster = Cluster::connect(addr, "Administrator".into(), "password".into());
        let bucket = cluster.bucket("default");
        let scope = bucket.scope("_default");
        let collection = bucket.collection("_default");

        Self {
            caves,
            config: Arc::new(TestConfig {
                cluster,
                bucket,
                scope,
                collection,
                support_matrix: SUPPORTS.to_vec(),
            }),
            _stream: stream,
        }
    }
}

impl ConfigAware for MockCluster {
    fn config(&self) -> Arc<TestConfig> {
        self.config.clone()
    }
}

impl Drop for MockCluster {
    fn drop(&mut self) {
        debug!("killing caves");
        match self.caves.kill() {
            Ok(_) => (),
            Err(e) => {
                debug!("Failed to kill gocaves instance: {}", e);
            }
        };
    }
}

async fn fetch_caves(path: &PathBuf, version: String) {
    let response = reqwest::get(format!("{}/{}/{}", CAVES_URL, version, CAVES_BINARY).as_str())
        .await
        .unwrap();

    if !response.status().is_success() {
        panic!("Response failed: {}", response.status())
    }

    let mut file = File::create(path).await.expect("Failed to create file");

    let content = response
        .bytes()
        .await
        .expect("Failed to read response into bytes");

    file.write_all(content.chunk())
        .await
        .expect("Failed to write response data to file");
    drop(file);

    set_permissions(path);
}

#[cfg(target_os = "windows")]
fn set_permissions() {}

#[cfg(not(target_os = "windows"))]
fn set_permissions(path: &PathBuf) {
    let meta = fs::metadata(path).expect("Failed to get file metadata");
    let mut perms = meta.permissions();
    perms.set_mode(0o744);
    fs::set_permissions(&path, perms).expect("Failed to set file permissions");
}

fn parse_create_cluster_response(msg: String) -> String {
    let j: HashMap<String, Value> =
        serde_json::from_str(msg.as_str()).expect("Failed to parse json response");

    let addr = j
        .get("connstr")
        .expect("Response did not have connstr field");

    addr.as_str()
        .expect("Failed to convert address to str")
        .to_string()
}

async fn read_from_stream(buf_reader: &mut BufReader<ReadHalf<'_>>) -> String {
    let mut buf = vec![];
    buf_reader
        .read_until(0, &mut buf)
        .await
        .expect("Failed to read from stream");

    let terminator_removed = &buf[0..buf.len() - 1];

    let msg = String::from_utf8_lossy(terminator_removed);
    msg.into()
}

fn start_caves_process(path: &PathBuf, port: u16) -> Child {
    let mut cmd = Command::new(path);

    // Caves outputs a lot of info, we need to redirect this so that our tests don't pick it up
    // on stdout.
    cmd.arg("-control-port")
        .arg(format!("{}", port))
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("Failed to spawn child process for caves")
}
