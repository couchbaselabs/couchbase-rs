use crate::common::test_config::{setup_tests, test_mem_addrs, test_password, test_username};
use bytes::Bytes;
use couchbase_core::analyticsx::query_respreader::Status;
use couchbase_core::httpx::client::{Client, ClientConfig, ReqwestClient};
use couchbase_core::httpx::decoder::Decoder;
use couchbase_core::httpx::raw_json_row_streamer::{RawJsonRowItem, RawJsonRowStreamer};
use couchbase_core::httpx::request::{Auth, BasicAuth, Request};
use http::Method;
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tokio_stream::StreamExt;

mod common;

// TODO: These integration tests will be superseded by higher-level httpx-based components once implemented

#[derive(Deserialize, Debug)]
struct TerseClusterInfo {
    #[serde(alias = "clusterCompatVersion")]
    compat_version: String,
    #[serde(alias = "clusterUUID")]
    cluster_uuid: String,
    #[serde(alias = "isBalanced")]
    is_balanced: bool,
    orchestrator: String,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetaData {
    #[serde(rename = "requestID")]
    pub request_id: Option<String>,
    #[serde(rename = "clientContextID")]
    pub client_context_id: Option<String>,
    pub status: Option<Status>,
    pub metrics: Option<QueryMetrics>,
    pub signature: Option<Box<RawValue>>,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetrics {
    #[serde(rename = "elapsedTime")]
    pub elapsed_time: Option<String>,
}

#[tokio::test]
async fn test_row_streamer() {
    setup_tests().await;

    let addrs = test_mem_addrs().await;

    let ip = addrs.first().unwrap().split(":").next().unwrap();

    let basic_auth = BasicAuth::new(test_username().await, test_password().await);

    let request_body = json!({"statement": "FROM RANGE(0, 999) AS i SELECT *"});
    let uri = format!("http://{}:8095/analytics/service", ip);

    let request = Request::new(Method::POST, uri)
        .user_agent("rscbcorex".to_string())
        .auth(Auth::BasicAuth(basic_auth))
        .content_type("application/json".to_string())
        .body(Bytes::from(serde_json::to_vec(&request_body).unwrap()));

    let client = ReqwestClient::new(ClientConfig::default()).unwrap();

    let resp = client.execute(request).await.unwrap();

    let mut streamer = RawJsonRowStreamer::new(Decoder::new(resp.bytes_stream()), "results");

    let prelude = String::from_utf8(
        streamer
            .read_prelude()
            .await
            .expect("Failed reading prelude"),
    )
    .unwrap();

    assert!(prelude.contains("signature"));
    assert!(prelude.contains("requestID"));

    let mut stream = Box::pin(streamer.into_stream());
    let mut rows = vec![];

    let mut epilog = None;
    while let Some(row) = stream.next().await {
        match row {
            Ok(RawJsonRowItem::Row(row)) => {
                rows.push(row);
            }
            Ok(RawJsonRowItem::Metadata(meta)) => {
                epilog = Some(meta);
            }
            Err(e) => {
                panic!("Failed reading from stream: {}", e);
            }
        }
    }

    let epilog = epilog.unwrap();

    assert_eq!(rows.len(), 1000);

    let epilog: QueryMetaData =
        serde_json::from_slice(&epilog).expect("failed parsing epilog as json");

    assert_eq!(epilog.status.unwrap(), Status::Success);
    assert!(!epilog.request_id.unwrap().is_empty());
    assert!(!epilog.metrics.unwrap().elapsed_time.unwrap().is_empty());
}

#[tokio::test]
async fn test_json_block_read() {
    setup_tests().await;

    let addrs = test_mem_addrs().await;
    let ip = addrs.first().unwrap().split(":").next().unwrap();

    let basic_auth = BasicAuth::new(test_username().await, test_password().await);
    let uri = format!("http://{}:8091/pools/default/terseClusterInfo", ip);

    let request = Request::new(Method::GET, uri)
        .user_agent("rscbcorex".to_string())
        .auth(Auth::BasicAuth(basic_auth))
        .content_type("application/json".to_string());

    let client = ReqwestClient::new(ClientConfig::default()).expect("could not create client");

    let res = client.execute(request).await.expect("Failed http request");

    let cluster_info: TerseClusterInfo = res.json().await.unwrap();

    assert!(!cluster_info.compat_version.is_empty());
    assert!(!cluster_info.cluster_uuid.is_empty());
    assert!(!cluster_info.orchestrator.is_empty());
}
