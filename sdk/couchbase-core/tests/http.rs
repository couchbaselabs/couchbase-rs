use crate::common::test_config::{setup_tests, test_mem_addrs, test_password, test_username};
use bytes::Bytes;
use couchbase_core::httpx::client::{Client, ClientConfig, ClientOptions, ReqwestClient};
use couchbase_core::httpx::json_row_stream::JsonRowStream;
use couchbase_core::httpx::raw_json_row_streamer::RawJsonRowStreamer;
use couchbase_core::httpx::request::{Auth, BasicAuth, Request};
use couchbase_core::log::LogContext;
use http::Method;
use serde::Deserialize;
use serde_json::{json, Value};
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

#[tokio::test]
async fn test_row_streamer() {
    setup_tests().await;

    let addrs = test_mem_addrs().await;

    let ip = addrs.first().unwrap().split(":").next().unwrap();

    let basic_auth = BasicAuth {
        username: test_username().await,
        password: test_password().await,
    };

    let request_body = json!({"statement": "FROM RANGE(0, 999) AS i SELECT *"});
    let uri = format!("http://{}:8095/analytics/service", ip);

    let request = Request::builder()
        .user_agent("rscbcorex".to_string())
        .auth(Auth::BasicAuth(basic_auth))
        .method(Method::POST)
        .content_type("application/json".to_string())
        .uri(uri.as_str())
        .body(Bytes::from(serde_json::to_vec(&request_body).unwrap()))
        .build();

    let client = ReqwestClient::new(
        ClientConfig::default(),
        ClientOptions::builder()
            .log_context(LogContext {
                parent_context: None,
                parent_component_type: "".to_string(),
                parent_component_id: "".to_string(),
                component_id: "".to_string(),
            })
            .build(),
    )
    .unwrap();

    let resp = client.execute(request).await.unwrap();

    let mut streamer = RawJsonRowStreamer::new(JsonRowStream::new(resp.bytes_stream()), "results");

    let prelude = String::from_utf8(
        streamer
            .read_prelude()
            .await
            .expect("Failed reading prelude"),
    )
    .unwrap();

    assert!(prelude.contains("signature"));
    assert!(prelude.contains("requestID"));

    let mut rows = vec![];

    while let Some(row) = streamer.next().await {
        rows.push(row.expect("Failed reading row"));
    }

    assert_eq!(rows.len(), 1000);

    let epilog = streamer.epilog().expect("Failed reading epilog");
    let epilog: Value = serde_json::from_slice(&epilog).expect("failed parsing epilog as json");

    let request_id = epilog
        .get("requestID")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let status = epilog
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let elapsed_time = epilog
        .get("metrics")
        .unwrap()
        .get("elapsedTime")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert_eq!(status, "success");
    assert!(!request_id.is_empty());
    assert!(!elapsed_time.is_empty());
}

#[tokio::test]
async fn test_json_block_read() {
    setup_tests().await;

    let addrs = test_mem_addrs().await;
    let ip = addrs.first().unwrap().split(":").next().unwrap();

    let basic_auth = BasicAuth {
        username: test_username().await,
        password: test_password().await,
    };
    let uri = format!("http://{}:8091/pools/default/terseClusterInfo", ip);

    let request = Request::builder()
        .user_agent("rscbcorex".to_string())
        .auth(Auth::BasicAuth(basic_auth))
        .method(Method::GET)
        .content_type("application/json".to_string())
        .uri(uri.as_str())
        .build();

    let client = ReqwestClient::new(
        ClientConfig::default(),
        ClientOptions::builder()
            .log_context(LogContext {
                parent_context: None,
                parent_component_type: "".to_string(),
                parent_component_id: "".to_string(),
                component_id: "".to_string(),
            })
            .build(),
    )
    .expect("could not create client");

    let res = client.execute(request).await.expect("Failed http request");

    let cluster_info: TerseClusterInfo = res.json().await.unwrap();

    assert!(!cluster_info.compat_version.is_empty());
    assert!(!cluster_info.cluster_uuid.is_empty());
    assert!(!cluster_info.orchestrator.is_empty());
}
