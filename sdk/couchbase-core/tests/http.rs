use bytes::Bytes;
use http::{Method, Response};
use serde::Deserialize;
use serde_json::{json, Value};

use rscbx_couchbase_core::httpx::client::{Client, ReqwestClient};
use rscbx_couchbase_core::httpx::json_row_stream::JsonRowStream;
use rscbx_couchbase_core::httpx::raw_json_row_streamer::RawJsonRowStreamer;
use rscbx_couchbase_core::httpx::request::{Auth, BasicAuth, Request};

use crate::common::helpers::{generate_key, generate_string_value};
use crate::common::test_config::{
    EnvTestConfig, setup_tests, TEST_CONFIG, test_mem_addrs, test_password, test_username,
};

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
    setup_tests();

    let addrs = test_mem_addrs();

    let ip = addrs.first().unwrap().split(":").next().unwrap();

    let basic_auth = BasicAuth {
        username: test_username(),
        password: test_password(),
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

    let client = ReqwestClient::new().unwrap();

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

    while streamer.has_more_rows() {
        if let Some(row) = streamer.read_row().await.expect("Failed reading row") {
            rows.push(row);
        } else {
            break;
        }
    }

    assert_eq!(rows.len(), 1000);

    let epilog = streamer.read_epilog().await.expect("Failed reading epilog");
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
    setup_tests();

    let addrs = test_mem_addrs();
    let ip = addrs.first().unwrap().split(":").next().unwrap();

    let basic_auth = BasicAuth {
        username: test_username(),
        password: test_password(),
    };
    let uri = format!("http://{}:8091/pools/default/terseClusterInfo", ip);

    let request = Request::builder()
        .user_agent("rscbcorex".to_string())
        .auth(Auth::BasicAuth(basic_auth))
        .method(Method::GET)
        .content_type("application/json".to_string())
        .uri(uri.as_str())
        .build();

    let client = ReqwestClient::new().expect("could not create client");

    let res = client.execute(request).await.expect("Failed http request");

    let cluster_info: TerseClusterInfo = res.json().await.unwrap();

    assert!(!cluster_info.compat_version.is_empty());
    assert!(!cluster_info.cluster_uuid.is_empty());
    assert!(!cluster_info.orchestrator.is_empty());
}
