use http::Method;
use serde::Deserialize;
use serde_json::{json, Value};
use rscbx_couchbase_core::httpx::base::{BasicAuth, OnBehalfOfInfo, RequestCreator, ResponseProvider};
use rscbx_couchbase_core::httpx::client::Client;
use rscbx_couchbase_core::httpx::json_block_read::read_as_json;
use rscbx_couchbase_core::httpx::json_row_stream::JsonRowStream;
use rscbx_couchbase_core::httpx::raw_json_row_streamer::RawJsonRowStreamer;
use crate::common::helpers::{generate_key, generate_string_value};
use crate::common::test_config::{setup_tests, EnvTestConfig, TEST_CONFIG, test_username, test_password, test_mem_addrs};

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

    let basic_auth = BasicAuth::builder().username(test_username()).password(test_password()).build();

    let builder = RequestCreator::builder()
        .user_agent(String::from("rscbcorex"))
        .basic_auth(basic_auth)
        .endpoint(format!("http://{}:8095", ip)
        ).build();

    let request_body = json!({"statement": "FROM RANGE(0, 999) AS i SELECT *"});

    let client: Client<reqwest::Client> = Client::new().unwrap();

    let request = builder
        .new_request(
            Method::POST,
            "/analytics/service".to_string(),
            Some("application/json".to_string()),
            None,
            Some(request_body.to_string()),
        );

    let resp = client.execute(request).await.unwrap();

    let mut streamer = RawJsonRowStreamer::new(
        JsonRowStream::new(resp.inner.get_stream()),
        "results".to_string(),
    );

    let prelude = streamer
        .read_prelude()
        .await
        .expect("Failed reading prelude");

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
    let epilog: Value = serde_json::from_str(&epilog).expect("failed parsing epilog as json");

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

    let basic_auth = BasicAuth::builder().username(test_username()).password(test_password()).build();

    let builder = RequestCreator::builder()
        .user_agent(String::from("rscbcorex"))
        .basic_auth(basic_auth)
        .endpoint(format!("http://{}:8091", ip)
        ).build();

    let client: Client<reqwest::Client> = Client::new().expect("could not create client");

    let request = builder
        .new_request(
            Method::GET,
            "/pools/default/terseClusterInfo".to_string(),
            None,
            None,
            None,
        );

    let res = client.execute(request).await.expect("Failed http request");

    let cluster_info = read_as_json::<TerseClusterInfo>(res.inner)
        .await
        .expect("Failed parsing response as json");

    assert!(!cluster_info.compat_version.is_empty());
    assert!(!cluster_info.cluster_uuid.is_empty());
    assert!(!cluster_info.orchestrator.is_empty());
}
