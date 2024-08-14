use std::sync::Arc;

use rscbx_couchbase_core::agent::Agent;
use rscbx_couchbase_core::crudoptions::{GetOptions, UpsertOptions};
use rscbx_couchbase_core::memdx::datatype::DataTypeFlag;
use rscbx_couchbase_core::memdx::error::{ErrorKind, ServerErrorKind};
use rscbx_couchbase_core::retrybesteffort::{
    BestEffortRetryStrategy, ExponentialBackoffCalculator,
};

use crate::common::default_agent_options::{create_default_options, create_options_without_bucket};
use crate::common::helpers::{generate_key, generate_string_value};
use crate::common::test_config::setup_tests;

mod common;

#[tokio::test]
async fn test_upsert_and_get() {
    setup_tests();

    let agent_opts = create_default_options();

    let agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = generate_string_value(32);

    let upsert_result = agent
        .upsert(UpsertOptions {
            key: &key,
            scope_name: "",
            collection_name: "",
            value: &value,
            flags: 0,
            expiry: None,
            preserve_expiry: None,
            cas: None,
            durability_level: None,
            retry_strategy: strat.clone(),
            datatype: DataTypeFlag::None,
        })
        .await
        .unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let get_result = agent
        .get(GetOptions {
            key: &key,
            scope_name: "",
            collection_name: "",
            retry_strategy: strat,
        })
        .await
        .unwrap();

    assert_eq!(value, get_result.value);
    assert_eq!(upsert_result.cas, get_result.cas);
}

#[tokio::test]
async fn test_kv_without_a_bucket() {
    setup_tests();

    let agent_opts = create_options_without_bucket();

    let agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = generate_string_value(32);

    let upsert_result = agent
        .upsert(UpsertOptions {
            key: &key,
            scope_name: "",
            collection_name: "",
            value: &value,
            flags: 0,
            expiry: None,
            preserve_expiry: None,
            cas: None,
            durability_level: None,
            retry_strategy: strat.clone(),
            datatype: DataTypeFlag::None,
        })
        .await;

    assert!(upsert_result.is_err());
    let err = upsert_result.err().unwrap();
    let memdx_err = err.is_memdx_error();
    assert!(memdx_err.is_some());
    match memdx_err.unwrap().kind.as_ref() {
        ErrorKind::Server(e) => {
            assert_eq!(ServerErrorKind::NoBucket, e.kind);
        }
        _ => panic!(
            "Error was not expected type, expected: {}, was {}",
            ServerErrorKind::NoBucket,
            memdx_err.unwrap().kind.as_ref()
        ),
    }
}
