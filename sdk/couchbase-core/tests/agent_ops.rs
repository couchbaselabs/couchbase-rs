extern crate core;

use std::sync::Arc;

use rscbx_couchbase_core::agent::Agent;
use rscbx_couchbase_core::crudoptions::{GetOptions, UpsertOptions};
use rscbx_couchbase_core::memdx::error::{ErrorKind, ServerErrorKind};
use rscbx_couchbase_core::retrybesteffort::{
    BestEffortRetryStrategy, ExponentialBackoffCalculator,
};
use rscbx_couchbase_core::retryfailfast::FailFastRetryStrategy;

use crate::common::default_agent_options::{create_default_options, create_options_without_bucket};
use crate::common::helpers::{generate_key, generate_string_value};
use crate::common::test_config::setup_tests;

mod common;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

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

    let upsert_opts = UpsertOptions::builder()
        .key(key.as_slice())
        .retry_strategy(strat.clone())
        .scope_name("")
        .collection_name("")
        .value(value.as_slice())
        .build();

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let get_result = agent
        .get(
            GetOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat)
                .build(),
        )
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
        .upsert(
            UpsertOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .value(value.as_slice())
                .build(),
        )
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

#[cfg(feature = "dhat-heap")]
#[tokio::test]
async fn upsert_allocations() {
    let profiler = dhat::Profiler::builder().build();

    setup_tests();

    let agent_opts = create_default_options();

    let agent = Agent::new(agent_opts).await.unwrap();

    let key = generate_key();
    let value = generate_string_value(32);

    let strat = Arc::new(FailFastRetryStrategy::default());

    let upsert_opts = UpsertOptions::builder()
        .key(key.as_slice())
        .retry_strategy(strat.clone())
        .scope_name("")
        .collection_name("")
        .value(value.as_slice())
        .build();

    // make sure that all the underlying resources are setup.
    agent.upsert(upsert_opts.clone()).await.unwrap();

    let stats1 = dhat::HeapStats::get();

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    let stats2 = dhat::HeapStats::get();
    dbg!(stats1);
    dbg!(stats2);

    drop(profiler);
}
