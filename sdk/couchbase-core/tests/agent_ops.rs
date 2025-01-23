extern crate core;

use crate::common::default_agent_options::{create_default_options, create_options_without_bucket};
use crate::common::helpers::{
    create_collection_and_wait_for_kv, delete_collection_and_wait_for_kv, generate_bytes_value,
    generate_key,
};
use crate::common::test_config::{setup_tests, test_bucket, test_is_ssl, test_scope};
use couchbase_core::agent::Agent;
use couchbase_core::crudoptions::{
    AddOptions, AppendOptions, DecrementOptions, DeleteOptions, GetAndLockOptions,
    GetAndTouchOptions, GetOptions, IncrementOptions, LookupInOptions, MutateInOptions,
    PrependOptions, ReplaceOptions, TouchOptions, UnlockOptions, UpsertOptions,
};
use couchbase_core::memdx::error::{ServerErrorKind, SubdocErrorKind};
use couchbase_core::memdx::subdoc::{LookupInOp, LookupInOpType, MutateInOp, MutateInOpType};
use couchbase_core::retrybesteffort::{BestEffortRetryStrategy, ExponentialBackoffCalculator};
use couchbase_core::retryfailfast::FailFastRetryStrategy;
use rand::distr::Alphanumeric;
use rand::{rng, Rng};
use serde::Serialize;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

mod common;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::test]
async fn test_upsert_and_get() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = generate_bytes_value(32);

    let upsert_opts =
        UpsertOptions::new(key.as_slice(), "", "", value.as_slice()).retry_strategy(strat.clone());

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let get_result = agent
        .get(GetOptions::new(&key, "", "").retry_strategy(strat))
        .await
        .unwrap();

    assert_eq!(value, get_result.value);
    assert_eq!(upsert_result.cas, get_result.cas);

    agent.close().await;
}

#[tokio::test]
async fn test_add_and_delete() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = generate_bytes_value(32);

    let add_opts =
        AddOptions::new(key.as_slice(), "", "", value.as_slice()).retry_strategy(strat.clone());

    let add_result = agent.add(add_opts.clone()).await.unwrap();

    assert_ne!(0, add_result.cas);
    assert!(add_result.mutation_token.is_some());

    let add_result = agent.add(add_opts.clone()).await;

    assert!(add_result
        .err()
        .unwrap()
        .is_memdx_error()
        .unwrap()
        .is_server_error_kind(ServerErrorKind::KeyExists));

    let delete_result = agent
        .delete(DeleteOptions::new(&key, "", "").retry_strategy(strat))
        .await
        .unwrap();

    assert_ne!(0, delete_result.cas);
    assert!(delete_result.mutation_token.is_some());

    let add_result = agent.add(add_opts).await.unwrap();

    assert_ne!(0, add_result.cas);
    assert!(add_result.mutation_token.is_some());

    agent.close().await;
}

#[tokio::test]
async fn test_replace() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = generate_bytes_value(32);

    let upsert_opts =
        UpsertOptions::new(key.as_slice(), "", "", value.as_slice()).retry_strategy(strat.clone());

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let new_value = generate_bytes_value(32);

    let replace_result = agent
        .replace(
            ReplaceOptions::new(&key, "", "", new_value.as_slice()).retry_strategy(strat.clone()),
        )
        .await
        .unwrap();

    assert_ne!(0, replace_result.cas);
    assert!(replace_result.mutation_token.is_some());

    let get_result = agent
        .get(GetOptions::new(&key, "", "").retry_strategy(strat.clone()))
        .await
        .unwrap();

    assert_eq!(new_value, get_result.value);
    assert_eq!(replace_result.cas, get_result.cas);

    agent.close().await;
}

#[tokio::test]
async fn test_lock_unlock() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = generate_bytes_value(32);

    let upsert_opts =
        UpsertOptions::new(key.as_slice(), "", "", value.as_slice()).retry_strategy(strat.clone());

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let get_result = agent
        .get(GetOptions::new(&key, "", "").retry_strategy(strat.clone()))
        .await
        .unwrap();

    let cas = get_result.cas;

    let unlock_result = agent
        .unlock(
            UnlockOptions::new(key.as_slice(), "", "", cas)
                .retry_strategy(Arc::new(FailFastRetryStrategy::default())),
        )
        .await
        .err()
        .unwrap();

    let memdx_err = unlock_result.is_memdx_error().unwrap();
    assert!(
        memdx_err.is_server_error_kind(ServerErrorKind::NotLocked)
            || memdx_err.is_server_error_kind(ServerErrorKind::TmpFail)
    );

    let get_and_lock_result = agent
        .get_and_lock(
            GetAndLockOptions::new(key.as_slice(), "", "", 10).retry_strategy(strat.clone()),
        )
        .await
        .unwrap();

    let cas = get_and_lock_result.cas;
    assert_eq!(value, get_and_lock_result.value);

    let unlock_result = agent
        .unlock(UnlockOptions::new(key.as_slice(), "", "", cas).retry_strategy(strat.clone()))
        .await;

    assert!(unlock_result.is_ok());

    agent.close().await;
}

#[tokio::test]
async fn test_touch_operations() {
    // TODO RSCBC-27 we can't fetch & check the expiry without subdoc

    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = generate_bytes_value(32);

    let upsert_opts = UpsertOptions::new(key.as_slice(), "", "", value.as_slice())
        .retry_strategy(strat.clone())
        .expiry(10);

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let touch_result = agent
        .touch(TouchOptions::new(key.as_slice(), "", "", 12).retry_strategy(strat.clone()))
        .await
        .unwrap();

    assert_ne!(0, touch_result.cas);

    let get_and_touch_result = agent
        .get_and_touch(
            GetAndTouchOptions::new(key.as_slice(), "", "", 15).retry_strategy(strat.clone()),
        )
        .await
        .unwrap();

    assert_eq!(value, get_and_touch_result.value);
    assert_ne!(0, get_and_touch_result.cas);

    agent.close().await;
}

#[tokio::test]
async fn test_append_and_prepend() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = "answer is".as_bytes().to_vec();

    let upsert_result = agent
        .upsert(
            UpsertOptions::new(key.as_slice(), "", "", value.as_slice())
                .retry_strategy(strat.clone()),
        )
        .await
        .unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let value = "the ".as_bytes();

    let prepend_result = agent
        .prepend(PrependOptions::new(&key, "", "", value).retry_strategy(strat.clone()))
        .await
        .unwrap();

    assert_ne!(0, prepend_result.cas);
    assert!(prepend_result.mutation_token.is_some());

    let value = " 42".as_bytes();

    let append_result = agent
        .append(AppendOptions::new(&key, "", "", value).retry_strategy(strat.clone()))
        .await
        .unwrap();

    assert_ne!(0, append_result.cas);
    assert!(append_result.mutation_token.is_some());

    let get_result = agent
        .get(GetOptions::new(&key, "", "").retry_strategy(strat.clone()))
        .await
        .unwrap();

    assert_eq!("the answer is 42".as_bytes(), get_result.value.as_slice());
    assert_eq!(append_result.cas, get_result.cas);

    agent.close().await;
}

#[tokio::test]
async fn test_append_and_prepend_cas_mismatch() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = "answer is".as_bytes().to_vec();

    let upsert_result = agent
        .upsert(
            UpsertOptions::new(key.as_slice(), "", "", value.as_slice())
                .retry_strategy(strat.clone()),
        )
        .await
        .unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let value = "the ".as_bytes();

    let prepend_result = agent
        .prepend(
            PrependOptions::new(&key, "", "", value)
                .retry_strategy(strat.clone())
                .cas(1234),
        )
        .await;

    assert!(prepend_result.is_err());
    let e = prepend_result.err().unwrap();
    let e = e.is_memdx_error().unwrap();
    assert!(e.is_server_error_kind(ServerErrorKind::CasMismatch));

    let value = " 42".as_bytes();

    let append_result = agent
        .append(
            AppendOptions::new(&key, "", "", value)
                .retry_strategy(strat.clone())
                .cas(1234),
        )
        .await;

    assert!(append_result.is_err());
    let e = append_result.err().unwrap();
    let e = e.is_memdx_error().unwrap();
    assert!(e.is_server_error_kind(ServerErrorKind::CasMismatch));

    agent.close().await;
}

#[tokio::test]
async fn test_increment_and_decrement() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();

    let increment_result = agent
        .increment(
            IncrementOptions::new(key.as_slice(), "", "")
                .retry_strategy(strat.clone())
                .delta(1)
                .initial(42),
        )
        .await
        .unwrap();

    assert_ne!(0, increment_result.cas);
    assert_eq!(increment_result.value, 42);
    assert!(increment_result.mutation_token.is_some());

    let decrement_result = agent
        .decrement(
            DecrementOptions::new(key.as_slice(), "", "")
                .retry_strategy(strat.clone())
                .delta(2),
        )
        .await
        .unwrap();

    assert_ne!(0, decrement_result.cas);
    assert_eq!(decrement_result.value, 40);
    assert!(decrement_result.mutation_token.is_some());
}
#[derive(Serialize)]
struct SubdocObject {
    foo: u32,
    bar: u32,
    baz: String,
    arr: Vec<u32>,
}

#[tokio::test]
async fn test_lookup_in() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();

    let obj = SubdocObject {
        foo: 14,
        bar: 2,
        baz: "hello".to_string(),
        arr: vec![1, 2, 3],
    };

    let value = serde_json::to_vec(&obj).unwrap();

    let upsert_opts =
        UpsertOptions::new(key.as_slice(), "", "", value.as_slice()).retry_strategy(strat.clone());

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let ops = [
        LookupInOp::new(LookupInOpType::Get, "baz".as_bytes()),
        LookupInOp::new(LookupInOpType::Exists, "not-exists".as_bytes()),
        LookupInOp::new(LookupInOpType::GetCount, "arr".as_bytes()),
        LookupInOp::new(LookupInOpType::GetDoc, "".as_bytes()),
    ];

    let lookup_in_opts =
        LookupInOptions::new(key.as_slice(), "", "", &ops).retry_strategy(strat.clone());

    let lookup_in_result = agent.lookup_in(lookup_in_opts).await.unwrap();

    assert_eq!(4, lookup_in_result.value.len());
    assert_ne!(0, lookup_in_result.cas);
    assert!(!lookup_in_result.doc_is_deleted);
    assert!(lookup_in_result.value[0].err.is_none());
    assert_eq!(
        std::str::from_utf8(lookup_in_result.value[0].value.as_ref().unwrap())
            .unwrap()
            .trim_matches('"'),
        "hello"
    );
    assert!(lookup_in_result.value[0].err.is_none());
    assert!(lookup_in_result.value[1]
        .err
        .as_ref()
        .is_some_and(|err| err.is_error_kind(SubdocErrorKind::PathNotFound)));
    assert_eq!(
        lookup_in_result.value[1].err.as_ref().unwrap().op_index(),
        Some(1)
    );
    assert!(lookup_in_result.value[2].err.is_none());
    assert_eq!(
        std::str::from_utf8(lookup_in_result.value[2].value.as_ref().unwrap())
            .unwrap()
            .trim_matches('"'),
        "3"
    );
    assert!(lookup_in_result.value[3].err.is_none());
    assert_eq!(
        lookup_in_result.value[3].value.as_ref().unwrap(),
        &serde_json::to_vec(&obj).unwrap()
    );
}

#[tokio::test]
async fn test_mutate_in() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();

    let obj = SubdocObject {
        foo: 14,
        bar: 2,
        baz: "hello".to_string(),
        arr: vec![1, 2, 3],
    };

    let value = serde_json::to_vec(&obj).unwrap();

    let upsert_opts =
        UpsertOptions::new(key.as_slice(), "", "", value.as_slice()).retry_strategy(strat.clone());

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let ops = [
        MutateInOp::new(MutateInOpType::Counter, "bar".as_bytes(), "3".as_bytes()),
        MutateInOp::new(
            MutateInOpType::DictSet,
            "baz".as_bytes(),
            "\"world\"".as_bytes(),
        ),
        MutateInOp::new(
            MutateInOpType::ArrayPushLast,
            "arr".as_bytes(),
            "4".as_bytes(),
        ),
    ];

    let mutate_in_options = MutateInOptions::new(key.as_slice(), "", "", &ops)
        .retry_strategy(strat.clone())
        .expiry(10);

    let mutate_in_result = agent.mutate_in(mutate_in_options).await.unwrap();

    assert_eq!(mutate_in_result.value.len(), 3);
    assert!(mutate_in_result.value[0].err.is_none());
    assert!(mutate_in_result.value[0]
        .clone()
        .value
        .is_some_and(|val| String::from_utf8(val).unwrap() == "5"));
    assert!(mutate_in_result.value[1].err.is_none());
    assert!(mutate_in_result.value[1].value.is_none());
    assert!(mutate_in_result.value[2].err.is_none());
    assert!(mutate_in_result.value[2].value.is_none());
}

#[tokio::test]
async fn test_kv_without_a_bucket() {
    setup_tests().await;

    let agent_opts = create_options_without_bucket().await;

    let agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    ));

    let key = generate_key();
    let value = generate_bytes_value(32);

    let upsert_result = agent
        .upsert(
            UpsertOptions::new(key.as_slice(), "", "", value.as_slice())
                .retry_strategy(strat.clone()),
        )
        .await;

    assert!(upsert_result.is_err());
    let err = upsert_result.err().unwrap();
    let memdx_err = err.is_memdx_error();
    assert!(memdx_err.is_some());
    assert!(memdx_err
        .unwrap()
        .is_server_error_kind(ServerErrorKind::NoBucket));
}

#[tokio::test]
async fn test_unknown_collection_id() {
    setup_tests().await;

    let agent_opts = create_default_options().await;
    let bucket = test_bucket().await;
    let scope_name = test_scope().await;
    let collection_name = rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect::<String>();

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(FailFastRetryStrategy::default());

    let key = generate_key();
    let value = generate_bytes_value(32);

    create_collection_and_wait_for_kv(
        agent.clone(),
        &bucket,
        &scope_name,
        &collection_name,
        Instant::now().add(Duration::from_secs(5)),
    )
    .await;

    // Do an upsert to prep the cid cache.
    let upsert_opts = UpsertOptions::new(
        key.as_slice(),
        &scope_name,
        &collection_name,
        value.as_slice(),
    )
    .retry_strategy(strat.clone());

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    delete_collection_and_wait_for_kv(
        agent.clone(),
        &bucket,
        &scope_name,
        &collection_name,
        Instant::now().add(Duration::from_secs(5)),
    )
    .await;

    let upsert_opts = UpsertOptions::new(
        key.as_slice(),
        &scope_name,
        &collection_name,
        value.as_slice(),
    )
    .retry_strategy(Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    )));

    let upsert_result = timeout_at(
        Instant::now().add(Duration::from_millis(2500)),
        agent.upsert(upsert_opts),
    )
    .await;

    match upsert_result {
        Ok(_) => {
            panic!("Expected error due to timeout");
        }
        Err(_e) => {}
    }

    agent.close().await;
}

#[tokio::test]
async fn test_changed_collection_id() {
    setup_tests().await;

    let agent_opts = create_default_options().await;
    let bucket = test_bucket().await;
    let scope_name = test_scope().await;
    let collection_name = rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect::<String>();

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let strat = Arc::new(FailFastRetryStrategy::default());

    let key = generate_key();
    let value = generate_bytes_value(32);

    create_collection_and_wait_for_kv(
        agent.clone(),
        &bucket,
        &scope_name,
        &collection_name,
        Instant::now().add(Duration::from_secs(5)),
    )
    .await;

    // Do an upsert to prep the cid cache.
    let upsert_opts = UpsertOptions::new(
        key.as_slice(),
        &scope_name,
        &collection_name,
        value.as_slice(),
    )
    .retry_strategy(strat.clone());

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    delete_collection_and_wait_for_kv(
        agent.clone(),
        &bucket,
        &scope_name,
        &collection_name,
        Instant::now().add(Duration::from_secs(5)),
    )
    .await;

    create_collection_and_wait_for_kv(
        agent.clone(),
        &bucket,
        &scope_name,
        &collection_name,
        Instant::now().add(Duration::from_secs(5)),
    )
    .await;

    let upsert_opts = UpsertOptions::new(
        key.as_slice(),
        &scope_name,
        &collection_name,
        value.as_slice(),
    )
    .retry_strategy(Arc::new(BestEffortRetryStrategy::new(
        ExponentialBackoffCalculator::default(),
    )));

    // This call should now get a cid unknown error and fetch the new one.
    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    agent.close().await;
}
#[cfg(feature = "dhat-heap")]
#[tokio::test]
async fn upsert_allocations() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let agent = Agent::new(agent_opts).await.unwrap();

    let key = generate_key();
    let value = generate_bytes_value(32);

    let strat = Arc::new(FailFastRetryStrategy::default());

    let upsert_opts =
        UpsertOptions::new(key.as_slice(), "", "", value.as_slice()).retry_strategy(strat.clone());

    // make sure that all the underlying resources are setup.
    agent.upsert(upsert_opts.clone()).await.unwrap();

    let profiler = dhat::Profiler::builder().build();

    let stats1 = dhat::HeapStats::get();

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    let stats2 = dhat::HeapStats::get();

    let total_allocs = stats2.total_blocks - stats1.total_blocks;

    let expected_allocs = if test_is_ssl().await { 19 } else { 21 };
    dhat::assert!(
        total_allocs <= expected_allocs,
        "Expected max {} allocations, was {}",
        expected_allocs,
        total_allocs
    );

    drop(profiler);
}
