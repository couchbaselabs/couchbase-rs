extern crate core;

use couchbase_core::agent::Agent;
use couchbase_core::crudoptions::{
    AddOptions, AppendOptions, DecrementOptions, DeleteOptions, GetAndLockOptions,
    GetAndTouchOptions, GetOptions, IncrementOptions, LookupInOptions, MutateInOptions,
    PrependOptions, ReplaceOptions, TouchOptions, UnlockOptions, UpsertOptions,
};
use couchbase_core::memdx::error::ErrorKind::Server;
use couchbase_core::memdx::error::ServerErrorKind::KeyExists;
use couchbase_core::memdx::error::{ErrorKind, ServerError, ServerErrorKind, SubdocErrorKind};
use couchbase_core::memdx::subdoc::{
    LookupInOp, LookupInOpType, MutateInOp, MutateInOpType, SubdocOpFlag,
};
use couchbase_core::retrybesteffort::{BestEffortRetryStrategy, ExponentialBackoffCalculator};
use couchbase_core::retryfailfast::FailFastRetryStrategy;
use serde::Serialize;
use std::sync::Arc;

use crate::common::default_agent_options::{create_default_options, create_options_without_bucket};
use crate::common::helpers::{generate_key, generate_string_value};
use crate::common::test_config::setup_tests;

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
    let value = generate_string_value(32);

    let add_opts = AddOptions::builder()
        .key(key.as_slice())
        .retry_strategy(strat.clone())
        .scope_name("")
        .collection_name("")
        .value(value.as_slice())
        .build();

    let add_result = agent.add(add_opts.clone()).await.unwrap();

    assert_ne!(0, add_result.cas);
    assert!(add_result.mutation_token.is_some());

    let add_result = agent.add(add_opts.clone()).await;

    assert!(matches!(
        add_result
            .err()
            .unwrap()
            .is_memdx_error()
            .unwrap()
            .kind
            .as_ref(),
        Server(ServerError {
            kind: KeyExists,
            ..
        })
    ));

    let delete_result = agent
        .delete(
            DeleteOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat)
                .build(),
        )
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

    let new_value = generate_string_value(32);

    let replace_result = agent
        .replace(
            ReplaceOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat.clone())
                .value(new_value.as_slice())
                .build(),
        )
        .await
        .unwrap();

    assert_ne!(0, replace_result.cas);
    assert!(replace_result.mutation_token.is_some());

    let get_result = agent
        .get(
            GetOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat.clone())
                .build(),
        )
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
                .retry_strategy(strat.clone())
                .build(),
        )
        .await
        .unwrap();

    let cas = get_result.cas;

    let unlock_result = agent
        .unlock(
            UnlockOptions::builder()
                .key(key.as_slice())
                .retry_strategy(Arc::new(FailFastRetryStrategy::default()))
                .scope_name("")
                .collection_name("")
                .cas(cas)
                .build(),
        )
        .await;

    assert!(matches!(
        unlock_result
            .err()
            .unwrap()
            .is_memdx_error()
            .unwrap()
            .kind
            .as_ref(),
        Server(ServerError {
            kind: ServerErrorKind::NotLocked,
            ..
        }) | Server(ServerError {
            kind: ServerErrorKind::TmpFail,
            ..
        })
    ));

    let get_and_lock_result = agent
        .get_and_lock(
            GetAndLockOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .lock_time(10)
                .build(),
        )
        .await
        .unwrap();

    let cas = get_and_lock_result.cas;
    assert_eq!(value, get_and_lock_result.value);

    let unlock_result = agent
        .unlock(
            UnlockOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .cas(cas)
                .build(),
        )
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
    let value = generate_string_value(32);

    let upsert_opts = UpsertOptions::builder()
        .key(key.as_slice())
        .retry_strategy(strat.clone())
        .scope_name("")
        .collection_name("")
        .value(value.as_slice())
        .expiry(10)
        .build();

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let touch_result = agent
        .touch(
            TouchOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .expiry(12)
                .build(),
        )
        .await
        .unwrap();

    assert_ne!(0, touch_result.cas);

    let get_and_touch_result = agent
        .get_and_touch(
            GetAndTouchOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .expiry(15)
                .build(),
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
            UpsertOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .value(value.as_slice())
                .build(),
        )
        .await
        .unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let value = "the ".as_bytes();

    let prepend_result = agent
        .prepend(
            PrependOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat.clone())
                .value(value)
                .build(),
        )
        .await
        .unwrap();

    assert_ne!(0, prepend_result.cas);
    assert!(prepend_result.mutation_token.is_some());

    let value = " 42".as_bytes();

    let append_result = agent
        .append(
            AppendOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat.clone())
                .value(value)
                .build(),
        )
        .await
        .unwrap();

    assert_ne!(0, append_result.cas);
    assert!(append_result.mutation_token.is_some());

    let get_result = agent
        .get(
            GetOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat.clone())
                .build(),
        )
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
            UpsertOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .value(value.as_slice())
                .build(),
        )
        .await
        .unwrap();

    assert_ne!(0, upsert_result.cas);
    assert!(upsert_result.mutation_token.is_some());

    let value = "the ".as_bytes();

    let prepend_result = agent
        .prepend(
            PrependOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat.clone())
                .value(value)
                .cas(1234)
                .build(),
        )
        .await;

    assert!(prepend_result.is_err());
    let e = prepend_result.err().unwrap();
    let e = e.is_memdx_error().unwrap();
    let kind = e.kind.clone();
    match *kind {
        Server(err) => assert_eq!(ServerErrorKind::CasMismatch, err.kind),
        _ => panic!("Error was not expected type, got {}", kind),
    }

    let value = " 42".as_bytes();

    let append_result = agent
        .append(
            AppendOptions::builder()
                .key(&key)
                .scope_name("")
                .collection_name("")
                .retry_strategy(strat.clone())
                .value(value)
                .cas(1234)
                .build(),
        )
        .await;

    assert!(append_result.is_err());
    let e = append_result.err().unwrap();
    let e = e.is_memdx_error().unwrap();
    let kind = e.kind.clone();
    match *kind {
        Server(err) => assert_eq!(ServerErrorKind::CasMismatch, err.kind),
        _ => panic!("Error was not expected type, got {}", kind),
    }

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
            IncrementOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .delta(1)
                .initial(42)
                .build(),
        )
        .await
        .unwrap();

    assert_ne!(0, increment_result.cas);
    assert_eq!(increment_result.value, 42);
    assert!(increment_result.mutation_token.is_some());

    let decrement_result = agent
        .decrement(
            DecrementOptions::builder()
                .key(key.as_slice())
                .retry_strategy(strat.clone())
                .scope_name("")
                .collection_name("")
                .delta(2)
                .build(),
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

    let ops = [
        LookupInOp {
            op: LookupInOpType::Get,
            flags: SubdocOpFlag::None,
            path: "baz".as_bytes(),
        },
        LookupInOp {
            op: LookupInOpType::Exists,
            flags: SubdocOpFlag::None,
            path: "not-exists".as_bytes(),
        },
        LookupInOp {
            op: LookupInOpType::GetCount,
            flags: SubdocOpFlag::None,
            path: "arr".as_bytes(),
        },
        LookupInOp {
            op: LookupInOpType::GetDoc,
            flags: SubdocOpFlag::None,
            path: "".as_bytes(),
        },
    ];

    let lookup_in_opts = LookupInOptions::builder()
        .key(key.as_slice())
        .retry_strategy(strat.clone())
        .scope_name("")
        .collection_name("")
        .ops(&ops)
        .build();

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
        .is_some_and(|err| err.kind == SubdocErrorKind::PathNotFound));
    assert_eq!(
        lookup_in_result.value[1].err.as_ref().unwrap().op_index,
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

    let ops = [
        MutateInOp {
            op: MutateInOpType::Counter,
            flags: SubdocOpFlag::None,
            path: "bar".as_bytes(),
            value: "3".as_bytes(),
        },
        MutateInOp {
            op: MutateInOpType::DictSet,
            flags: SubdocOpFlag::None,
            path: "baz".as_bytes(),
            value: "\"world\"".as_bytes(),
        },
        MutateInOp {
            op: MutateInOpType::ArrayPushLast,
            flags: SubdocOpFlag::None,
            path: "arr".as_bytes(),
            value: "4".as_bytes(),
        },
    ];

    let mutate_in_options = MutateInOptions::builder()
        .key(key.as_slice())
        .retry_strategy(strat.clone())
        .scope_name("")
        .collection_name("")
        .ops(&ops)
        .expiry(10)
        .build();

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
    setup_tests().await;

    let profiler = dhat::Profiler::builder().build();

    let agent_opts = create_default_options().await;

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
