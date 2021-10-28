use crate::tests::*;
use crate::util::TestConfig;
use crate::TestResult;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;

// Sad panda noises
pub fn tests(config: Arc<TestConfig>) -> Vec<TestFn> {
    vec![
        TestFn::new(
            "test_upsert_get",
            Box::pin(kv::test_upsert_get(config.clone())),
        ),
        TestFn::new(
            "test_upsert_replace_get",
            Box::pin(kv::test_upsert_replace_get(config.clone())),
        ),
        TestFn::new(
            "test_upsert_preserve_expiry",
            Box::pin(kv::test_upsert_preserve_expiry(config.clone())),
        ),
        TestFn::new(
            "test_replace_preserve_expiry",
            Box::pin(kv::test_replace_preserve_expiry(config.clone())),
        ),
        TestFn::new(
            "test_get_with_expiry",
            Box::pin(kv::test_get_with_expiry(config.clone())),
        ),
        TestFn::new(
            "test_get_non_existant",
            Box::pin(kv::test_get_non_existant(config.clone())),
        ),
        TestFn::new(
            "test_double_insert",
            Box::pin(kv::test_double_insert(config.clone())),
        ),
        TestFn::new(
            "test_upsert_get_remove",
            Box::pin(kv::test_upsert_get_remove(config.clone())),
        ),
        TestFn::new(
            "test_remove_with_cas",
            Box::pin(kv::test_remove_with_cas(config.clone())),
        ),
        TestFn::new(
            "test_get_and_touch",
            Box::pin(kv::test_get_and_touch(config.clone())),
        ),
        TestFn::new(
            "test_get_and_lock",
            Box::pin(kv::test_get_and_lock(config.clone())),
        ),
        TestFn::new("test_unlock", Box::pin(kv::test_unlock(config.clone()))),
        TestFn::new(
            "test_unlock_invalid_cas",
            Box::pin(kv::test_unlock_invalid_cas(config.clone())),
        ),
        TestFn::new(
            "test_double_lock",
            Box::pin(kv::test_double_lock(config.clone())),
        ),
        TestFn::new("test_touch", Box::pin(kv::test_touch(config.clone()))),
        TestFn::new(
            "test_replicate_to_get_any_replica",
            Box::pin(kv::test_replicate_to_get_any_replica(config.clone())),
        ),
        TestFn::new(
            "test_persist_to_get_any_replica",
            Box::pin(kv::test_persist_to_get_any_replica(config.clone())),
        ),
        TestFn::new(
            "test_durability_majority_get_any_replica",
            Box::pin(kv::test_durability_majority_get_any_replica(config.clone())),
        ),
        TestFn::new(
            "test_durability_persist_to_majority_get_any_replica",
            Box::pin(kv::test_durability_persist_to_majority_get_any_replica(
                config.clone(),
            )),
        ),
        TestFn::new(
            "test_durability_majority_persist_on_master_get_any_replica",
            Box::pin(
                kv::test_durability_majority_persist_on_master_get_any_replica(config.clone()),
            ),
        ),
        TestFn::new("test_query", Box::pin(query::test_query(config.clone()))),
        TestFn::new(
            "test_query_named_params",
            Box::pin(query::test_query_named_params(config.clone())),
        ),
        TestFn::new(
            "test_query_positional_params",
            Box::pin(query::test_query_positional_params(config.clone())),
        ),
        TestFn::new(
            "test_query_prepared",
            Box::pin(query::test_query_prepared(config.clone())),
        ),
        TestFn::new(
            "test_query_adhoc",
            Box::pin(query::test_query_adhoc(config.clone())),
        ),
        TestFn::new(
            "test_scope_query",
            Box::pin(query::test_scope_query(config.clone())),
        ),
        TestFn::new(
            "test_upsert_lookupin",
            Box::pin(subdoc::test_upsert_lookupin(config.clone())),
        ),
        TestFn::new(
            "test_mutatein_basic",
            Box::pin(subdoc::test_mutatein_basic(config.clone())),
        ),
        TestFn::new(
            "test_mutatein_arrays",
            Box::pin(subdoc::test_mutatein_arrays(config.clone())),
        ),
        TestFn::new(
            "test_mutatein_counters",
            Box::pin(subdoc::test_mutatein_counters(config.clone())),
        ),
        TestFn::new(
            "test_mutatein_blank_path_remove",
            Box::pin(subdoc::test_mutatein_blank_path_remove(config.clone())),
        ),
        TestFn::new(
            "test_mutatein_blank_path_get",
            Box::pin(subdoc::test_mutatein_blank_path_get(config.clone())),
        ),
        TestFn::new("test_xattrs", Box::pin(subdoc::test_xattrs(config.clone()))),
        TestFn::new("test_macros", Box::pin(subdoc::test_macros(config.clone()))),
        TestFn::new(
            "test_mutatein_preserve_expiry",
            Box::pin(subdoc::test_mutatein_preserve_expiry(config.clone())),
        ),
    ]
}

pub struct TestFn {
    pub name: String,
    pub func: Pin<Box<dyn Future<Output = TestResult<bool>> + Send + 'static>>,
}

impl TestFn {
    pub fn new(
        name: impl Into<String>,
        func: Pin<Box<dyn Future<Output = TestResult<bool>> + Send + 'static>>,
    ) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}
