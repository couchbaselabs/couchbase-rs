use crate::tests::{kv, query};
use crate::util::TestConfig;
use couchbase::CouchbaseResult;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;

// Sad panda noises
pub fn tests(config: Arc<TestConfig>) -> Vec<TestFn> {
    vec![
        TestFn::new("upsert_get", Box::pin(kv::upsert_get(config.clone()))),
        TestFn::new("query", Box::pin(query::query(config.clone()))),
        TestFn::new(
            "upsert_replace_get",
            Box::pin(kv::upsert_replace_get(config.clone())),
        ),
    ]
}

pub struct TestFn {
    pub name: String,
    pub func: Pin<Box<dyn Future<Output = CouchbaseResult<bool>> + Send + 'static>>,
}

impl TestFn {
    pub fn new(
        name: impl Into<String>,
        func: Pin<Box<dyn Future<Output = CouchbaseResult<bool>> + Send + 'static>>,
    ) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}
