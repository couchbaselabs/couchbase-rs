use crate::common::test_collection::TestCollection;
use crate::common::test_manager::TestCollectionManager;
use crate::common::test_scope::TestScope;
use couchbase::bucket::Bucket;

#[derive(Clone)]
pub struct TestBucket {
    inner: Bucket,
}

impl std::ops::Deref for TestBucket {
    type Target = Bucket;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestBucket {
    pub fn new(inner: Bucket) -> Self {
        Self { inner }
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn scope(&self, name: impl Into<String>) -> TestScope {
        TestScope::new(self.inner.scope(name))
    }

    pub fn collection(&self, name: impl Into<String>) -> TestCollection {
        TestCollection::new(self.inner.collection(name))
    }

    pub fn default_collection(&self) -> TestCollection {
        TestCollection::new(self.inner.default_collection())
    }

    pub fn collections(&self) -> TestCollectionManager {
        TestCollectionManager::new(self.inner.collections())
    }
}
