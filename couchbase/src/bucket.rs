use crate::collection::Collection;
use crate::instance::Instance;
use std::sync::Arc;

pub struct Bucket {
    instance: Arc<Instance>,
}

impl Bucket {
    pub fn new(cs: &str, user: &str, pw: &str) -> Self {
        let instance = Instance::new(cs, user, pw).expect("Could not init instance");
        Bucket {
            instance: Arc::new(instance),
        }
    }

    pub fn default_collection(&self) -> Collection {
        Collection::new(self.instance.clone())
    }

    pub(crate) fn close(&self) {
        self.instance.shutdown();
    }
}
