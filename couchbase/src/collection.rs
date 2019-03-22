use crate::instance::Instance;
use crate::options::*;
use crate::result::*;
use crate::util::JSON_COMMON_FLAG;
use futures::Future;
use std::sync::Arc;

use serde::Serialize;
use serde_json::to_vec;

pub struct Collection {
    instance: Arc<Instance>,
}

impl Collection {
    pub fn new(instance: Arc<Instance>) -> Self {
        Collection { instance }
    }

    pub fn get<S>(&self, id: S, options: Option<GetOptions>) -> Result<Option<GetResult>, ()>
    where
        S: Into<String>,
    {
        self.instance.get(id.into(), options).wait()
    }

    pub fn upsert<S, T>(&self, id: S, content: T) -> Result<MutationResult, ()>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = to_vec(&content).expect("Could not encode content for upsert");
        let flags = JSON_COMMON_FLAG;
        self.instance.upsert(id.into(), serialized, flags).wait()
    }

    pub fn insert<S, T>(&self, id: S, content: T) -> Result<MutationResult, ()>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = to_vec(&content).expect("Could not encode content for insert");
        let flags = JSON_COMMON_FLAG;
        self.instance.insert(id.into(), serialized, flags).wait()
    }

    pub fn replace<S, T>(&self, id: S, content: T) -> Result<MutationResult, ()>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = to_vec(&content).expect("Could not encode content for replace");
        let flags = JSON_COMMON_FLAG;
        self.instance.replace(id.into(), serialized, flags).wait()
    }
}
