use crate::error::CouchbaseError;
use crate::instance::Instance;
use crate::options::*;
use crate::result::*;
use crate::util::JSON_COMMON_FLAG;
use futures::Future;
use serde::Serialize;
use serde_json::to_vec;
use std::sync::Arc;
use std::time::Duration;

pub struct Collection {
    instance: Arc<Instance>,
}

impl Collection {
    pub fn new(instance: Arc<Instance>) -> Self {
        Collection { instance }
    }

    pub fn get<S>(
        &self,
        id: S,
        options: Option<GetOptions>,
    ) -> Result<Option<GetResult>, CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.get(id.into(), options).wait()
    }

    pub fn get_and_lock<S>(
        &self,
        id: S,
        options: Option<GetAndLockOptions>,
    ) -> Result<Option<GetResult>, CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.get_and_lock(id.into(), options).wait()
    }

    pub fn get_and_touch<S>(
        &self,
        id: S,
        expiration: Duration,
        options: Option<GetAndTouchOptions>,
    ) -> Result<Option<GetResult>, CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance
            .get_and_touch(id.into(), expiration, options)
            .wait()
    }

    pub fn upsert<S, T>(
        &self,
        id: S,
        content: T,
        options: Option<UpsertOptions>,
    ) -> Result<MutationResult, CouchbaseError>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = to_vec(&content).expect("Could not encode content for upsert");
        let flags = JSON_COMMON_FLAG;
        self.instance
            .upsert(id.into(), serialized, flags, options)
            .wait()
    }

    pub fn insert<S, T>(
        &self,
        id: S,
        content: T,
        options: Option<InsertOptions>,
    ) -> Result<MutationResult, CouchbaseError>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = to_vec(&content).expect("Could not encode content for insert");
        let flags = JSON_COMMON_FLAG;
        self.instance
            .insert(id.into(), serialized, flags, options)
            .wait()
    }

    pub fn replace<S, T>(
        &self,
        id: S,
        content: T,
        options: Option<ReplaceOptions>,
    ) -> Result<MutationResult, CouchbaseError>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = to_vec(&content).expect("Could not encode content for replace");
        let flags = JSON_COMMON_FLAG;
        self.instance
            .replace(id.into(), serialized, flags, options)
            .wait()
    }

    pub fn remove<S>(
        &self,
        id: S,
        options: Option<RemoveOptions>,
    ) -> Result<MutationResult, CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.remove(id.into(), options).wait()
    }
}
