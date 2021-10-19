use crate::io::request::{CounterRequest, MutateRequest, MutateRequestType, Request};
use crate::io::Core;
use crate::{
    AppendOptions, CouchbaseError, CouchbaseResult, CounterOptions, CounterResult,
    DecrementOptions, ErrorContext, IncrementOptions, MutationResult, PrependOptions,
};
use futures::channel::oneshot;
use std::convert::TryFrom;
use std::sync::Arc;

pub struct BinaryCollection {
    core: Arc<Core>,
    name: String,
    scope_name: String,
    bucket_name: String,
}

impl BinaryCollection {
    pub(crate) fn new(
        core: Arc<Core>,
        name: String,
        scope_name: String,
        bucket_name: String,
    ) -> Self {
        Self {
            core,
            name,
            scope_name,
            bucket_name,
        }
    }

    pub async fn append<S: Into<String>>(
        &self,
        id: S,
        content: Vec<u8>,
        options: AppendOptions,
    ) -> CouchbaseResult<MutationResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Mutate(MutateRequest {
            id: id.into(),
            content,
            sender,
            bucket: self.bucket_name.clone(),
            ty: MutateRequestType::Append { options },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn prepend<S: Into<String>>(
        &self,
        id: S,
        content: Vec<u8>,
        options: PrependOptions,
    ) -> CouchbaseResult<MutationResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Mutate(MutateRequest {
            id: id.into(),
            content,
            sender,
            bucket: self.bucket_name.clone(),
            ty: MutateRequestType::Prepend { options },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn increment<S: Into<String>>(
        &self,
        id: S,
        options: IncrementOptions,
    ) -> CouchbaseResult<CounterResult> {
        let delta = match options.delta {
            Some(d) => i64::try_from(d).map_err(|_e| CouchbaseError::Generic {
                // TODO: we shouldn't swallow the error detail.
                ctx: ErrorContext::default(),
            })?,
            None => 1,
        };
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Counter(CounterRequest {
            id: id.into(),
            sender,
            bucket: self.bucket_name.clone(),
            options: CounterOptions {
                timeout: options.timeout,
                cas: options.cas,
                expiry: options.expiry,
                delta,
            },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn decrement<S: Into<String>>(
        &self,
        id: S,
        options: DecrementOptions,
    ) -> CouchbaseResult<CounterResult> {
        let delta = match options.delta {
            Some(d) => {
                -(i64::try_from(d).map_err(|_e| CouchbaseError::Generic {
                    // TODO: we shouldn't swallow the error detail.
                    ctx: ErrorContext::default(),
                })?)
            }
            None => -1,
        };
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Counter(CounterRequest {
            id: id.into(),
            sender,
            bucket: self.bucket_name.clone(),
            options: CounterOptions {
                timeout: options.timeout,
                cas: options.cas,
                expiry: options.expiry,
                delta,
            },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }
}
