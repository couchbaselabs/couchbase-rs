use super::*;
use crate::io::request::{CounterRequest, MutateRequest, MutateRequestType, Request};
use crate::{
    AppendOptions, CouchbaseError, CouchbaseResult, CounterOptions, CounterResult,
    DecrementOptions, DurabilityLevel, ErrorContext, IncrementOptions, MutationResult,
    PrependOptions,
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
        options: impl Into<Option<AppendOptions>>,
    ) -> CouchbaseResult<MutationResult> {
        let options = unwrap_or_default!(options.into());
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
        options: impl Into<Option<PrependOptions>>,
    ) -> CouchbaseResult<MutationResult> {
        let options = unwrap_or_default!(options.into());
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
        options: impl Into<Option<IncrementOptions>>,
    ) -> CouchbaseResult<CounterResult> {
        let options = unwrap_or_default!(options.into());

        // lcb doesn't support observe based durability for counters.
        if let Some(durability) = options.durability {
            match durability {
                DurabilityLevel::ClientVerified(_) => {
                    return Err(CouchbaseError::InvalidArgument {
                        ctx: ErrorContext::from((
                            "durability",
                            "cannot use client verified durability with increment",
                        )),
                    })
                }
                _ => {}
            }
        }

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
                durability: options.durability,
            },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn decrement<S: Into<String>>(
        &self,
        id: S,
        options: impl Into<Option<DecrementOptions>>,
    ) -> CouchbaseResult<CounterResult> {
        let options = unwrap_or_default!(options.into());

        // lcb doesn't support observe based durability for counters.
        if let Some(durability) = options.durability {
            match durability {
                DurabilityLevel::ClientVerified(_) => {
                    return Err(CouchbaseError::InvalidArgument {
                        ctx: ErrorContext::from((
                            "durability",
                            "cannot use client verified durability with decrement",
                        )),
                    })
                }
                _ => {}
            }
        }

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
                durability: options.durability,
            },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }
}
