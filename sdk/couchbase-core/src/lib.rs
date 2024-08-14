#![feature(async_closure)]
#![feature(async_fn_traits)]
#![feature(unboxed_closures)]

pub mod agent;
pub mod agent_ops;
pub mod agentoptions;
pub mod authenticator;
pub mod cbconfig;
mod collection_resolver_cached;
mod collection_resolver_memd;
mod collectionresolver;
mod compressionmanager;
mod configparser;
mod configwatcher;
mod crudcomponent;
pub mod crudoptions;
pub mod crudresults;
mod error;
mod kvclient;
mod kvclient_ops;
mod kvclientmanager;
mod kvclientpool;
pub mod memdx;
mod mutationtoken;
mod networktypeheuristic;
mod nmvbhandler;
mod parsedconfig;
pub mod retry;
pub mod retrybesteffort;
pub mod retryfailfast;
mod scram;
pub mod service_type;
mod vbucketmap;
mod vbucketrouter;
