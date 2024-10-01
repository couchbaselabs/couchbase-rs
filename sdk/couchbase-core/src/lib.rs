#![feature(async_closure)]
#![feature(async_fn_traits)]
#![feature(unboxed_closures)]
extern crate core;
#[macro_use]
extern crate lazy_static;

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
pub mod error;
mod helpers;
mod httpcomponent;
pub mod httpx;
mod kvclient;
mod kvclient_ops;
mod kvclientmanager;
mod kvclientpool;
pub mod memdx;
pub mod mutationtoken;
mod networktypeheuristic;
mod nmvbhandler;
pub mod ondemand_agentmanager;
mod parsedconfig;
mod querycomponent;
pub mod queryoptions;
pub mod queryx;
pub mod retry;
pub mod retrybesteffort;
pub mod retryfailfast;
mod scram;
pub mod service_type;
mod util;
mod vbucketmap;
mod vbucketrouter;
