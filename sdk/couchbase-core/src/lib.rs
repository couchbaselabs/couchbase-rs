#![feature(async_closure)]
// #![feature(unboxed_closures)]
#![feature(async_fn_traits)]
#![feature(unboxed_closures)]

pub mod authenticator;
pub mod cbconfig;
mod configparser;
mod configwatcher;
mod crudcomponent;
mod crudoptions;
mod crudresults;
mod error;
mod kvclient;
mod kvclient_ops;
mod kvclientmanager;
mod kvclientpool;
pub mod memdx;
mod mutationtoken;
mod parsedconfig;
mod scram;
pub mod service_type;
mod vbucketmap;
mod vbucketrouter;
