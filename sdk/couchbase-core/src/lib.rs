#![feature(async_closure)]

pub mod authenticator;
pub mod cbconfig;
mod configparser;
mod configwatcher;
mod error;
mod kvclient;
mod kvclient_ops;
mod kvclientmanager;
mod kvclientpool;
pub mod memdx;
mod parsedconfig;
mod scram;
pub mod service_type;
mod vbucketmap;
mod vbucketrouter;
