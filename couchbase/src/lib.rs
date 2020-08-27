#![doc(html_root_url = "https://docs.rs/couchbase/1.0.0-alpha.5")]

mod api;
mod io;

pub use api::error::*;
pub use api::options::*;
pub use api::results::*;
pub use api::search::*;
pub use api::users::*;
pub use api::{
    Bucket, Cluster, Collection, LookupInSpec, MutateInSpec, MutationState, MutationToken,
};

#[cfg(feature = "volatile")]
pub use api::Scope;

#[cfg(feature = "volatile")]
pub use io::request::{GenericManagementRequest, KvStatsRequest, Request};
