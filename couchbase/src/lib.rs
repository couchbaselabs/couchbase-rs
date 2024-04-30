#![doc(html_root_url = "https://docs.rs/couchbase/1.0.0-alpha.5")]
#![allow(warnings)]
mod api;
mod io;
#[cfg(test)]
mod tests;
pub use api::analytics_indexes::*;
pub use api::analytics_options::*;
pub use api::analytics_result::*;
pub use api::authenticator::*;
pub use api::binary_collection::*;
pub use api::bucket::*;
pub use api::buckets::*;
pub use api::cluster::*;
pub use api::collection::*;
pub use api::collections::*;
pub use api::error::*;
pub use api::keyvalue_options::*;
pub use api::keyvalue_results::*;
pub use api::query_indexes::*;
pub use api::query_options::*;
pub use api::query_result::*;
pub use api::results::*;
pub use api::scope::*;
pub use api::search::*;
pub use api::search_indexes::*;
pub use api::search_options::*;
pub use api::search_result::*;
pub use api::subdoc::*;
pub use api::subdoc_options::*;
pub use api::subdoc_results::*;
pub use api::users::*;
pub use api::view_indexes::*;
pub use api::view_options::*;
pub use api::view_result::*;

pub(crate) use api::options::*;

#[cfg(feature = "volatile")]
pub use io::request::{GenericManagementRequest, Request};
