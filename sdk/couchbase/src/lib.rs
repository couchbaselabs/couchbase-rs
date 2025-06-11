extern crate core;

pub mod bucket;
mod capella_ca;
mod clients;
pub mod cluster;
pub mod collection;
pub mod collection_binary_crud;
pub mod collection_crud;
pub mod collection_ds;
pub mod durability_level;
pub mod error;
pub mod error_context;
pub mod management;
pub mod mutation_state;
pub mod options;
pub mod results;
pub mod scope;
pub mod search;
pub mod service_type;
pub mod subdoc;
pub mod transcoding;

pub mod authenticator {
    pub use couchbase_core::authenticator::Authenticator;
    pub use couchbase_core::authenticator::PasswordAuthenticator;
}
