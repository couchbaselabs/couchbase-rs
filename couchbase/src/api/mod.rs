#[macro_use]
pub mod macros;
pub mod analytics_indexes;
pub mod analytics_options;
pub mod analytics_result;
pub mod authenticator;
pub mod binary_collection;
pub mod bucket;
pub mod buckets;
pub mod cluster;
pub mod collection;
pub mod collections;
pub mod error;
pub mod keyvalue_options;
pub mod keyvalue_results;
pub mod options;
pub mod query_indexes;
pub mod query_options;
pub mod query_result;
pub mod results;
pub mod scope;
pub mod search;
pub mod search_indexes;
pub mod search_options;
pub mod search_result;
pub mod subdoc;
pub mod subdoc_options;
pub mod subdoc_results;
pub mod users;
pub mod view_indexes;
pub mod view_options;
pub mod view_result;

pub use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(test)] {
        pub use crate::tests::mock::MockCore as Core;
    } else {
        pub use crate::io::Core;
    }
}
