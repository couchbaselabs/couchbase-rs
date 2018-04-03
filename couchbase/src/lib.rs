//! This crate represents the official, yet under heavy development, Rust SDK for Couchbase.
//!
//! It is based on the `couchbase_sys`-crate, which in turn consists of Rust bindings to the
//! [libcouchbase](https://github.com/couchbase/libcouchbase) c-library.
//!
//! # Examples
//!
//! Reading and writing a `Document` is simple:
//!
//! ```rust,no_run
//! extern crate couchbase;
//! extern crate futures;
//!
//! use couchbase::{Document, Cluster};
//! use couchbase::document::BinaryDocument;
//! use futures::Future;
//! use futures::executor::block_on;
//!
//! /// A very simple example which connects to the `default` bucket and writes and loads
//! /// a document.
//! fn main() {
//!     // Initialize the Cluster
//!     let cluster = Cluster::new("localhost").expect("Could not initialize Cluster");
//!
//!     // If you auth with 5.0 / RBAC, use this:
//!     // cluster.authenticate("Administrator", "password");
//!
//!     // Open the travel-sample bucket
//!     let bucket = cluster.open_bucket("default", None).expect("Could not open Bucket");
//!
//!     // Create a document and store it in the bucket
//!     let document = BinaryDocument::create("hello", None, Some("abc".as_bytes().to_owned()), None);
//!    println!("Wrote Document {:?}",
//!              block_on(bucket.upsert(document))
//!                  .expect("Upsert failed!"));
//!
//!     // Load the previously written document and print it out
//!     let document: BinaryDocument = block_on(bucket.get("hello")).expect("Could not load Document");
//!     println!("Found Document {:?}", document);
//!
//! }
//! ```
//!
//! For now, more examples can be found under `examples`. Note that for all the `serde`-based
//! examples you need to at least have Rust 1.15.0 installed.
//!
#![doc(html_root_url = "https://docs.rs/couchbase/0.4.0")]
extern crate couchbase_sys;
#[macro_use]
extern crate log;
extern crate futures;
extern crate parking_lot;
extern crate serde;
extern crate url;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

pub mod bucket;
pub mod cluster;
pub mod document;
pub mod sync;
pub mod error;
pub mod query;
mod connstr;

pub use document::{BinaryDocument, Document, JsonDocument};
pub use bucket::Bucket;
pub use cluster::Cluster;
pub use sync::{CouchbaseFuture, CouchbaseStream};
pub use error::CouchbaseError;
pub use query::n1ql::{N1qlMeta, N1qlResult, N1qlRow};
pub use query::views::{ViewMeta, ViewQuery, ViewResult, ViewRow};
