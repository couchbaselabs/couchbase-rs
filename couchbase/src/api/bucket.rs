use super::*;
use crate::io::request::{PingRequest, Request, ViewRequest};
use crate::{
    Collection, CollectionManager, CouchbaseError, CouchbaseResult, ErrorContext, PingOptions,
    PingResult, Scope, ViewIndexManager, ViewOptions, ViewResult,
};
use futures::channel::oneshot;
use std::sync::Arc;
/// Provides bucket-level access to collections and view operations
#[derive(Debug)]
pub struct Bucket {
    name: String,
    core: Arc<Core>,
}

impl Bucket {
    pub(crate) fn new(core: Arc<Core>, name: String) -> Self {
        Self { name, core }
    }

    /// Opens the `default` collection (also used when a cluster with no collection support is used)
    ///
    /// The collection API provides acess to the Key/Value operations. The default collection is also
    /// implicitly using the default scope.
    pub fn default_collection(&self) -> Collection {
        Collection::new(self.core.clone(), "".into(), "".into(), self.name.clone())
    }

    /// The name of the bucket
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Opens a custom collection inside the `default` scope
    ///
    /// # Arguments
    ///
    /// * `name` - the collection name
    pub fn collection<S: Into<String>>(&self, name: S) -> Collection {
        Collection::new(self.core.clone(), name.into(), "".into(), self.name.clone())
    }

    /// Opens a custom scope
    ///
    /// # Arguments
    ///
    /// * `name` - the scope name
    pub fn scope<S: Into<String>>(&self, name: S) -> Scope {
        Scope::new(self.core.clone(), name.into(), self.name.clone())
    }

    /// Executes a ping request
    ///
    /// # Arguments
    ///
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a ping with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// # let bucket = cluster.bucket("travel-sample");
    /// # let result = bucket.ping(couchbase::PingOptions::default());
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// match  bucket.ping(couchbase::PingOptions::default()).await {
    ///     Ok(mut result) => {
    ///         println!("Ping results {:?}", result);
    ///     },
    ///     Err(e) => panic!("Ping failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [PingResult](struct.PingResult.html) for more information on what and how it can be consumed.
    pub async fn ping(
        &self,
        options: impl Into<Option<PingOptions>>,
    ) -> CouchbaseResult<PingResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::Ping(PingRequest { options, sender }));
        receiver.await.unwrap()
    }

    /// Returns a new `CollectionsManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// let manager = bucket.collections();
    /// ```
    pub fn collections(&self) -> CollectionManager {
        CollectionManager::new(self.core.clone(), self.name.clone())
    }

    /// Returns a new `QueryIndexManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// let manager = bucket.view_indexes();
    /// ```
    pub fn view_indexes(&self) -> ViewIndexManager {
        ViewIndexManager::new(self.core.clone(), self.name.clone())
    }

    /// Executes a view query
    ///
    /// # Arguments
    ///
    /// * `design_document` - the design document name to use
    /// * `view_name` - the view name to use
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a view query with default options.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// let result = bucket.view_query(
    ///    "my_design_doc",
    ///    "my_view",
    ///    couchbase::ViewOptions::default(),
    ///);
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// match bucket.view_query(
    ///    "my_design_doc",
    ///    "my_view",
    ///    couchbase::ViewOptions::default(),
    /// ).await {
    ///     Ok(mut result) => {
    ///         for row in result.rows().next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [ViewResult](struct.ViewResult.html) for more information on what and how it can be consumed.
    pub async fn view_query(
        &self,
        design_document: impl Into<String>,
        view_name: impl Into<String>,
        options: impl Into<Option<ViewOptions>>,
    ) -> CouchbaseResult<ViewResult> {
        let options = unwrap_or_default!(options.into());
        let form_data = options.form_data()?;
        let payload = match serde_urlencoded::to_string(form_data) {
            Ok(p) => p,
            Err(e) => {
                return Err(CouchbaseError::EncodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::Other, e),
                    ctx: ErrorContext::default(),
                });
            }
        };

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::View(ViewRequest {
            design_document: design_document.into(),
            view_name: view_name.into(),
            options: payload.into_bytes(),
            sender,
        }));
        receiver.await.unwrap()
    }
}
