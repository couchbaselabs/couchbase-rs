use crate::error::CouchbaseError;
use crate::instance::Instance;
use crate::options::*;
use crate::result::*;
use crate::subdoc::*;
use crate::util::JSON_COMMON_FLAG;
use futures::future::err;
use futures::future::Either;
use futures::Future;
use serde::Serialize;
use serde_json::to_vec;
use std::sync::Arc;
use std::time::Duration;

/// `Collection` level access to operations.
pub struct Collection {
    instance: Arc<Instance>,
}

impl Collection {
    /// Creates a new `Collection`.
    ///
    /// This function is not intended to be called directly, but rather a new
    /// `Collection` should be retrieved through the `Bucket`.
    ///
    pub(crate) fn new(instance: Arc<Instance>) -> Self {
        Collection { instance }
    }

    /// Fetches a document from the collection.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use serde_json::Value;
    /// use futures::Future;
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #   .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #   .bucket("travel-sample")
    /// #   .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    /// #
    /// let found_doc = collection
    ///     .get("airport_1297", None)
    ///     .wait()
    ///     .expect("Error while loading doc");
    ///
    /// if found_doc.is_some() {
    ///     println!(
    ///         "Content Decoded {:?}",
    ///         found_doc.unwrap().content_as::<Value>()
    ///     );
    /// }
    /// ```
    pub fn get<S>(
        &self,
        id: S,
        options: Option<GetOptions>,
    ) -> impl Future<Item = Option<GetResult>, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.get(id.into(), options)
    }

    /// Fetches a document from the collection and write locks it.
    ///
    /// Note that the `lock` time can be overridden in the options struct. If none is set explicitly,
    /// the default duration of 30 seconds is used.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use futures::Future;
    /// use serde_json::Value;
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #   .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #   .bucket("travel-sample")
    /// #   .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    /// #
    /// let found_doc = collection
    ///     .get_and_lock("airport_1297", None)
    ///     .wait()
    ///     .expect("Error while loading and locking doc");
    ///
    /// if found_doc.is_some() {
    ///     println!(
    ///         "Content Decoded {:?}",
    ///         found_doc.unwrap().content_as::<Value>()
    ///     );
    /// }
    /// ```
    pub fn get_and_lock<S>(
        &self,
        id: S,
        options: Option<GetAndLockOptions>,
    ) -> impl Future<Item = Option<GetResult>, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.get_and_lock(id.into(), options)
    }

    /// Fetches a document from the collection and modifies its expiry.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `expiration` - The new expiration of the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use std::time::Duration;
    /// use serde_json::Value;
    /// use futures::Future;
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #   .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #   .bucket("travel-sample")
    /// #   .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    /// #
    /// let found_doc = collection
    ///     .get_and_touch("airport_1297", Duration::from_secs(5), None)
    ///     .wait()
    ///     .expect("Error while loading and touching doc");
    ///
    /// if found_doc.is_some() {
    ///     println!(
    ///         "Content Decoded {:?}",
    ///         found_doc.unwrap().content_as::<Value>()
    ///     );
    /// }
    /// ```
    pub fn get_and_touch<S>(
        &self,
        id: S,
        expiration: Duration,
        options: Option<GetAndTouchOptions>,
    ) -> impl Future<Item = Option<GetResult>, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.get_and_touch(id.into(), expiration, options)
    }

    /// Inserts or replaces a new document into the collection.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `content` - The content to store inside the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use serde_derive::Serialize;
    /// use futures::Future;
    ///
    /// #[derive(Debug, Serialize)]
    /// struct Airport {
    ///     airportname: String,
    ///     icao: String,
    ///     iata: String,
    /// }
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #     .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #     .bucket("travel-sample")
    /// #     .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    ///
    /// let airport = Airport {
    ///     airportname: "Vienna Airport".into(),
    ///     icao: "LOWW".into(),
    ///     iata: "VIE".into(),
    /// };
    ///
    /// collection
    ///     .upsert("airport_999", airport, None)
    ///     .wait()
    ///     .expect("could not upsert airport!");
    /// ```
    pub fn upsert<S, T>(
        &self,
        id: S,
        content: T,
        options: Option<UpsertOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = match to_vec(&content) {
            Ok(v) => v,
            Err(_e) => return Either::A(err(CouchbaseError::EncodingError)),
        };
        let flags = JSON_COMMON_FLAG;
        Either::B(self.instance.upsert(id.into(), serialized, flags, options))
    }

    /// Inserts a document into the collection.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `content` - The content to store inside the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use serde_derive::Serialize;
    /// use futures::Future;
    ///
    /// #[derive(Debug, Serialize)]
    /// struct Airport {
    ///     airportname: String,
    ///     icao: String,
    ///     iata: String,
    /// }
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #     .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #     .bucket("travel-sample")
    /// #     .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    ///
    /// let airport = Airport {
    ///     airportname: "Vienna Airport".into(),
    ///     icao: "LOWW".into(),
    ///     iata: "VIE".into(),
    /// };
    ///
    /// collection
    ///     .insert("airport_999", airport, None)
    ///     .wait()
    ///     .expect("could not insert airport!");
    /// ```
    pub fn insert<S, T>(
        &self,
        id: S,
        content: T,
        options: Option<InsertOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = match to_vec(&content) {
            Ok(v) => v,
            Err(_e) => return Either::A(err(CouchbaseError::EncodingError)),
        };
        let flags = JSON_COMMON_FLAG;
        Either::B(self.instance.insert(id.into(), serialized, flags, options))
    }

    /// Replaces an existing document in the collection.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `content` - The content to store inside the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use serde_derive::Serialize;
    /// use futures::Future;
    ///
    /// #[derive(Debug, Serialize)]
    /// struct Airport {
    ///     airportname: String,
    ///     icao: String,
    ///     iata: String,
    /// }
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #     .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #     .bucket("travel-sample")
    /// #     .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    ///
    /// let airport = Airport {
    ///     airportname: "Vienna Airport".into(),
    ///     icao: "LOWW".into(),
    ///     iata: "VIE".into(),
    /// };
    ///
    /// collection
    ///     .replace("airport_999", airport, None)
    ///     .wait()
    ///     .expect("could not replace airport!");
    /// ```
    pub fn replace<S, T>(
        &self,
        id: S,
        content: T,
        options: Option<ReplaceOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError>
    where
        S: Into<String>,
        T: Serialize,
    {
        let serialized = match to_vec(&content) {
            Ok(v) => v,
            Err(_e) => return Either::A(err(CouchbaseError::EncodingError)),
        };
        let flags = JSON_COMMON_FLAG;
        Either::B(self.instance.replace(id.into(), serialized, flags, options))
    }

    /// Removes a document from the collection.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use futures::Future;
    /// # use couchbase::Cluster;
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #   .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #   .bucket("travel-sample")
    /// #   .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    /// let result = collection.remove("document_id", None).wait();
    /// ```
    pub fn remove<S>(
        &self,
        id: S,
        options: Option<RemoveOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.remove(id.into(), options)
    }

    /// Changes the expiration time on a document.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `expiration` - The new expiration of the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::time::Duration;
    /// use futures::Future;
    /// # use couchbase::Cluster;
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #   .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #   .bucket("travel-sample")
    /// #   .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    /// let result = collection.touch("document_id", Duration::from_secs(5), None).wait();
    /// ```
    pub fn touch<S>(
        &self,
        id: S,
        expiration: Duration,
        options: Option<TouchOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.touch(id.into(), expiration, options)
    }

    /// Unlocks a write-locked document.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `cas` - The cas needed to remove the write lock.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use futures::Future;
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #   .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #   .bucket("travel-sample")
    /// #   .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    /// let cas = 1234; // retrieved from a `getAndLock`
    /// let result = collection.unlock("document_id", cas, None).wait();
    /// ```
    pub fn unlock<S>(
        &self,
        id: S,
        cas: u64,
        options: Option<UnlockOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.unlock(id.into(), cas, options)
    }

    /// Checks if a document exists and if so returns a cas value with it.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use futures::Future;
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #   .expect("Could not create Cluster reference!");
    /// # let bucket = cluster
    /// #   .bucket("travel-sample")
    /// #   .expect("Could not open bucket");
    /// # let collection = bucket.default_collection();
    /// let result = collection.exists("document_id", None).wait();
    /// ```
    pub fn exists<S>(
        &self,
        id: S,
        options: Option<ExistsOptions>,
    ) -> impl Future<Item = Option<ExistsResult>, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.exists(id.into(), options)
    }

    pub fn lookup_in<S>(
        &self,
        id: S,
        specs: Vec<LookupInSpec>,
        options: Option<LookupInOptions>,
    ) -> impl Future<Item = Option<LookupInResult>, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.lookup_in(id.into(), specs, options)
    }
}
