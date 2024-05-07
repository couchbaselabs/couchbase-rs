use super::*;
use crate::api::bucket::Bucket;
use crate::io::request::{AnalyticsRequest, QueryRequest, Request, SearchRequest};
use crate::{
    AnalyticsIndexManager, AnalyticsOptions, AnalyticsResult, Authenticator, BucketManager,
    CouchbaseError, CouchbaseResult, ErrorContext, QueryIndexManager, QueryOptions, QueryResult,
    SearchIndexManager, SearchOptions, SearchQuery, SearchResult, UserManager,
};
use futures::channel::oneshot;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
/// Connect to a Couchbase cluster and perform cluster-level operations
///
/// This `Cluster` object is also your main and only entry point into the SDK.
#[derive(Debug)]
pub struct Cluster {
    core: Arc<Core>,
}

impl Cluster {
    #[cfg(test)]
    pub(crate) fn new(core: Arc<crate::api::Core>) -> Self {
        Self { core }
    }
    /// Connect to a couchbase cluster
    ///
    /// # Arguments
    ///
    /// * `connection_string` - the connection string containing the bootstrap hosts
    /// * `username` - the name of the user, used for authentication
    /// * `password` - the password of the user
    ///
    /// # Examples
    ///
    /// Connecting to localhost with the `username` and its `password`.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// ```
    ///
    /// Using three nodes for bootstrapping (recommended for production):
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("couchbase://hosta,hostb,hostc", "username", "password");
    /// ```
    pub fn connect<S: Into<String>>(connection_string: S, username: S, password: S) -> Self {
        Cluster {
            core: Arc::new(Core::new(
                connection_string.into(),
                Some(username.into()),
                Some(password.into()),
            )),
        }
    }

    // This will likely move to become the actual connect function before beta.
    pub fn connect_with_options(
        connection_string: impl Into<String>,
        opts: ClusterOptions,
    ) -> Self {
        let mut connection_string = connection_string.into();
        let to_append = opts.to_conn_string();
        if !to_append.is_empty() {}
        if connection_string.contains('?') {
            connection_string = format!("{}&{}", connection_string, to_append);
        } else {
            connection_string = format!("{}?{}", connection_string, to_append);
        }
        let mut username = opts.username;
        let mut password = opts.password;
        if let Some(auth) = opts.authenticator {
            if let Some(u) = auth.username() {
                username = Some(u.clone());
            }
            if let Some(p) = auth.password() {
                password = Some(p.clone());
            }
            if let Some(path) = auth.certificate_path() {
                connection_string = format!("{}&certpath={}", connection_string, path.clone());
            }
            if let Some(path) = auth.key_path() {
                connection_string = format!("{}&keypath={}", connection_string, path.clone());
            }
        }

        Cluster {
            core: Arc::new(Core::new(connection_string, username, password)),
        }
    }

    /// Open and connect to a couchbase `Bucket`
    ///
    /// # Arguments
    ///
    /// * `name` - the name of the bucket
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// ```
    pub fn bucket<S: Into<String>>(&self, name: S) -> Bucket {
        let name = name.into();
        self.core.open_bucket(name.clone());
        Bucket::new(self.core.clone(), name)
    }

    /// Executes a N1QL statement
    ///
    /// # Arguments
    ///
    /// * `statement` - the N1QL statement to execute
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a N1QL query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.query("select * from bucket", couchbase::QueryOptions::default());
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
    /// match cluster.query("select 1=1", couchbase::QueryOptions::default()).await {
    ///     Ok(mut result) => {
    ///         let mut rows = result.rows::<serde_json::Value>();
    ///         while let Some(row) = rows.next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [QueryResult](struct.QueryResult.html) for more information on what and how it can be consumed.
    pub async fn query(
        &self,
        statement: impl Into<String>,
        options: impl Into<Option<QueryOptions>>,
    ) -> CouchbaseResult<QueryResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement: statement.into(),
            options,
            sender,
            scope: None,
        }));
        receiver.await.unwrap()
    }

    /// Executes an analytics query
    ///
    /// # Arguments
    ///
    /// * `statement` - the analyticss statement to execute
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run an analytics query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.analytics_query("select * from dataset", couchbase::AnalyticsOptions::default());
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
    /// match cluster.analytics_query("select 1=1", couchbase::AnalyticsOptions::default()).await {
    ///     Ok(mut result) => {
    ///         let mut rows = result.rows::<serde_json::Value>();
    ///         while let Some(row) = rows.next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [AnalyticsResult](struct.AnalyticsResult.html) for more information on what and how it can be consumed.
    pub async fn analytics_query(
        &self,
        statement: impl Into<String>,
        options: impl Into<Option<AnalyticsOptions>>,
    ) -> CouchbaseResult<AnalyticsResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement: statement.into(),
            options,
            sender,
            scope: None,
        }));
        receiver.await.unwrap()
    }

    /// Executes a search query
    ///
    /// # Arguments
    ///
    /// * `index` - the search index name to use
    /// * `query` - the search query to perform
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a search query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.search_query(
    ///    String::from("test"),
    ///    couchbase::QueryStringQuery::new(String::from("swanky")),
    ///    couchbase::SearchOptions::default(),
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
    /// match cluster.search_query(
    ///    String::from("test"),
    ///    couchbase::QueryStringQuery::new(String::from("swanky")),
    ///    couchbase::SearchOptions::default(),
    ///).await {
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
    /// See the [SearchResult](struct.SearchResult.html) for more information on what and how it can be consumed.
    pub async fn search_query(
        &self,
        index: impl Into<String>,
        query: impl SearchQuery,
        options: impl Into<Option<SearchOptions>>,
    ) -> CouchbaseResult<SearchResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Search(SearchRequest {
            index: index.into(),
            query: query
                .to_json()
                .map_err(|e| CouchbaseError::EncodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    ctx: ErrorContext::default(),
                })?,
            options,
            sender,
        }));
        receiver.await.unwrap()
    }

    /// Returns a new `UserManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let users = cluster.users();
    /// ```
    pub fn users(&self) -> UserManager {
        UserManager::new(self.core.clone())
    }

    /// Returns a new `BucketManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.buckets();
    /// ```
    pub fn buckets(&self) -> BucketManager {
        BucketManager::new(self.core.clone())
    }

    /// Returns a new `AnalyticsIndexManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let indexes = cluster.analytics_indexes();
    /// ```
    pub fn analytics_indexes(&self) -> AnalyticsIndexManager {
        AnalyticsIndexManager::new(self.core.clone())
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
    /// let indexes = cluster.query_indexes();
    /// ```
    pub fn query_indexes(&self) -> QueryIndexManager {
        QueryIndexManager::new(self.core.clone())
    }

    /// Returns a new `SearchIndexManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let indexes = cluster.search_indexes();
    /// ```
    pub fn search_indexes(&self) -> SearchIndexManager {
        SearchIndexManager::new(self.core.clone())
    }

    /// Returns a reference to the underlying core.
    ///
    /// Note that this API is unsupported and not stable, so you need to opt in via the
    /// `volatile` feature to access it.
    #[cfg(feature = "volatile")]
    pub fn core(&self) -> Arc<Core> {
        self.core.clone()
    }
}

#[derive(Debug, Default)]
pub struct TimeoutOptions {
    pub(crate) kv_connect_timeout: Option<Duration>,
    pub(crate) kv_timeout: Option<Duration>,
    pub(crate) kv_durable_timeout: Option<Duration>,
    pub(crate) view_timeout: Option<Duration>,
    pub(crate) query_timeout: Option<Duration>,
    pub(crate) analytics_timeout: Option<Duration>,
    pub(crate) search_timeout: Option<Duration>,
    pub(crate) management_timeout: Option<Duration>,
}

impl TimeoutOptions {
    pub fn kv_connect_timeout(mut self, timeout: Duration) -> Self {
        self.kv_connect_timeout = Some(timeout);
        self
    }

    pub fn kv_timeout(mut self, timeout: Duration) -> Self {
        self.kv_timeout = Some(timeout);
        self
    }

    pub fn kv_durable_timeout(mut self, timeout: Duration) -> Self {
        self.kv_durable_timeout = Some(timeout);
        self
    }

    pub fn view_timeout(mut self, timeout: Duration) -> Self {
        self.view_timeout = Some(timeout);
        self
    }

    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    pub fn analytics_timeout(mut self, timeout: Duration) -> Self {
        self.analytics_timeout = Some(timeout);
        self
    }

    pub fn search_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    pub fn management_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }
}

fn duration_to_conn_str_format(t: Duration) -> String {
    let v = (t.as_millis() as f32) / (1000_f32);
    v.to_string()
}

impl Display for TimeoutOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut timeouts = vec![];
        if let Some(t) = self.kv_connect_timeout {
            timeouts.push(format!(
                "kv_connect_timeout={}",
                duration_to_conn_str_format(t)
            ));
        }
        if let Some(t) = self.kv_timeout {
            timeouts.push(format!("kv_timeout={}", duration_to_conn_str_format(t)));
        }
        if let Some(t) = self.kv_durable_timeout {
            timeouts.push(format!(
                "kv_durable_timeout={}",
                duration_to_conn_str_format(t)
            ));
        }
        if let Some(t) = self.view_timeout {
            timeouts.push(format!("view_timeout={}", duration_to_conn_str_format(t)));
        }
        if let Some(t) = self.query_timeout {
            timeouts.push(format!("query_timeout={}", duration_to_conn_str_format(t)));
        }
        if let Some(t) = self.analytics_timeout {
            timeouts.push(format!(
                "analytics_timeout={}",
                duration_to_conn_str_format(t)
            ));
        }
        if let Some(t) = self.search_timeout {
            timeouts.push(format!("search_timeout={}", duration_to_conn_str_format(t)));
        }
        if let Some(t) = self.management_timeout {
            timeouts.push(format!("http_timeout={}", duration_to_conn_str_format(t)));
        }

        if timeouts.is_empty() {
            write!(f, "")
        } else {
            write!(f, "{}", timeouts.join("&"))
        }
    }
}

#[derive(Debug, Default)]
pub struct SecurityOptions {
    pub(crate) trust_store_path: Option<String>,
    pub(crate) skip_verify: Option<bool>,
    pub(crate) ciphers: Option<Vec<String>>,
}

impl SecurityOptions {
    pub fn trust_store_path(mut self, trust_store_path: impl Into<String>) -> Self {
        self.trust_store_path = Some(trust_store_path.into());
        self
    }
    pub fn skip_verify(mut self, skip_verify: bool) -> Self {
        self.skip_verify = Some(skip_verify);
        self
    }
    pub fn ciphers(mut self, ciphers: Vec<String>) -> Self {
        self.ciphers = Some(ciphers);
        self
    }
}

impl Display for SecurityOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut opts = vec![];
        if let Some(t) = &self.trust_store_path {
            opts.push(format!("truststorepath={}", t));
        }
        if let Some(skip) = self.skip_verify {
            if skip {
                opts.push(String::from("ssl=no_verify"));
            }
        }
        if let Some(ciphers) = &self.ciphers {
            opts.push(format!("sasl_mech_force={}", ciphers.join(",")));
        }

        if opts.is_empty() {
            write!(f, "")
        } else {
            write!(f, "{}", opts.join("&"))
        }
    }
}

#[derive(Debug)]
pub struct ClusterOptions {
    pub(crate) authenticator: Option<Box<dyn Authenticator>>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) timeouts: Option<TimeoutOptions>,
    pub(crate) security: Option<SecurityOptions>,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        Self {
            authenticator: None,
            username: None,
            password: None,
            timeouts: None,
            security: None,
        }
    }
}

impl ClusterOptions {
    pub fn authenticator(mut self, authenticator: Box<dyn Authenticator>) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn timeouts(mut self, timeouts: TimeoutOptions) -> Self {
        self.timeouts = Some(timeouts);
        self
    }

    pub fn security_config(mut self, security: SecurityOptions) -> Self {
        self.security = Some(security);
        self
    }

    pub(crate) fn to_conn_string(&self) -> String {
        let mut opts = vec![];
        if let Some(t) = &self.timeouts {
            opts.push(t.to_string());
        }
        if let Some(t) = &self.security {
            opts.push(t.to_string());
        }

        if opts.is_empty() {
            String::from("")
        } else {
            opts.join("&")
        }
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum ServiceType {
    Management,
    KeyValue,
    Views,
    Query,
    Search,
    Analytics,
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
