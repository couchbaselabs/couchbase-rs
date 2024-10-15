use crate::error;
use crate::options::query_options::QueryOptions;
use crate::results::query_results::QueryResult;
use couchbase_core::agent::Agent;
use uuid::Uuid;

pub(crate) struct QueryClient {
    backend: QueryClientBackend,
}

impl QueryClient {
    pub fn new(backend: QueryClientBackend) -> Self {
        Self { backend }
    }

    pub async fn query(
        &self,
        statement: String,
        opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        match &self.backend {
            QueryClientBackend::CouchbaseQueryClientBackend(backend) => {
                backend.query(statement, opts).await
            }
            QueryClientBackend::Couchbase2QueryClientBackend(backend) => {
                backend.query(statement, opts).await
            }
        }
    }
}

pub(crate) enum QueryClientBackend {
    CouchbaseQueryClientBackend(CouchbaseQueryClient),
    Couchbase2QueryClientBackend(Couchbase2QueryClient),
}

pub(crate) struct QueryKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
}

pub(crate) struct CouchbaseQueryClient {
    agent: Agent,
    keyspace: Option<QueryKeyspace>,
}

impl CouchbaseQueryClient {
    pub fn new(agent: Agent) -> Self {
        Self {
            agent,
            keyspace: None,
        }
    }

    pub fn with_keyspace(agent: Agent, keyspace: QueryKeyspace) -> Self {
        Self {
            agent,
            keyspace: Some(keyspace),
        }
    }

    async fn query(
        &self,
        statement: String,
        opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        let mut opts = opts.unwrap_or_default();
        if opts.client_context_id.is_none() {
            opts.client_context_id = Some(Uuid::new_v4().to_string());
        }

        let ad_hoc = opts.ad_hoc.unwrap_or(true);

        let mut query_opts = couchbase_core::queryoptions::QueryOptions::try_from(opts)?;
        query_opts.statement = Some(statement);

        if let Some(keyspace) = &self.keyspace {
            query_opts.query_context = Some(format!(
                "{}.{}",
                keyspace.bucket_name.clone(),
                keyspace.scope_name.clone()
            ));
        }

        if ad_hoc {
            Ok(QueryResult::from(self.agent.query(query_opts).await?))
        } else {
            Ok(QueryResult::from(
                self.agent.prepared_query(query_opts).await?,
            ))
        }
    }
}

pub(crate) struct Couchbase2QueryClient {}

impl Couchbase2QueryClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn query(
        &self,
        statement: String,
        opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        unimplemented!()
    }
}
