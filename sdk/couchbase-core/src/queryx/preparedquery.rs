use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::httpx::client::Client;
use crate::queryx::error;
use crate::queryx::error::Error;
use crate::queryx::query::Query;
use crate::queryx::query_options::QueryOptions;
use crate::queryx::query_respreader::QueryRespReader;
use crate::queryx::query_result::ResultStream;

#[derive(Clone, Debug, Default)]
pub struct PreparedStatementCache {
    cache: HashMap<String, String>,
}

impl PreparedStatementCache {
    pub fn get(&self, statement: &str) -> Option<&String> {
        self.cache.get(statement)
    }

    pub fn put(&mut self, statement: &str, prepared_name: &str) {
        self.cache
            .insert(statement.to_string(), prepared_name.to_string());
    }
}

pub struct PreparedQuery<C: Client> {
    pub executor: Query<C>,
    pub cache: Arc<Mutex<PreparedStatementCache>>,
}

impl<C: Client> PreparedQuery<C> {
    pub async fn prepared_query(&self, opts: &QueryOptions) -> error::Result<QueryRespReader> {
        // We need to clone the options so that we can modify it with any cached statement.
        let mut opts = (*opts).clone();

        if let Some(ae) = opts.auto_execute {
            // If this is already marked as auto-execute, we just pass it through
            if ae {
                return self.executor.query(&opts).await;
            }
        }

        let statement = if let Some(statement) = opts.statement {
            statement
        } else {
            return Err(Error::new_generic_error(
                "Statement must be present if auto_execute is true".to_string(),
                &self.executor.endpoint,
                "",
                opts.client_context_id.unwrap_or_default(),
            ));
        };

        // We have to manage the scope of the cache here, static analysis will flag us as holding
        // the mutex across the await even if we manually drop just before it.
        let cached;
        {
            let cache = self.cache.lock().unwrap();
            cached = cache.get(&statement).cloned();
        }

        if let Some(cached_statement) = cached {
            opts.statement = None;
            opts.prepared = Some(cached_statement);

            let res = self.executor.query(&opts).await;
            if let Ok(reader) = res {
                return Ok(reader);
            }
        };

        opts.statement = Some(format!("PREPARE {}", &statement));
        opts.auto_execute = Some(true);

        let res = self.executor.query(&opts).await?;

        if let Some(early_metadata) = res.early_metadata() {
            if let Some(prepared) = &early_metadata.prepared {
                let mut cache = self.cache.lock().unwrap();
                cache.put(&statement, prepared);
                drop(cache);
            }
        };

        Ok(res)
    }
}
