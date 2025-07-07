use crate::httpx::client::Client;
use crate::httpx::request::{Auth, BasicAuth, OnBehalfOfInfo, Request};
use crate::httpx::response::Response;
use crate::queryx::error;
use crate::queryx::error::{Error, ErrorKind, ServerError, ServerErrorKind};
use crate::queryx::index::{Index, IndexState};
use crate::queryx::query_options::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, GetAllIndexesOptions, PingOptions, QueryOptions, WatchIndexesOptions,
};
use crate::queryx::query_respreader::QueryRespReader;
use crate::retry::RetryStrategy;
use bytes::Bytes;
use futures::StreamExt;
use http::{Method, StatusCode};
use log::debug;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::format;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug)]
pub struct Query<C: Client> {
    pub http_client: Arc<C>,
    pub user_agent: String,
    pub endpoint: String,
    pub username: String,
    pub password: String,
}

impl<C: Client> Query<C> {
    pub fn new_request(
        &self,
        method: Method,
        path: impl Into<String>,
        content_type: impl Into<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        body: Option<Bytes>,
    ) -> Request {
        let auth = if let Some(obo) = on_behalf_of {
            Auth::OnBehalfOf(OnBehalfOfInfo {
                username: obo.username,
                password_or_domain: obo.password_or_domain,
            })
        } else {
            Auth::BasicAuth(BasicAuth {
                username: self.username.clone(),
                password: self.password.clone(),
            })
        };

        Request::new(method, format!("{}/{}", self.endpoint, path.into()))
            .auth(auth)
            .user_agent(self.user_agent.clone())
            .content_type(content_type.into())
            .body(body)
    }

    pub async fn execute(
        &self,
        method: Method,
        path: impl Into<String>,
        content_type: impl Into<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        body: Option<Bytes>,
    ) -> crate::httpx::error::Result<Response> {
        let req = self.new_request(method, path, content_type, on_behalf_of, body);

        self.http_client.execute(req).await
    }

    pub async fn query(&self, opts: &QueryOptions) -> error::Result<QueryRespReader> {
        let statement = if let Some(statement) = &opts.statement {
            statement.clone()
        } else {
            String::new()
        };

        //TODO; this needs re-embedding into options
        let client_context_id = if let Some(id) = &opts.client_context_id {
            id.clone()
        } else {
            Uuid::new_v4().to_string()
        };

        let on_behalf_of = opts.on_behalf_of.clone();

        let mut serialized = serde_json::to_value(opts)
            .map_err(|e| Error::new_encoding_error(format!("failed to encode options: {}", e)))?;

        let mut obj = serialized.as_object_mut().unwrap();
        let mut client_context_id_entry = obj.get("client_context_id");
        if client_context_id_entry.is_none() {
            obj.insert(
                "client_context_id".to_string(),
                Value::String(client_context_id.clone()),
            );
        }

        if let Some(named_args) = &opts.named_args {
            for (k, v) in named_args.iter() {
                let key = if k.starts_with("$") {
                    k.clone()
                } else {
                    format!("${}", k)
                };
                obj.insert(key, v.clone());
            }
        }

        if let Some(raw) = &opts.raw {
            for (k, v) in raw.iter() {
                obj.insert(k.to_string(), v.clone());
            }
        }

        let body =
            Bytes::from(serde_json::to_vec(&serialized).map_err(|e| {
                Error::new_encoding_error(format!("failed to encode options: {}", e))
            })?);

        let res = match self
            .execute(
                Method::POST,
                "query/service",
                "application/json",
                on_behalf_of,
                Some(body),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(
                    Error::new_http_error(&self.endpoint, statement, client_context_id)
                        .with(Arc::new(e)),
                );
            }
        };

        QueryRespReader::new(res, &self.endpoint, statement, client_context_id).await
    }

    pub async fn get_all_indexes(
        &self,
        opts: &GetAllIndexesOptions<'_>,
    ) -> error::Result<Vec<Index>> {
        let mut where_clause = match (&opts.collection_name, &opts.scope_name) {
            (None, None) => {
                if !opts.bucket_name.is_empty() {
                    let encoded_bucket = encode_value(&opts.bucket_name)?;
                    format!(
                        "(keyspace_id={} AND bucket_id IS MISSING) OR bucket_id={}",
                        encoded_bucket, encoded_bucket
                    )
                } else {
                    "1=1".to_string()
                }
            }
            (Some(collection_name), Some(scope_name)) => {
                let scope_name = normalise_default_name(scope_name);
                let collection_name = normalise_default_name(collection_name);

                let encoded_bucket = encode_value(&opts.bucket_name)?;
                let encoded_scope = encode_value(&scope_name)?;
                let encoded_collection = encode_value(&collection_name)?;

                let temp_where = format!(
                    "bucket_id={} AND scope_id={} AND keyspace_id={}",
                    encoded_bucket, encoded_scope, encoded_collection
                );

                if scope_name == "_default" && collection_name == "_default" {
                    format!(
                        "({}) OR (keyspace_id={} AND bucket_id IS MISSING)",
                        temp_where, encoded_bucket
                    )
                } else {
                    temp_where
                }
            }
            (None, Some(scope_name)) => {
                let scope_name = normalise_default_name(scope_name);

                let encoded_bucket = encode_value(&opts.bucket_name)?;
                let encoded_scope = encode_value(&scope_name)?;

                format!(
                    "bucket_id={} AND scope_id={}",
                    encoded_bucket, encoded_scope
                )
            }
            _ => {
                return Err(Error::new_invalid_argument_error(
                    "invalid combination of bucket, scope and collection".to_string(),
                    None,
                ));
            }
        };

        where_clause = format!("({}) AND `using`=\"gsi\"", where_clause);
        let qs = format!(
            "SELECT `idx`.* FROM system:indexes AS idx WHERE {} ORDER BY is_primary DESC, name ASC",
            where_clause
        );

        let opts = QueryOptions::new()
            .statement(qs)
            .on_behalf_of(opts.on_behalf_of.cloned());
        let mut res = self.query(&opts).await?;

        let mut indexes = vec![];

        while let Some(row) = res.next().await {
            let bytes = row?;
            let index: Index = serde_json::from_slice(&bytes).map_err(|e| {
                Error::new_message_error(
                    format!("failed to parse index from response: {}", e),
                    None,
                    None,
                    None,
                )
            })?;

            indexes.push(index);
        }

        Ok(indexes)
    }

    pub async fn create_primary_index(
        &self,
        opts: &CreatePrimaryIndexOptions<'_>,
    ) -> error::Result<()> {
        // TODO (MW) - Maybe add IF NOT EXISTS & amend error handling if we don't need backwards compat with <=7.0
        let mut qs = String::from("CREATE PRIMARY INDEX");
        if let Some(index_name) = &opts.index_name {
            qs.push_str(&format!(" {}", encode_identifier(index_name)));
        }

        qs.push_str(&format!(
            " ON {}",
            build_keyspace(opts.bucket_name, &opts.scope_name, &opts.collection_name)
        ));

        let mut with: HashMap<&str, Value> = HashMap::new();

        if let Some(deferred) = opts.deferred {
            with.insert("defer_build", Value::Bool(deferred));
        }

        if let Some(num_replicas) = opts.num_replicas {
            with.insert("num_replica", Value::Number(num_replicas.into()));
        }

        if !with.is_empty() {
            let with = encode_value(&with)?;
            qs.push_str(&format!(" WITH {}", with));
        }

        let query_opts = QueryOptions::new()
            .statement(qs)
            .on_behalf_of(opts.on_behalf_of.cloned());

        let mut res = self.query(&query_opts).await;

        match res {
            Err(e) => {
                if e.is_index_exists() {
                    if opts.ignore_if_exists.unwrap_or(false) {
                        Ok(())
                    } else {
                        Err(e)
                    }
                } else if e.is_build_already_in_progress() {
                    Ok(())
                } else {
                    Err(e)
                }
            }
            Ok(_) => Ok(()),
        }
    }

    pub async fn create_index(&self, opts: &CreateIndexOptions<'_>) -> error::Result<()> {
        let mut qs = String::from("CREATE INDEX");
        qs.push_str(&format!(" {}", encode_identifier(opts.index_name)));
        qs.push_str(&format!(
            " ON {}",
            build_keyspace(opts.bucket_name, &opts.scope_name, &opts.collection_name)
        ));

        let mut encoded_fields: Vec<String> = Vec::with_capacity(opts.fields.len());
        for field in opts.fields {
            encoded_fields.push(encode_identifier(field));
        }
        qs.push_str(&format!(" ( {})", encoded_fields.join(",")));

        let mut with: HashMap<&str, Value> = HashMap::new();

        if let Some(deferred) = opts.deferred {
            with.insert("defer_build", Value::Bool(deferred));
        }

        if let Some(num_replicas) = opts.num_replicas {
            with.insert("num_replica", Value::Number(num_replicas.into()));
        }

        if !with.is_empty() {
            let with = encode_value(&with)?;
            qs.push_str(&format!(" WITH {}", with));
        }

        let query_opts = QueryOptions::new()
            .statement(qs)
            .on_behalf_of(opts.on_behalf_of.cloned());

        let mut res = self.query(&query_opts).await;

        match res {
            Err(e) => {
                if e.is_index_exists() {
                    if opts.ignore_if_exists.unwrap_or(false) {
                        Ok(())
                    } else {
                        Err(e)
                    }
                } else if e.is_build_already_in_progress() {
                    Ok(())
                } else {
                    Err(e)
                }
            }
            Ok(_) => Ok(()),
        }
    }

    pub async fn drop_primary_index(
        &self,
        opts: &DropPrimaryIndexOptions<'_>,
    ) -> error::Result<()> {
        // TODO (MW) - Maybe add IF EXISTS & amend error handling if we don't need backwards compat with <=7.0
        let keyspace = build_keyspace(opts.bucket_name, &opts.scope_name, &opts.collection_name);

        let mut qs = String::new();
        if let Some(index_name) = &opts.index_name {
            let encoded_name = encode_identifier(index_name);

            if opts.scope_name.is_some() || opts.collection_name.is_some() {
                qs.push_str(&format!("DROP INDEX {}", encoded_name));
                qs.push_str(&format!(" ON {}", keyspace));
            } else {
                qs.push_str(&format!("DROP INDEX {}.{}", keyspace, encoded_name));
            }
        } else {
            qs.push_str(&format!("DROP PRIMARY INDEX ON {}", keyspace));
        }

        let query_opts = QueryOptions::new()
            .statement(qs)
            .on_behalf_of(opts.on_behalf_of.cloned());

        let mut res = self.query(&query_opts).await;

        match res {
            Err(e) => {
                if e.is_index_not_found() {
                    if opts.ignore_if_not_exists.unwrap_or(false) {
                        Ok(())
                    } else {
                        Err(e)
                    }
                } else {
                    Err(e)
                }
            }
            Ok(_) => Ok(()),
        }
    }

    pub async fn drop_index(&self, opts: &DropIndexOptions<'_>) -> error::Result<()> {
        let encoded_name = encode_identifier(opts.index_name);
        let keyspace = build_keyspace(opts.bucket_name, &opts.scope_name, &opts.collection_name);

        let mut qs = String::new();
        if opts.scope_name.is_some() || opts.collection_name.is_some() {
            qs.push_str(&format!("DROP INDEX {}", encoded_name));
            qs.push_str(&format!(" ON {}", keyspace));
        } else {
            qs.push_str(&format!("DROP INDEX {}.{}", keyspace, encoded_name));
        }

        let query_opts = QueryOptions::new()
            .statement(qs)
            .on_behalf_of(opts.on_behalf_of.cloned());

        let res = self.query(&query_opts).await;

        match res {
            Err(e) => {
                if e.is_index_not_found() {
                    if opts.ignore_if_not_exists.unwrap_or(false) {
                        Ok(())
                    } else {
                        Err(e)
                    }
                } else {
                    Err(e)
                }
            }
            Ok(_) => Ok(()),
        }
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: &BuildDeferredIndexesOptions<'_>,
    ) -> error::Result<()> {
        let opts = GetAllIndexesOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            on_behalf_of: opts.on_behalf_of,
        };

        let indexes = self.get_all_indexes(&opts).await?;

        let deferred_items: Vec<_> = indexes
            .iter()
            .filter(|index| index.state == IndexState::Deferred)
            .map(|index| {
                let (bucket, scope, collection) = index_to_namespace_parts(index);
                let deferred_index = DeferredIndexName {
                    bucket_name: bucket,
                    scope_name: scope,
                    collection_name: collection,
                    index_name: &index.name,
                };
                let keyspace = build_keyspace(bucket, &Some(scope), &Some(collection));
                (keyspace, deferred_index)
            })
            .collect();

        let mut deferred_indexes: HashMap<String, Vec<DeferredIndexName>> =
            HashMap::with_capacity(deferred_items.len());

        for (keyspace, deferred_index) in deferred_items {
            deferred_indexes
                .entry(keyspace)
                .or_default()
                .push(deferred_index);
        }

        if deferred_indexes.is_empty() {
            return Ok(());
        }

        for (keyspace, indexes) in deferred_indexes {
            let mut escaped_index_names: Vec<String> = Vec::with_capacity(indexes.len());
            for index in indexes {
                escaped_index_names.push(encode_identifier(index.index_name));
            }

            let qs = format!(
                "BUILD INDEX ON {}({})",
                keyspace,
                escaped_index_names.join(",")
            );
            let query_opts = QueryOptions::new()
                .statement(qs)
                .on_behalf_of(opts.on_behalf_of.cloned());

            let res = self.query(&query_opts).await;

            match res {
                Err(e) => {
                    if e.is_build_already_in_progress() {
                        continue;
                    }

                    return Err(e);
                }
                Ok(_) => continue,
            }
        }

        Ok(())
    }

    pub async fn watch_indexes(&self, opts: &WatchIndexesOptions<'_>) -> error::Result<()> {
        let mut cur_interval = Duration::from_millis(50);
        let mut watch_list = opts.indexes.to_vec();

        if opts.watch_primary.unwrap_or(false) {
            watch_list.push("#primary");
        }

        loop {
            let indexes = self
                .get_all_indexes(&GetAllIndexesOptions {
                    bucket_name: opts.bucket_name,
                    scope_name: opts.scope_name,
                    collection_name: opts.collection_name,
                    on_behalf_of: opts.on_behalf_of,
                })
                .await?;

            let all_online = check_indexes_active(&indexes, &watch_list)?;

            if all_online {
                debug!("All watched indexes are ready");
                return Ok(());
            }

            cur_interval = std::cmp::min(
                cur_interval + Duration::from_millis(500),
                Duration::from_secs(1),
            );

            sleep(cur_interval).await;
        }
    }

    pub async fn ping(&self, opts: &PingOptions<'_>) -> error::Result<()> {
        let res = match self
            .execute(
                Method::GET,
                "admin/ping",
                "",
                opts.on_behalf_of.cloned(),
                None,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(Error::new_http_error(&self.endpoint, None, None).with(Arc::new(e)));
            }
        };

        if res.status().is_success() {
            return Ok(());
        }

        Err(Error::new_message_error(
            format!("ping failed with status code: {}", res.status()),
            Some(self.endpoint.clone()),
            None,
            None,
        ))
    }
}

struct DeferredIndexName<'a> {
    bucket_name: &'a str,
    scope_name: &'a str,
    collection_name: &'a str,
    index_name: &'a str,
}

pub fn normalise_default_name(name: &str) -> String {
    if name.is_empty() {
        "_default".to_string()
    } else {
        name.to_string()
    }
}

pub fn build_keyspace(
    bucket_name: &str,
    scope_name: &Option<&str>,
    collection_name: &Option<&str>,
) -> String {
    match (scope_name, collection_name) {
        (Some(scope), Some(collection)) => format!(
            "{}.{}.{}",
            encode_identifier(bucket_name),
            encode_identifier(scope),
            encode_identifier(collection)
        ),
        (Some(scope), None) => format!(
            "{}.{}._default",
            encode_identifier(bucket_name),
            encode_identifier(scope)
        ),
        (None, Some(collection)) => format!(
            "{}._default.{}",
            encode_identifier(bucket_name),
            encode_identifier(collection)
        ),
        (None, None) => encode_identifier(bucket_name),
    }
}

fn index_to_namespace_parts(index: &Index) -> (&str, &str, &str) {
    if index.bucket_id.is_none() {
        let default_scope_coll = "_default";
        (
            index.keyspace_id.as_deref().unwrap_or(""),
            default_scope_coll,
            default_scope_coll,
        )
    } else {
        (
            index.bucket_id.as_deref().unwrap_or(""),
            index.scope_id.as_deref().unwrap_or(""),
            index.keyspace_id.as_deref().unwrap_or(""),
        )
    }
}

fn check_indexes_active(indexes: &[Index], check_list: &Vec<&str>) -> error::Result<bool> {
    let mut check_indexes = Vec::new();

    for index_name in check_list {
        if let Some(index) = indexes.iter().find(|idx| idx.name == *index_name) {
            check_indexes.push(index);
        } else {
            return Ok(false);
        }
    }

    for index in check_indexes {
        if index.state != IndexState::Online {
            debug!(
                "Index {} is not ready yet, current state is {}",
                index.name, index.state
            );
            return Ok(false);
        }
    }

    Ok(true)
}

fn encode_identifier(identifier: &str) -> String {
    let mut out = identifier.replace("\\", "\\\\");
    out = out.replace("`", "\\`");
    format!("`{}`", out)
}

fn encode_value<T: serde::Serialize>(value: &T) -> error::Result<String> {
    let bytes = serde_json::to_string(value).map_err(|e| {
        Error::new_message_error(format!("failed to encode value: {}", e), None, None, None)
    })?;
    Ok(bytes)
}
