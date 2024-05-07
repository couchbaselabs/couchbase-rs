use super::*;
use crate::api::query_options::QueryOptions;
use crate::io::request::*;
use crate::{CouchbaseError, CouchbaseResult, ErrorContext};
use futures::channel::oneshot;
use futures::StreamExt;
use serde_derive::Deserialize;
use serde_json::Value;
use std::ops::Add;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

#[derive(Debug, Copy, Clone, Deserialize)]
pub enum QueryIndexType {
    #[serde(rename = "view")]
    VIEW,
    #[serde(rename = "gsi")]
    GSI,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryIndex {
    name: String,
    #[serde(default)]
    is_primary: bool,
    using: QueryIndexType,
    state: String,
    #[serde(rename = "keyspace_id")]
    keyspace: String,
    index_key: Vec<String>,
    condition: Option<String>,
    partition: Option<String>,
}

impl QueryIndex {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn is_primary(&self) -> bool {
        self.is_primary
    }
    pub fn using(&self) -> QueryIndexType {
        self.using
    }
    pub fn state(&self) -> &str {
        &self.state
    }
    pub fn keyspace(&self) -> &str {
        &self.name
    }
    pub fn index_key(&self) -> &Vec<String> {
        &self.index_key
    }
    pub fn condition(&self) -> Option<&String> {
        self.condition.as_ref()
    }
    pub fn partition(&self) -> Option<&String> {
        self.partition.as_ref()
    }
}

pub struct QueryIndexManager {
    core: Arc<Core>,
}

impl QueryIndexManager {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    pub async fn get_all_indexes(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<GetAllQueryIndexOptions>>,
    ) -> CouchbaseResult<impl IntoIterator<Item = QueryIndex>> {
        let opts = unwrap_or_default!(opts.into());
        let statement = format!("SELECT idx.* FROM system:indexes AS idx WHERE keyspace_id = \"{}\" AND `using`=\"gsi\" ORDER BY is_primary DESC, name ASC", bucket_name.into());

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement,
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));

        let mut result = receiver.await.unwrap()?;

        let mut indexes = vec![];
        let mut rows = result.rows::<QueryIndex>();
        while let Some(index) = rows.next().await {
            indexes.push(index?);
        }

        Ok(indexes)
    }

    pub async fn create_index(
        &self,
        bucket_name: impl Into<String>,
        index_name: impl Into<String>,
        fields: impl IntoIterator<Item = impl Into<String>>,
        opts: impl Into<Option<CreateQueryIndexOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let mut statement = format!(
            "CREATE INDEX  `{}` ON `{}` ({})",
            index_name.into(),
            bucket_name.into(),
            fields
                .into_iter()
                .map(|field| field.into())
                .collect::<Vec<String>>()
                .join(",")
        );

        let with = opts.with.to_string();
        if !with.is_empty() {
            statement = format!("{} WITH {}", statement, with);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement,
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        let result_err = receiver.await.unwrap().err();
        if let Some(e) = result_err {
            if opts.ignore_exists.unwrap_or(false) {
                match e {
                    CouchbaseError::IndexExists { ctx: _ } => Ok(()),
                    _ => Err(e),
                }
            } else {
                Err(e)
            }
        } else {
            Ok(())
        }?;

        Ok(())
    }

    pub async fn create_primary_index(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<CreatePrimaryQueryIndexOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let mut statement = match &opts.name {
            Some(n) => {
                format!("CREATE PRiMARY INDEX `{}` ON `{}`", n, bucket_name.into())
            }
            None => format!("CREATE PRIMARY INDEX ON `{}`", bucket_name.into()),
        };

        let with = opts.with.to_string();
        if !with.is_empty() {
            statement = format!("{} WITH {}", statement, with);
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement,
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        let result_err = receiver.await.unwrap().err();
        if let Some(e) = result_err {
            if opts.ignore_exists.unwrap_or(false) {
                match e {
                    CouchbaseError::IndexExists { ctx: _ctx } => Ok(()),
                    _ => Err(e),
                }
            } else {
                Err(e)
            }
        } else {
            Ok(())
        }?;

        Ok(())
    }

    pub async fn drop_index(
        &self,
        bucket_name: impl Into<String>,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropQueryIndexOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let statement = format!(
            "DROP INDEX  `{}` ON `{}`",
            index_name.into(),
            bucket_name.into()
        );

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement,
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        let result_err = receiver.await.unwrap().err();
        if let Some(e) = result_err {
            if opts.ignore_not_exists.unwrap_or(false) {
                match e {
                    CouchbaseError::IndexNotFound { ctx: _ctx } => Ok(()),
                    _ => Err(e),
                }
            } else {
                Err(e)
            }
        } else {
            Ok(())
        }?;

        Ok(())
    }

    pub async fn drop_primary_index(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<DropPrimaryQueryIndexOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let statement = match &opts.name {
            Some(n) => {
                format!("DROP INDEX `{}` ON `{}`", n, bucket_name.into())
            }
            None => format!("DROP PRIMARY INDEX ON `{}`", bucket_name.into()),
        };

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement,
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        let result_err = receiver.await.unwrap().err();
        if let Some(e) = result_err {
            if opts.ignore_not_exists.unwrap_or(false) {
                match e {
                    CouchbaseError::IndexNotFound { ctx: _ctx } => Ok(()),
                    _ => Err(e),
                }
            } else {
                Err(e)
            }
        } else {
            Ok(())
        }?;

        Ok(())
    }

    pub async fn watch_indexes(
        &self,
        bucket_name: impl Into<String>,
        index_names: impl IntoIterator<Item = impl Into<String>>,
        timeout: Duration,
        opts: impl Into<Option<WatchIndexesQueryIndexOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let bucket_name = bucket_name.into();
        let mut indexes: Vec<String> = index_names.into_iter().map(|index| index.into()).collect();
        if let Some(w) = opts.watch_primary {
            if w {
                indexes.push(String::from("#primary"));
            }
        }
        let interval = Duration::from_millis(50);
        let deadline = Instant::now().add(timeout);
        loop {
            if Instant::now() > deadline {
                return Err(CouchbaseError::Timeout {
                    ambiguous: false,
                    ctx: ErrorContext::default(),
                });
            }

            let all_indexes = self
                .get_all_indexes(
                    bucket_name.clone(),
                    GetAllQueryIndexOptions::default().timeout(deadline - Instant::now()),
                )
                .await?;

            let all_online = self.check_indexes_online(all_indexes, &indexes)?;
            if all_online {
                break;
            }

            let now = Instant::now();
            let mut sleep_deadline = now.add(interval);
            if sleep_deadline > deadline {
                sleep_deadline = deadline;
            }

            sleep(sleep_deadline - now);
        }

        Ok(())
    }

    pub async fn build_deferred_indexes(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<BuildDeferredQueryIndexOptions>>,
    ) -> CouchbaseResult<impl IntoIterator<Item = String>> {
        let opts = unwrap_or_default!(opts.into());
        let bucket_name = bucket_name.into();
        let indexes = self
            .get_all_indexes(bucket_name.clone(), GetAllQueryIndexOptions::from(&opts))
            .await?;

        let mut deferred_list = vec![];
        for index in indexes {
            if index.state == "deferred" || index.state == "pending" {
                deferred_list.push(index.name);
            }
        }

        if deferred_list.is_empty() {
            return Ok(deferred_list);
        }

        let escaped: Vec<String> = deferred_list
            .clone()
            .into_iter()
            .map(|i| format!("`{}`", i))
            .collect();

        let statement = format!("BUILD INDEX ON `{}` ({})", bucket_name, escaped.join(","));

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement,
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        receiver.await.unwrap()?;

        Ok(deferred_list)
    }

    fn check_indexes_online(
        &self,
        all_indexes: impl IntoIterator<Item = QueryIndex>,
        watch_indexes: &[String],
    ) -> CouchbaseResult<bool> {
        let mut checked_indexes = vec![];
        for index in all_indexes.into_iter() {
            for watch in watch_indexes {
                if index.name() == watch.clone() {
                    checked_indexes.push(index);
                    break;
                }
            }
        }

        if checked_indexes.len() != watch_indexes.len() {
            return Err(CouchbaseError::IndexNotFound {
                ctx: ErrorContext::default(),
            });
        }

        for index in checked_indexes {
            if index.state() != "online" {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[derive(Debug, Default)]
pub struct CreateQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_exists: Option<bool>,
    pub(crate) with: Value,
}

impl CreateQueryIndexOptions {
    timeout!();
    pub fn ignore_if_exists(mut self, ignore_exists: bool) -> Self {
        self.ignore_exists = Some(ignore_exists);
        self
    }
    pub fn num_replicas(mut self, num_replicas: i32) -> Self {
        self.with["num_replica"] = Value::from(num_replicas);
        self
    }
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.with["defer_build"] = Value::from(deferred);
        self
    }
}

#[derive(Debug, Default)]
pub struct GetAllQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllQueryIndexOptions {
    timeout!();
}

impl From<&BuildDeferredQueryIndexOptions> for GetAllQueryIndexOptions {
    fn from(opts: &BuildDeferredQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

#[derive(Debug, Default)]
pub struct CreatePrimaryQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_exists: Option<bool>,
    pub(crate) name: Option<String>,
    pub(crate) with: Value,
}

impl CreatePrimaryQueryIndexOptions {
    timeout!();
    pub fn ignore_if_exists(mut self, ignore_exists: bool) -> Self {
        self.ignore_exists = Some(ignore_exists);
        self
    }
    pub fn num_replicas(mut self, num_replicas: i32) -> Self {
        self.with["num_replica"] = Value::from(num_replicas);
        self
    }
    pub fn index_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.with["defer_build"] = Value::from(deferred);
        self
    }
}

#[derive(Debug, Default)]
pub struct DropQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_not_exists: Option<bool>,
}

impl DropQueryIndexOptions {
    timeout!();
    pub fn ignore_if_not_exists(mut self, ignore_not_exists: bool) -> Self {
        self.ignore_not_exists = Some(ignore_not_exists);
        self
    }
}

#[derive(Debug, Default)]
pub struct DropPrimaryQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_not_exists: Option<bool>,
    pub(crate) name: Option<String>,
}

impl DropPrimaryQueryIndexOptions {
    timeout!();
    pub fn ignore_if_not_exists(mut self, ignore_not_exists: bool) -> Self {
        self.ignore_not_exists = Some(ignore_not_exists);
        self
    }
    pub fn index_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct BuildDeferredQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
}

impl BuildDeferredQueryIndexOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct WatchIndexesQueryIndexOptions {
    pub(crate) watch_primary: Option<bool>,
}

impl WatchIndexesQueryIndexOptions {
    pub fn watch_primary(mut self, watch: bool) -> Self {
        self.watch_primary = Some(watch);
        self
    }
}

impl From<&GetAllQueryIndexOptions> for QueryOptions {
    fn from(opts: &GetAllQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&CreateQueryIndexOptions> for QueryOptions {
    fn from(opts: &CreateQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&CreatePrimaryQueryIndexOptions> for QueryOptions {
    fn from(opts: &CreatePrimaryQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&DropQueryIndexOptions> for QueryOptions {
    fn from(opts: &DropQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&DropPrimaryQueryIndexOptions> for QueryOptions {
    fn from(opts: &DropPrimaryQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&BuildDeferredQueryIndexOptions> for QueryOptions {
    fn from(opts: &BuildDeferredQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}
