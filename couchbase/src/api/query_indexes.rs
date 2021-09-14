use crate::io::request::*;
use crate::io::Core;
use crate::{
    BuildDeferredQueryIndexOptions, CouchbaseError, CouchbaseResult,
    CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions, DropPrimaryQueryIndexOptions,
    DropQueryIndexOptions, ErrorContext, GetAllQueryIndexOptions, QueryOptions,
    WatchIndexesQueryIndexOptions,
};
use futures::channel::oneshot;
use futures::StreamExt;
use serde_derive::Deserialize;
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
    pub fn name(&self) -> String {
        self.name.to_string()
    }
    pub fn is_primary(&self) -> bool {
        self.is_primary
    }
    pub fn using(&self) -> QueryIndexType {
        self.using
    }
    pub fn state(&self) -> String {
        self.state.to_string()
    }
    pub fn keyspace(&self) -> String {
        self.name.to_string()
    }
    pub fn index_key(&self) -> &Vec<String> {
        &self.index_key
    }
    pub fn condition(&self) -> Option<String> {
        self.condition.clone()
    }
    pub fn partition(&self) -> Option<String> {
        self.partition.clone()
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
        opts: GetAllQueryIndexOptions,
    ) -> CouchbaseResult<impl IntoIterator<Item = QueryIndex>> {
        let statement = format!("SELECT idx.* FROM system:indexes AS idx WHERE keyspace_id = \"{}\" AND `using`=\"gsi\" ORDER BY is_primary DESC, name ASC", bucket_name.into());

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement: statement.into(),
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
        opts: CreateQueryIndexOptions,
    ) -> CouchbaseResult<()> {
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
            statement: statement.into(),
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        let result_err = receiver.await.unwrap().err();
        if let Some(e) = result_err {
            if opts.ignore_exists.unwrap_or_else(|| false) {
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
        opts: CreatePrimaryQueryIndexOptions,
    ) -> CouchbaseResult<()> {
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
            statement: statement.into(),
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        let result_err = receiver.await.unwrap().err();
        if let Some(e) = result_err {
            if opts.ignore_exists.unwrap_or_else(|| false) {
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
        opts: DropQueryIndexOptions,
    ) -> CouchbaseResult<()> {
        let statement = format!(
            "DROP INDEX  `{}` ON `{}`",
            index_name.into(),
            bucket_name.into()
        );

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement: statement.into(),
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        let result_err = receiver.await.unwrap().err();
        if let Some(e) = result_err {
            if opts.ignore_not_exists.unwrap_or_else(|| false) {
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
        opts: DropPrimaryQueryIndexOptions,
    ) -> CouchbaseResult<()> {
        let statement = match &opts.name {
            Some(n) => {
                format!("DROP INDEX `{}` ON `{}`", n, bucket_name.into())
            }
            None => format!("DROP PRIMARY INDEX ON `{}`", bucket_name.into()),
        };

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement: statement.into(),
            options: QueryOptions::from(&opts),
            sender,
            scope: None,
        }));
        let result_err = receiver.await.unwrap().err();
        if let Some(e) = result_err {
            if opts.ignore_not_exists.unwrap_or_else(|| false) {
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
        opts: WatchIndexesQueryIndexOptions,
    ) -> CouchbaseResult<()> {
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
        opts: BuildDeferredQueryIndexOptions,
    ) -> CouchbaseResult<impl IntoIterator<Item = String>> {
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
            statement: statement.into(),
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
        watch_indexes: &Vec<String>,
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
            if index.state() != String::from("online") {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
