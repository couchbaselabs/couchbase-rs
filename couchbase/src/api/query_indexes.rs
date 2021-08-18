use crate::io::request::*;
use crate::io::Core;
use crate::{
    BuildDeferredQueryIndexOptions, CouchbaseError, CouchbaseResult,
    CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions, DropPrimaryQueryIndexOptions,
    DropQueryIndexOptions, GetAllQueryIndexOptions, QueryOptions,
};
use futures::channel::oneshot;
use futures::StreamExt;
use serde_derive::Deserialize;
use std::sync::Arc;

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
    ) -> CouchbaseResult<Vec<QueryIndex>> {
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
        fields: Vec<String>,
        opts: CreateQueryIndexOptions,
    ) -> CouchbaseResult<()> {
        let mut statement = format!(
            "CREATE INDEX  `{}` ON `{}` ({})",
            index_name.into(),
            bucket_name.into(),
            fields.join(",")
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
                    CouchbaseError::IndexExists { ctx } => Ok(()),
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
                    CouchbaseError::IndexNotFound { ctx } => Ok(()),
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
                    CouchbaseError::IndexNotFound { ctx } => Ok(()),
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

    pub async fn build_deferred_indexes(
        &self,
        bucket_name: impl Into<String>,
        opts: BuildDeferredQueryIndexOptions,
    ) -> CouchbaseResult<Vec<String>> {
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
}
