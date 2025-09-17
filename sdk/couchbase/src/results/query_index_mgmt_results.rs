use couchbase_core::queryx;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QueryIndex {
    pub(crate) name: String,
    pub(crate) is_primary: bool,
    pub(crate) index_type: QueryIndexType,
    pub(crate) state: String,
    pub(crate) keyspace: String,
    pub(crate) index_key: Vec<String>,
    pub(crate) condition: Option<String>,
    pub(crate) partition: Option<String>,
    pub(crate) bucket_name: String,
    pub(crate) scope_name: Option<String>,
    pub(crate) collection_name: Option<String>,
}

impl QueryIndex {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub fn index_type(&self) -> &QueryIndexType {
        &self.index_type
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    pub fn index_key(&self) -> &[String] {
        &self.index_key
    }

    pub fn condition(&self) -> Option<&String> {
        self.condition.as_ref()
    }

    pub fn partition(&self) -> Option<&String> {
        self.partition.as_ref()
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn scope_name(&self) -> Option<&String> {
        self.scope_name.as_ref()
    }

    pub fn collection_name(&self) -> Option<&String> {
        self.collection_name.as_ref()
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum QueryIndexType {
    Unknown,
    View,
    Gsi,
}

impl From<&str> for QueryIndexType {
    fn from(s: &str) -> Self {
        match s {
            "view" => QueryIndexType::View,
            "gsi" => QueryIndexType::Gsi,
            _ => QueryIndexType::Unknown,
        }
    }
}

impl From<queryx::index::Index> for QueryIndex {
    fn from(index: queryx::index::Index) -> Self {
        let (bucket_name, scope_name, collection_name, keyspace) =
            if let Some(bucket_id) = index.bucket_id {
                // Collections are in use so keyspace is the collection name
                (
                    bucket_id,
                    index.scope_id,
                    index.keyspace_id.clone(),
                    index.keyspace_id.unwrap_or_default(),
                )
            } else {
                // Collections are not in use so keyspace is the bucket name
                let keyspace = index.keyspace_id.clone().unwrap_or_default();
                (keyspace.clone(), None, None, keyspace)
            };

        QueryIndex {
            name: index.name,
            is_primary: index.is_primary.unwrap_or(false),
            index_type: QueryIndexType::from(index.using.as_str()),
            state: index.state,
            keyspace,
            index_key: index.index_key.unwrap_or_default(),
            condition: index.condition,
            partition: index.partition,
            bucket_name,
            scope_name,
            collection_name,
        }
    }
}
