use crate::error;
use couchbase_core::searchx::query_options::{KnnOperator, KnnQuery};

#[derive(Debug)]
pub struct VectorSearch {
    pub(crate) vector_queries: Vec<VectorQuery>,
    pub(crate) query_combination: Option<VectorQueryCombination>,
}

impl VectorSearch {
    pub fn new(
        vector_queries: Vec<VectorQuery>,
        opts: impl Into<Option<VectorSearchOptions>>,
    ) -> Self {
        let opts = opts.into();
        let query_combination = if let Some(opts) = opts {
            opts.query_combination
        } else {
            None
        };

        Self {
            vector_queries,
            query_combination,
        }
    }
}

#[derive(Debug, Default)]
pub struct VectorSearchOptions {
    pub(crate) query_combination: Option<VectorQueryCombination>,
}

impl VectorSearchOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn query_combination(mut self, query_combination: VectorQueryCombination) -> Self {
        self.query_combination = Some(query_combination);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum VectorQueryCombination {
    And,
    Or,
}

impl From<VectorQueryCombination> for KnnOperator {
    fn from(value: VectorQueryCombination) -> Self {
        match value {
            VectorQueryCombination::And => KnnOperator::And,
            VectorQueryCombination::Or => KnnOperator::Or,
        }
    }
}

#[derive(Debug)]
pub struct VectorQuery {
    pub(crate) field_name: String,
    pub(crate) query: Option<Vec<f32>>,
    pub(crate) base64_query: Option<String>,
    pub(crate) boost: Option<f32>,
    pub(crate) num_candidates: Option<u32>,
}

impl VectorQuery {
    pub fn with_vector(vector_field_name: impl Into<String>, vector_query: Vec<f32>) -> Self {
        Self {
            field_name: vector_field_name.into(),
            query: Some(vector_query),
            base64_query: None,
            boost: None,
            num_candidates: None,
        }
    }

    pub fn with_base64_vector(
        vector_field_name: impl Into<String>,
        base_64_vector_query: impl Into<String>,
    ) -> Self {
        Self {
            field_name: vector_field_name.into(),
            query: None,
            base64_query: Some(base_64_vector_query.into()),
            boost: None,
            num_candidates: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn num_candidates(mut self, num_candidates: u32) -> Self {
        self.num_candidates = Some(num_candidates);
        self
    }
}

impl TryFrom<VectorQuery> for KnnQuery {
    type Error = error::Error;

    fn try_from(value: VectorQuery) -> error::Result<KnnQuery> {
        if value.query.is_none() && value.base64_query.is_none() {
            return Err(error::Error::other_failure(
                "vector search query or base64_query must be set",
            ));
        }

        if value.query.is_some() && value.base64_query.is_some() {
            return Err(error::Error::other_failure(
                "only one of vector search query or base64_query may be set",
            ));
        }

        Ok(KnnQuery::new(value.field_name)
            .boost(value.boost)
            .k(value.num_candidates.map(|n| n as i64))
            .vector(value.query)
            .vector_base64(value.base64_query))
    }
}
