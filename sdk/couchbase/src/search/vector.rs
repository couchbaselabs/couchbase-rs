use crate::error;
use couchbase_core::searchx::query_options::{KnnOperator, KnnQuery};
use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
pub struct VectorSearch {
    #[builder(mutators(
        fn add_vector_query(&mut self, query: VectorQuery) {
            self.vector_queries.push(query);
        }
    ))]
    pub vector_queries: Vec<VectorQuery>,
    #[builder(default)]
    pub options: Option<VectorSearchOptions>,
}

#[derive(Debug, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
pub struct VectorSearchOptions {
    #[builder(default)]
    pub query_combination: Option<VectorQueryCombination>,
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

#[derive(Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct VectorQuery {
    #[builder(!default)]
    pub field_name: String,
    pub query: Option<Vec<f32>>,
    pub base64_query: Option<String>,
    pub boost: Option<f32>,
    pub num_candidates: Option<u32>,
}

impl TryFrom<VectorQuery> for KnnQuery {
    type Error = error::Error;

    fn try_from(value: VectorQuery) -> error::Result<KnnQuery> {
        if value.query.is_none() && value.base64_query.is_none() {
            return Err(error::Error {
                msg: "vector search query or base64_query must be set".to_string(),
            });
        }

        if value.query.is_some() && value.base64_query.is_some() {
            return Err(error::Error {
                msg: "only one of vector search query or base64_query must be set".to_string(),
            });
        }

        Ok(KnnQuery::builder()
            .boost(value.boost)
            .field(value.field_name)
            .k(value.num_candidates.map(|n| n as i64))
            .vector(value.query)
            .vector_base64(value.base64_query)
            .build())
    }
}
