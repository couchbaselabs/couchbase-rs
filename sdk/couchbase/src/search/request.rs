use crate::error;
use crate::search::queries::Query;
use crate::search::vector::VectorSearch;

#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub(crate) search_query: Option<Query>,
    pub(crate) vector_search: Option<VectorSearch>,
}

impl SearchRequest {
    pub fn with_search_query(search_query: Query) -> Self {
        Self {
            search_query: Some(search_query),
            vector_search: None,
        }
    }

    pub fn with_vector_search(vector_search: VectorSearch) -> Self {
        Self {
            search_query: None,
            vector_search: Some(vector_search),
        }
    }

    pub fn vector_search(mut self, vector_search: VectorSearch) -> error::Result<Self> {
        if self.vector_search.is_some() {
            return Err(error::Error::invalid_argument(
                "vector_search",
                "vector search already set",
            ));
        }
        self.vector_search = Some(vector_search);
        Ok(self)
    }

    pub fn search_query(mut self, search_query: Query) -> error::Result<Self> {
        if self.search_query.is_some() {
            return Err(error::Error::invalid_argument(
                "search_query",
                "search query already set",
            ));
        }
        self.search_query = Some(search_query);
        Ok(self)
    }
}
