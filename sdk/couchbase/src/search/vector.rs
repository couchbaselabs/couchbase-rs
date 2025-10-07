/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::error;
use crate::search::queries::Query;
use couchbase_core::searchx::query_options::{KnnOperator, KnnQuery};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VectorSearch {
    pub vector_queries: Vec<VectorQuery>,
    pub query_combination: Option<VectorQueryCombination>,
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
#[non_exhaustive]
pub struct VectorSearchOptions {
    pub query_combination: Option<VectorQueryCombination>,
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
#[non_exhaustive]
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

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VectorQuery {
    pub field_name: String,
    pub num_candidates: u32,
    pub query: Option<Vec<f32>>,
    pub base64_query: Option<String>,
    pub boost: Option<f32>,
    pub prefilter: Option<Query>,
}

impl VectorQuery {
    pub fn with_vector(vector_field_name: impl Into<String>, vector_query: Vec<f32>) -> Self {
        Self {
            field_name: vector_field_name.into(),
            query: Some(vector_query),
            base64_query: None,
            boost: None,
            num_candidates: 3,
            prefilter: None,
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
            num_candidates: 3,
            prefilter: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn num_candidates(mut self, num_candidates: u32) -> Self {
        self.num_candidates = num_candidates;
        self
    }

    pub fn prefilter(mut self, prefilter: Query) -> Self {
        self.prefilter = Some(prefilter);
        self
    }
}

impl TryFrom<VectorQuery> for KnnQuery {
    type Error = error::Error;

    fn try_from(value: VectorQuery) -> error::Result<KnnQuery> {
        if value.query.is_none() && value.base64_query.is_none() {
            return Err(error::Error::invalid_argument(
                "query, base64_query",
                "one of vector search query or base64_query must be set",
            ));
        }

        if value.query.is_some() && value.base64_query.is_some() {
            return Err(error::Error::invalid_argument(
                "query, base64_query",
                "only one of vector search query or base64_query may be set",
            ));
        }

        if let Some(q) = &value.query {
            if q.is_empty() {
                return Err(error::Error::invalid_argument(
                    "query",
                    "vector search query must be non-empty",
                ));
            }
        }

        if let Some(ref q) = value.base64_query {
            if q.is_empty() {
                return Err(error::Error::invalid_argument(
                    "base64_query",
                    "base64_query must be a non-empty string",
                ));
            }
        }

        if value.num_candidates == 0 {
            return Err(error::Error::invalid_argument(
                "num_candidates",
                "if set num_candidates must be greater than zero",
            ));
        }

        Ok(KnnQuery::new(value.field_name, value.num_candidates)
            .boost(value.boost)
            .vector(value.query)
            .vector_base64(value.base64_query)
            .filter(value.prefilter.map(|q| q.into())))
    }
}
