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

#![warn(missing_docs)]
//! Vector search types for similarity-based queries.
//!
//! Vector search allows you to find documents by the similarity of vector embeddings.
//! Use [`VectorQuery`] to define individual vector queries, and [`VectorSearch`] to
//! combine one or more of them into a request.
//!
//! # Example
//!
//! ```rust
//! use couchbase::search::vector::{VectorSearch, VectorQuery};
//!
//! let query = VectorQuery::with_vector("embedding_field", vec![0.1, 0.2, 0.3])
//!     .num_candidates(5);
//! let search = VectorSearch::new(vec![query], None);
//! ```

use crate::error;
use crate::search::queries::Query;
use couchbase_core::searchx::query_options::{KnnOperator, KnnQuery};

/// A vector search consisting of one or more [`VectorQuery`] instances.
///
/// Multiple vector queries can be combined with [`VectorQueryCombination::And`]
/// or [`VectorQueryCombination::Or`] logic.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VectorSearch {
    /// The individual vector queries to execute.
    pub vector_queries: Vec<VectorQuery>,
    /// How to combine multiple vector queries. Defaults to `Or`.
    pub query_combination: Option<VectorQueryCombination>,
}

impl VectorSearch {
    /// Creates a new `VectorSearch` from one or more vector queries.
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

/// Options for configuring a [`VectorSearch`].
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct VectorSearchOptions {
    /// How to combine multiple vector queries.
    pub query_combination: Option<VectorQueryCombination>,
}

impl VectorSearchOptions {
    /// Creates a new `VectorSearchOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets how to combine multiple vector queries.
    pub fn query_combination(mut self, query_combination: VectorQueryCombination) -> Self {
        self.query_combination = Some(query_combination);
        self
    }
}

/// Determines how multiple vector queries are combined in a [`VectorSearch`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum VectorQueryCombination {
    /// All vector queries must match (intersection).
    And,
    /// Any vector query may match (union, default).
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

/// A single vector similarity query against a specific field.
///
/// Create using [`with_vector`](VectorQuery::with_vector) for a float vector or
/// [`with_base64_vector`](VectorQuery::with_base64_vector) for a base64-encoded vector.
///
/// # Example
///
/// ```rust
/// use couchbase::search::vector::VectorQuery;
///
/// let query = VectorQuery::with_vector("embedding", vec![0.1, 0.2, 0.3])
///     .num_candidates(10)
///     .boost(1.5);
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VectorQuery {
    /// The name of the vector field to query.
    pub field_name: String,
    /// The number of nearest candidates to consider (default: 3).
    pub num_candidates: u32,
    /// The vector query as a list of floats (mutually exclusive with `base64_query`).
    pub query: Option<Vec<f32>>,
    /// The vector query as a base64-encoded string (mutually exclusive with `query`).
    pub base64_query: Option<String>,
    /// Boost factor for this query.
    pub boost: Option<f32>,
    /// An optional search query to use as a pre-filter before the vector search.
    pub prefilter: Option<Query>,
}

impl VectorQuery {
    /// Creates a vector query from a float vector.
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

    /// Creates a vector query from a base64-encoded vector string.
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

    /// Sets the boost factor for this query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the number of nearest candidates to consider.
    pub fn num_candidates(mut self, num_candidates: u32) -> Self {
        self.num_candidates = num_candidates;
        self
    }

    /// Sets an optional search query to use as a pre-filter.
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
