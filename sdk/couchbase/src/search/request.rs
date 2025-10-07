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
