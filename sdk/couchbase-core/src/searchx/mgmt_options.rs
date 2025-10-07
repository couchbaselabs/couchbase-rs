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

use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::node_target::NodeTarget;
use crate::searchx::ensure_index_helper::DesiredState;
use crate::searchx::index::Index;
use std::sync::Arc;

#[derive(Debug)]
#[non_exhaustive]
pub struct UpsertIndexOptions<'a> {
    pub index: &'a Index,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> UpsertIndexOptions<'a> {
    pub fn new(index: &'a Index) -> Self {
        Self {
            index,
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct DeleteIndexOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> DeleteIndexOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct GetIndexOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> GetIndexOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Default, Debug)]
#[non_exhaustive]
pub struct GetAllIndexesOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> GetAllIndexesOptions<'a> {
    pub fn new() -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct AnalyzeDocumentOptions<'a> {
    pub index_name: &'a str,
    pub doc_content: &'a [u8],
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> AnalyzeDocumentOptions<'a> {
    pub fn new(index_name: &'a str, doc_content: &'a [u8]) -> Self {
        Self {
            index_name,
            doc_content,
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct GetIndexedDocumentsCountOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub index_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> GetIndexedDocumentsCountOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            index_name,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct PauseIngestOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub index_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> PauseIngestOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            index_name,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct ResumeIngestOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub index_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> ResumeIngestOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            index_name,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct AllowQueryingOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub index_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> AllowQueryingOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            index_name,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct DisallowQueryingOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub index_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> DisallowQueryingOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            index_name,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct FreezePlanOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub index_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> FreezePlanOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            index_name,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct UnfreezePlanOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub index_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> UnfreezePlanOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            index_name,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Default, Debug)]
#[non_exhaustive]
pub struct RefreshConfigOptions<'a> {
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> RefreshConfigOptions<'a> {
    pub fn new() -> Self {
        Self { on_behalf_of: None }
    }
    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureIndexPollOptions<C: Client> {
    pub desired_state: DesiredState,
    pub client: Arc<C>,
    pub targets: Vec<NodeTarget>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PingOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> PingOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}
