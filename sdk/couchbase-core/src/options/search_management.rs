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

use crate::httpx::request::OnBehalfOfInfo;
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};
use crate::searchx;
use crate::searchx::ensure_index_helper::DesiredState;
use std::sync::Arc;

#[derive(Debug)]
#[non_exhaustive]
pub struct GetIndexOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
}

impl<'a> GetIndexOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct GetAllIndexesOptions<'a> {
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
}

impl<'a> Default for GetAllIndexesOptions<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> GetAllIndexesOptions<'a> {
    pub fn new() -> Self {
        Self {
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct UpsertIndexOptions<'a> {
    pub index: &'a searchx::index::Index,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,

    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,

    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> UpsertIndexOptions<'a> {
    pub fn new(index: &'a searchx::index::Index) -> Self {
        Self {
            index,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct DeleteIndexOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,

    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,

    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> DeleteIndexOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
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
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> AnalyzeDocumentOptions<'a> {
    pub fn new(index_name: &'a str, doc_content: &'a [u8]) -> Self {
        Self {
            index_name,
            doc_content,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct GetIndexedDocumentsCountOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> GetIndexedDocumentsCountOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct PauseIngestOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> PauseIngestOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct ResumeIngestOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> ResumeIngestOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct AllowQueryingOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> AllowQueryingOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct DisallowQueryingOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> DisallowQueryingOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct FreezePlanOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> FreezePlanOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct UnfreezePlanOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
    pub endpoint: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> UnfreezePlanOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
            endpoint: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &'a str) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnsureIndexOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub desired_state: DesiredState,
}

impl<'a> EnsureIndexOptions<'a> {
    pub fn new(
        index_name: &'a str,
        bucket_name: impl Into<Option<&'a str>>,
        scope_name: impl Into<Option<&'a str>>,
        desired_state: DesiredState,
        on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    ) -> Self {
        Self {
            index_name,
            bucket_name: bucket_name.into(),
            scope_name: scope_name.into(),
            on_behalf_of_info,
            desired_state,
        }
    }
}

impl<'a> From<&'a GetIndexOptions<'a>> for searchx::mgmt_options::GetIndexOptions<'a> {
    fn from(opts: &'a GetIndexOptions<'a>) -> searchx::mgmt_options::GetIndexOptions<'a> {
        searchx::mgmt_options::GetIndexOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a GetAllIndexesOptions<'a>> for searchx::mgmt_options::GetAllIndexesOptions<'a> {
    fn from(opts: &'a GetAllIndexesOptions<'a>) -> searchx::mgmt_options::GetAllIndexesOptions<'a> {
        searchx::mgmt_options::GetAllIndexesOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&UpsertIndexOptions<'a>> for searchx::mgmt_options::UpsertIndexOptions<'a> {
    fn from(opts: &UpsertIndexOptions<'a>) -> searchx::mgmt_options::UpsertIndexOptions<'a> {
        searchx::mgmt_options::UpsertIndexOptions {
            index: opts.index,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&DeleteIndexOptions<'a>> for searchx::mgmt_options::DeleteIndexOptions<'a> {
    fn from(opts: &DeleteIndexOptions<'a>) -> searchx::mgmt_options::DeleteIndexOptions<'a> {
        searchx::mgmt_options::DeleteIndexOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a AnalyzeDocumentOptions<'a>>
    for searchx::mgmt_options::AnalyzeDocumentOptions<'a>
{
    fn from(
        opts: &'a AnalyzeDocumentOptions<'a>,
    ) -> searchx::mgmt_options::AnalyzeDocumentOptions<'a> {
        searchx::mgmt_options::AnalyzeDocumentOptions {
            index_name: opts.index_name,
            doc_content: opts.doc_content,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a GetIndexedDocumentsCountOptions<'a>>
    for searchx::mgmt_options::GetIndexedDocumentsCountOptions<'a>
{
    fn from(
        opts: &'a GetIndexedDocumentsCountOptions<'a>,
    ) -> searchx::mgmt_options::GetIndexedDocumentsCountOptions<'a> {
        searchx::mgmt_options::GetIndexedDocumentsCountOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a PauseIngestOptions<'a>> for searchx::mgmt_options::PauseIngestOptions<'a> {
    fn from(opts: &'a PauseIngestOptions<'a>) -> searchx::mgmt_options::PauseIngestOptions<'a> {
        searchx::mgmt_options::PauseIngestOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a ResumeIngestOptions<'a>> for searchx::mgmt_options::ResumeIngestOptions<'a> {
    fn from(opts: &'a ResumeIngestOptions<'a>) -> searchx::mgmt_options::ResumeIngestOptions<'a> {
        searchx::mgmt_options::ResumeIngestOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a AllowQueryingOptions<'a>> for searchx::mgmt_options::AllowQueryingOptions<'a> {
    fn from(opts: &'a AllowQueryingOptions<'a>) -> searchx::mgmt_options::AllowQueryingOptions<'a> {
        searchx::mgmt_options::AllowQueryingOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a DisallowQueryingOptions<'a>>
    for searchx::mgmt_options::DisallowQueryingOptions<'a>
{
    fn from(
        opts: &'a DisallowQueryingOptions<'a>,
    ) -> searchx::mgmt_options::DisallowQueryingOptions<'a> {
        searchx::mgmt_options::DisallowQueryingOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a FreezePlanOptions<'a>> for searchx::mgmt_options::FreezePlanOptions<'a> {
    fn from(opts: &'a FreezePlanOptions<'a>) -> searchx::mgmt_options::FreezePlanOptions<'a> {
        searchx::mgmt_options::FreezePlanOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&'a UnfreezePlanOptions<'a>> for searchx::mgmt_options::UnfreezePlanOptions<'a> {
    fn from(opts: &'a UnfreezePlanOptions<'a>) -> searchx::mgmt_options::UnfreezePlanOptions<'a> {
        searchx::mgmt_options::UnfreezePlanOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}
