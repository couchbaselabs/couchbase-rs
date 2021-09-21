use crate::io::request::*;
use crate::io::Core;
use crate::{
    AllowQueryingSearchIndexOptions, AnalyzeDocumentSearchIndexOptions, CouchbaseError,
    CouchbaseResult, DisallowQueryingSearchIndexOptions, DropSearchIndexOptions, ErrorContext,
    FreezePlanSearchIndexOptions, GenericManagementResult, GetAllSearchIndexesOptions,
    GetSearchIndexOptions, GetSearchIndexedDocumentsCountOptions, PauseIngestSearchIndexOptions,
    ServiceType, UnfreezePlanSearchIndexOptions, UpsertSearchIndexOptions,
};
use futures::channel::oneshot;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SearchIndexBuilder {
    uuid: Option<String>,
    name: String,
    source_name: String,
    index_type: Option<String>,
    params: Option<Value>,
    source_uuid: Option<String>,
    source_params: Option<Value>,
    source_type: Option<String>,
    plan_params: Option<Value>,
}

// TODO: can we defer serialize until build?
impl SearchIndexBuilder {
    pub fn new(name: impl Into<String>, source_name: impl Into<String>) -> Self {
        Self {
            uuid: None,
            name: name.into(),
            source_name: source_name.into(),
            index_type: None,
            params: None,
            source_uuid: None,
            source_params: None,
            source_type: None,
            plan_params: None,
        }
    }

    pub fn params<T>(mut self, params: HashMap<String, T>) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        self.params =
            Some(
                serde_json::to_value(params).map_err(|e| CouchbaseError::EncodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    ctx: ErrorContext::default(),
                })?,
            );

        Ok(self)
    }

    pub fn source_uuid(mut self, uuid: impl Into<String>) -> Self {
        self.source_uuid = Some(uuid.into());
        self
    }

    pub fn source_params<T>(mut self, params: HashMap<String, T>) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        self.source_params =
            Some(
                serde_json::to_value(params).map_err(|e| CouchbaseError::EncodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    ctx: ErrorContext::default(),
                })?,
            );

        Ok(self)
    }

    pub fn source_type(mut self, source_type: impl Into<String>) -> Self {
        self.source_type = Some(source_type.into());
        self
    }

    pub fn plan_params<T>(mut self, params: HashMap<String, T>) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        self.plan_params =
            Some(
                serde_json::to_value(params).map_err(|e| CouchbaseError::EncodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    ctx: ErrorContext::default(),
                })?,
            );

        Ok(self)
    }

    pub fn build(self) -> SearchIndex {
        SearchIndex {
            uuid: self.uuid,
            name: self.name,
            source_name: self.source_name,
            index_type: self.index_type,
            params: self.params,
            source_uuid: self.source_uuid,
            source_params: self.source_params,
            source_type: self.source_type,
            plan_params: self.plan_params,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SearchIndex {
    #[serde(skip_serializing_if = "Option::is_none")]
    uuid: Option<String>,
    name: String,
    source_name: String,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    index_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
    #[serde(rename = "sourceUUID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    source_uuid: Option<String>,
    #[serde(rename = "sourceParams")]
    #[serde(skip_serializing_if = "Option::is_none")]
    source_params: Option<Value>,
    #[serde(rename = "sourceType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    source_type: Option<String>,
    #[serde(rename = "planParams")]
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_params: Option<Value>,
}

impl SearchIndex {
    pub fn uuid(&self) -> Option<&String> {
        self.uuid.as_ref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn index_type(&self) -> Option<&String> {
        self.index_type.as_ref()
    }

    pub fn params<T>(&self) -> CouchbaseResult<HashMap<String, T>>
    where
        T: DeserializeOwned,
    {
        match &self.params {
            Some(p) => serde_json::from_value(p.clone())
                .map_err(|e| CouchbaseError::decoding_failure_from_serde(e)),
            None => Ok(HashMap::new()),
        }
    }

    pub fn source_uuid(&self) -> Option<&String> {
        self.source_uuid.as_ref()
    }

    pub fn source_params<T>(&self) -> CouchbaseResult<HashMap<String, T>>
    where
        T: DeserializeOwned,
    {
        match &self.source_params {
            Some(p) => {
                serde_json::from_value(p.clone()).map_err(|e| CouchbaseError::DecodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    ctx: ErrorContext::default(),
                })
            }
            None => Ok(HashMap::new()),
        }
    }

    pub fn source_type(&self) -> Option<&String> {
        self.source_type.as_ref()
    }

    pub fn plan_params<T>(&self) -> CouchbaseResult<HashMap<String, T>>
    where
        T: DeserializeOwned,
    {
        match &self.plan_params {
            Some(p) => {
                serde_json::from_value(p.clone()).map_err(|e| CouchbaseError::DecodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    ctx: ErrorContext::default(),
                })
            }
            None => Ok(HashMap::new()),
        }
    }

    pub fn set_uuid(&mut self, uuid: impl Into<String>) {
        self.uuid = Some(uuid.into());
    }

    pub fn unset_uuid(&mut self) {
        self.uuid = None;
    }

    pub fn set_params<T>(&mut self, params: HashMap<String, T>) -> CouchbaseResult<()>
    where
        T: Serialize,
    {
        self.params = Some(
            serde_json::to_value(params)
                .map_err(|e| CouchbaseError::encoding_failure_from_serde(e))?,
        );

        Ok(())
    }

    pub fn set_source_uuid(&mut self, uuid: impl Into<String>) {
        self.source_uuid = Some(uuid.into());
    }

    pub fn set_source_params<T>(&mut self, params: HashMap<String, T>) -> CouchbaseResult<()>
    where
        T: Serialize,
    {
        self.source_params = Some(
            serde_json::to_value(params)
                .map_err(|e| CouchbaseError::encoding_failure_from_serde(e))?,
        );

        Ok(())
    }

    pub fn set_source_type(&mut self, source_type: impl Into<String>) {
        self.source_type = Some(source_type.into());
    }

    pub fn set_plan_params<T>(&mut self, params: HashMap<String, T>) -> CouchbaseResult<()>
    where
        T: Serialize,
    {
        self.plan_params = Some(
            serde_json::to_value(params)
                .map_err(|e| CouchbaseError::encoding_failure_from_serde(e))?,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SearchIndexDocumentsCount {
    count: u32,
}

pub struct SearchIndexManager {
    core: Arc<Core>,
}

impl SearchIndexManager {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    async fn do_request<T>(
        &self,
        path: String,
        method: String,
        payload: Option<String>,
        content_type: Option<String>,
        timeout: Option<Duration>,
    ) -> CouchbaseResult<T>
    where
        T: DeserializeOwned,
    {
        let (sender, receiver) = oneshot::channel();

        self.core.send(Request::GenericManagementRequest(
            GenericManagementRequest {
                sender,
                path,
                method,
                payload,
                content_type,
                timeout,
                service_type: Some(ServiceType::Search),
            },
        ));

        let result: GenericManagementResult = receiver.await.unwrap().unwrap();
        let content: T = match result.http_status() {
            200 => serde_json::from_slice(result.payload().unwrap()).map_err(|e| {
                CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }
            }),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload().unwrap().to_owned())
                    .unwrap()
                    .to_lowercase(),
            }),
        }?;

        Ok(content)
    }

    pub async fn get_index(
        &self,
        index_name: impl Into<String>,
        opts: GetSearchIndexOptions,
    ) -> CouchbaseResult<SearchIndex> {
        let res: SearchIndex = self
            .do_request(
                format!("/api/index/{}", index_name.into()),
                String::from("get"),
                None,
                None,
                opts.timeout,
            )
            .await?;

        Ok(res)
    }

    pub async fn get_all_indexes(
        &self,
        opts: GetAllSearchIndexesOptions,
    ) -> CouchbaseResult<impl IntoIterator<Item = SearchIndex>> {
        let res: Vec<SearchIndex> = self
            .do_request(
                String::from("/api/index"),
                String::from("get"),
                None,
                None,
                opts.timeout,
            )
            .await?;

        Ok(res)
    }

    pub async fn upsert_index(
        &self,
        index_definition: SearchIndex,
        opts: UpsertSearchIndexOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!("/api/index/{}", index_definition.name()),
                String::from("put"),
                Some(serde_json::to_string(&index_definition).map_err(|e| {
                    CouchbaseError::DecodingFailure {
                        ctx: ErrorContext::default(),
                        source: e.into(),
                    }
                })?),
                Some(String::from("application/json")),
                opts.timeout,
            )
            .await?)
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: DropSearchIndexOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!("/api/index/{}", index_name.into()),
                String::from("delete"),
                None,
                None,
                opts.timeout,
            )
            .await?)
    }

    pub async fn get_indexed_documents_count(
        &self,
        index_name: impl Into<String>,
        opts: GetSearchIndexedDocumentsCountOptions,
    ) -> CouchbaseResult<u32> {
        let res: SearchIndexDocumentsCount = self
            .do_request(
                format!("/api/index/{}/count", index_name.into()),
                String::from("get"),
                None,
                None,
                opts.timeout,
            )
            .await?;

        Ok(res.count)
    }

    pub async fn pause_ingest(
        &self,
        index_name: impl Into<String>,
        opts: PauseIngestSearchIndexOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!("/api/index/{}/ingestControl/pause", index_name.into()),
                String::from("post"),
                None,
                None,
                opts.timeout,
            )
            .await?)
    }

    pub async fn resume_ingest(
        &self,
        index_name: impl Into<String>,
        opts: PauseIngestSearchIndexOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!("/api/index/{}/ingestControl/resume", index_name.into()),
                String::from("post"),
                None,
                None,
                opts.timeout,
            )
            .await?)
    }

    pub async fn allow_querying(
        &self,
        index_name: impl Into<String>,
        opts: AllowQueryingSearchIndexOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!("/api/index/{}/queryControl/allow", index_name.into()),
                String::from("post"),
                None,
                None,
                opts.timeout,
            )
            .await?)
    }

    pub async fn disallow_querying(
        &self,
        index_name: impl Into<String>,
        opts: DisallowQueryingSearchIndexOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!("/api/index/{}/queryControl/disallow", index_name.into()),
                String::from("post"),
                None,
                None,
                opts.timeout,
            )
            .await?)
    }

    pub async fn freeze_plan(
        &self,
        index_name: impl Into<String>,
        opts: FreezePlanSearchIndexOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!("/api/index/{}/planFreezeControl/freeze", index_name.into()),
                String::from("post"),
                None,
                None,
                opts.timeout,
            )
            .await?)
    }

    pub async fn unfreeze_plan(
        &self,
        index_name: impl Into<String>,
        opts: UnfreezePlanSearchIndexOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!(
                    "/api/index/{}/planFreezeControl/unfreeze",
                    index_name.into()
                ),
                String::from("post"),
                None,
                None,
                opts.timeout,
            )
            .await?)
    }

    // TODO: this probably isn't particularly friendly to use.
    pub async fn analyze_document<I, T>(
        &self,
        index_name: impl Into<String>,
        document: I,
        opts: AnalyzeDocumentSearchIndexOptions,
    ) -> CouchbaseResult<T>
    where
        I: Serialize,
        T: DeserializeOwned,
    {
        let res: T = self
            .do_request(
                format!("/api/index/{}/analyzeDoc", index_name.into()),
                String::from("post"),
                Some(serde_json::to_string(&document).map_err(|e| {
                    CouchbaseError::DecodingFailure {
                        ctx: ErrorContext::default(),
                        source: e.into(),
                    }
                })?),
                Some(String::from("application/json")),
                opts.timeout,
            )
            .await?;

        Ok(res)
    }
}
