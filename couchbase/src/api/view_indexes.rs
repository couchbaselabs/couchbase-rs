use super::*;
use crate::api::view_options::DesignDocumentNamespace;
use crate::io::request::*;
use crate::{CouchbaseError, CouchbaseResult, GenericManagementResult, ServiceType};
use futures::channel::oneshot;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct View {
    map: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reduce: Option<String>,
}

impl View {
    pub fn map(&self) -> &str {
        &self.map
    }
    pub fn reduce(&self) -> Option<&String> {
        self.reduce.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct ViewBuilder {
    map: String,
    reduce: Option<String>,
}

impl ViewBuilder {
    pub fn new(map: impl Into<String>) -> Self {
        Self {
            map: map.into(),
            reduce: None,
        }
    }
    pub fn reduce(mut self, reduce: impl Into<String>) -> Self {
        self.reduce = Some(reduce.into());
        self
    }
    pub fn build(self) -> View {
        View {
            map: self.map,
            reduce: self.reduce,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DesignDocumentBuilder {
    name: String,
    views: HashMap<String, View>,
}

impl DesignDocumentBuilder {
    pub fn new(name: impl Into<String>, views: HashMap<String, View>) -> Self {
        Self {
            name: name.into(),
            views,
        }
    }
    pub fn build(self) -> DesignDocument {
        DesignDocument {
            name: self.name,
            views: self.views,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DesignDocument {
    name: String,
    views: HashMap<String, View>,
}

impl DesignDocument {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn views(&self) -> &HashMap<String, View> {
        &self.views
    }
    pub fn views_mut(&mut self) -> &mut HashMap<String, View> {
        self.views.borrow_mut()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct JSONDesignDocument {
    views: HashMap<String, View>,
}

#[derive(Debug, Clone, Deserialize)]
struct AllDesignDocumentDocMeta {
    id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct AllDesignDocumentDocJSON {
    views: HashMap<String, View>,
}

#[derive(Debug, Clone, Deserialize)]
struct AllDesignDocumentDoc {
    meta: AllDesignDocumentDocMeta,
    json: AllDesignDocumentDocJSON,
}

#[derive(Debug, Clone, Deserialize)]
struct AllDesignDocumentsRow {
    doc: AllDesignDocumentDoc,
}

#[derive(Debug, Clone, Deserialize)]
struct AllDesignDocuments {
    rows: Vec<AllDesignDocumentsRow>,
}

pub struct ViewIndexManager {
    core: Arc<Core>,
    bucket: String,
}

impl ViewIndexManager {
    pub(crate) fn new(core: Arc<Core>, bucket: String) -> Self {
        Self { core, bucket }
    }

    async fn mutation_request(
        &self,
        path: String,
        method: String,
        payload: Option<String>,
        content_type: Option<String>,
        timeout: Option<Duration>,
        service: ServiceType,
    ) -> CouchbaseResult<()> {
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path,
                method,
                payload,
                content_type,
                timeout,
                service_type: Some(service),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;
        match result.http_status() {
            200 => Ok(()),
            201 => Ok(()),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    async fn get_request<T>(
        &self,
        path: String,
        timeout: Option<Duration>,
        service: ServiceType,
    ) -> CouchbaseResult<T>
    where
        T: DeserializeOwned,
    {
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path,
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout,
                service_type: Some(service),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;
        let content: T = match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }?;

        Ok(content)
    }

    pub async fn get_design_document(
        &self,
        name: impl Into<String>,
        namespace: DesignDocumentNamespace,
        opts: impl Into<Option<GetDesignDocumentOptions>>,
    ) -> CouchbaseResult<DesignDocument> {
        let opts = unwrap_or_default!(opts.into());
        let ddoc_name = self.build_ddoc_name(name.into(), namespace);
        let res: JSONDesignDocument = self
            .get_request(
                format!("/_design/{}", ddoc_name),
                opts.timeout,
                ServiceType::Views,
            )
            .await?;

        let name = ddoc_name
            .strip_prefix("dev_")
            .unwrap_or_else(|| ddoc_name.as_str())
            .to_string();

        Ok(DesignDocument {
            name,
            views: res.views,
        })
    }

    pub async fn get_all_design_documents(
        &self,
        namespace: DesignDocumentNamespace,
        opts: impl Into<Option<GetAllDesignDocumentsOptions>>,
    ) -> CouchbaseResult<impl IntoIterator<Item = DesignDocument>> {
        let opts = unwrap_or_default!(opts.into());
        let res: AllDesignDocuments = self
            .get_request(
                format!("/pools/default/buckets/{}/ddocs", self.bucket.clone()),
                opts.timeout,
                ServiceType::Management,
            )
            .await?;

        let mut ddocs = vec![];
        for row in res.rows {
            let name = row.doc.meta.id;
            match namespace {
                DesignDocumentNamespace::Production => {
                    if name.starts_with("dev_") {
                        continue;
                    }

                    ddocs.push(DesignDocument {
                        name,
                        views: row.doc.json.views,
                    });
                }
                DesignDocumentNamespace::Development => {
                    let trimmed = match name.strip_prefix("dev_") {
                        Some(n) => n.to_string(),
                        None => continue,
                    };

                    ddocs.push(DesignDocument {
                        name: trimmed,
                        views: row.doc.json.views,
                    });
                }
            }
        }

        Ok(ddocs)
    }

    pub async fn upsert_design_document(
        &self,
        design_doc: DesignDocument,
        namespace: DesignDocumentNamespace,
        opts: impl Into<Option<UpsertDesignDocumentOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let json_ddoc = JSONDesignDocument {
            views: design_doc.views,
        };
        Ok(self
            .mutation_request(
                format!(
                    "/_design/{}",
                    self.build_ddoc_name(design_doc.name, namespace)
                ),
                String::from("put"),
                Some(
                    serde_json::to_string(&json_ddoc)
                        .map_err(CouchbaseError::encoding_failure_from_serde)?,
                ),
                Some(String::from("application/json")),
                opts.timeout,
                ServiceType::Views,
            )
            .await?)
    }

    pub async fn drop_design_document(
        &self,
        name: impl Into<String>,
        namespace: DesignDocumentNamespace,
        opts: impl Into<Option<DropDesignDocumentsOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        Ok(self
            .mutation_request(
                format!("/_design/{}", self.build_ddoc_name(name.into(), namespace)),
                String::from("delete"),
                None,
                None,
                opts.timeout,
                ServiceType::Views,
            )
            .await?)
    }

    pub async fn publish_design_document(
        &self,
        name: impl Into<String>,
        opts: impl Into<Option<PublishDesignDocumentsOptions>>,
    ) -> CouchbaseResult<()> {
        let opts = unwrap_or_default!(opts.into());
        let ddoc = self
            .get_design_document(
                name.into(),
                DesignDocumentNamespace::Development,
                GetDesignDocumentOptions {
                    timeout: opts.timeout,
                },
            )
            .await?;

        self.upsert_design_document(
            ddoc,
            DesignDocumentNamespace::Production,
            UpsertDesignDocumentOptions {
                timeout: opts.timeout,
            },
        )
        .await
    }

    fn build_ddoc_name(&self, name: String, namespace: DesignDocumentNamespace) -> String {
        let mut new_name = name;
        match namespace {
            DesignDocumentNamespace::Development => {
                if !new_name.starts_with("dev_") {
                    new_name = format!("dev_{}", new_name);
                }
            }
            DesignDocumentNamespace::Production => {
                new_name = match new_name.strip_prefix("dev_") {
                    Some(n) => n.to_string(),
                    None => new_name,
                };
            }
        }

        new_name
    }
}

#[derive(Debug, Default)]
pub struct GetDesignDocumentOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetDesignDocumentOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAllDesignDocumentsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllDesignDocumentsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct UpsertDesignDocumentOptions {
    pub(crate) timeout: Option<Duration>,
}

impl UpsertDesignDocumentOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropDesignDocumentsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropDesignDocumentsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct PublishDesignDocumentsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl PublishDesignDocumentsOptions {
    timeout!();
}
