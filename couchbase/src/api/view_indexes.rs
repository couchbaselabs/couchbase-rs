use crate::io::request::*;
use crate::io::Core;
use crate::{
    CouchbaseError, CouchbaseResult, DropDesignDocumentsOptions, GenericManagementResult,
    GetAllDesignDocumentsOptions, GetDesignDocumentOptions, ServiceType,
    UpsertDesignDocumentOptions,
};
use futures::channel::oneshot;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DesignDocumentNamespace {
    Production,
    Development,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct View {
    map: String,
    reduce: String,
}

#[derive(Debug, Clone)]
pub struct DesignDocument {
    name: String,
    views: HashMap<String, View>,
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
struct AllDesignDocuments {
    rows: Vec<AllDesignDocumentDoc>,
}

pub struct ViewIndexManager {
    core: Arc<Core>,
    bucket: String,
}

impl ViewIndexManager {
    pub(crate) fn new(core: Arc<Core>, bucket: String) -> Self {
        Self { core, bucket }
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
                service_type: Some(ServiceType::Views),
            },
        ));

        let result: GenericManagementResult = receiver.await.unwrap().unwrap();
        let content: T = match result.http_status() {
            200 => serde_json::from_slice(result.payload().unwrap())
                .map_err(CouchbaseError::decoding_failure_from_serde),
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

    pub async fn get_design_document(
        &self,
        name: impl Into<String>,
        namespace: DesignDocumentNamespace,
        opts: GetDesignDocumentOptions,
    ) -> CouchbaseResult<DesignDocument> {
        let ddoc_name = self.build_ddoc_name(name.into(), namespace);
        let res: JSONDesignDocument = self
            .do_request(
                format!("/{}/_design/{}", self.bucket.clone(), ddoc_name),
                String::from("get"),
                None,
                None,
                opts.timeout,
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
        opts: GetAllDesignDocumentsOptions,
    ) -> CouchbaseResult<impl IntoIterator<Item = DesignDocument>> {
        let res: AllDesignDocuments = self
            .do_request(
                format!("/pools/default/buckets/{}/ddocs", self.bucket.clone()),
                String::from("get"),
                None,
                None,
                opts.timeout,
            )
            .await?;

        let mut ddocs = vec![];
        for row in res.rows {
            let name = row.meta.id;
            match namespace {
                DesignDocumentNamespace::Production => {
                    if name.starts_with("dev_") {
                        continue;
                    }

                    ddocs.push(DesignDocument {
                        name,
                        views: row.json.views,
                    });
                }
                DesignDocumentNamespace::Development => {
                    let trimmed = match name.strip_prefix("dev_") {
                        Some(n) => n.to_string(),
                        None => continue,
                    };

                    ddocs.push(DesignDocument {
                        name: trimmed,
                        views: row.json.views,
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
        opts: UpsertDesignDocumentOptions,
    ) -> CouchbaseResult<()> {
        let json_ddoc = JSONDesignDocument {
            views: design_doc.views,
        };
        Ok(self
            .do_request(
                format!(
                    "/{}/design/{}",
                    self.bucket,
                    self.build_ddoc_name(design_doc.name, namespace)
                ),
                String::from("put"),
                Some(
                    serde_json::to_string(&json_ddoc)
                        .map_err(CouchbaseError::encoding_failure_from_serde)?,
                ),
                Some(String::from("application/json")),
                opts.timeout,
            )
            .await?)
    }

    pub async fn drop_design_document(
        &self,
        name: impl Into<String>,
        namespace: DesignDocumentNamespace,
        opts: DropDesignDocumentsOptions,
    ) -> CouchbaseResult<()> {
        Ok(self
            .do_request(
                format!(
                    "/{}/design/{}",
                    self.bucket,
                    self.build_ddoc_name(name.into(), namespace)
                ),
                String::from("delete"),
                None,
                None,
                opts.timeout,
            )
            .await?)
    }

    pub async fn publish_design_document(
        &self,
        name: impl Into<String>,
        namespace: DesignDocumentNamespace,
        opts: DropDesignDocumentsOptions,
    ) -> CouchbaseResult<()> {
        let ddoc = self
            .get_design_document(
                name.into(),
                namespace.clone(),
                GetDesignDocumentOptions {
                    timeout: opts.timeout.clone(),
                },
            )
            .await?;

        self.upsert_design_document(
            ddoc,
            namespace.clone(),
            UpsertDesignDocumentOptions {
                timeout: opts.timeout.clone(),
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
