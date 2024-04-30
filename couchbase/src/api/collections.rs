use super::*;
use crate::io::request::*;
use crate::CouchbaseError::{CollectionExists, CollectionNotFound, ScopeExists, ScopeNotFound};
use crate::{CouchbaseError, CouchbaseResult, ErrorContext, GenericManagementResult, ServiceType};
use futures::channel::oneshot;
use serde_derive::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
#[derive(Debug)]
pub struct ScopeSpec {
    name: String,
    collections: Vec<CollectionSpec>,
}

impl ScopeSpec {
    pub(crate) fn new(name: String, collections: Vec<CollectionSpec>) -> Self {
        Self { name, collections }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn collections(&self) -> &Vec<CollectionSpec> {
        &self.collections
    }

    pub fn collections_mut(&mut self) -> &mut Vec<CollectionSpec> {
        self.collections.as_mut()
    }
}

#[derive(Debug)]
pub struct CollectionSpec {
    name: String,
    scope_name: String,
    max_expiry: Duration,
}

impl CollectionSpec {
    pub fn new(
        name: impl Into<String>,
        scope_name: impl Into<String>,
        max_expiry: Duration,
    ) -> Self {
        Self {
            name: name.into(),
            scope_name: scope_name.into(),
            max_expiry,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn max_expiry(&self) -> Duration {
        self.max_expiry
    }
}

#[derive(Debug, Deserialize)]
struct ManifestCollection {
    uid: String,
    name: String,
    #[serde(rename = "maxTTL")]
    max_expiry: u64,
}

#[derive(Debug, Deserialize)]
struct ManifestScope {
    uid: String,
    name: String,
    collections: Vec<ManifestCollection>,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    uid: String,
    scopes: Vec<ManifestScope>,
}

pub struct CollectionManager {
    core: Arc<Core>,
    bucket_name: String,
}

impl CollectionManager {
    pub(crate) fn new(core: Arc<Core>, bucket_name: String) -> Self {
        Self { core, bucket_name }
    }

    pub async fn get_all_scopes(
        &self,
        options: impl Into<Option<GetAllScopesOptions>>,
    ) -> CouchbaseResult<Vec<ScopeSpec>> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/pools/default/buckets/{}/scopes", self.bucket_name),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;
        let manifest: Manifest = match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }?;

        let mut scopes = vec![];
        for scope in manifest.scopes {
            let mut collections = vec![];
            for col in scope.collections {
                collections.push(CollectionSpec::new(
                    col.name,
                    scope.name.clone(),
                    Duration::from_secs(col.max_expiry),
                ))
            }
            scopes.push(ScopeSpec::new(scope.name, collections));
        }

        Ok(scopes)
    }

    pub async fn create_scope<S: Into<String>>(
        &self,
        scope_name: S,
        options: CreateScopeOptions,
    ) -> CouchbaseResult<()> {
        // The server expects form data so we need to build that, serde expects each value to be an
        // Option.
        let scope = scope_name.into();

        let form = &[("name", scope.clone())];

        let form_encoded = serde_urlencoded::to_string(&form)?;
        let content_type = String::from("application/x-www-form-urlencoded");
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/pools/default/buckets/{}/scopes", self.bucket_name),
                method: String::from("post"),
                payload: Some(form_encoded),
                content_type: Some(content_type),
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                scope,
            )),
        }
    }

    pub async fn create_collection(
        &self,
        collection: CollectionSpec,
        options: impl Into<Option<CreateCollectionOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        // The server expects form data so we need to build that, serde expects each value to be an
        // Option.
        let mut form = vec![("name", collection.name.clone())];
        if collection.max_expiry.as_secs() > 0 {
            form.push(("maxTTL", collection.max_expiry.as_secs().to_string()));
        }

        let form_encoded = serde_urlencoded::to_string(&form)?;
        let content_type = String::from("application/x-www-form-urlencoded");
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!(
                    "/pools/default/buckets/{}/scopes/{}/collections/",
                    self.bucket_name, collection.scope_name
                ),
                method: String::from("post"),
                payload: Some(form_encoded),
                content_type: Some(content_type),
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                collection.name,
            )),
        }
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        options: impl Into<Option<DropScopeOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();

        let scope = scope_name.into();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!(
                    "/pools/default/buckets/{}/scopes/{}",
                    self.bucket_name, &scope,
                ),
                method: String::from("delete"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                scope,
            )),
        }
    }

    pub async fn drop_collection(
        &self,
        collection: CollectionSpec,
        options: impl Into<Option<DropCollectionOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!(
                    "/pools/default/buckets/{}/scopes/{}/collections/{}",
                    self.bucket_name, collection.scope_name, collection.name
                ),
                method: String::from("delete"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                collection.name,
            )),
        }
    }

    fn parse_error(&self, status: u16, message: String, object_name: String) -> CouchbaseError {
        if message.contains("not_found") && message.contains("collection") {
            let mut ctx = ErrorContext::default();
            ctx.insert("name", Value::String(object_name));
            return CollectionNotFound { ctx };
        } else if message.contains("not_found") && message.contains("scope") {
            let mut ctx = ErrorContext::default();
            ctx.insert("name", Value::String(object_name));
            return ScopeNotFound { ctx };
        }

        if message.contains("already exists") && message.contains("collection") {
            let mut ctx = ErrorContext::default();
            ctx.insert("name", Value::String(object_name));
            return CollectionExists { ctx };
        } else if message.contains("already exists") && message.contains("scope") {
            let mut ctx = ErrorContext::default();
            ctx.insert("name", Value::String(object_name));
            return ScopeExists { ctx };
        }

        CouchbaseError::GenericHTTP {
            ctx: Default::default(),
            status,
            message,
        }
    }
}

#[derive(Debug, Default)]
pub struct GetAllScopesOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllScopesOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct CreateScopeOptions {
    pub(crate) timeout: Option<Duration>,
}

impl CreateScopeOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct CreateCollectionOptions {
    pub(crate) timeout: Option<Duration>,
}

impl CreateCollectionOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropScopeOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropScopeOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropCollectionOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropCollectionOptions {
    timeout!();
}
