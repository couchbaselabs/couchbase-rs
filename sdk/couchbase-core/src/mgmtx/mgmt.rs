use crate::cbconfig::{CollectionManifest, TerseConfig};
use crate::httpx::client::Client;
use crate::httpx::request::{Auth, BasicAuth, OnBehalfOfInfo, Request};
use crate::httpx::response::Response;
use crate::mgmtx::bucket_settings::{encode_bucket_settings, BucketDef};
use crate::mgmtx::bucket_settings_json::BucketSettingsJson;
use crate::mgmtx::error;
use crate::mgmtx::options::{
    CreateBucketOptions, CreateCollectionOptions, CreateScopeOptions, DeleteBucketOptions,
    DeleteCollectionOptions, DeleteScopeOptions, FlushBucketOptions, GetAllBucketsOptions,
    GetBucketOptions, GetCollectionManifestOptions, GetTerseBucketConfigOptions,
    GetTerseClusterConfigOptions, UpdateBucketOptions, UpdateCollectionOptions,
};
use crate::mgmtx::responses::{
    CreateCollectionResponse, CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
    UpdateCollectionResponse,
};
use bytes::Bytes;
use http::Method;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

lazy_static! {
    static ref FIELD_NAME_MAP: HashMap<String, String> = {
        HashMap::from([
            (
                "durability_min_level".to_string(),
                "DurabilityMinLevel".to_string(),
            ),
            ("ramquota".to_string(), "RamQuotaMB".to_string()),
            ("replicanumber".to_string(), "ReplicaNumber".to_string()),
            ("maxttl".to_string(), "MaxTTL".to_string()),
            ("history".to_string(), "HistoryEnabled".to_string()),
        ])
    };
}

#[derive(Debug)]
pub struct Management<C: Client> {
    pub http_client: Arc<C>,
    pub user_agent: String,
    pub endpoint: String,
    pub username: String,
    pub password: String,
}

impl<C: Client> Management<C> {
    pub fn new_request(
        &self,
        method: Method,
        path: impl Into<String>,
        content_type: impl Into<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        headers: Option<HashMap<&str, &str>>,
        body: Option<Bytes>,
    ) -> Request {
        let auth = if let Some(obo) = on_behalf_of {
            Auth::OnBehalfOf(OnBehalfOfInfo {
                username: obo.username,
                password_or_domain: obo.password_or_domain,
            })
        } else {
            Auth::BasicAuth(BasicAuth {
                username: self.username.clone(),
                password: self.password.clone(),
            })
        };

        let mut req = Request::new(method, format!("{}/{}", self.endpoint, path.into()))
            .auth(auth)
            .user_agent(self.user_agent.clone())
            .content_type(content_type.into())
            .body(body);

        if let Some(headers) = headers {
            for (key, value) in headers.into_iter() {
                req = req.add_header(key, value);
            }
        }

        req
    }

    pub async fn execute(
        &self,
        method: Method,
        path: impl Into<String>,
        content_type: impl Into<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        headers: Option<HashMap<&str, &str>>,
        body: Option<Bytes>,
    ) -> error::Result<Response> {
        let req = self.new_request(method, path, content_type, on_behalf_of, headers, body);

        self.http_client
            .execute(req)
            .await
            .map_err(|e| error::Error::new_message_error("could not execute request").with(e))
    }

    async fn decode_common_error(response: Response) -> error::Error {
        let status = response.status();
        let body = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                return error::Error::new_message_error("could not parse response body").with(e)
            }
        };

        let body_str = match String::from_utf8(body.to_vec()) {
            Ok(s) => s.to_lowercase(),
            Err(e) => {
                return error::Error::new_message_error("could not parse error response").with(e)
            }
        };

        let kind = if body_str.contains("not found") && body_str.contains("collection") {
            error::ServerErrorKind::CollectionNotFound
        } else if body_str.contains("not found") && body_str.contains("scope") {
            error::ServerErrorKind::ScopeNotFound
        } else if body_str.contains("not found") && body_str.contains("bucket") {
            error::ServerErrorKind::BucketNotFound
        } else if body_str.contains("not found") && body_str.contains("user") {
            error::ServerErrorKind::UserNotFound
        } else if body_str.contains("already exists") && body_str.contains("collection") {
            error::ServerErrorKind::CollectionExists
        } else if body_str.contains("already exists") && body_str.contains("scope") {
            error::ServerErrorKind::ScopeExists
        } else if body_str.contains("already exists") && body_str.contains("bucket") {
            error::ServerErrorKind::BucketExists
        } else if body_str.contains("flush is disabled") {
            error::ServerErrorKind::FlushDisabled
        } else if body_str.contains("requested resource not found")
            || body_str.contains("non existent bucket")
        {
            error::ServerErrorKind::BucketNotFound
        } else if body_str.contains("not yet complete, but will continue") {
            error::ServerErrorKind::OperationDelayed
        } else if status == 400 {
            let s_err = Self::parse_for_invalid_arg(&body_str);
            if let Some(ia) = s_err {
                let key = ia.0;
                if FIELD_NAME_MAP.contains_key(&key) {
                    error::ServerErrorKind::ServerInvalidArg
                } else {
                    error::ServerErrorKind::Unknown
                }
            } else if body_str.contains("not allowed on this type of bucket") {
                error::ServerErrorKind::ServerInvalidArg
            } else {
                error::ServerErrorKind::Unknown
            }
        } else if status == 404 {
            error::ServerErrorKind::UnsupportedFeature
        } else if status == 401 {
            error::ServerErrorKind::AccessDenied
        } else {
            error::ServerErrorKind::Unknown
        };

        error::ServerError::new(status, body_str, kind).into()
    }

    fn parse_for_invalid_arg(body: &str) -> Option<(String, String)> {
        let inv_arg: ServerErrors = match serde_json::from_str(body) {
            Ok(i) => i,
            Err(_e) => {
                return None;
            }
        };

        if let Some((k, v)) = inv_arg.errors.into_iter().next() {
            return Some((k, v));
        }

        None
    }

    pub async fn get_terse_cluster_config(
        &self,
        opts: &GetTerseClusterConfigOptions<'_>,
    ) -> error::Result<TerseConfig> {
        let resp = self
            .execute(
                Method::GET,
                "/pools/default/nodeServicesStreaming",
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Self::parse_response_json(resp).await
    }

    pub async fn get_terse_bucket_config(
        &self,
        opts: &GetTerseBucketConfigOptions<'_>,
    ) -> error::Result<TerseConfig> {
        let resp = self
            .execute(
                Method::GET,
                format!("/pools/default/b/{}", opts.bucket_name),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Self::parse_response_json(resp).await
    }

    pub async fn get_collection_manifest(
        &self,
        opts: &GetCollectionManifestOptions<'_>,
    ) -> error::Result<CollectionManifest> {
        let resp = self
            .execute(
                Method::GET,
                format!("/pools/default/buckets/{}/scopes", opts.bucket_name),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await
            .map_err(|e| {
                error::Error::new_message_error("could not get collections manifest").with(e)
            })?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Self::parse_response_json(resp).await
    }

    pub async fn create_scope(
        &self,
        opts: &CreateScopeOptions<'_>,
    ) -> error::Result<CreateScopeResponse> {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("name", opts.scope_name)
            .finish();

        let resp = self
            .execute(
                Method::POST,
                format!("/pools/default/buckets/{}/scopes", opts.bucket_name),
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(Bytes::from(body)),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let manifest_uid: ManifestUidJson = Self::parse_response_json(resp).await?;

        Ok(CreateScopeResponse {
            manifest_uid: manifest_uid.manifest_uid,
        })
    }

    pub async fn delete_scope(
        &self,
        opts: &DeleteScopeOptions<'_>,
    ) -> error::Result<DeleteScopeResponse> {
        let resp = self
            .execute(
                Method::DELETE,
                format!(
                    "/pools/default/buckets/{}/scopes/{}",
                    opts.bucket_name, opts.scope_name
                ),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let manifest_uid: ManifestUidJson = Self::parse_response_json(resp).await?;

        Ok(DeleteScopeResponse {
            manifest_uid: manifest_uid.manifest_uid,
        })
    }

    pub async fn create_collection(
        &self,
        opts: &CreateCollectionOptions<'_>,
    ) -> error::Result<CreateCollectionResponse> {
        let body = {
            // Serializer is not Send so we need to drop it before making the request.
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair("name", opts.collection_name);

            let max_ttl = opts.max_ttl.map(|m| m.to_string());
            let max_ttl = max_ttl.as_deref();
            let history = opts.history_enabled.map(|h| h.to_string());
            let history = history.as_deref();
            if let Some(max_ttl) = max_ttl {
                form.append_pair("maxTTL", max_ttl);
            }
            if let Some(history) = history {
                form.append_pair("history", history);
            }

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::POST,
                format!(
                    "/pools/default/buckets/{}/scopes/{}/collections",
                    opts.bucket_name, opts.scope_name
                ),
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let manifest_uid: ManifestUidJson = Self::parse_response_json(resp).await?;

        Ok(CreateCollectionResponse {
            manifest_uid: manifest_uid.manifest_uid,
        })
    }

    pub async fn update_collection(
        &self,
        opts: &UpdateCollectionOptions<'_>,
    ) -> error::Result<UpdateCollectionResponse> {
        let body = {
            // Serializer is not Send so we need to drop it before making the request.
            let mut form = url::form_urlencoded::Serializer::new(String::new());

            let max_ttl = opts.max_ttl.map(|m| m.to_string());
            let max_ttl = max_ttl.as_deref();
            let history = opts.history_enabled.map(|h| h.to_string());
            let history = history.as_deref();
            if let Some(max_ttl) = max_ttl {
                form.append_pair("maxTTL", max_ttl);
            }
            if let Some(history) = history {
                form.append_pair("history", history);
            }

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::PATCH,
                format!(
                    "/pools/default/buckets/{}/scopes/{}/collections/{}",
                    opts.bucket_name, opts.scope_name, opts.collection_name
                ),
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let manifest_uid: ManifestUidJson = Self::parse_response_json(resp).await?;

        Ok(UpdateCollectionResponse {
            manifest_uid: manifest_uid.manifest_uid,
        })
    }

    pub async fn delete_collection(
        &self,
        opts: &DeleteCollectionOptions<'_>,
    ) -> error::Result<DeleteCollectionResponse> {
        let resp = self
            .execute(
                Method::DELETE,
                format!(
                    "/pools/default/buckets/{}/scopes/{}/collections/{}",
                    opts.bucket_name, opts.scope_name, opts.collection_name
                ),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let manifest_uid: ManifestUidJson = Self::parse_response_json(resp).await?;

        Ok(DeleteCollectionResponse {
            manifest_uid: manifest_uid.manifest_uid,
        })
    }

    pub async fn get_all_buckets(
        &self,
        opts: &GetAllBucketsOptions<'_>,
    ) -> error::Result<Vec<BucketDef>> {
        let resp = self
            .execute(
                Method::GET,
                "/pools/default/buckets",
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let json_buckets: Vec<BucketSettingsJson> = Self::parse_response_json(resp).await?;
        let mut buckets = Vec::with_capacity(json_buckets.len());
        for bucket in json_buckets {
            buckets.push(bucket.into());
        }

        Ok(buckets)
    }

    pub async fn get_bucket(&self, opts: &GetBucketOptions<'_>) -> error::Result<BucketDef> {
        let resp = self
            .execute(
                Method::GET,
                format!("/pools/default/buckets/{}", opts.bucket_name),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let bucket: BucketSettingsJson = Self::parse_response_json(resp).await?;

        Ok(bucket.into())
    }

    pub async fn create_bucket(&self, opts: &CreateBucketOptions<'_>) -> error::Result<()> {
        let body = {
            // Serializer is not Send so we need to drop it before making the request.
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair("name", opts.bucket_name);
            encode_bucket_settings(&mut form, opts.bucket_settings);

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::POST,
                "/pools/default/buckets",
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status() != 202 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    pub async fn update_bucket(&self, opts: &UpdateBucketOptions<'_>) -> error::Result<()> {
        let body = {
            // Serializer is not Send so we need to drop it before making the request.
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            encode_bucket_settings(&mut form, opts.bucket_settings);

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::POST,
                format!("/pools/default/buckets/{}", opts.bucket_name),
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    pub async fn delete_bucket(&self, opts: &DeleteBucketOptions<'_>) -> error::Result<()> {
        let resp = self
            .execute(
                Method::DELETE,
                format!("/pools/default/buckets/{}", opts.bucket_name),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            let e = Self::decode_common_error(resp).await;
            match e.kind() {
                error::ErrorKind::Server(e) => {
                    // A delayed operation is considered a success for deletion, since
                    // bucket management is already eventually consistent anyways.
                    if e.kind() == &error::ServerErrorKind::OperationDelayed {
                        return Ok(());
                    }
                }
                _ => return Err(e),
            }
        }

        Ok(())
    }

    pub async fn flush_bucket(&self, opts: &FlushBucketOptions<'_>) -> error::Result<()> {
        let resp = self
            .execute(
                Method::POST,
                format!(
                    "/pools/default/buckets/{}/controller/doFlush",
                    opts.bucket_name
                ),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    async fn parse_response_json<T: DeserializeOwned>(resp: Response) -> error::Result<T> {
        let body = resp
            .bytes()
            .await
            .map_err(|e| error::Error::new_message_error("could not read response").with(e))?;

        serde_json::from_slice(&body)
            .map_err(|e| error::Error::new_message_error("could not parse response").with(e))
    }
}

#[derive(Deserialize)]
struct ServerErrors {
    errors: HashMap<String, String>,
}

#[derive(Deserialize)]
struct ManifestUidJson {
    #[serde(rename = "uid")]
    pub manifest_uid: String,
}
