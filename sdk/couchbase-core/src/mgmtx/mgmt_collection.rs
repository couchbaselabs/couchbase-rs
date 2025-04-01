use crate::cbconfig::CollectionManifest;
use crate::httpx::client::Client;
use crate::mgmtx::error;
use crate::mgmtx::mgmt::{parse_response_json, Management};
use crate::mgmtx::options::{
    CreateCollectionOptions, CreateScopeOptions, DeleteCollectionOptions, DeleteScopeOptions,
    GetCollectionManifestOptions, UpdateCollectionOptions,
};
use crate::mgmtx::responses::{
    CreateCollectionResponse, CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
    UpdateCollectionResponse,
};
use bytes::Bytes;
use http::Method;
use serde::Deserialize;

impl<C: Client> Management<C> {
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

        parse_response_json(resp).await
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

        let manifest_uid: ManifestUidJson = parse_response_json(resp).await?;

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

        let manifest_uid: ManifestUidJson = parse_response_json(resp).await?;

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

        let manifest_uid: ManifestUidJson = parse_response_json(resp).await?;

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

        let manifest_uid: ManifestUidJson = parse_response_json(resp).await?;

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

        let manifest_uid: ManifestUidJson = parse_response_json(resp).await?;

        Ok(DeleteCollectionResponse {
            manifest_uid: manifest_uid.manifest_uid,
        })
    }
}

#[derive(Deserialize)]
struct ManifestUidJson {
    #[serde(rename = "uid")]
    pub manifest_uid: String,
}
