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

use crate::cbconfig::TerseConfig;
use crate::httpx::client::Client;
use crate::httpx::request::{Auth, BasicAuth, OnBehalfOfInfo, Request};
use crate::httpx::response::Response;
use crate::mgmtx::error;
use crate::mgmtx::options::{GetTerseBucketConfigOptions, GetTerseClusterConfigOptions};
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
            ("numvbuckets".to_string(), "numVBuckets".to_string()),
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
            .map_err(|e| error::Error::new_message_error(format!("could not execute request: {e}")))
    }

    pub(crate) async fn decode_common_error(
        method: Method,
        path: String,
        feature: impl Into<String>,
        response: Response,
    ) -> error::Error {
        let status = response.status();
        let url = response.url().to_string();
        let body = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                return error::Error::new_message_error(format!(
                    "could not parse response body: {e}"
                ))
            }
        };

        let body_str = match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(e) => {
                return error::Error::new_message_error(format!(
                    "could not parse error response: {e}"
                ))
            }
        };

        let lower_body_str = body_str.to_lowercase();

        let kind = if lower_body_str.contains("not found") && lower_body_str.contains("collection")
        {
            error::ServerErrorKind::CollectionNotFound
        } else if lower_body_str.contains("not found") && lower_body_str.contains("scope") {
            error::ServerErrorKind::ScopeNotFound
        } else if lower_body_str.contains("not found") && lower_body_str.contains("bucket") {
            error::ServerErrorKind::BucketNotFound
        } else if (lower_body_str.contains("not found") && lower_body_str.contains("user"))
            || lower_body_str.contains("unknown user")
        {
            error::ServerErrorKind::UserNotFound
        } else if (lower_body_str.contains("not found") && lower_body_str.contains("group"))
            || lower_body_str.contains("unknown group")
        {
            error::ServerErrorKind::GroupNotFound
        } else if lower_body_str.contains("already exists") && lower_body_str.contains("collection")
        {
            error::ServerErrorKind::CollectionExists
        } else if lower_body_str.contains("already exists") && lower_body_str.contains("scope") {
            error::ServerErrorKind::ScopeExists
        } else if lower_body_str.contains("already exists") && lower_body_str.contains("bucket") {
            error::ServerErrorKind::BucketExists
        } else if lower_body_str.contains("flush is disabled") {
            error::ServerErrorKind::FlushDisabled
        } else if lower_body_str.contains("requested resource not found")
            || lower_body_str.contains("non existent bucket")
        {
            error::ServerErrorKind::BucketNotFound
        } else if lower_body_str.contains("not yet complete, but will continue") {
            error::ServerErrorKind::OperationDelayed
        } else if status == 400 {
            let s_err = Self::parse_for_invalid_arg(&lower_body_str);
            if let Some(ia) = s_err {
                let key = ia.0;
                if FIELD_NAME_MAP.contains_key(&key) {
                    error::ServerErrorKind::ServerInvalidArg {
                        arg: key,
                        reason: ia.1,
                    }
                } else {
                    error::ServerErrorKind::Unknown
                }
            } else if lower_body_str.contains("not allowed on this type of bucket") {
                error::ServerErrorKind::ServerInvalidArg {
                    arg: "historyEnabled".to_string(),
                    reason: body_str.to_string(),
                }
            } else {
                error::ServerErrorKind::Unknown
            }
        } else if status == 404 {
            error::ServerErrorKind::UnsupportedFeature {
                feature: feature.into(),
            }
        } else if status == 401 {
            error::ServerErrorKind::AccessDenied
        } else {
            error::ServerErrorKind::Unknown
        };

        error::ServerError::new(status, url, method, path, body_str, kind).into()
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
        let method = Method::GET;
        let path = "pools/default/nodeServicesStreaming".to_string();

        let resp = self
            .execute(
                method.clone(),
                &path,
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(
                Self::decode_common_error(method, path, "get_terse_cluster_config", resp).await,
            );
        }

        parse_response_json(resp).await
    }

    pub async fn get_terse_bucket_config(
        &self,
        opts: &GetTerseBucketConfigOptions<'_>,
    ) -> error::Result<TerseConfig> {
        let method = Method::GET;
        let path = format!("pools/default/b/{}", opts.bucket_name);

        let resp = self
            .execute(
                method.clone(),
                &path,
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(
                Self::decode_common_error(method, path, "get_terse_bucket_config", resp).await,
            );
        }

        parse_response_json(resp).await
    }
}

pub(crate) async fn parse_response_json<T: DeserializeOwned>(resp: Response) -> error::Result<T> {
    let body = resp
        .bytes()
        .await
        .map_err(|e| error::Error::new_message_error(format!("could not read response: {e}")))?;

    serde_json::from_slice(&body)
        .map_err(|e| error::Error::new_message_error(format!("could not parse response: {e}")))
}

#[derive(Deserialize)]
struct ServerErrors {
    errors: HashMap<String, String>,
}
