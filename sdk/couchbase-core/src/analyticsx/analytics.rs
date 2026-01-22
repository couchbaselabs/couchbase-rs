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
use crate::analyticsx::error;
use crate::analyticsx::error::Error;
use crate::analyticsx::query_options::{GetPendingMutationsOptions, PingOptions, QueryOptions};
use crate::analyticsx::query_respreader::QueryRespReader;
use crate::httpx::client::Client;
use crate::httpx::request::{Auth, OnBehalfOfInfo, Request};
use crate::httpx::response::Response;
use bytes::Bytes;
use http::Method;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct Query<C: Client> {
    pub http_client: Arc<C>,
    pub user_agent: String,
    pub endpoint: String,
    pub auth: Auth,
}

impl<C: Client> Query<C> {
    pub fn new_request(
        &self,
        method: Method,
        path: impl Into<String>,
        content_type: impl Into<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        body: Option<Bytes>,
    ) -> Request {
        let auth = if let Some(obo) = on_behalf_of {
            Auth::OnBehalfOf(OnBehalfOfInfo {
                username: obo.username,
                password_or_domain: obo.password_or_domain,
            })
        } else {
            self.auth.clone()
        };

        Request::new(method, format!("{}/{}", self.endpoint, path.into()))
            .auth(auth)
            .user_agent(self.user_agent.clone())
            .content_type(content_type.into())
            .body(body)
    }

    pub async fn execute(
        &self,
        method: Method,
        path: impl Into<String>,
        content_type: impl Into<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        body: Option<Bytes>,
    ) -> crate::httpx::error::Result<Response> {
        let req = self.new_request(method, path, content_type, on_behalf_of, body);

        self.http_client.execute(req).await
    }

    pub async fn query(&self, opts: &QueryOptions) -> error::Result<QueryRespReader> {
        let statement = if let Some(statement) = &opts.statement {
            statement.clone()
        } else {
            String::new()
        };

        //TODO: this needs re-embedding into options
        let client_context_id = if let Some(id) = &opts.client_context_id {
            id.clone()
        } else {
            Uuid::new_v4().to_string()
        };

        let on_behalf_of = opts.on_behalf_of.clone();

        let mut serialized = serde_json::to_value(opts)
            .map_err(|e| Error::new_encoding_error(format!("failed to encode options: {e}")))?;

        let mut obj = serialized.as_object_mut().unwrap();
        let mut client_context_id_entry = obj.get("client_context_id");
        if client_context_id_entry.is_none() {
            obj.insert(
                "client_context_id".to_string(),
                Value::String(client_context_id.clone()),
            );
        }

        if let Some(named_args) = &opts.named_args {
            for (k, v) in named_args.iter() {
                let key = if k.starts_with('$') {
                    k.clone()
                } else {
                    format!("${k}")
                };
                obj.insert(key, v.clone());
            }
        }

        if let Some(raw) = &opts.raw {
            for (k, v) in raw.iter() {
                obj.insert(k.to_string(), v.clone());
            }
        }

        let body =
            Bytes::from(serde_json::to_vec(&serialized).map_err(|e| {
                Error::new_encoding_error(format!("failed to encode options: {e}"))
            })?);

        let res = match self
            .execute(
                Method::POST,
                "analytics/service",
                "application/json",
                on_behalf_of,
                Some(body),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(Error::new_http_error(
                    e,
                    self.endpoint.to_string(),
                    statement,
                    client_context_id,
                ));
            }
        };

        QueryRespReader::new(res, &self.endpoint, statement, client_context_id).await
    }

    pub async fn get_pending_mutations(
        &self,
        opts: &GetPendingMutationsOptions<'_>,
    ) -> error::Result<HashMap<String, HashMap<String, i64>>> {
        let res = match self
            .execute(
                Method::GET,
                "analytics/node/agg/stats/remaining",
                "application/json",
                opts.on_behalf_of.cloned(),
                None,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(Error::new_http_error(
                    e,
                    self.endpoint.to_string(),
                    None,
                    None,
                ));
            }
        };

        if !res.status().is_success() {
            return Err(Error::new_message_error(
                format!(
                    "get_pending_mutations failed with status code: {}",
                    res.status()
                ),
                Some(self.endpoint.clone()),
                None,
                None,
            ));
        }

        let pending = serde_json::from_slice(
            &res.bytes()
                .await
                .map_err(|e| Error::new_http_error(e, self.endpoint.clone(), None, None))?,
        )
        .map_err(|e| {
            Error::new_message_error(
                format!("failed to decode get_pending_mutations response: {}", e),
                self.endpoint.clone(),
                None,
                None,
            )
        })?;

        Ok(pending)
    }

    pub async fn ping(&self, opts: &PingOptions<'_>) -> error::Result<()> {
        let res = match self
            .execute(
                Method::GET,
                "admin/ping",
                "",
                opts.on_behalf_of.cloned(),
                None,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(Error::new_http_error(
                    e,
                    self.endpoint.to_string(),
                    None,
                    None,
                ));
            }
        };

        if res.status().is_success() {
            return Ok(());
        }

        Err(Error::new_message_error(
            format!("ping failed with status code: {}", res.status()),
            Some(self.endpoint.clone()),
            None,
            None,
        ))
    }
}
