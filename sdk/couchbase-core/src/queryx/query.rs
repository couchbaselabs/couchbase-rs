use std::sync::Arc;

use bytes::Bytes;
use http::Method;
use serde_json::Value;
use uuid::Uuid;

use crate::httpx::client::Client;
use crate::httpx::request::{Auth, BasicAuth, OnBehalfOfInfo, Request};
use crate::httpx::response::Response;
use crate::queryx::error;
use crate::queryx::error::Error;
use crate::queryx::query_options::QueryOptions;
use crate::queryx::query_respreader::QueryRespReader;

#[derive(Debug)]
pub struct Query<C: Client> {
    pub http_client: Arc<C>,
    pub user_agent: String,
    pub endpoint: String,
    pub username: String,
    pub password: String,
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
            Auth::BasicAuth(BasicAuth {
                username: self.username.clone(),
                password: self.password.clone(),
            })
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

        //TODO; this needs re-embedding into options
        let client_context_id = if let Some(id) = &opts.client_context_id {
            id.clone()
        } else {
            Uuid::new_v4().to_string()
        };

        let on_behalf_of = opts.on_behalf_of.clone();

        let mut serialized = serde_json::to_value(opts).map_err(|e| {
            Error::new_generic_error(
                e.to_string(),
                &self.endpoint,
                &statement,
                &client_context_id,
            )
        })?;

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
                let key = if k.starts_with("$") {
                    k.clone()
                } else {
                    format!("${}", k)
                };
                obj.insert(key, v.clone());
            }
        }

        if let Some(raw) = &opts.raw {
            for (k, v) in raw.iter() {
                obj.insert(k.to_string(), v.clone());
            }
        }

        let body = Bytes::from(serde_json::to_vec(&serialized).map_err(|e| {
            Error::new_generic_error(
                e.to_string(),
                &self.endpoint,
                &statement,
                &client_context_id,
            )
        })?);

        let res = self
            .execute(
                Method::POST,
                "query/service",
                "application/json",
                on_behalf_of,
                Some(body),
            )
            .await
            .map_err(|e| {
                Error::new_http_error(e, &self.endpoint, &statement, &client_context_id)
            })?;

        QueryRespReader::new(res, &self.endpoint, statement, client_context_id).await
    }
}
