use crate::analyticsx::error;
use crate::analyticsx::error::Error;
use crate::analyticsx::query_options::QueryOptions;
use crate::analyticsx::query_respreader::QueryRespReader;
use crate::httpx::client::Client;
use crate::httpx::request::{Auth, BasicAuth, OnBehalfOfInfo, Request};
use crate::httpx::response::Response;
use bytes::Bytes;
use http::Method;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct Analytics<C: Client> {
    pub http_client: Arc<C>,
    pub user_agent: String,
    pub endpoint: String,
    pub username: String,
    pub password: String,
}

impl<C: Client> Analytics<C> {
    pub fn new_request(
        &self,
        method: Method,
        path: impl AsRef<str>,
        content_type: impl AsRef<str>,
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

        let mut builder = Request::builder()
            .method(method)
            .uri(format!("{}/{}", self.endpoint, path.as_ref()))
            .auth(auth)
            .user_agent(self.user_agent.clone())
            .content_type(content_type.as_ref().to_string())
            .body(body);

        if let Some(headers) = headers {
            for (key, value) in headers.into_iter() {
                builder = builder.add_header(key, value);
            }
        }

        builder.build()
    }

    pub async fn execute(
        &self,
        method: Method,
        path: impl AsRef<str>,
        content_type: impl AsRef<str>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        headers: Option<HashMap<&str, &str>>,
        body: Option<Bytes>,
    ) -> crate::httpx::error::Result<Response> {
        let req = self.new_request(method, path, content_type, on_behalf_of, headers, body);

        self.http_client.execute(req).await
    }

    pub async fn query<'a>(&self, opts: &QueryOptions<'a>) -> error::Result<QueryRespReader> {
        let client_context_id = opts.client_context_id.map(|c| c.to_string());
        let statement = opts.statement;
        let on_behalf_of = opts.on_behalf_of;

        let body = serde_json::to_vec(opts).map_err(|e| {
            Error::new_generic_error(
                e.to_string(),
                &self.endpoint,
                statement,
                client_context_id.clone().map(|s| s.to_string()),
            )
        })?;

        let priority = opts.priority.map(|p| p.to_string());

        let headers = if let Some(priority) = &priority {
            let mut headers = HashMap::new();
            headers.insert("Analytics-Priority", priority.as_str());
            Some(headers)
        } else {
            None
        };

        let res = self
            .execute(
                Method::POST,
                "query/service",
                "application/json",
                on_behalf_of.cloned(),
                headers,
                Some(Bytes::from(body)),
            )
            .await
            .map_err(|e| {
                Error::new_generic_error_with_source(
                    e.to_string(),
                    &self.endpoint,
                    statement,
                    client_context_id.clone(),
                    Arc::new(e),
                )
            })?;

        QueryRespReader::new(res, &self.endpoint, statement, client_context_id).await
    }
}