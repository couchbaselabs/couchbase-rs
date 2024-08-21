use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{StatusCode};
use serde::de::DeserializeOwned;
use typed_builder::TypedBuilder;
use crate::httpx::error::ErrorKind::Generic;
use crate::httpx::error::{Result as HttpxResult};

#[derive(Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct Request {
    pub method: http::Method,
    pub uri: String,
    pub auth: Option<Auth>,
    pub user_agent: Option<String>,
    pub content_type: Option<String>,
    pub body: Option<String>,
}

#[derive(PartialEq, Eq, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

#[derive(PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum Auth {
    BasicAuth(BasicAuth),
    OnBehalfOf(OnBehalfOfInfo)
}

#[derive(PartialEq, Eq, Debug, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
pub struct OnBehalfOfInfo {
    pub username: String,
    pub password_or_domain: OboPasswordOrDomain,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OboPasswordOrDomain {
    Password(String),
    Domain(String),
}

#[derive(PartialEq, Eq, Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct RequestCreator {
    endpoint: String,
    user_agent: Option<String>,
    basic_auth: Option<BasicAuth>,
}

#[derive(Debug)]
pub struct Response<R> {
    pub inner: R,
}

#[async_trait]
pub trait ResponseProvider {
    fn get_status(self) -> StatusCode;

    async fn get_text(self) -> HttpxResult<String>;

    fn get_stream(self) -> Box<dyn Stream<Item = HttpxResult<Bytes>> + Unpin>;

    async fn read_response_as_json<T: DeserializeOwned>(self) -> HttpxResult<T>;
}

#[async_trait]
impl ResponseProvider for reqwest::Response {
    fn get_status(self) -> StatusCode {
        self.status()
    }
    async fn get_text(self) -> HttpxResult<String> {
        let text = self.text().await?;
        Ok(text)
    }

    fn get_stream(self) -> Box<dyn Stream<Item=HttpxResult<Bytes>> + Unpin> {
        let x = self.bytes_stream().map(|result| {
            result.map_err(|e| Generic { msg: e.to_string() }.into())
        });
        Box::new(x)
    }

    async fn read_response_as_json<T: DeserializeOwned>(self) -> HttpxResult<T> {
        self.json::<T>()
            .await
            .map_err(|e| Generic { msg: e.to_string() }.into())
    }
}

impl RequestCreator {
    pub fn new_request(
        &self,
        method: http::Method,
        path: String,
        content_type: Option<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        body: Option<String>
    ) -> Request {
        let uri = format!("{}{}", self.endpoint, path);

        let mut req_auth = None;
        if let Some(auth) = self.basic_auth.clone() {
            req_auth = Some(Auth::BasicAuth(auth));
        } else if let Some(obo) = on_behalf_of {
            req_auth = Some(Auth::OnBehalfOf(obo));
        }

        Request {
            user_agent: self.user_agent.clone(),
            method,
            content_type,
            uri,
            body,
            auth: req_auth,
        }
    }
}
