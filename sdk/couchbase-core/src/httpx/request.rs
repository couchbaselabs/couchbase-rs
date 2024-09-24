use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use http::header::{CONTENT_TYPE, USER_AGENT};
use http::request::Builder;
use serde::Serialize;
use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct Request {
    #[builder(setter(!strip_option))]
    pub method: http::Method,
    #[builder(setter(!strip_option))]
    pub uri: String,
    pub auth: Option<Auth>,
    pub user_agent: Option<String>,
    pub content_type: Option<String>,
    pub body: Option<Bytes>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

#[derive(PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum Auth {
    BasicAuth(BasicAuth),
    OnBehalfOf(OnBehalfOfInfo),
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct OnBehalfOfInfo {
    pub username: String,
    pub password_or_domain: OboPasswordOrDomain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum OboPasswordOrDomain {
    Password(String),
    Domain(String),
}
