use bytes::Bytes;
use serde::Serialize;
use std::collections::HashMap;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[derive(Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)), mutators(
        pub fn add_header(&mut self, key: impl Into<String>, value: impl Into<String>) {
            self.headers.insert(key.into(), value.into());
        }
))]
#[non_exhaustive]
pub struct Request {
    pub method: http::Method,
    pub uri: String,
    pub auth: Option<Auth>,
    pub user_agent: Option<String>,
    pub content_type: Option<String>,
    pub body: Option<Bytes>,
    #[builder(via_mutators, default = HashMap::new())]
    pub headers: HashMap<String, String>,
    #[builder(default = Uuid::new_v4().to_string())]
    pub unique_id: String,
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
