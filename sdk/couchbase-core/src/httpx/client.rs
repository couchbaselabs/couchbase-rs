use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use http::header::{CONTENT_TYPE, USER_AGENT};
use reqwest::redirect::Policy;

use crate::httpx::error::ErrorKind::{Connect, Generic};
use crate::httpx::error::Result as HttpxResult;
use crate::httpx::request::{Auth, OboPasswordOrDomain, Request};
use crate::httpx::response::Response;

#[async_trait]
pub trait Client: Send + Sync {
    async fn execute(&self, req: Request) -> HttpxResult<Response>;
}

#[derive(Debug)]
pub struct ReqwestClient {
    inner: ArcSwap<reqwest::Client>,
}

impl ReqwestClient {
    pub fn new() -> HttpxResult<Self> {
        let inner = Self::new_client()?;
        Ok(Self {
            inner: ArcSwap::from_pointee(inner),
        })
    }

    // TODO: once options are supported we need to check if they've changed before creating
    // a new client provider.
    pub fn reconfigure(&self) -> HttpxResult<()> {
        let new_inner = Self::new_client()?;
        let old_inner = self.inner.swap(Arc::new(new_inner));

        // TODO: This will close any in flight requests, do we actually need to do this or will
        // it get dropped once requests complete anyway?
        drop(old_inner);

        Ok(())
    }

    fn new_client() -> HttpxResult<reqwest::Client> {
        let client = reqwest::Client::builder()
            .redirect(Policy::limited(10))
            .build()
            .map_err(|err| Generic {
                msg: err.to_string(),
            })?;
        Ok(client)
    }
}

#[async_trait]
impl Client for ReqwestClient {
    async fn execute(&self, req: Request) -> HttpxResult<Response> {
        let inner = self.inner.load();

        let mut builder = inner.request(req.method, req.uri);

        if let Some(body) = req.body {
            builder = builder.body(body);
        }

        if let Some(content_type) = req.content_type {
            builder = builder.header(CONTENT_TYPE, content_type);
        }

        if let Some(user_agent) = req.user_agent {
            builder = builder.header(USER_AGENT, user_agent);
        }

        if let Some(auth) = &req.auth {
            match auth {
                Auth::BasicAuth(basic) => {
                    builder = builder.basic_auth(&basic.username, Some(&basic.password))
                }
                Auth::OnBehalfOf(obo) => {
                    match &obo.password_or_domain {
                        OboPasswordOrDomain::Password(password) => {
                            // If we have the OBO users password, we just directly set the basic auth
                            // on the request with those credentials rather than using an on-behalf-of
                            // header.  This enables support for older server versions.
                            builder = builder.basic_auth(&obo.username, Some(password));
                        }
                        OboPasswordOrDomain::Domain(domain) => {
                            // Otherwise we send the user/domain using an OBO header.
                            let obo_hdr_string =
                                BASE64_STANDARD.encode(format!("{}:{}", obo.username, domain));
                            builder = builder.header("cb-on-behalf-of", obo_hdr_string);
                        }
                    }
                }
            }
        }

        match builder.send().await {
            Ok(response) => Ok(Response::from(response)),
            // TODO improve error handling
            Err(err) => {
                if err.is_connect() {
                    Err(Connect {
                        msg: err.to_string(),
                    }
                    .into())
                } else {
                    Err(Generic {
                        msg: err.to_string(),
                    }
                    .into())
                }
            }
        }
    }
}
