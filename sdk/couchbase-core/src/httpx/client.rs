use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use crate::httpx::base::{Auth, OboPasswordOrDomain, Request, Response, ResponseProvider};
use crate::httpx::error::ErrorKind::{Connect, Generic};
use crate::httpx::error::Result as HttpxResult;
use reqwest::{redirect::Policy, Client as ReqwestClient};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};

#[derive(Debug, Clone)]
pub struct Client<C> {
    pub inner: C,
}

#[async_trait]
pub trait ClientProvider {
    type R: ResponseProvider;
    fn new() -> HttpxResult<Self> where Self: Sized; // TODO RSCBC-44: Add TLS
    async fn execute(self, req: Request) -> HttpxResult<Response<Self::R>>;
}

impl<C> Client<C> where C: ClientProvider,
{
    pub fn new() -> HttpxResult<Self> {
        let inner = C::new()?;
        Ok(Self {
            inner
        })
    }

    pub async fn execute(self, req: Request) -> HttpxResult<Response<C::R>> {
        self.inner.execute(req).await
    }
}

#[async_trait]
impl ClientProvider for reqwest::Client
{
    type R = reqwest::Response;
    fn new() -> HttpxResult<Self> {
        let client = ReqwestClient::builder()
            .redirect(Policy::limited(10))
            .build()
            .map_err(|err| Generic {
                msg: err.to_string(),
            })?;
        Ok(client)
    }

    async fn execute(self, req: Request) -> HttpxResult<Response<Self::R>>{
        let mut builder = self.request(req.method, req.uri);

        if let Some(body) = req.body {
            builder = builder.body(body);
        }

        if let Some(content_type) = req.content_type {
            builder = builder.header(CONTENT_TYPE, content_type);
        }

        if let Some(user_agent) = req.user_agent {
            builder = builder.header(USER_AGENT, user_agent);
        }

        if let Some(auth) = req.auth {
            match auth {
                Auth::BasicAuth(basic) => {
                    builder = builder.basic_auth(basic.username, Some(basic.password))
                }
                Auth::OnBehalfOf(obo) => {
                    match obo.password_or_domain {
                        OboPasswordOrDomain::Password(password) => {
                            // If we have the OBO users password, we just directly set the basic auth
                            // on the request with those credentials rather than using an on-behalf-of
                            // header.  This enables support for older server versions.
                            builder = builder.basic_auth(obo.username, Some(password));
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
            Ok(response) => Ok(Response { inner: response }),
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
