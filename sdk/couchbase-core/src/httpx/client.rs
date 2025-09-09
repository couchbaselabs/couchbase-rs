use crate::httpx::error::ErrorKind::Connection;
use crate::httpx::error::{Error, Result as HttpxResult};
use crate::httpx::request::{Auth, OboPasswordOrDomain, Request};
use crate::httpx::response::Response;
use crate::tls_config::TlsConfig;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use http::header::{CONTENT_TYPE, USER_AGENT};
use log::{debug, trace};
use reqwest::redirect::Policy;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[async_trait]
pub trait Client: Send + Sync {
    async fn execute(&self, req: Request) -> HttpxResult<Response>;
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ClientConfig {
    pub tls_config: Option<TlsConfig>,
    pub idle_connection_timeout: Duration,
    pub max_idle_connections_per_host: Option<usize>,
    pub tcp_keep_alive_time: Duration,
}

impl ClientConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn tls_config(mut self, tls_config: impl Into<Option<TlsConfig>>) -> Self {
        self.tls_config = tls_config.into();
        self
    }

    pub fn idle_connection_timeout(mut self, timeout: Duration) -> Self {
        self.idle_connection_timeout = timeout;
        self
    }

    pub fn max_idle_connections_per_host(mut self, max_idle_connections_per_host: usize) -> Self {
        self.max_idle_connections_per_host = Some(max_idle_connections_per_host);
        self
    }
}

#[derive(Debug)]
pub struct ReqwestClient {
    inner: ArcSwap<reqwest::Client>,
    client_id: String,
}

impl ReqwestClient {
    pub fn new(cfg: ClientConfig) -> HttpxResult<Self> {
        let inner = Self::new_client(cfg)?;
        Ok(Self {
            inner: ArcSwap::from_pointee(inner),
            client_id: Uuid::new_v4().to_string(),
        })
    }

    // TODO: once options are supported we need to check if they've changed before creating
    // a new client provider.
    pub fn reconfigure(&self, cfg: ClientConfig) -> HttpxResult<()> {
        let new_inner = Self::new_client(cfg)?;
        let old_inner = self.inner.swap(Arc::new(new_inner));

        // TODO: This will close any in flight requests, do we actually need to do this or will
        // it get dropped once requests complete anyway?
        drop(old_inner);

        Ok(())
    }

    fn new_client(cfg: ClientConfig) -> HttpxResult<reqwest::Client> {
        let mut builder = reqwest::Client::builder()
            .redirect(Policy::limited(10))
            .pool_idle_timeout(cfg.idle_connection_timeout)
            .tcp_keepalive(cfg.tcp_keep_alive_time);

        if let Some(max_idle) = cfg.max_idle_connections_per_host {
            builder = builder.pool_max_idle_per_host(max_idle);
        }

        if let Some(config) = cfg.tls_config {
            builder = Self::add_tls_config(builder, config);
        }

        let client = builder
            .build()
            .map_err(|e| Error::new_message_error(format!("failed to build http client {e}")))?;
        Ok(client)
    }

    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    fn add_tls_config(
        builder: reqwest::ClientBuilder,
        tls_config: TlsConfig,
    ) -> reqwest::ClientBuilder {
        // We have to deref the Arc, otherwise we'll get a runtime error from reqwest.
        builder.use_preconfigured_tls((*tls_config).clone())
    }

    #[cfg(feature = "native-tls")]
    fn add_tls_config(
        builder: reqwest::ClientBuilder,
        tls_config: TlsConfig,
    ) -> reqwest::ClientBuilder {
        builder.use_preconfigured_tls(tls_config)
    }
}

#[async_trait]
impl Client for ReqwestClient {
    async fn execute(&self, req: Request) -> HttpxResult<Response> {
        let inner = self.inner.load();

        let id = if let Some(unique_id) = req.unique_id {
            unique_id
        } else {
            Uuid::new_v4().to_string()
        };

        trace!(
            "Writing request on {} to {}. Method={}. Request id={}",
            &self.client_id,
            &req.uri,
            &req.method,
            &id
        );

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
            Ok(response) => Ok({
                trace!(
                    "Received response on {}. Request id={}. Status: {}",
                    &self.client_id,
                    &id,
                    response.status()
                );
                Response::from(response)
            }),
            Err(err) => {
                let mut msg = format!(
                    "Received error on {}. Request id={}. Err: {}",
                    &self.client_id, &id, &err,
                );

                if let Some(source) = err.source() {
                    msg = format!("{msg}. Source: {source}");
                }

                trace!("{msg}");

                if err.is_connect() {
                    Err(Error::new_connection_error(err.to_string()))
                } else {
                    Err(Error::new_message_error(err.to_string()))
                }
            }
        }
    }
}

impl Drop for ReqwestClient {
    fn drop(&mut self) {
        debug!("Dropping HTTP Client {}", &self.client_id);
    }
}
