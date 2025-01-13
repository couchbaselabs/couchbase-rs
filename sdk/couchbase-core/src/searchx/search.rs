use crate::httpx::client::Client;
use crate::httpx::request::{Auth, BasicAuth, OnBehalfOfInfo, Request};
use crate::httpx::response::Response;
use crate::searchx::error;
use crate::searchx::index::Index;
use crate::searchx::query_options::QueryOptions;
use crate::searchx::search_respreader::SearchRespReader;
use bytes::Bytes;
use http::{Method, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct Search<C: Client> {
    pub http_client: Arc<C>,
    pub user_agent: String,
    pub endpoint: String,
    pub username: String,
    pub password: String,

    pub vector_search_enabled: bool,
}

impl<C: Client> Search<C> {
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
    ) -> crate::httpx::error::Result<Response> {
        let req = self.new_request(method, path, content_type, on_behalf_of, headers, body);

        self.http_client.execute(req).await
    }

    pub async fn query(&self, opts: &QueryOptions) -> error::Result<SearchRespReader> {
        if !self.vector_search_enabled && (opts.knn.is_some() || opts.knn_operator.is_some()) {
            return Err(error::Error {
                kind: Box::new(error::ErrorKind::UnsupportedFeature {
                    feature: "vector search".to_string(),
                }),
                endpoint: self.endpoint.clone(),
                status_code: None,
                source: None,
            });
        }

        let req_uri = if let Some(bucket) = &opts.bucket_name {
            if let Some(scope) = &opts.scope_name {
                format!(
                    "api/bucket/{}/scope/{}/index/{}/query",
                    bucket, scope, opts.index_name
                )
            } else {
                return Err(error::Error::new_generic_error(
                    "must specify both or neither scope and bucket names",
                    "".to_string(),
                ));
            }
        } else {
            format!("api/index/{}/query", &opts.index_name)
        };

        let on_behalf_of = opts.on_behalf_of.clone();

        let body = serde_json::to_vec(&opts).map_err(|e| error::Error {
            kind: Box::new(error::ErrorKind::Json {
                msg: format!("could not serialize query options: {}", e),
            }),
            endpoint: self.endpoint.clone(),
            status_code: None,
            source: Some(Arc::new(e)),
        })?;

        let res = self
            .execute(
                Method::POST,
                req_uri,
                "application/json",
                on_behalf_of,
                None,
                Some(Bytes::from(body)),
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, &self.endpoint))?;

        SearchRespReader::new(res, &self.endpoint).await
    }

    pub async fn upsert_index<'a>(&self, opts: &UpsertIndexOptions<'a>) -> error::Result<()> {
        let req_uri = if let Some(bucket) = &opts.bucket_name {
            if let Some(scope) = &opts.scope_name {
                format!(
                    "api/bucket/{}/scope/{}/index/{}",
                    bucket, scope, &opts.index.name
                )
            } else {
                return Err(error::Error::new_generic_error(
                    "must specify both or neither scope and bucket names",
                    "".to_string(),
                ));
            }
        } else {
            format!("api/index/{}", &opts.index.name)
        };

        let body = serde_json::to_vec(&opts.index).map_err(|e| error::Error {
            kind: Box::new(error::ErrorKind::Json {
                msg: format!("could not serialize index: {}", e),
            }),
            endpoint: self.endpoint.clone(),
            status_code: None,
            source: Some(Arc::new(e)),
        })?;

        let mut headers = HashMap::new();
        headers.insert("cache-control", "no-cache");

        let res = self
            .execute(
                Method::PUT,
                req_uri,
                "application/json",
                // TODO: change when we change ownership on execute
                opts.on_behalf_of.cloned(),
                Some(headers),
                Some(Bytes::from(body)),
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, &self.endpoint))?;

        if res.status() != 200 {
            return Err(decode_response_error(res, self.endpoint.clone()).await);
        }

        Ok(())
    }

    pub async fn delete_index<'a>(&self, opts: &DeleteIndexOptions<'a>) -> error::Result<()> {
        let req_uri = if let Some(bucket) = &opts.bucket_name {
            if let Some(scope) = &opts.scope_name {
                format!(
                    "api/bucket/{}/scope/{}/index/{}",
                    bucket, scope, &opts.index_name
                )
            } else {
                return Err(error::Error::new_generic_error(
                    "must specify both or neither scope and bucket names",
                    "".to_string(),
                ));
            }
        } else {
            format!("api/index/{}", &opts.index_name)
        };

        let mut headers = HashMap::new();
        headers.insert("cache-control", "no-cache");

        let res = self
            .execute(
                Method::DELETE,
                req_uri,
                "application/json",
                // TODO: change when we change ownership on execute
                opts.on_behalf_of.cloned(),
                Some(headers),
                None,
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, &self.endpoint))?;

        if res.status() != 200 {
            return Err(decode_response_error(res, self.endpoint.clone()).await);
        }

        Ok(())
    }
}

pub(crate) async fn decode_response_error(response: Response, endpoint: String) -> error::Error {
    let status = response.status();
    let body = match response.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return error::Error {
                kind: Box::new(error::ErrorKind::Generic {
                    msg: format!("could not parse response body: {}", e),
                }),
                endpoint,
                status_code: Some(status),
                source: Some(Arc::new(e)),
            }
        }
    };

    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s.to_lowercase(),
        Err(e) => {
            return error::Error {
                kind: Box::new(error::ErrorKind::Generic {
                    msg: format!("could not parse error response: {}", e),
                }),
                endpoint,
                status_code: Some(status),
                source: Some(Arc::new(e)),
            }
        }
    };

    decode_common_error(status, &body_str, endpoint)
}

pub(crate) fn decode_common_error(
    status: StatusCode,
    body_str: &str,
    endpoint: String,
) -> error::Error {
    let error_kind = if status == 401 || status == 403 {
        error::ServerErrorKind::AuthenticationFailure
    } else if body_str.contains("index not found") {
        error::ServerErrorKind::IndexNotFound
    } else if body_str.contains("index with the same name already exists")
        || (body_str.contains("current index uuuid")
            && body_str.contains("did not match input uuid"))
    {
        error::ServerErrorKind::IndexExists
    } else if body_str.contains("unknown indextype") {
        error::ServerErrorKind::UnknownIndexType
    } else if body_str.contains("error obtaining vbucket count for bucket")
        || body_str.contains("requested resource not found")
        || body_str.contains("non existent bucket")
    {
        // In server 7.2.4 and later, ns_server produces "non existent bucket" instead of "requested resource not found".
        // However in 7.6.0, FTS reordered their handling here and produces the "vbucket count for bucket" instead.
        // So we need to check for all the variants of this.
        error::ServerErrorKind::SourceNotFound
    } else if body_str
        .contains("failed to connect to or retrieve information from source, sourcetype")
    {
        error::ServerErrorKind::SourceTypeIncorrect
    } else if body_str.contains("no planpindexes for indexname") {
        error::ServerErrorKind::NoIndexPartitionsPlanned
    } else if body_str.contains("no local pindexes found") {
        error::ServerErrorKind::NoIndexPartitionsFound
    } else if status == 500 {
        error::ServerErrorKind::Internal
    } else if status == 429 {
        if body_str.contains("num_concurrent_requests")
            || body_str.contains("num_queries_per_min")
            || body_str.contains("ingress_mib_per_min")
            || body_str.contains("egress_mib_per_min")
        {
            error::ServerErrorKind::RateLimitedFailure
        } else {
            error::ServerErrorKind::Unknown
        }
    } else {
        error::ServerErrorKind::Unknown
    };

    error::Error::new_server_error(error_kind, body_str, endpoint, status)
}

#[derive(Debug)]
#[non_exhaustive]
pub struct UpsertIndexOptions<'a> {
    pub index: &'a Index,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> UpsertIndexOptions<'a> {
    pub fn new(index: &'a Index) -> Self {
        Self {
            index,
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct DeleteIndexOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> DeleteIndexOptions<'a> {
    pub fn new(index_name: &'a str) -> Self {
        Self {
            index_name,
            bucket_name: None,
            scope_name: None,
            on_behalf_of: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<&'a str>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn scope_name(mut self, scope_name: impl Into<Option<&'a str>>) -> Self {
        self.scope_name = scope_name.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}
