use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use rand::Rng;

use crate::authenticator::Authenticator;
use crate::error;
use crate::error::ErrorKind;
use crate::httpx::client::Client;
use crate::service_type::ServiceType;
use crate::util::get_host_from_uri;

pub(crate) struct HttpComponent<C: Client> {
    service_type: ServiceType,
    user_agent: String,
    client: Arc<C>,

    state: Mutex<HttpComponentState>,
}

pub(crate) struct HttpComponentState {
    endpoints: HashMap<String, String>,
    authenticator: Arc<Authenticator>,
}

pub(crate) struct HttpEndpointProperties {
    pub username: String,
    pub password: String,
    pub endpoint: String,
    pub endpoint_id: Option<String>,
}

impl<C: Client> HttpComponent<C> {
    pub fn new(
        service_type: ServiceType,
        user_agent: String,
        client: Arc<C>,
        state: HttpComponentState,
    ) -> Self {
        Self {
            service_type,
            user_agent,
            client,
            state: Mutex::new(state),
        }
    }

    pub fn reconfigure(&self, state: HttpComponentState) {
        let mut state_guard = self.state.lock().unwrap();
        *state_guard = state;
    }

    pub fn select_specific_endpoint(
        &self,
        endpoint_id: &str,
    ) -> error::Result<(Arc<C>, HttpEndpointProperties)> {
        let mut guard = self.state.lock().unwrap();
        let state = &*guard;

        let mut found_endpoint = None;
        for (ep_id, endpoint) in &state.endpoints {
            if ep_id == endpoint_id {
                found_endpoint = Some(endpoint);
            }
        }

        let found_endpoint = if let Some(ep) = found_endpoint {
            ep
        } else {
            return Err(ErrorKind::Generic {
                msg: "invalid endpoint".to_string(),
            }
            .into());
        };

        let user_pass = if let Some(host) = get_host_from_uri(found_endpoint)? {
            match state.authenticator.as_ref() {
                Authenticator::PasswordAuthenticator(authenticator) => {
                    authenticator.get_credentials(self.service_type, host)?
                }
            }
        } else {
            return Err(ErrorKind::Generic {
                msg: "invalid endpoint".to_string(),
            }
            .into());
        };

        Ok((
            self.client.clone(),
            HttpEndpointProperties {
                endpoint_id: None,
                endpoint: found_endpoint.clone(),
                username: user_pass.username,
                password: user_pass.password,
            },
        ))
    }

    pub fn select_endpoint(
        &self,
        endpoint_ids_to_ignore: Vec<String>,
    ) -> error::Result<Option<(Arc<C>, HttpEndpointProperties)>> {
        let mut guard = self.state.lock().unwrap();
        let state = &*guard;

        // If there are no endpoints to query, we can't proceed.
        if state.endpoints.is_empty() {
            return Ok(None);
        }

        let mut remaining_endpoints = HashMap::new();
        let mut endpoint_ids = vec![];
        for (ep_id, endpoint) in &state.endpoints {
            if !endpoint_ids_to_ignore.contains(ep_id) {
                remaining_endpoints.insert(ep_id, endpoint);
                endpoint_ids.push(ep_id);
            }
        }

        // If there are no more endpoints to try, we can't proceed.
        if remaining_endpoints.is_empty() {
            return Ok(None);
        }

        let endpoint_idx = rand::thread_rng().gen_range(0..remaining_endpoints.len());
        let endpoint_id = endpoint_ids[endpoint_idx];
        let endpoint = remaining_endpoints[endpoint_id];

        let user_pass = if let Some(host) = get_host_from_uri(endpoint)? {
            match state.authenticator.as_ref() {
                Authenticator::PasswordAuthenticator(authenticator) => {
                    authenticator.get_credentials(self.service_type, host)?
                }
            }
        } else {
            return Err(ErrorKind::Generic {
                msg: "invalid endpoint".to_string(),
            }
            .into());
        };

        Ok(Some((
            self.client.clone(),
            HttpEndpointProperties {
                endpoint_id: Some(endpoint_id.clone()),
                endpoint: endpoint.clone(),
                username: user_pass.username,
                password: user_pass.password,
            },
        )))
    }

    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    pub async fn orchestrate_endpoint<Resp, Fut>(
        &self,
        endpoint_id: Option<String>,
        operation: impl Fn(Arc<C>, String, String, String, String) -> Fut + Send + Sync,
    ) -> error::Result<Resp>
    where
        C: Client,
        Fut: Future<Output = error::Result<Resp>> + Send,
        Resp: Send,
    {
        if let Some(endpoint_id) = endpoint_id {
            let (client, endpoint_properties) = self.select_specific_endpoint(&endpoint_id)?;

            return operation(
                client,
                endpoint_id,
                endpoint_properties.endpoint,
                endpoint_properties.username,
                endpoint_properties.password,
            )
            .await;
        }

        let (client, endpoint_properties) = if let Some(selected) = self.select_endpoint(vec![])? {
            selected
        } else {
            return Err(ErrorKind::ServiceNotAvailable {
                service: self.service_type,
            }
            .into());
        };

        operation(
            client,
            endpoint_properties.endpoint_id.unwrap_or_default(),
            endpoint_properties.endpoint,
            endpoint_properties.username,
            endpoint_properties.password,
        )
        .await
    }
}

impl HttpComponentState {
    pub fn new(endpoints: HashMap<String, String>, authenticator: Arc<Authenticator>) -> Self {
        Self {
            endpoints,
            authenticator,
        }
    }
}
