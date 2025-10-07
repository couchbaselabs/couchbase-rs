/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::authenticator::Authenticator;
use crate::error;
use crate::error::ErrorKind;
use crate::httpx::client::Client;
use crate::retrybesteffort::BackoffCalculator;
use crate::service_type::ServiceType;
use crate::util::get_host_port_from_uri;
use log::debug;
use rand::Rng;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

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
            return Err(ErrorKind::EndpointNotKnown {
                endpoint: endpoint_id.to_string(),
            }
            .into());
        };

        let host = get_host_port_from_uri(found_endpoint)?;
        let user_pass = match state.authenticator.as_ref() {
            Authenticator::PasswordAuthenticator(authenticator) => {
                authenticator.get_credentials(&self.service_type, host)?
            }
            Authenticator::CertificateAuthenticator(a) => {
                a.get_credentials(&self.service_type, host)?
            }
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
        endpoint_ids_to_ignore: &[String],
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

        let endpoint_idx = rand::rng().random_range(0..remaining_endpoints.len());
        let endpoint_id = endpoint_ids[endpoint_idx];
        let endpoint = remaining_endpoints[endpoint_id];

        let host = get_host_port_from_uri(endpoint)?;
        let user_pass = match state.authenticator.as_ref() {
            Authenticator::PasswordAuthenticator(authenticator) => {
                authenticator.get_credentials(&self.service_type, host)?
            }
            Authenticator::CertificateAuthenticator(a) => {
                a.get_credentials(&self.service_type, host)?
            }
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

        let (client, endpoint_properties) = if let Some(selected) = self.select_endpoint(&[])? {
            selected
        } else {
            return Err(ErrorKind::ServiceNotAvailable {
                service: self.service_type.clone(),
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

    pub fn get_all_targets<T>(
        &self,
        endpoint_ids_to_ignore: &[String],
    ) -> error::Result<(Arc<C>, Vec<T>)>
    where
        T: NodeTarget,
    {
        let guard = self.state.lock().unwrap();
        let state = &*guard;

        let mut remaining_endpoints = HashMap::new();
        for (ep_id, endpoint) in &state.endpoints {
            if !endpoint_ids_to_ignore.contains(ep_id) {
                remaining_endpoints.insert(ep_id, endpoint);
            }
        }

        let mut targets = Vec::with_capacity(remaining_endpoints.len());
        for (_ep_id, endpoint) in remaining_endpoints {
            let host = get_host_port_from_uri(endpoint)?;

            let user_pass = match state.authenticator.as_ref() {
                Authenticator::PasswordAuthenticator(authenticator) => {
                    authenticator.get_credentials(&self.service_type, host)?
                }
                Authenticator::CertificateAuthenticator(a) => {
                    a.get_credentials(&self.service_type, host)?
                }
            };

            targets.push(T::new(
                endpoint.clone(),
                user_pass.username,
                user_pass.password,
            ));
        }

        Ok((self.client.clone(), targets))
    }

    pub async fn ensure_resource<B, Fut, T>(
        &self,
        backoff: B,
        mut poll_fn: impl FnMut(Arc<C>, Vec<T>) -> Fut + Send + Sync,
    ) -> error::Result<()>
    where
        B: BackoffCalculator,
        Fut: Future<Output = error::Result<bool>> + Send,
        T: NodeTarget,
    {
        let mut attempt_idx = 0;
        loop {
            let (client, targets) = self.get_all_targets::<T>(&[])?;

            let success = poll_fn(client, targets).await?;
            if success {
                return Ok(());
            }

            let sleep = backoff.backoff(attempt_idx);
            debug!("Retrying ensure_resource, after {sleep:?}, attempt number: {attempt_idx}");

            tokio::time::sleep(sleep).await;
            attempt_idx += 1;
        }
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

pub(crate) trait NodeTarget {
    fn new(endpoint: String, username: String, password: String) -> Self;
}
