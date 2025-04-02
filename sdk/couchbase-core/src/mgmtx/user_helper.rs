use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::error;
use crate::mgmtx::mgmt::Management;
use crate::mgmtx::node_target::NodeTarget;
use crate::mgmtx::options::{EnsureUserPollOptions, GetUserOptions};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EnsureUserHelper<'a> {
    user_agent: &'a str,
    username: &'a str,
    auth_domain: &'a str,
    want_missing: bool,

    on_behalf_of_info: Option<&'a OnBehalfOfInfo>,

    confirmed_endpoints: Vec<&'a str>,
}

impl<'a> EnsureUserHelper<'a> {
    pub fn new(
        user_agent: &'a str,
        username: &'a str,
        auth_domain: &'a str,
        want_missing: bool,
        on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    ) -> Self {
        Self {
            user_agent,
            username,
            auth_domain,
            want_missing,
            on_behalf_of_info,
            confirmed_endpoints: vec![],
        }
    }

    async fn poll_one<C: Client>(
        &self,
        client: Arc<C>,
        target: &NodeTarget,
    ) -> error::Result<bool> {
        let resp = Management {
            http_client: client,
            user_agent: self.user_agent.to_string(),
            endpoint: target.endpoint.to_string(),
            username: target.username.to_string(),
            password: target.password.to_string(),
        }
        .get_user(&GetUserOptions {
            on_behalf_of_info: self.on_behalf_of_info,
            username: self.username,
            auth_domain: self.auth_domain,
        })
        .await;

        match resp {
            Ok(_) => Ok(true),
            Err(e) => {
                if let error::ErrorKind::Server(e) = e.kind() {
                    if e.kind() == &error::ServerErrorKind::UserNotFound {
                        if self.want_missing {
                            return Ok(true);
                        }

                        return Ok(false);
                    }
                }

                Err(e)
            }
        }
    }

    pub async fn poll<C: Client>(
        &mut self,
        opts: &'a EnsureUserPollOptions<C>,
    ) -> error::Result<bool> {
        let mut filtered_targets = vec![];
        for target in &opts.targets {
            if !self.confirmed_endpoints.contains(&target.endpoint.as_str()) {
                filtered_targets.push(target);
            }
        }

        let mut success_endpoints = vec![];
        for target in &opts.targets {
            if self.poll_one(opts.client.clone(), target).await? {
                success_endpoints.push(target.endpoint.as_str());
            }
        }

        self.confirmed_endpoints
            .extend_from_slice(success_endpoints.as_slice());

        Ok(success_endpoints.len() == filtered_targets.len())
    }
}
