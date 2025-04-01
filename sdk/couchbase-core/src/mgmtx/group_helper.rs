use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::error;
use crate::mgmtx::mgmt::Management;
use crate::mgmtx::node_target::NodeTarget;
use crate::mgmtx::options::{EnsureGroupPollOptions, GetGroupOptions};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EnsureGroupHelper<'a> {
    user_agent: &'a str,
    group_name: &'a str,
    want_missing: bool,

    on_behalf_of_info: Option<&'a OnBehalfOfInfo>,

    confirmed_endpoints: Vec<&'a str>,
}

impl<'a> EnsureGroupHelper<'a> {
    pub fn new(
        user_agent: &'a str,
        group_name: &'a str,
        want_missing: bool,
        on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    ) -> Self {
        Self {
            user_agent,
            group_name,
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
        .get_group(&GetGroupOptions {
            on_behalf_of_info: self.on_behalf_of_info,
            group_name: self.group_name,
        })
        .await;

        match resp {
            Ok(_) => Ok(true),
            Err(e) => {
                if let error::ErrorKind::Server(e) = e.kind() {
                    if e.kind() == &error::ServerErrorKind::GroupNotFound {
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
        opts: &'a EnsureGroupPollOptions<C>,
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
