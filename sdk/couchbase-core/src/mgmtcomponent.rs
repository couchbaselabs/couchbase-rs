use crate::authenticator::Authenticator;
use crate::cbconfig::CollectionManifest;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::mgmtoptions::{
    CreateCollectionOptions, CreateScopeOptions, DeleteCollectionOptions, DeleteScopeOptions,
    EnsureManifestOptions, GetCollectionManifestOptions, UpdateCollectionOptions,
};
use crate::mgmtx::manifest_helper::EnsureManifestHelper;
use crate::mgmtx::node_target::NodeTarget;
use crate::mgmtx::options::EnsureManifestPollOptions;
use crate::mgmtx::responses::{
    CreateCollectionResponse, CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
    UpdateCollectionResponse,
};
use crate::retry::{orchestrate_retries, RetryInfo, RetryManager};
use crate::retrybesteffort::ExponentialBackoffCalculator;
use crate::service_type::ServiceType;
use crate::tracingcomponent::TracingComponent;
use crate::{error, mgmtx};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct MgmtComponent<C: Client> {
    http_component: HttpComponent<C>,
    tracing: Arc<TracingComponent>,

    retry_manager: Arc<RetryManager>,
}

#[derive(Debug)]
pub(crate) struct MgmtComponentConfig {
    pub endpoints: HashMap<String, String>,
    pub authenticator: Arc<Authenticator>,
}

pub(crate) struct MgmtComponentOptions {
    pub user_agent: String,
}

impl<C: Client> MgmtComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        tracing: Arc<TracingComponent>,
        config: MgmtComponentConfig,
        opts: MgmtComponentOptions,
    ) -> Self {
        Self {
            http_component: HttpComponent::new(
                ServiceType::Mgmt,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
            tracing,
            retry_manager,
        }
    }

    pub fn reconfigure(&self, config: MgmtComponentConfig) {
        self.http_component.reconfigure(HttpComponentState::new(
            config.endpoints,
            config.authenticator,
        ))
    }

    pub async fn get_collection_manifest(
        &self,
        opts: &GetCollectionManifestOptions<'_>,
    ) -> error::Result<CollectionManifest> {
        let retry_info = RetryInfo::new(false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                            tracing: Some(self.tracing.clone()),
                        }
                        .get_collection_manifest(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }

    pub async fn create_scope(
        &self,
        opts: &CreateScopeOptions<'_>,
    ) -> error::Result<CreateScopeResponse> {
        let retry_info = RetryInfo::new(false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                            tracing: Some(self.tracing.clone()),
                        }
                        .create_scope(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }

    pub async fn delete_scope(
        &self,
        opts: &DeleteScopeOptions<'_>,
    ) -> error::Result<DeleteScopeResponse> {
        let retry_info = RetryInfo::new(false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                            tracing: Some(self.tracing.clone()),
                        }
                        .delete_scope(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }

    pub async fn create_collection(
        &self,
        opts: &CreateCollectionOptions<'_>,
    ) -> error::Result<CreateCollectionResponse> {
        let retry_info = RetryInfo::new(false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                            tracing: Some(self.tracing.clone()),
                        }
                        .create_collection(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }

    pub async fn delete_collection(
        &self,
        opts: &DeleteCollectionOptions<'_>,
    ) -> error::Result<DeleteCollectionResponse> {
        let retry_info = RetryInfo::new(false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                            tracing: Some(self.tracing.clone()),
                        }
                        .delete_collection(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }

    pub async fn update_collection(
        &self,
        opts: &UpdateCollectionOptions<'_>,
    ) -> error::Result<UpdateCollectionResponse> {
        let retry_info = RetryInfo::new(false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                            tracing: Some(self.tracing.clone()),
                        }
                        .update_collection(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Mgmt(e).into()),
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }

    pub async fn ensure_manifest(&self, opts: &EnsureManifestOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureManifestHelper::new(
            self.http_component.user_agent(),
            opts.bucket_name,
            opts.manifest_uid,
            opts.on_behalf_of_info,
        );

        let backoff = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        self.http_component
            .ensure_resource(backoff, async |client: Arc<C>, targets: Vec<NodeTarget>| {
                helper
                    .clone()
                    .poll(&EnsureManifestPollOptions { client, targets })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }
}
