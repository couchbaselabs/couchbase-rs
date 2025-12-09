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
use crate::cbconfig::CollectionManifest;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::httpx::request::Auth;
use crate::mgmtx::bucket_helper::EnsureBucketHelper;
use crate::mgmtx::bucket_settings::BucketDef;
use crate::mgmtx::group_helper::EnsureGroupHelper;
use crate::mgmtx::manifest_helper::EnsureManifestHelper;
use crate::mgmtx::node_target::NodeTarget;
use crate::mgmtx::options::{
    EnsureBucketPollOptions, EnsureGroupPollOptions, EnsureManifestPollOptions,
    EnsureUserPollOptions,
};
use crate::mgmtx::responses::{
    CreateCollectionResponse, CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
    UpdateCollectionResponse,
};
use crate::mgmtx::user::{Group, RoleAndDescription, UserAndMetadata};
use crate::mgmtx::user_helper::EnsureUserHelper;
use crate::options::management::{
    ChangePasswordOptions, CreateBucketOptions, CreateCollectionOptions, CreateScopeOptions,
    DeleteBucketOptions, DeleteCollectionOptions, DeleteGroupOptions, DeleteScopeOptions,
    DeleteUserOptions, EnsureBucketOptions, EnsureGroupOptions, EnsureManifestOptions,
    EnsureUserOptions, FlushBucketOptions, GetAllBucketsOptions, GetAllGroupsOptions,
    GetAllUsersOptions, GetBucketOptions, GetCollectionManifestOptions, GetGroupOptions,
    GetRolesOptions, GetUserOptions, UpdateBucketOptions, UpdateCollectionOptions,
    UpsertGroupOptions, UpsertUserOptions,
};
use crate::retry::{orchestrate_retries, RetryInfo, RetryManager};
use crate::retrybesteffort::ExponentialBackoffCalculator;
use crate::service_type::ServiceType;
use crate::{error, mgmtx};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct MgmtComponent<C: Client> {
    http_component: HttpComponent<C>,

    retry_manager: Arc<RetryManager>,
}

pub(crate) struct MgmtComponentConfig {
    pub endpoints: HashMap<String, String>,
    pub authenticator: Authenticator,
}

pub(crate) struct MgmtComponentOptions {
    pub user_agent: String,
}

impl<C: Client> MgmtComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        config: MgmtComponentConfig,
        opts: MgmtComponentOptions,
    ) -> Self {
        Self {
            http_component: HttpComponent::new(
                ServiceType::MGMT,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
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
        let retry_info = RetryInfo::new(
            "get_collection_manifest",
            false,
            opts.retry_strategy.clone(),
        );

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
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
        let retry_info = RetryInfo::new("create_scope", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
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
        let retry_info = RetryInfo::new("delete_scope", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
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
        let retry_info = RetryInfo::new("create_collection", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
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
        let retry_info = RetryInfo::new("delete_collection", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
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
        let retry_info = RetryInfo::new("update_collection", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        let res = match (mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
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

    pub async fn get_all_buckets(
        &self,
        opts: &GetAllBucketsOptions<'_>,
    ) -> error::Result<Vec<BucketDef>> {
        let retry_info = RetryInfo::new("get_all_buckets", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .get_all_buckets(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn get_bucket(&self, opts: &GetBucketOptions<'_>) -> error::Result<BucketDef> {
        let retry_info = RetryInfo::new("get_bucket", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .get_bucket(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn create_bucket(&self, opts: &CreateBucketOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("create_bucket", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .create_bucket(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn update_bucket(&self, opts: &UpdateBucketOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("update_bucket", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .update_bucket(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn delete_bucket(&self, opts: &DeleteBucketOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("delete_bucket", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .delete_bucket(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn flush_bucket(&self, opts: &FlushBucketOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("flush_bucket", false, opts.retry_strategy.clone());

        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .flush_bucket(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
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

    pub async fn ensure_bucket(&self, opts: &EnsureBucketOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureBucketHelper::new(
            self.http_component.user_agent(),
            opts.bucket_name,
            opts.bucket_uuid,
            opts.want_missing,
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
                    .poll(&EnsureBucketPollOptions { client, targets })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }

    pub async fn get_user(&self, opts: &GetUserOptions<'_>) -> error::Result<UserAndMetadata> {
        let retry_info = RetryInfo::new("get_user", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .get_user(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn get_all_users(
        &self,
        opts: &GetAllUsersOptions<'_>,
    ) -> error::Result<Vec<UserAndMetadata>> {
        let retry_info = RetryInfo::new("get_all_users", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .get_all_users(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn upsert_user(&self, opts: &UpsertUserOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("upsert_user", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .upsert_user(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn delete_user(&self, opts: &DeleteUserOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("delete_user", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .delete_user(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn get_roles(
        &self,
        opts: &GetRolesOptions<'_>,
    ) -> error::Result<Vec<RoleAndDescription>> {
        let retry_info = RetryInfo::new("get_roles", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .get_roles(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn get_group(&self, opts: &GetGroupOptions<'_>) -> error::Result<Group> {
        let retry_info = RetryInfo::new("get_group", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .get_group(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn get_all_groups(
        &self,
        opts: &GetAllGroupsOptions<'_>,
    ) -> error::Result<Vec<Group>> {
        let retry_info = RetryInfo::new("get_all_groups", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .get_all_groups(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn upsert_group(&self, opts: &UpsertGroupOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("upsert_group", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .upsert_group(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn delete_group(&self, opts: &DeleteGroupOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("delete_group", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .delete_group(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn change_password(&self, opts: &ChangePasswordOptions<'_>) -> error::Result<()> {
        let retry_info = RetryInfo::new("change_password", false, opts.retry_strategy.clone());
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    None,
                    async |client: Arc<C>, endpoint_id: String, endpoint: String, auth: Auth| {
                        mgmtx::mgmt::Management::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            auth,
                        }
                        .change_password(&copts)
                        .await
                        .map_err(|e| ErrorKind::Mgmt(e).into())
                    },
                )
                .await
        })
        .await
    }

    pub async fn ensure_user(&self, opts: &EnsureUserOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureUserHelper::new(
            self.http_component.user_agent(),
            opts.username,
            opts.auth_domain,
            opts.want_missing,
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
                    .poll(&EnsureUserPollOptions { client, targets })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }

    pub async fn ensure_group(&self, opts: &EnsureGroupOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureGroupHelper::new(
            self.http_component.user_agent(),
            opts.group_name,
            opts.want_missing,
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
                    .poll(&EnsureGroupPollOptions { client, targets })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }
}
