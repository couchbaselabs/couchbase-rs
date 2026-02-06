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

use crate::clients::tracing_client::Keyspace;
use crate::clients::user_mgmt_client::UserMgmtClient;
use crate::error;
use crate::management::users::user::{Group, RoleAndDescription, User, UserAndMetadata};
use crate::options::user_mgmt_options::{
    ChangePasswordOptions, DropGroupOptions, DropUserOptions, GetAllGroupsOptions,
    GetAllUsersOptions, GetGroupOptions, GetRolesOptions, GetUserOptions, UpsertGroupOptions,
    UpsertUserOptions,
};
use crate::tracing::{
    SERVICE_VALUE_MANAGEMENT, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use std::sync::Arc;
use tracing::{instrument, Level};

#[derive(Clone)]
pub struct UserManager {
    client: Arc<UserMgmtClient>,
}

impl UserManager {
    pub(crate) fn new(client: Arc<UserMgmtClient>) -> Self {
        Self { client }
    }

    pub async fn get_all_users(
        &self,
        opts: impl Into<Option<GetAllUsersOptions>>,
    ) -> error::Result<Vec<UserAndMetadata>> {
        self.get_all_users_internal(opts).await
    }

    pub async fn get_user(
        &self,
        username: impl Into<String>,
        opts: impl Into<Option<GetUserOptions>>,
    ) -> error::Result<UserAndMetadata> {
        self.get_user_internal(username, opts).await
    }

    pub async fn upsert_user(
        &self,
        settings: User,
        opts: impl Into<Option<UpsertUserOptions>>,
    ) -> error::Result<()> {
        self.upsert_user_internal(settings, opts).await
    }

    pub async fn drop_user(
        &self,
        username: impl Into<String>,
        opts: impl Into<Option<DropUserOptions>>,
    ) -> error::Result<()> {
        self.drop_user_internal(username, opts).await
    }

    pub async fn get_roles(
        &self,
        opts: impl Into<Option<GetRolesOptions>>,
    ) -> error::Result<Vec<RoleAndDescription>> {
        self.get_roles_internal(opts).await
    }

    pub async fn get_group(
        &self,
        group_name: impl Into<String>,
        opts: impl Into<Option<GetGroupOptions>>,
    ) -> error::Result<Group> {
        self.get_group_internal(group_name, opts).await
    }

    pub async fn get_all_groups(
        &self,
        opts: impl Into<Option<GetAllGroupsOptions>>,
    ) -> error::Result<Vec<Group>> {
        self.get_all_groups_internal(opts).await
    }

    pub async fn upsert_group(
        &self,
        group: Group,
        opts: impl Into<Option<UpsertGroupOptions>>,
    ) -> error::Result<()> {
        self.upsert_group_internal(group, opts).await
    }

    pub async fn drop_group(
        &self,
        group_name: impl Into<String>,
        opts: impl Into<Option<DropGroupOptions>>,
    ) -> error::Result<()> {
        self.drop_group_internal(group_name, opts).await
    }

    pub async fn change_password(
        &self,
        password: impl Into<String>,
        opts: impl Into<Option<ChangePasswordOptions>>,
    ) -> error::Result<()> {
        self.change_password_internal(password, opts).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_get_all_users",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_get_all_users",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_all_users_internal(
        &self,
        opts: impl Into<Option<GetAllUsersOptions>>,
    ) -> error::Result<Vec<UserAndMetadata>> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_get_all_users",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .get_all_users(opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_get_user",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_get_user",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_user_internal(
        &self,
        username: impl Into<String>,
        opts: impl Into<Option<GetUserOptions>>,
    ) -> error::Result<UserAndMetadata> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_get_user",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .get_user(username.into(), opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_upsert_user",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_upsert_user",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn upsert_user_internal(
        &self,
        settings: User,
        opts: impl Into<Option<UpsertUserOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_upsert_user",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .upsert_user(settings, opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_drop_user",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_drop_user",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn drop_user_internal(
        &self,
        username: impl Into<String>,
        opts: impl Into<Option<DropUserOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_drop_user",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .drop_user(username.into(), opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_get_roles",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_get_roles",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_roles_internal(
        &self,
        opts: impl Into<Option<GetRolesOptions>>,
    ) -> error::Result<Vec<RoleAndDescription>> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_get_roles",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client.get_roles(opts.into().unwrap_or_default()).await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_get_group",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_get_group",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_group_internal(
        &self,
        group_name: impl Into<String>,
        opts: impl Into<Option<GetGroupOptions>>,
    ) -> error::Result<Group> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_get_group",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .get_group(group_name.into(), opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_get_all_groups",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_get_all_groups",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_all_groups_internal(
        &self,
        opts: impl Into<Option<GetAllGroupsOptions>>,
    ) -> error::Result<Vec<Group>> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_get_all_groups",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .get_all_groups(opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_upsert_group",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_upsert_group",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn upsert_group_internal(
        &self,
        group: Group,
        opts: impl Into<Option<UpsertGroupOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_upsert_group",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .upsert_group(group, opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_drop_group",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_drop_group",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn drop_group_internal(
        &self,
        group_name: impl Into<String>,
        opts: impl Into<Option<DropGroupOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_drop_group",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .drop_group(group_name.into(), opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_users_change_password",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "manager_users_change_password",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        couchbase.service = SERVICE_VALUE_MANAGEMENT,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn change_password_internal(
        &self,
        password: impl Into<String>,
        opts: impl Into<Option<ChangePasswordOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_metered_operation(
                "manager_users_change_password",
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                async move {
                    self.client.tracing_client().record_generic_fields().await;
                    self.client
                        .change_password(password.into(), opts.into().unwrap_or_default())
                        .await
                },
            )
            .await
    }
}
