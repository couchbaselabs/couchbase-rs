use crate::clients::user_mgmt_client::UserMgmtClient;
use crate::error;
use crate::management::users::user::{Group, RoleAndDescription, User, UserAndMetadata};
use crate::options::user_mgmt_options::{
    ChangePasswordOptions, DropGroupOptions, DropUserOptions, GetAllGroupsOptions,
    GetAllUsersOptions, GetGroupOptions, GetRolesOptions, GetUserOptions, UpsertGroupOptions,
    UpsertUserOptions,
};
use std::sync::Arc;

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
        self.client
            .get_all_users(opts.into().unwrap_or_default())
            .await
    }

    pub async fn get_user(
        &self,
        username: impl Into<String>,
        opts: impl Into<Option<GetUserOptions>>,
    ) -> error::Result<UserAndMetadata> {
        self.client
            .get_user(username.into(), opts.into().unwrap_or_default())
            .await
    }

    pub async fn upsert_user(
        &self,
        settings: User,
        opts: impl Into<Option<UpsertUserOptions>>,
    ) -> error::Result<()> {
        self.client
            .upsert_user(settings, opts.into().unwrap_or_default())
            .await
    }

    pub async fn drop_user(
        &self,
        username: impl Into<String>,
        opts: impl Into<Option<DropUserOptions>>,
    ) -> error::Result<()> {
        self.client
            .drop_user(username.into(), opts.into().unwrap_or_default())
            .await
    }

    pub async fn get_roles(
        &self,
        opts: impl Into<Option<GetRolesOptions>>,
    ) -> error::Result<Vec<RoleAndDescription>> {
        self.client.get_roles(opts.into().unwrap_or_default()).await
    }

    pub async fn get_group(
        &self,
        group_name: impl Into<String>,
        opts: impl Into<Option<GetGroupOptions>>,
    ) -> error::Result<Group> {
        self.client
            .get_group(group_name.into(), opts.into().unwrap_or_default())
            .await
    }

    pub async fn get_all_groups(
        &self,
        opts: impl Into<Option<GetAllGroupsOptions>>,
    ) -> error::Result<Vec<Group>> {
        self.client
            .get_all_groups(opts.into().unwrap_or_default())
            .await
    }

    pub async fn upsert_group(
        &self,
        group: Group,
        opts: impl Into<Option<UpsertGroupOptions>>,
    ) -> error::Result<()> {
        self.client
            .upsert_group(group, opts.into().unwrap_or_default())
            .await
    }

    pub async fn drop_group(
        &self,
        group_name: impl Into<String>,
        opts: impl Into<Option<DropGroupOptions>>,
    ) -> error::Result<()> {
        self.client
            .drop_group(group_name.into(), opts.into().unwrap_or_default())
            .await
    }

    pub async fn change_password(
        &self,
        password: impl Into<String>,
        opts: impl Into<Option<ChangePasswordOptions>>,
    ) -> error::Result<()> {
        self.client
            .change_password(password.into(), opts.into().unwrap_or_default())
            .await
    }
}
