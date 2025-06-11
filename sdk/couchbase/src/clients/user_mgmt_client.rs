use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::management::users::user::{Group, RoleAndDescription, User, UserAndMetadata};
use crate::options::user_mgmt_options::{
    ChangePasswordOptions, DropGroupOptions, DropUserOptions, GetAllGroupsOptions,
    GetAllUsersOptions, GetGroupOptions, GetRolesOptions, GetUserOptions, UpsertGroupOptions,
    UpsertUserOptions,
};
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;

const DEFAULT_AUTH_DOMAIN: &str = "local";

pub(crate) struct UserMgmtClient {
    backend: UserMgmtClientBackend,
}

impl UserMgmtClient {
    pub fn new(backend: UserMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn get_user(
        &self,
        username: String,
        opts: GetUserOptions,
    ) -> error::Result<UserAndMetadata> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_user(username, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_user(username, opts).await
            }
        }
    }

    pub async fn get_all_users(
        &self,
        opts: GetAllUsersOptions,
    ) -> error::Result<Vec<UserAndMetadata>> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_all_users(opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_all_users(opts).await
            }
        }
    }

    pub async fn upsert_user(&self, user: User, opts: UpsertUserOptions) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.upsert_user(user, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.upsert_user(user, opts).await
            }
        }
    }

    pub async fn drop_user(&self, username: String, opts: DropUserOptions) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.drop_user(username, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.drop_user(username, opts).await
            }
        }
    }

    pub async fn get_roles(&self, opts: GetRolesOptions) -> error::Result<Vec<RoleAndDescription>> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_roles(opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_roles(opts).await
            }
        }
    }

    pub async fn get_group(
        &self,
        group_name: String,
        opts: GetGroupOptions,
    ) -> error::Result<Group> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_group(group_name, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_group(group_name, opts).await
            }
        }
    }

    pub async fn get_all_groups(&self, opts: GetAllGroupsOptions) -> error::Result<Vec<Group>> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.get_all_groups(opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.get_all_groups(opts).await
            }
        }
    }

    pub async fn upsert_group(&self, group: Group, opts: UpsertGroupOptions) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.upsert_group(group, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.upsert_group(group, opts).await
            }
        }
    }

    pub async fn drop_group(
        &self,
        group_name: String,
        opts: DropGroupOptions,
    ) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.drop_group(group_name, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.drop_group(group_name, opts).await
            }
        }
    }

    pub async fn change_password(
        &self,
        new_password: String,
        opts: ChangePasswordOptions,
    ) -> error::Result<()> {
        match &self.backend {
            UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(client) => {
                client.change_password(new_password, opts).await
            }
            UserMgmtClientBackend::Couchbase2UserMgmtClientBackend(client) => {
                client.change_password(new_password, opts).await
            }
        }
    }
}

pub(crate) enum UserMgmtClientBackend {
    CouchbaseUserMgmtClientBackend(CouchbaseUserMgmtClient),
    Couchbase2UserMgmtClientBackend(Couchbase2UserMgmtClient),
}

pub(crate) struct CouchbaseUserMgmtClient {
    agent_provider: CouchbaseAgentProvider,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseUserMgmtClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            default_retry_strategy,
        }
    }

    pub async fn get_all_users(
        &self,
        opts: GetAllUsersOptions,
    ) -> error::Result<Vec<UserAndMetadata>> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::mgmtoptions::GetAllUsersOptions::new(
            opts.auth_domain.as_deref().unwrap_or(DEFAULT_AUTH_DOMAIN),
        )
        .retry_strategy(self.default_retry_strategy.clone());

        let users = agent.get_all_users(&opts).await?;

        Ok(users.into_iter().map(|u| u.into()).collect())
    }

    pub async fn get_user(
        &self,
        username: String,
        opts: GetUserOptions,
    ) -> error::Result<UserAndMetadata> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::mgmtoptions::GetUserOptions::new(
            &username,
            opts.auth_domain.as_deref().unwrap_or(DEFAULT_AUTH_DOMAIN),
        )
        .retry_strategy(self.default_retry_strategy.clone());

        let user = agent.get_user(&opts).await?;

        Ok(user.into())
    }

    pub async fn upsert_user(&self, user: User, opts: UpsertUserOptions) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let cuser = user.into();

        let opts = couchbase_core::mgmtoptions::UpsertUserOptions::new(
            &cuser,
            opts.auth_domain.as_deref().unwrap_or(DEFAULT_AUTH_DOMAIN),
        )
        .retry_strategy(self.default_retry_strategy.clone());

        agent.upsert_user(&opts).await?;

        Ok(())
    }

    pub async fn drop_user(&self, username: String, opts: DropUserOptions) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::mgmtoptions::DeleteUserOptions::new(
            &username,
            opts.auth_domain.as_deref().unwrap_or(DEFAULT_AUTH_DOMAIN),
        )
        .retry_strategy(self.default_retry_strategy.clone());

        agent.delete_user(&opts).await?;

        Ok(())
    }

    pub async fn get_roles(&self, opts: GetRolesOptions) -> error::Result<Vec<RoleAndDescription>> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::mgmtoptions::GetRolesOptions::new()
            .retry_strategy(self.default_retry_strategy.clone());

        let roles = agent.get_roles(&opts).await?;

        Ok(roles.into_iter().map(|r| r.into()).collect())
    }

    pub async fn get_group(
        &self,
        group_name: String,
        opts: GetGroupOptions,
    ) -> error::Result<Group> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::mgmtoptions::GetGroupOptions::new(&group_name)
            .retry_strategy(self.default_retry_strategy.clone());

        let group = agent.get_group(&opts).await?;

        Ok(group.into())
    }

    pub async fn get_all_groups(&self, opts: GetAllGroupsOptions) -> error::Result<Vec<Group>> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::mgmtoptions::GetAllGroupsOptions::new()
            .retry_strategy(self.default_retry_strategy.clone());

        let groups = agent.get_all_groups(&opts).await?;

        Ok(groups.into_iter().map(|g| g.into()).collect())
    }

    pub async fn upsert_group(&self, group: Group, opts: UpsertGroupOptions) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let cgroup = group.into();

        let opts = couchbase_core::mgmtoptions::UpsertGroupOptions::new(&cgroup)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.upsert_group(&opts).await?;

        Ok(())
    }

    pub async fn drop_group(
        &self,
        group_name: String,
        opts: DropGroupOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::mgmtoptions::DeleteGroupOptions::new(&group_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.delete_group(&opts).await?;

        Ok(())
    }

    pub async fn change_password(
        &self,
        new_password: String,
        opts: ChangePasswordOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::mgmtoptions::ChangePasswordOptions::new(&new_password)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.change_password(&opts).await?;

        Ok(())
    }
}

pub(crate) struct Couchbase2UserMgmtClient {}

impl Couchbase2UserMgmtClient {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_user(
        &self,
        _username: String,
        _opts: GetUserOptions,
    ) -> error::Result<UserAndMetadata> {
        unimplemented!()
    }

    pub async fn get_all_users(
        &self,
        _opts: GetAllUsersOptions,
    ) -> error::Result<Vec<UserAndMetadata>> {
        unimplemented!()
    }

    pub async fn upsert_user(&self, _user: User, _opts: UpsertUserOptions) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_user(&self, _username: String, _opts: DropUserOptions) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn get_roles(
        &self,
        _opts: GetRolesOptions,
    ) -> error::Result<Vec<RoleAndDescription>> {
        unimplemented!()
    }

    pub async fn get_group(
        &self,
        _group_name: String,
        _opts: GetGroupOptions,
    ) -> error::Result<Group> {
        unimplemented!()
    }

    pub async fn get_all_groups(&self, _opts: GetAllGroupsOptions) -> error::Result<Vec<Group>> {
        unimplemented!()
    }

    pub async fn upsert_group(
        &self,
        _group: Group,
        _opts: UpsertGroupOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_group(
        &self,
        _group_name: String,
        _opts: DropGroupOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn change_password(
        &self,
        _new_password: String,
        _opts: ChangePasswordOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }
}
