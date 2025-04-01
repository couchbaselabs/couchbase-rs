use crate::httpx::client::Client;
use crate::mgmtx::error;
use crate::mgmtx::mgmt::{parse_response_json, Management};
use crate::mgmtx::options::{
    ChangePasswordOptions, DeleteGroupOptions, DeleteUserOptions, GetAllGroupsOptions,
    GetAllUsersOptions, GetGroupOptions, GetRolesOptions, GetUserOptions, UpsertGroupOptions,
    UpsertUserOptions,
};
use crate::mgmtx::user::{Group, Role, RoleAndDescription, UserAndMetadata};
use crate::mgmtx::user_json::{GroupJson, RoleAndDescriptionJson, UserAndMetadataJson};
use bytes::Bytes;
use http::Method;

impl<C: Client> Management<C> {
    pub async fn get_user(&self, opts: &GetUserOptions<'_>) -> error::Result<UserAndMetadata> {
        let resp = self
            .execute(
                Method::GET,
                format!(
                    "/settings/rbac/users/{}/{}",
                    urlencoding::encode(opts.auth_domain),
                    urlencoding::encode(opts.username)
                ),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let user_json: UserAndMetadataJson = parse_response_json(resp).await?;

        user_json.try_into()
    }

    pub async fn get_all_users(
        &self,
        opts: &GetAllUsersOptions<'_>,
    ) -> error::Result<Vec<UserAndMetadata>> {
        let resp = self
            .execute(
                Method::GET,
                format!(
                    "/settings/rbac/users/{}",
                    urlencoding::encode(opts.auth_domain),
                ),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let users_json: Vec<UserAndMetadataJson> = parse_response_json(resp).await?;

        users_json
            .into_iter()
            .map(UserAndMetadata::try_from)
            .collect()
    }

    pub async fn upsert_user(&self, opts: &UpsertUserOptions<'_>) -> error::Result<()> {
        let body = {
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair("name", opts.user.display_name.as_str())
                .append_pair(
                    "roles",
                    &opts
                        .user
                        .roles
                        .iter()
                        .map(Self::build_role)
                        .collect::<Vec<String>>()
                        .join(","),
                );

            if let Some(password) = &opts.user.password {
                form.append_pair("password", password.as_str());
            }

            if !opts.user.groups.is_empty() {
                form.append_pair("groups", &opts.user.groups.join(","));
            }

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::PUT,
                format!(
                    "/settings/rbac/users/{}/{}",
                    urlencoding::encode(opts.auth_domain),
                    urlencoding::encode(&opts.user.username),
                ),
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status().as_u16() < 200 || resp.status().as_u16() >= 300 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    pub async fn delete_user(&self, opts: &DeleteUserOptions<'_>) -> error::Result<()> {
        let resp = self
            .execute(
                Method::DELETE,
                format!(
                    "/settings/rbac/users/{}/{}",
                    urlencoding::encode(opts.auth_domain),
                    urlencoding::encode(opts.username),
                ),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    pub async fn get_roles(
        &self,
        opts: &GetRolesOptions<'_>,
    ) -> error::Result<Vec<RoleAndDescription>> {
        let resp = self
            .execute(
                Method::GET,
                "/settings/rbac/roles",
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let roles_json: Vec<RoleAndDescriptionJson> = parse_response_json(resp).await?;

        Ok(roles_json
            .into_iter()
            .map(RoleAndDescription::from)
            .collect())
    }

    pub async fn get_group(&self, opts: &GetGroupOptions<'_>) -> error::Result<Group> {
        let resp = self
            .execute(
                Method::GET,
                format!("/settings/rbac/groups/{}", opts.group_name),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let group_json: GroupJson = parse_response_json(resp).await?;

        Ok(group_json.into())
    }

    pub async fn get_all_groups(
        &self,
        opts: &GetAllGroupsOptions<'_>,
    ) -> error::Result<Vec<Group>> {
        let resp = self
            .execute(
                Method::GET,
                "/settings/rbac/groups",
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        let groups_json: Vec<GroupJson> = parse_response_json(resp).await?;

        Ok(groups_json.into_iter().map(Group::from).collect())
    }

    pub async fn upsert_group(&self, opts: &UpsertGroupOptions<'_>) -> error::Result<()> {
        let body = {
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair(
                "roles",
                &opts
                    .group
                    .roles
                    .iter()
                    .map(Self::build_role)
                    .collect::<Vec<String>>()
                    .join(","),
            );

            if let Some(desc) = &opts.group.description {
                form.append_pair("description", desc.as_str());
            }

            if let Some(group_ref) = &opts.group.ldap_group_reference {
                form.append_pair("ldap_group_ref", group_ref.as_str());
            }

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::PUT,
                format!(
                    "/settings/rbac/groups/{}",
                    urlencoding::encode(&opts.group.name),
                ),
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status().as_u16() < 200 || resp.status().as_u16() >= 300 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    pub async fn delete_group(&self, opts: &DeleteGroupOptions<'_>) -> error::Result<()> {
        let resp = self
            .execute(
                Method::DELETE,
                format!(
                    "/settings/rbac/groups/{}",
                    urlencoding::encode(opts.group_name),
                ),
                "",
                opts.on_behalf_of_info.cloned(),
                None,
                None,
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    pub async fn change_password(&self, opts: &ChangePasswordOptions<'_>) -> error::Result<()> {
        let body = {
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair("password", opts.new_password);

            Bytes::from(form.finish())
        };

        let resp = self
            .execute(
                Method::POST,
                "/controller/changePassword",
                "application/x-www-form-urlencoded",
                opts.on_behalf_of_info.cloned(),
                None,
                Some(body),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(resp).await);
        }

        Ok(())
    }

    fn build_role(role: &Role) -> String {
        let mut role_str = role.name.clone();

        if let Some(bucket) = &role.bucket {
            role_str = format!("{}[{}", role_str, bucket);

            if let Some(scope) = &role.scope {
                role_str = format!("{}:{}", role_str, scope);
            }
            if let Some(collection) = &role.collection {
                role_str = format!("{}:{}", role_str, collection);
            }

            role_str = format!("{}]", role_str);
        }

        role_str
    }
}
