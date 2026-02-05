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
use crate::tracingcomponent::{BeginDispatchFields, EndDispatchFields};
use crate::util::get_host_port_tuple_from_uri;
use bytes::Bytes;
use http::Method;

impl<C: Client> Management<C> {
    pub async fn get_user(&self, opts: &GetUserOptions<'_>) -> error::Result<UserAndMetadata> {
        let method = Method::GET;
        let path = format!(
            "settings/rbac/users/{}/{}",
            urlencoding::encode(opts.auth_domain),
            urlencoding::encode(opts.username)
        )
        .to_string();

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    None,
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(method, path, "get_user", resp).await);
        }

        let user_json: UserAndMetadataJson = parse_response_json(resp).await?;

        user_json.try_into()
    }

    pub async fn get_all_users(
        &self,
        opts: &GetAllUsersOptions<'_>,
    ) -> error::Result<Vec<UserAndMetadata>> {
        let method = Method::GET;
        let path = format!(
            "settings/rbac/users/{}",
            urlencoding::encode(opts.auth_domain),
        )
        .to_string();

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    None,
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(method, path, "get_all_users", resp).await);
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

        let method = Method::PUT;
        let path = format!(
            "settings/rbac/users/{}/{}",
            urlencoding::encode(opts.auth_domain),
            urlencoding::encode(&opts.user.username),
        );

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "application/x-www-form-urlencoded",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    Some(body),
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status().as_u16() < 200 || resp.status().as_u16() >= 300 {
            return Err(Self::decode_common_error(method, path, "upsert_user", resp).await);
        }

        Ok(())
    }

    pub async fn delete_user(&self, opts: &DeleteUserOptions<'_>) -> error::Result<()> {
        let method = Method::DELETE;
        let path = format!(
            "settings/rbac/users/{}/{}",
            urlencoding::encode(opts.auth_domain),
            urlencoding::encode(opts.username),
        )
        .to_string();

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    None,
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(method, path, "delete_user", resp).await);
        }

        Ok(())
    }

    pub async fn get_roles(
        &self,
        opts: &GetRolesOptions<'_>,
    ) -> error::Result<Vec<RoleAndDescription>> {
        let method = Method::GET;

        let path = if let Some(p) = opts.permission {
            format!("settings/rbac/roles?permission={}", urlencoding::encode(p))
        } else {
            "settings/rbac/roles".to_string()
        };

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    None,
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(method, path, "get_roles", resp).await);
        }

        let roles_json: Vec<RoleAndDescriptionJson> = parse_response_json(resp).await?;

        Ok(roles_json
            .into_iter()
            .map(RoleAndDescription::from)
            .collect())
    }

    pub async fn get_group(&self, opts: &GetGroupOptions<'_>) -> error::Result<Group> {
        let method = Method::GET;
        let path = format!("settings/rbac/groups/{}", opts.group_name).to_string();

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    None,
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(method, path, "get_group", resp).await);
        }

        let group_json: GroupJson = parse_response_json(resp).await?;

        Ok(group_json.into())
    }

    pub async fn get_all_groups(
        &self,
        opts: &GetAllGroupsOptions<'_>,
    ) -> error::Result<Vec<Group>> {
        let method = Method::GET;
        let path = "settings/rbac/groups".to_string();

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    None,
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(method, path, "get_all_groups", resp).await);
        }

        let groups_json: Vec<GroupJson> = parse_response_json(resp).await?;

        Ok(groups_json.into_iter().map(Group::from).collect())
    }

    pub async fn upsert_group(&self, opts: &UpsertGroupOptions<'_>) -> error::Result<()> {
        let method = Method::PUT;
        let path = format!(
            "settings/rbac/groups/{}",
            urlencoding::encode(&opts.group.name),
        )
        .to_string();

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
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "application/x-www-form-urlencoded",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    Some(body),
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status().as_u16() < 200 || resp.status().as_u16() >= 300 {
            return Err(Self::decode_common_error(method, path, "upsert_group", resp).await);
        }

        Ok(())
    }

    pub async fn delete_group(&self, opts: &DeleteGroupOptions<'_>) -> error::Result<()> {
        let method = Method::DELETE;
        let path = format!(
            "settings/rbac/groups/{}",
            urlencoding::encode(opts.group_name),
        )
        .to_string();

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    None,
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(method, path, "delete_group", resp).await);
        }

        Ok(())
    }

    pub async fn change_password(&self, opts: &ChangePasswordOptions<'_>) -> error::Result<()> {
        let method = Method::POST;
        let path = "controller/changePassword".to_string();

        let body = {
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair("password", opts.new_password);

            Bytes::from(form.finish())
        };

        let resp = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::from_strings(
                    get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default(),
                    None,
                ),
                self.execute(
                    method.clone(),
                    &path,
                    "application/x-www-form-urlencoded",
                    opts.on_behalf_of_info.cloned(),
                    None,
                    Some(body),
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await?;

        if resp.status() != 200 {
            return Err(Self::decode_common_error(method, path, "change_password", resp).await);
        }

        Ok(())
    }

    fn build_role(role: &Role) -> String {
        let mut role_str = role.name.clone();

        if let Some(bucket) = &role.bucket {
            role_str = format!("{role_str}[{bucket}");

            if let Some(scope) = &role.scope {
                role_str = format!("{role_str}:{scope}");
            }
            if let Some(collection) = &role.collection {
                role_str = format!("{role_str}:{collection}");
            }

            role_str = format!("{role_str}]");
        }

        role_str
    }
}
