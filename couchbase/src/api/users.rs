use super::*;
use crate::io::request::*;
use crate::{CouchbaseError, CouchbaseResult, GenericManagementResult, ServiceType};
use futures::channel::oneshot;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::time::Duration;
#[derive(Debug, Deserialize, Clone, Copy, Eq, PartialEq)]
pub enum AuthDomain {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "external")]
    External,
}

impl fmt::Display for AuthDomain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Role {
    #[serde(rename = "role")]
    name: String,
    bucket_name: Option<String>,
}

impl Role {
    pub fn new(name: String, bucket_name: impl Into<Option<String>>) -> Self {
        Self {
            name,
            bucket_name: bucket_name.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn bucket(&self) -> Option<&String> {
        self.bucket_name.as_ref()
    }
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct RoleAndDescription {
    #[serde(flatten)]
    role: Role,
    name: String,
    desc: String,
}

impl RoleAndDescription {
    pub fn role(&self) -> &Role {
        self.role.borrow()
    }

    pub fn display_name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &str {
        &self.desc
    }
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct Origin {
    #[serde(rename = "type")]
    origin_type: String,
    name: Option<String>,
}

impl Origin {
    pub fn new(origin_type: impl Into<String>, name: impl Into<Option<String>>) -> Self {
        Self {
            origin_type: origin_type.into(),
            name: name.into(),
        }
    }

    pub fn origin_type(&self) -> &str {
        self.origin_type.as_str()
    }

    pub fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct RoleAndOrigins {
    #[serde(flatten)]
    role: Role,
    origins: Vec<Origin>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Group {
    #[serde(rename = "id")]
    name: String,
    description: Option<String>,
    roles: Vec<Role>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ldap_group_ref")]
    ldap_group: Option<String>,
}

impl Group {
    pub fn new(name: String, roles: Vec<Role>) -> Self {
        Self {
            name,
            description: None,
            roles,
            ldap_group: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }

    pub fn set_description(&mut self, description: String) {
        self.description = Some(description)
    }

    pub fn roles(&self) -> &Vec<Role> {
        self.roles.as_ref()
    }

    pub fn roles_mut(&mut self) -> &mut Vec<Role> {
        self.roles.as_mut()
    }

    pub fn ldap_group_reference(&self) -> Option<String> {
        self.ldap_group.clone()
    }

    pub fn set_ldap_group_reference(&mut self, reference: String) {
        self.ldap_group = Some(reference)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    #[serde(rename = "id")]
    username: String,
    #[serde(rename = "name")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    groups: Option<Vec<String>>,
    roles: Vec<Role>,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
}

impl User {
    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn display_name(&self) -> Option<&String> {
        self.display_name.as_ref()
    }

    pub fn set_display_name(&mut self, display_name: String) {
        self.display_name = Some(display_name);
    }

    pub fn groups(&self) -> Option<&Vec<String>> {
        self.groups.as_ref()
    }

    pub fn groups_mut(&mut self) -> Option<&mut Vec<String>> {
        self.groups.as_mut()
    }

    pub fn roles(&self) -> &Vec<Role> {
        self.roles.as_ref()
    }

    pub fn roles_mut(&mut self) -> &mut Vec<Role> {
        self.roles.as_mut()
    }

    pub fn set_password(&mut self, password: String) {
        self.password = Some(password)
    }
}

pub struct UserBuilder {
    username: String,
    display_name: Option<String>,
    groups: Option<Vec<String>>,
    roles: Vec<Role>,
    password: Option<String>,
}

impl UserBuilder {
    pub fn new(username: String, password: Option<String>, roles: Vec<Role>) -> Self {
        Self {
            username,
            display_name: None,
            groups: None,
            roles,
            password,
        }
    }

    pub fn display_name(mut self, name: String) -> UserBuilder {
        self.display_name = Some(name);
        self
    }

    pub fn groups(mut self, groups: Vec<String>) -> UserBuilder {
        self.groups = Some(groups);
        self
    }

    pub fn build(self) -> User {
        User {
            username: self.username,
            display_name: self.display_name,
            groups: self.groups,
            roles: self.roles,
            password: self.password,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct UserAndMetadata {
    #[serde(rename = "id")]
    username: String,
    #[serde(rename = "name")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    groups: Option<Vec<String>>,
    roles: Vec<RoleAndOrigins>,
    domain: AuthDomain,
    password_change_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_groups: Option<Vec<String>>,
}

impl UserAndMetadata {
    pub fn domain(&self) -> AuthDomain {
        self.domain
    }

    pub fn user(&self) -> User {
        let mut builder = UserBuilder::new(self.username.clone(), None, self.user_roles());
        if let Some(display_name) = &self.display_name {
            builder = builder.display_name(display_name.clone());
        }
        if let Some(groups) = &self.groups {
            builder = builder.groups(groups.clone());
        }

        builder.build()
    }

    pub fn effective_roles(&self) -> &Vec<RoleAndOrigins> {
        self.roles.as_ref()
    }

    pub fn password_changed(&self) -> Option<&String> {
        self.password_change_date.as_ref()
    }

    pub fn external_groups(&self) -> Option<&Vec<String>> {
        self.groups.as_ref()
    }

    fn user_roles(&self) -> Vec<Role> {
        self.roles
            .iter()
            .filter(|role| {
                role.origins
                    .iter()
                    .any(|origin| origin.origin_type.as_str() == "user")
            })
            .map(|role| Role::new(role.role.name.clone(), role.role.bucket_name.clone()))
            .collect()
    }
}

pub struct UserManager {
    core: Arc<Core>,
}

impl UserManager {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    pub async fn get_user(
        &self,
        username: impl Into<String>,
        options: impl Into<Option<GetUserOptions>>,
    ) -> CouchbaseResult<UserAndMetadata> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        let domain = match options.domain_name {
            Some(name) => name,
            None => AuthDomain::Local.to_string().to_lowercase(),
        };

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/settings/rbac/users/{}/{}", domain, username.into()),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn get_all_users(
        &self,
        options: impl Into<Option<GetAllUsersOptions>>,
    ) -> CouchbaseResult<Vec<UserAndMetadata>> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        let domain = match options.domain_name {
            Some(name) => name,
            None => AuthDomain::Local.to_string().to_lowercase(),
        };

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/settings/rbac/users/{}/", domain),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn upsert_user(
        &self,
        user: User,
        options: impl Into<Option<UpsertUserOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let roles: Vec<String> = user
            .roles
            .into_iter()
            .map(|role| match &role.bucket_name {
                Some(name) => format!("{}[{}]", role.name, name),
                None => role.name,
            })
            .collect();

        // The server expects form data so we need to build that, serde expects each value to be an
        // Option.
        let user_form = &[
            ("name", user.display_name),
            ("groups", user.groups.map(|groups| groups.join(","))),
            ("roles", Some(roles.join(","))),
            ("password", user.password),
        ];

        let domain = match options.domain_name {
            Some(name) => name,
            None => AuthDomain::Local.to_string().to_lowercase(),
        };
        let user_encoded = serde_urlencoded::to_string(&user_form)?;
        let content_type = String::from("application/x-www-form-urlencoded");
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/settings/rbac/users/{}/{}", domain, user.username),
                method: String::from("put"),
                payload: Some(user_encoded),
                content_type: Some(content_type),
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn drop_user(
        &self,
        username: impl Into<String>,
        options: impl Into<Option<DropUserOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        let domain = match options.domain_name {
            Some(name) => name,
            None => AuthDomain::Local.to_string().to_lowercase(),
        };

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/settings/rbac/users/{}/{}", domain, username.into()),
                method: String::from("delete"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn get_roles(
        &self,
        options: impl Into<Option<GetRolesOptions>>,
    ) -> CouchbaseResult<Vec<RoleAndDescription>> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: String::from("/settings/rbac/roles"),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn get_group(
        &self,
        name: impl Into<String>,
        options: impl Into<Option<GetGroupOptions>>,
    ) -> CouchbaseResult<Vec<Group>> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/settings/rbac/groups/{}", name.into()),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn get_all_groups(
        &self,
        options: GetAllGroupsOptions,
    ) -> CouchbaseResult<Vec<Group>> {
        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: String::from("/settings/rbac/groups"),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn upsert_group(
        &self,
        group: Group,
        options: impl Into<Option<UpsertGroupOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let roles: Vec<String> = group
            .roles
            .into_iter()
            .map(|role| match &role.bucket_name {
                Some(name) => format!("{}[{}]", role.name, name),
                None => role.name,
            })
            .collect();

        // The server expects form data so we need to build that, serde expects each value to be an
        // Option.
        let group_form = &[
            ("description", group.description),
            ("roles", Some(roles.join(","))),
            ("ldap_group_ref", group.ldap_group),
        ];

        let group_encoded = serde_urlencoded::to_string(&group_form)?;
        let content_type = String::from("application/x-www-form-urlencoded");
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/settings/rbac/groups/{}", group.name),
                method: String::from("put"),
                payload: Some(group_encoded),
                content_type: Some(content_type),
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }

    pub async fn drop_group(
        &self,
        name: impl Into<String>,
        options: impl Into<Option<DropGroupOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/settings/rbac/groups/{}", name.into()),
                method: String::from("delete"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(CouchbaseError::GenericHTTP {
                ctx: Default::default(),
                status: result.http_status(),
                message: String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
            }),
        }
    }
}

macro_rules! domain_name {
    () => {
        pub fn domain_name(mut self, domain_name: String) -> Self {
            self.domain_name = Some(domain_name);
            self
        }
    };
}

#[derive(Debug, Default)]
pub struct GetUserOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) domain_name: Option<String>,
}

impl GetUserOptions {
    timeout!();
    domain_name!();
}

#[derive(Debug, Default)]
pub struct GetAllUsersOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) domain_name: Option<String>,
}

impl GetAllUsersOptions {
    timeout!();
    domain_name!();
}

#[derive(Debug, Default)]
pub struct UpsertUserOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) domain_name: Option<String>,
}

impl UpsertUserOptions {
    timeout!();
    domain_name!();
}

#[derive(Debug, Default)]
pub struct DropUserOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) domain_name: Option<String>,
}

impl DropUserOptions {
    timeout!();
    domain_name!();
}

#[derive(Debug, Default)]
pub struct GetRolesOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetRolesOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetGroupOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetGroupOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAllGroupsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllGroupsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct UpsertGroupOptions {
    pub(crate) timeout: Option<Duration>,
}

impl UpsertGroupOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropGroupOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropGroupOptions {
    timeout!();
}
