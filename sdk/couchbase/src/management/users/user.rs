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

use chrono::{DateTime, FixedOffset};
use couchbase_core::mgmtx;
use std::fmt::Display;

/// A role that can be assigned to a user or group.
#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct Role {
    /// The name of the role (e.g. `"admin"`, `"bucket_full_access"`).
    pub name: String,
    /// The bucket the role applies to, if scoped.
    pub bucket: Option<String>,
    /// The scope the role applies to, if scoped.
    pub scope: Option<String>,
    /// The collection the role applies to, if scoped.
    pub collection: Option<String>,
}

impl Role {
    /// Creates a new role with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bucket: None,
            scope: None,
            collection: None,
        }
    }

    /// Scopes the role to a specific bucket.
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    /// Scopes the role to a specific scope within the bucket.
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Scopes the role to a specific collection within the scope.
    pub fn collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }
}

/// A role along with its display name and description.
#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct RoleAndDescription {
    /// The role.
    pub role: Role,
    /// Human-readable display name.
    pub display_name: String,
    /// Description of the role.
    pub description: String,
}

/// The origin of a role assignment (e.g. user-level or group-level).
#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct Origin {
    /// The origin type (e.g. `"user"` or `"group"`).
    pub origin_type: String,
    /// The name of the group, if the origin is a group.
    pub name: Option<String>,
}

/// A role together with the origins from which it was granted.
#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct RoleAndOrigins {
    /// The role.
    pub role: Role,
    /// The origins from which this role was granted.
    pub origins: Vec<Origin>,
}

/// A user group with associated roles and optional LDAP mapping.
#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct Group {
    /// The name of the group.
    pub name: String,
    /// A description of the group.
    pub description: Option<String>,
    /// The roles assigned to this group.
    pub roles: Vec<Role>,
    /// An optional LDAP group reference for external authentication.
    pub ldap_group_reference: Option<String>,
}

impl Group {
    /// Creates a new group.
    pub fn new(name: impl Into<String>, description: impl Into<String>, roles: Vec<Role>) -> Self {
        Self {
            name: name.into(),
            description: Some(description.into()),
            roles,
            ldap_group_reference: None,
        }
    }

    /// Adds a role to the group.
    pub fn add_role(mut self, role: Role) -> Self {
        self.roles.push(role);
        self
    }

    /// Sets the LDAP group reference.
    pub fn ldap_group_reference(mut self, reference: impl Into<String>) -> Self {
        self.ldap_group_reference = Some(reference.into());
        self
    }
}

/// A Couchbase user definition.
#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct User {
    /// The username.
    pub username: String,
    /// The display name.
    pub display_name: String,
    /// The groups the user belongs to.
    pub groups: Vec<String>,
    /// The roles directly assigned to the user.
    pub roles: Vec<Role>,

    pub(crate) password: Option<String>,
}

impl User {
    /// Creates a new user with the given username, display name, and roles.
    pub fn new(
        username: impl Into<String>,
        display_name: impl Into<String>,
        roles: Vec<Role>,
    ) -> Self {
        Self {
            username: username.into(),
            display_name: display_name.into(),
            groups: Vec::new(),
            roles,
            password: None,
        }
    }

    /// Sets the groups the user belongs to.
    pub fn groups(mut self, groups: Vec<String>) -> Self {
        self.groups = groups;
        self
    }

    /// Adds a group membership.
    pub fn add_group(mut self, group: impl Into<String>) -> Self {
        self.groups.push(group.into());
        self
    }

    /// Sets the user's password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }
}

/// A user along with server-side metadata.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct UserAndMetadata {
    /// The authentication domain (`"local"` or `"external"`).
    pub domain: String,
    /// The user definition.
    pub user: User,
    /// The effective roles, including those inherited from groups.
    pub effective_roles: Vec<RoleAndOrigins>,
    /// When the user's password was last changed.
    pub password_changed: Option<DateTime<FixedOffset>>,
    /// External groups (from LDAP).
    pub external_groups: Vec<String>,
}

impl From<User> for mgmtx::user::User {
    fn from(user: User) -> Self {
        Self {
            username: user.username,
            display_name: user.display_name,
            groups: user.groups,
            roles: user
                .roles
                .into_iter()
                .map(mgmtx::user::Role::from)
                .collect(),
            password: user.password,
        }
    }
}

impl From<mgmtx::user::RoleAndDescription> for RoleAndDescription {
    fn from(role_and_description: mgmtx::user::RoleAndDescription) -> Self {
        Self {
            role: Role {
                name: role_and_description.role.name,
                bucket: role_and_description.role.bucket,
                scope: role_and_description.role.scope,
                collection: role_and_description.role.collection,
            },
            display_name: role_and_description.display_name,
            description: role_and_description.description,
        }
    }
}

impl From<mgmtx::user::Origin> for Origin {
    fn from(origin: mgmtx::user::Origin) -> Self {
        Self {
            origin_type: origin.origin_type,
            name: origin.name,
        }
    }
}

impl From<mgmtx::user::RoleAndOrigins> for RoleAndOrigins {
    fn from(role_and_origins: mgmtx::user::RoleAndOrigins) -> Self {
        Self {
            role: Role {
                name: role_and_origins.role.name,
                bucket: role_and_origins.role.bucket,
                scope: role_and_origins.role.scope,
                collection: role_and_origins.role.collection,
            },
            origins: role_and_origins
                .origins
                .into_iter()
                .map(Origin::from)
                .collect(),
        }
    }
}

impl From<mgmtx::user::UserAndMetadata> for UserAndMetadata {
    fn from(user_and_metadata: mgmtx::user::UserAndMetadata) -> Self {
        Self {
            domain: user_and_metadata.domain,
            user: User {
                username: user_and_metadata.user.username,
                display_name: user_and_metadata.user.display_name,
                groups: user_and_metadata.user.groups,
                roles: user_and_metadata
                    .user
                    .roles
                    .into_iter()
                    .map(Role::from)
                    .collect(),
                password: user_and_metadata.user.password,
            },
            effective_roles: user_and_metadata
                .effective_roles
                .into_iter()
                .map(RoleAndOrigins::from)
                .collect(),
            password_changed: user_and_metadata.password_changed,
            external_groups: user_and_metadata.external_groups,
        }
    }
}

impl From<Role> for mgmtx::user::Role {
    fn from(role: Role) -> Self {
        Self {
            name: role.name,
            bucket: role.bucket,
            scope: role.scope,
            collection: role.collection,
        }
    }
}

impl From<mgmtx::user::Role> for Role {
    fn from(role: mgmtx::user::Role) -> Self {
        Self {
            name: role.name,
            bucket: role.bucket,
            scope: role.scope,
            collection: role.collection,
        }
    }
}

impl From<Group> for mgmtx::user::Group {
    fn from(group: Group) -> Self {
        Self {
            name: group.name,
            description: group.description,
            roles: group
                .roles
                .into_iter()
                .map(mgmtx::user::Role::from)
                .collect(),
            ldap_group_reference: group.ldap_group_reference,
        }
    }
}

impl From<mgmtx::user::Group> for Group {
    fn from(group: mgmtx::user::Group) -> Self {
        Self {
            name: group.name,
            description: group.description,
            roles: group.roles.into_iter().map(Role::from).collect(),
            ldap_group_reference: group.ldap_group_reference,
        }
    }
}
