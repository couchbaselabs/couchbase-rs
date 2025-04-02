use chrono::{DateTime, FixedOffset};
use couchbase_core::mgmtx;
use std::fmt::Display;

#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct Role {
    pub name: String,
    pub bucket: Option<String>,
    pub scope: Option<String>,
    pub collection: Option<String>,
}

impl Role {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            bucket: None,
            scope: None,
            collection: None,
        }
    }

    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    pub fn collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }
}

#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct RoleAndDescription {
    pub role: Role,
    pub display_name: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct Origin {
    pub origin_type: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct RoleAndOrigins {
    pub role: Role,
    pub origins: Vec<Origin>,
}

#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct Group {
    pub name: String,
    pub description: Option<String>,
    pub roles: Vec<Role>,
    pub ldap_group_reference: Option<String>,
}

impl Group {
    pub fn new(name: impl Into<String>, description: impl Into<String>, roles: Vec<Role>) -> Self {
        Self {
            name: name.into(),
            description: Some(description.into()),
            roles,
            ldap_group_reference: None,
        }
    }

    pub fn add_role(mut self, role: Role) -> Self {
        self.roles.push(role);
        self
    }

    pub fn ldap_group_reference(mut self, reference: impl Into<String>) -> Self {
        self.ldap_group_reference = Some(reference.into());
        self
    }
}

#[derive(Debug, Clone, PartialOrd, Eq, PartialEq)]
pub struct User {
    pub username: String,
    pub display_name: String,
    pub groups: Vec<String>,
    pub roles: Vec<Role>,

    pub(crate) password: Option<String>,
}

impl User {
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

    pub fn groups(mut self, groups: Vec<String>) -> Self {
        self.groups = groups;
        self
    }

    pub fn add_group(mut self, group: impl Into<String>) -> Self {
        self.groups.push(group.into());
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct UserAndMetadata {
    pub domain: String,
    pub user: User,
    pub effective_roles: Vec<RoleAndOrigins>,
    pub password_changed: Option<DateTime<FixedOffset>>,
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
