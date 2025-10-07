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

    pub password: Option<String>,
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
