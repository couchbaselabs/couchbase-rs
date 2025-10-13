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

use crate::common::features::TestFeatureCode;
use crate::common::helpers::{generate_key_with_letter_prefix, try_until};
use crate::common::test_agent::TestAgent;
use crate::common::test_config::run_test;
use couchbase_core::agent::Agent;
use couchbase_core::mgmtx::user::{Group, Role, User, UserAndMetadata};
use couchbase_core::options::management::{
    DeleteGroupOptions, DeleteUserOptions, EnsureGroupOptions, EnsureUserOptions,
    GetAllGroupsOptions, GetAllUsersOptions, GetGroupOptions, GetRolesOptions, GetUserOptions,
    UpsertGroupOptions, UpsertUserOptions,
};
use couchbase_core::{error, mgmtx};
use log::error;
use std::ops::Add;
use std::time::Duration;
use tokio::time::{sleep, timeout_at, Instant};

mod common;

#[test]
fn test_get_all_roles() {
    run_test(async |mut agent| {
        let opts = GetRolesOptions::new();
        let roles = agent.get_roles(&opts).await.unwrap();

        assert!(!roles.is_empty(), "expected roles to not be empty");
        assert!(!roles[0].display_name.is_empty());
        assert!(!roles[0].description.is_empty());
        assert!(!roles[0].role.name.is_empty());
    });
}

#[test]
fn test_delete_group() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::UserGroups) {
            return;
        }

        let group_name = generate_key_with_letter_prefix();
        let desc = generate_key_with_letter_prefix();
        let roles = vec![
            Role::new("bucket_full_access").bucket(&agent.test_setup_config.bucket),
            Role::new("ro_admin"),
        ];

        let group = Group::new(&group_name, desc, roles);
        create_and_ensure_group(&agent, &group).await;

        delete_and_ensure_group(&agent, &group_name).await;

        let opts = GetGroupOptions::new(&group_name);
        let err = agent
            .get_group(&opts)
            .await
            .expect_err("expected get after delete to error");

        match err.kind() {
            error::ErrorKind::Mgmt(e) => {
                if let mgmtx::error::ErrorKind::Server(e, ..) = e.kind() {
                    assert_eq!(e.kind(), &mgmtx::error::ServerErrorKind::GroupNotFound);
                } else {
                    panic!("expected get after delete to error with GroupNotFound");
                }
            }
            _ => panic!("expected get after delete to error with GroupNotFound"),
        };
    });
}

#[test]
fn test_get_group() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::UserGroups) {
            return;
        }

        let group_name = generate_key_with_letter_prefix();
        let desc = generate_key_with_letter_prefix();
        let roles = vec![
            Role::new("bucket_full_access").bucket(&agent.test_setup_config.bucket),
            Role::new("ro_admin"),
        ];

        let group = Group::new(&group_name, desc, roles);
        create_and_ensure_group(&agent, &group).await;

        let opts = GetGroupOptions::new(&group_name);
        let actual = agent.get_group(&opts).await.unwrap();

        assert_eq!(actual.name, group.name);
        assert_eq!(actual.roles, group.roles);
        assert_eq!(actual.description, group.description);
        assert_eq!(actual.ldap_group_reference, group.ldap_group_reference);
    });
}

#[test]
fn test_get_all_groups() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::UserGroups) {
            return;
        }

        let group_name = generate_key_with_letter_prefix();
        let desc = generate_key_with_letter_prefix();
        let roles = vec![
            Role::new("bucket_full_access").bucket(&agent.test_setup_config.bucket),
            Role::new("ro_admin"),
        ];

        let group = Group::new(&group_name, desc, roles);
        create_and_ensure_group(&agent, &group).await;

        let opts = GetAllGroupsOptions::new();
        let groups = agent.get_all_groups(&opts).await.unwrap();

        let mut actual = None;
        for actual_group in groups {
            if actual_group.name == group_name {
                actual = Some(actual_group);
            }
        }

        let actual = actual.unwrap();

        assert_eq!(actual.name, group.name);
        assert_eq!(actual.roles, group.roles);
        assert_eq!(actual.description, group.description);
        assert_eq!(actual.ldap_group_reference, group.ldap_group_reference);
    });
}

#[test]
fn test_delete_user() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::UsersMB69096) {
            return;
        }

        let username = generate_key_with_letter_prefix();
        let display_name = generate_key_with_letter_prefix();
        let roles = vec![
            Role::new("bucket_full_access").bucket(&agent.test_setup_config.bucket),
            Role::new("ro_admin"),
        ];

        let user = User::new(&username, display_name, roles).password("password");
        create_and_ensure_user(&agent, &user).await;

        delete_and_ensure_user(&agent, &username).await;

        let opts = GetUserOptions::new(&username, "local");
        let err = agent
            .get_user(&opts)
            .await
            .expect_err("expected get after delete to error");

        match err.kind() {
            error::ErrorKind::Mgmt(e) => {
                if let mgmtx::error::ErrorKind::Server(e, ..) = e.kind() {
                    assert_eq!(e.kind(), &mgmtx::error::ServerErrorKind::UserNotFound);
                } else {
                    panic!("expected get after delete to error with UserNotFound");
                }
            }
            _ => panic!("expected get after delete to error with UserNotFound"),
        };
    });
}

#[test]
fn test_get_user() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::UsersMB69096) {
            return;
        }

        let username = generate_key_with_letter_prefix();
        let display_name = generate_key_with_letter_prefix();
        let roles = vec![
            Role::new("bucket_full_access").bucket(&agent.test_setup_config.bucket),
            Role::new("ro_admin"),
        ];

        let user = User::new(&username, display_name, roles).password("password");
        create_and_ensure_user(&agent, &user).await;

        let opts = GetUserOptions::new(&username, "local");
        let actual = agent.get_user(&opts).await.unwrap();

        assert_user(&user, &actual);
    });
}

#[test]
fn test_get_all_users() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::UsersMB69096) {
            return;
        }

        let username = generate_key_with_letter_prefix();
        let display_name = generate_key_with_letter_prefix();
        let roles = vec![
            Role::new("bucket_full_access").bucket(&agent.test_setup_config.bucket),
            Role::new("ro_admin"),
        ];

        let user = User::new(username, display_name, roles).password("password");
        create_and_ensure_user(&agent, &user).await;

        let opts = GetAllUsersOptions::new("local");
        let users = agent.get_all_users(&opts).await.unwrap();

        let mut actual = None;
        for actual_user in users {
            if actual_user.user.username == user.username {
                actual = Some(actual_user);
            }
        }

        assert_user(&user, actual.as_ref().unwrap());
    });
}

fn assert_user(expected: &User, actual: &UserAndMetadata) {
    assert_eq!(actual.domain, "local");
    assert_eq!(2, actual.effective_roles.len());
    assert!(actual.external_groups.is_empty());
    assert_eq!(actual.user.username, expected.username);
    assert_eq!(actual.user.display_name, expected.display_name);
    assert_eq!(actual.user.groups, expected.groups);
    assert_eq!(actual.user.roles, expected.roles);
}

async fn create_and_ensure_user(agent: &TestAgent, user: &User) {
    agent
        .upsert_user(&UpsertUserOptions::new(user, "local"))
        .await
        .unwrap();

    try_until(
        Instant::now().add(Duration::from_secs(30)),
        Duration::from_millis(500),
        "failed to ensure group in time",
        async || match agent
            .ensure_user(&EnsureUserOptions::new(&user.username, "local", false))
            .await
        {
            Ok(_) => Ok(Some(())),
            Err(e) => {
                error!("failed to ensure user: {e}");
                Err(e)
            }
        },
    )
    .await;
}

async fn create_and_ensure_group(agent: &TestAgent, group: &Group) {
    agent
        .upsert_group(&UpsertGroupOptions::new(group))
        .await
        .unwrap();

    try_until(
        Instant::now().add(Duration::from_secs(30)),
        Duration::from_millis(500),
        "failed to ensure group in time",
        async || match agent
            .ensure_group(&EnsureGroupOptions::new(&group.name, false))
            .await
        {
            Ok(_) => Ok(Some(())),
            Err(e) => {
                error!("failed to ensure group: {e}");
                Err(e)
            }
        },
    )
    .await;
}

async fn delete_and_ensure_user(agent: &TestAgent, username: &str) {
    agent
        .delete_user(&DeleteUserOptions::new(username, "local"))
        .await
        .unwrap();

    try_until(
        Instant::now().add(Duration::from_secs(30)),
        Duration::from_millis(500),
        "failed to ensure group in time",
        async || match agent
            .ensure_user(&EnsureUserOptions::new(username, "local", true))
            .await
        {
            Ok(_) => Ok(Some(())),
            Err(e) => {
                error!("failed to ensure user: {e}");
                Err(e)
            }
        },
    )
    .await;
}

async fn delete_and_ensure_group(agent: &TestAgent, group_name: &str) {
    agent
        .delete_group(&DeleteGroupOptions::new(group_name))
        .await
        .unwrap();

    try_until(
        Instant::now().add(Duration::from_secs(30)),
        Duration::from_millis(500),
        "failed to ensure group in time",
        async || match agent
            .ensure_group(&EnsureGroupOptions::new(group_name, true))
            .await
        {
            Ok(_) => Ok(Some(())),
            Err(e) => {
                error!("failed to ensure group: {e}");
                Err(e)
            }
        },
    )
    .await;
}
