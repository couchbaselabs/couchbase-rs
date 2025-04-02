use crate::common::test_config::run_test;
use crate::common::{new_key, try_until};
use couchbase::management::users::user::{Group, Role, User, UserAndMetadata};
use couchbase::management::users::user_manager::UserManager;
use couchbase::options::user_mgmt_options::{
    DropUserOptions, GetAllGroupsOptions, GetAllUsersOptions, GetGroupOptions, GetRolesOptions,
    GetUserOptions, UpsertUserOptions,
};
use std::ops::Add;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

mod common;

#[test]
fn test_get_all_roles() {
    run_test(async |cluster| {
        let opts = GetRolesOptions::new();
        let mgr = cluster.users();
        let roles = mgr.get_roles(opts).await.unwrap();

        assert!(!roles.is_empty(), "expected roles to not be empty");
        assert!(!roles[0].display_name.is_empty());
        assert!(!roles[0].description.is_empty());
        assert!(!roles[0].role.name.is_empty());
    });
}

#[test]
fn test_delete_group() {
    run_test(async |cluster| {
        let group_name = new_key();
        let desc = new_key();
        let roles = vec![
            Role::new("replication_target").bucket(&cluster.default_bucket),
            Role::new("replication_admin"),
        ];

        let group = Group::new(&group_name, desc, roles);
        create_group(&cluster.users(), group).await;

        delete_group(&cluster.users(), &group_name).await;

        let opts = GetGroupOptions::new();
        try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(500),
            "get group after delete did not fail in time",
            async || {
                let err = match cluster.users().get_group(&group_name, opts.clone()).await {
                    Ok(_) => return Ok(None),
                    Err(e) => e,
                };

                if err.msg.to_ascii_lowercase().contains("not found") {
                    return Ok(Some(()));
                }

                Ok(None)
            },
        )
        .await;
    });
}

#[test]
fn test_get_group() {
    run_test(async |cluster| {
        let group_name = new_key();
        let desc = new_key();
        let roles = vec![
            Role::new("replication_target").bucket(&cluster.default_bucket),
            Role::new("replication_admin"),
        ];

        let group = Group::new(&group_name, desc, roles);
        create_group(&cluster.users(), group.clone()).await;

        let opts = GetGroupOptions::new();

        let actual = try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(500),
            "failed to get group in time",
            async || match cluster.users().get_group(&group_name, opts.clone()).await {
                Ok(g) => Ok(Some(g)),
                Err(e) => Err(e),
            },
        )
        .await;

        assert_eq!(actual.name, group.name);
        assert_eq!(actual.roles, group.roles);
        assert_eq!(actual.description, group.description);
        assert_eq!(actual.ldap_group_reference, group.ldap_group_reference);
    });
}

#[test]
fn test_get_all_groups() {
    run_test(async |cluster| {
        let group_name = new_key();
        let desc = new_key();
        let roles = vec![
            Role::new("replication_target").bucket(&cluster.default_bucket),
            Role::new("replication_admin"),
        ];

        let group = Group::new(&group_name, desc, roles);
        create_group(&cluster.users(), group.clone()).await;

        let opts = GetAllGroupsOptions::new();

        let actual: Group = try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(500),
            "failed to get group in time",
            async || {
                let groups = match cluster.users().get_all_groups(opts.clone()).await {
                    Ok(g) => g,
                    Err(e) => return Err(e),
                };

                Ok(groups.into_iter().find(|g| g.name == group_name))
            },
        )
        .await;

        assert_eq!(actual.name, group.name);
        assert_eq!(actual.roles, group.roles);
        assert_eq!(actual.description, group.description);
        assert_eq!(actual.ldap_group_reference, group.ldap_group_reference);
    });
}

#[test]
fn test_delete_user() {
    run_test(async |cluster| {
        let username = new_key();
        let display_name = new_key();
        let roles = vec![
            Role::new("replication_target").bucket(&cluster.default_bucket),
            Role::new("replication_admin"),
        ];

        let user = User::new(&username, display_name, roles).password("password");
        create_user(&cluster.users(), user).await;

        delete_user(&cluster.users(), &username).await;

        let opts = GetUserOptions::new();
        try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(500),
            "get user after delete did not fail in time",
            async || {
                let err = match cluster.users().get_user(&username, opts.clone()).await {
                    Ok(_) => return Ok(None),
                    Err(e) => e,
                };

                if err.msg.to_ascii_lowercase().contains("not found") {
                    return Ok(Some(()));
                }

                Ok(None)
            },
        )
        .await;
    });
}

#[test]
fn test_get_user() {
    run_test(async |cluster| {
        let username = new_key();
        let display_name = new_key();
        let roles = vec![
            Role::new("replication_target").bucket(&cluster.default_bucket),
            Role::new("replication_admin"),
        ];

        let user = User::new(&username, display_name, roles).password("password");
        create_user(&cluster.users(), user.clone()).await;

        let opts = GetUserOptions::new();

        let actual = try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(500),
            "failed to get group in time",
            async || match cluster.users().get_user(&username, opts.clone()).await {
                Ok(u) => Ok(Some(u)),
                Err(e) => Err(e),
            },
        )
        .await;

        assert_user(&user, &actual);
    });
}

#[test]
fn test_get_all_users() {
    run_test(async |cluster| {
        let username = new_key();
        let display_name = new_key();
        let roles = vec![
            Role::new("replication_target").bucket(&cluster.default_bucket),
            Role::new("replication_admin"),
        ];

        let user = User::new(&username, display_name, roles).password("password");
        create_user(&cluster.users(), user.clone()).await;

        let opts = GetAllUsersOptions::new();

        let actual: UserAndMetadata = try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(500),
            "failed to get group in time",
            async || {
                let users = match cluster.users().get_all_users(opts.clone()).await {
                    Ok(u) => u,
                    Err(e) => return Err(e),
                };

                Ok(users.into_iter().find(|u| u.user.username == username))
            },
        )
        .await;

        assert_user(&user, &actual);
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

async fn create_user(mgr: &UserManager, user: User) {
    timeout_at(
        Instant::now().add(Duration::from_secs(5)),
        mgr.upsert_user(user, UpsertUserOptions::new().auth_domain("local")),
    )
    .await
    .unwrap()
    .unwrap();
}

async fn create_group(mgr: &UserManager, group: Group) {
    timeout_at(
        Instant::now().add(Duration::from_secs(5)),
        mgr.upsert_group(group, None),
    )
    .await
    .unwrap()
    .unwrap();
}

async fn delete_user(mgr: &UserManager, name: &str) {
    timeout_at(
        Instant::now().add(Duration::from_secs(5)),
        mgr.drop_user(name, DropUserOptions::new().auth_domain("local")),
    )
    .await
    .unwrap()
    .unwrap();
}

async fn delete_group(mgr: &UserManager, name: &str) {
    timeout_at(
        Instant::now().add(Duration::from_secs(5)),
        mgr.drop_group(name, None),
    )
    .await
    .unwrap()
    .unwrap();
}
