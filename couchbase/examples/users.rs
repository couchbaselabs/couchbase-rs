use couchbase::{
    Cluster, GetAllGroupsOptions, GetAllUsersOptions, GetRolesOptions, GetUserOptions, Group, Role,
    UpsertGroupOptions, UpsertUserOptions, User, UserBuilder,
};
use futures::executor::block_on;

/// Users Examples
///
/// This file shows how to manage users against the Cluster. Note that if you
/// are using a cluster pre 6.5 you need to open at least one bucket to make cluster-level
/// operations.
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://172.23.111.1", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("default");
    // Use the default collection (needs to be used for all server 6.5 and earlier)
    let _collection = bucket.default_collection();

    // Create a user manager
    let manager = cluster.users();

    // Create a user
    let user = UserBuilder::new(
        String::from("my_favourite_user"),
        Some(String::from("password")),
        vec![Role::new(
            String::from("bucket_admin"),
            Some(String::from("default")),
        )],
    )
    .display_name(String::from("favourite"))
    .build();

    // Upsert the user
    match block_on(manager.upsert_user(user, UpsertUserOptions::default())) {
        Ok(_) => {}
        Err(e) => panic!("got error! {}", e),
    }

    // Gets all the users
    let mut user_returned: User;
    match block_on(manager.get_all_users(GetAllUsersOptions::default())) {
        Ok(result) => {
            println!("users: {:?}", result);
            if result.len() == 0 {
                panic!("no users returned!");
            }
            user_returned = result[0].user();
        }
        Err(e) => panic!("got error! {}", e),
    }
    user_returned.set_display_name(String::from("billy_bob"));

    // Upsert the modified user
    match block_on(manager.upsert_user(user_returned, UpsertUserOptions::default())) {
        Ok(_) => {}
        Err(e) => panic!("got error! {}", e),
    }

    // Gets the updated user by username
    match block_on(manager.get_user(String::from("my_favourite_user"), GetUserOptions::default())) {
        Ok(result) => {
            println!("user and metadata: {:?}", result);
            println!("user: {:?}", result.user());
        }
        Err(e) => println!("got error! {}", e),
    }

    let group = Group::new(
        String::from("my_favourite_group"),
        vec![Role::new(String::from("admin"), None)],
    );

    // Upsert the group
    match block_on(manager.upsert_group(group, UpsertGroupOptions::default())) {
        Ok(_) => {}
        Err(e) => panic!("got error! {}", e),
    }

    // Gets all of the groups
    match block_on(manager.get_all_groups(GetAllGroupsOptions::default())) {
        Ok(result) => {
            println!("groups: {:?}", result);
        }
        Err(e) => println!("got error! {}", e),
    }

    // Gets all of the roles
    match block_on(manager.get_roles(GetRolesOptions::default())) {
        Ok(result) => {
            println!("roles: {:?}", result);
        }
        Err(e) => println!("got error! {}", e),
    }
}
