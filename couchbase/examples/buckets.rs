use couchbase::{
    BucketSettings, BucketSettingsBuilder, Cluster, CreateBucketOptions, DropBucketOptions,
    FlushBucketOptions, GetAllBucketsOptions, GetBucketOptions, UpdateBucketOptions,
};
use futures::executor::block_on;

/// Bucket manager Examples
///
/// This file shows how to manage buckets against the Cluster.
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://10.112.210.101", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("default");

    let mgr = cluster.buckets();

    let settings = BucketSettingsBuilder::new("mybucket", 100)
        .flush_enabled(true)
        .build();

    match block_on(mgr.create_bucket(settings, CreateBucketOptions::default())) {
        Ok(_result) => {}
        Err(e) => println!("got error! {}", e),
    };

    let get_settings: BucketSettings =
        match block_on(mgr.get_bucket("mybucket", GetBucketOptions::default())) {
            Ok(result) => {
                dbg!(&result);
                result
            }
            Err(e) => {
                panic!(e)
            }
        };

    match block_on(mgr.flush_bucket("mybucket", FlushBucketOptions::default())) {
        Ok(result) => {}
        Err(e) => println!("got error! {}", e),
    };

    match block_on(mgr.update_bucket(get_settings, UpdateBucketOptions::default())) {
        Ok(_result) => {}
        Err(e) => println!("got error! {}", e),
    };

    match block_on(mgr.get_all_buckets(GetAllBucketsOptions::default())) {
        Ok(result) => {
            dbg!(result);
        }
        Err(e) => println!("got error! {}", e),
    };

    match block_on(mgr.drop_bucket("mybucket", DropBucketOptions::default())) {
        Ok(result) => {}
        Err(e) => println!("got error! {}", e),
    };
}
