use couchbase::{
    Cluster, DesignDocumentBuilder, DesignDocumentNamespace, DropDesignDocumentsOptions,
    GetAllDesignDocumentsOptions, PublishDesignDocumentsOptions, UpsertDesignDocumentOptions,
    ViewBuilder,
};
use futures::executor::block_on;
use std::collections::HashMap;

/// Query Index Examples
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://localhost", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("travel-sample");
    // Use the default collection (needs to be used for all server 6.5 and earlier)
    let _collection = bucket.default_collection();

    let index_manager = bucket.view_indexes();

    let view = ViewBuilder::new(
        "
    function (doc, meta) {
      emit(meta.id, null);
    }
",
    )
    .build();

    let mut views = HashMap::new();
    views.insert(String::from("testv"), view);

    match block_on(index_manager.upsert_design_document(
        DesignDocumentBuilder::new("test", views).build(),
        DesignDocumentNamespace::Development,
        UpsertDesignDocumentOptions::default(),
    )) {
        Ok(_result) => {
            println!("ddoc created");
        }
        Err(e) => println!("got error! {}", e),
    }

    match block_on(index_manager.get_all_design_documents(
        DesignDocumentNamespace::Development,
        GetAllDesignDocumentsOptions::default(),
    )) {
        Ok(result) => {
            for index in result {
                println!("Got all ddocs {:?}", index);
            }
        }
        Err(e) => println!("got error! {}", e),
    }

    match block_on(
        index_manager.publish_design_document("test", PublishDesignDocumentsOptions::default()),
    ) {
        Ok(_result) => {
            println!("ddoc published");
        }
        Err(e) => println!("got error! {}", e),
    }

    match block_on(index_manager.get_all_design_documents(
        DesignDocumentNamespace::Production,
        GetAllDesignDocumentsOptions::default(),
    )) {
        Ok(result) => {
            for index in result {
                println!("Got all ddocs {:?}", index);
            }
        }
        Err(e) => println!("got error! {}", e),
    }

    // Drops a named primary index
    match block_on(index_manager.drop_design_document(
        "test",
        DesignDocumentNamespace::Production,
        DropDesignDocumentsOptions::default(),
    )) {
        Ok(_result) => {
            println!("ddoc dropped");
        }
        Err(e) => println!("got error! {}", e),
    }

    match block_on(index_manager.get_all_design_documents(
        DesignDocumentNamespace::Production,
        GetAllDesignDocumentsOptions::default(),
    )) {
        Ok(result) => {
            for index in result {
                println!("Got all ddocs {:?}", index);
            }
        }
        Err(e) => println!("got error! {}", e),
    }

    match block_on(index_manager.get_all_design_documents(
        DesignDocumentNamespace::Development,
        GetAllDesignDocumentsOptions::default(),
    )) {
        Ok(result) => {
            for index in result {
                println!("Got all ddocs {:?}", index);
            }
        }
        Err(e) => println!("got error! {}", e),
    }
}
