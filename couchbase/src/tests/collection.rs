use crate::api::keyvalue_options::{GetReplicaOptions, ReplicaMode};
use crate::api::subdoc_results::SubDocField;
use crate::io::request::{
    ExistsRequest, GetReplicaRequest, GetRequest, GetRequestType, LookupInRequest, MutateInRequest,
    MutateRequest, MutateRequestType, RemoveRequest, Request, TouchRequest, UnlockRequest,
};
use crate::io::LOOKUPIN_MACRO_EXPIRYTIME;
use crate::tests::mock::*;
use crate::{
    ClientVerifiedDurability, Collection, CouchbaseResult, DurabilityLevel, ExistsOptions,
    ExistsResult, GetAndLockOptions, GetAndTouchOptions, GetAnyReplicaOptions, GetOptions,
    GetReplicaResult, GetResult, GetSpecOptions, InsertOptions, LookupInOptions, LookupInResult,
    LookupInSpec, MutateInOptions, MutateInResult, MutateInSpec, MutationResult, MutationToken,
    PersistTo, RemoveOptions, ReplaceOptions, ReplaceSpecOptions, TouchOptions, UnlockOptions,
    UpsertOptions, UpsertSpecOptions,
};
use chrono::NaiveDateTime;
use futures::channel::oneshot;
use log::debug;
use mockall::predicate::eq;
use serde_json::to_vec;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn get_direct_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Get(GetRequest {
        id: key.clone(),
        ty: GetRequestType::Get {
            options: GetOptions::default(),
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"Hello": "Rust!"}"#.as_bytes().to_vec(),
                    0,
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<GetResult> =
        mocked_collection.get(key, GetOptions::default()).await;
    assert_eq!(
        result.unwrap().content,
        r#"{"Hello": "Rust!"}"#.as_bytes().to_vec()
    );
}

#[tokio::test]
#[should_panic]
async fn get_direct_panic_for_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();

    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });

    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection.get(key, GetOptions::default()).await;
}

#[tokio::test]
async fn get_with_expiry_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let specs = vec![
        LookupInSpec::get(
            LOOKUPIN_MACRO_EXPIRYTIME,
            GetSpecOptions::default().xattr(true),
        ),
        LookupInSpec::get("", GetSpecOptions::default()),
    ];
    let request = Request::LookupIn(LookupInRequest {
        id: key.clone(),
        specs,
        sender,
        bucket: BUCKET.to_string(),
        options: LookupInOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::LookupIn(r) = x {
                let _ = r.sender.send(Ok(LookupInResult::new(
                    vec![
                        SubDocField {
                            status: 0,
                            value: "1713950100".as_bytes().to_vec(),
                        },
                        SubDocField {
                            status: 0,
                            value: "test".as_bytes().to_vec(),
                        },
                    ],
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );

    let result: CouchbaseResult<GetResult> = mocked_collection
        .get(key, GetOptions::default().with_expiry(true))
        .await;
    assert_eq!(
        result.unwrap().expiry_time,
        Some(NaiveDateTime::from_timestamp(1713950100, 0))
    );
}

#[tokio::test]
#[should_panic]
async fn get_with_expiry_panic_for_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });

    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );

    mocked_collection
        .get(key, GetOptions::default().with_expiry(true))
        .await;
}

#[tokio::test]
async fn get_any_replica_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let (get_replica_sender, _) = oneshot::channel();
    let options = GetAnyReplicaOptions::default();

    let replica_request = Request::GetReplica(GetReplicaRequest {
        id: key.clone(),
        options: GetReplicaOptions {
            timeout: options.timeout,
        },
        bucket: BUCKET.to_string(),
        sender: get_replica_sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        mode: ReplicaMode::Any,
    });

    let request = Request::Get(GetRequest {
        id: key.clone(),
        ty: GetRequestType::Get {
            options: GetOptions {
                timeout: options.timeout,
                with_expiry: false,
            },
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"Hello": "Rust!"}"#.as_bytes().to_vec(),
                    1,
                    0,
                )));
            }
            ()
        });
    mock_core
        .expect_send()
        .with(eq(replica_request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"ReplicaHello": "Rust!"}"#.as_bytes().to_vec(),
                    1,
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<GetReplicaResult> =
        mocked_collection.get_any_replica(key, None).await;

    assert_eq!(
        result.unwrap().content,
        r#"{"ReplicaHello": "Rust!"}"#.as_bytes().to_vec()
    );
}

#[tokio::test]
#[should_panic]
async fn get_any_replica_panic_for_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();

    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection.get_any_replica(key, None).await;
}

#[tokio::test]
async fn get_and_lock_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let options = GetAndLockOptions::default();
    let lock_time = Duration::from_secs(2);
    let request = Request::Get(GetRequest {
        id: key.clone(),
        ty: GetRequestType::GetAndLock { options, lock_time },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"Hello": "Rust!"}"#.as_bytes().to_vec(),
                    1,
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<GetResult> =
        mocked_collection.get_and_lock(key, lock_time, None).await;
    assert_eq!(
        result.unwrap().content,
        r#"{"Hello": "Rust!"}"#.as_bytes().to_vec()
    );
}

#[tokio::test]
#[should_panic]
async fn get_and_lock_panic_for_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let lock_time = Duration::from_secs(2);
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection.get_and_lock(key, lock_time, None).await;
}

#[tokio::test]
async fn get_and_touch_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let options = GetAndTouchOptions::default();
    let expiry = Duration::from_secs(2);
    let request = Request::Get(GetRequest {
        id: key.clone(),
        ty: GetRequestType::GetAndTouch { options, expiry },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"Hello": "Rust!"}"#.as_bytes().to_vec(),
                    1,
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<GetResult> =
        mocked_collection.get_and_touch(key, expiry, None).await;
    assert_eq!(
        result.unwrap().content,
        r#"{"Hello": "Rust!"}"#.as_bytes().to_vec()
    );
}

#[tokio::test]
#[should_panic]
async fn get_and_touch_panic_for_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let expiry = Duration::from_secs(2);
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection.get_and_touch(key, expiry, None).await;
}

#[tokio::test]
async fn exists_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let options = ExistsOptions::default();
    let request = Request::Exists(ExistsRequest {
        id: key.clone(),
        options,
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Exists(r) = x {
                let _ = r.sender.send(Ok(ExistsResult::new(true, Some(1u64))));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<ExistsResult> = mocked_collection.exists(key, None).await;
    assert!(result.unwrap().exists());
}

#[tokio::test]
#[should_panic]
async fn exists_panic_for_wrong_request() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection.exists(key, None).await;
}
#[tokio::test]
async fn upsert_works() {
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");
    let (sender, _) = oneshot::channel();
    let request = Request::Mutate(MutateRequest {
        id: key.clone(),
        content: to_vec(&content).unwrap(),
        ty: MutateRequestType::Upsert {
            options: UpsertOptions::default(),
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Mutate(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<MutationResult> = mocked_collection
        .upsert(key, content, UpsertOptions::default())
        .await;
    assert_eq!(result.unwrap().cas(), 1);
}

#[tokio::test]
#[should_panic]
async fn upsert_panic_for_wrong_request() {
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");
    let (sender, _) = oneshot::channel();
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection
        .upsert(key, content, UpsertOptions::default())
        .await;
}
#[tokio::test]
async fn insert_works() {
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");
    let (sender, _) = oneshot::channel();
    let request = Request::Mutate(MutateRequest {
        id: key.clone(),
        content: to_vec(&content).unwrap(),
        ty: MutateRequestType::Insert {
            options: InsertOptions::default(),
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Mutate(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<MutationResult> = mocked_collection
        .insert(key, content, InsertOptions::default())
        .await;
    assert_eq!(result.unwrap().cas(), 1);
}

#[tokio::test]
#[should_panic]
async fn insert_panic_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");
    let (sender, _) = oneshot::channel();
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection
        .insert(key, content, InsertOptions::default())
        .await;
}

#[tokio::test]
async fn replace_works() {
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");
    let (sender, _) = oneshot::channel();
    let request = Request::Mutate(MutateRequest {
        id: key.clone(),
        content: to_vec(&content).unwrap(),
        ty: MutateRequestType::Replace {
            options: ReplaceOptions::default(),
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Mutate(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<MutationResult> = mocked_collection
        .replace(key, content, ReplaceOptions::default())
        .await;
    assert_eq!(result.unwrap().cas(), 1);
}

#[tokio::test]
#[should_panic]
async fn replace_panic_for_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");
    let (sender, _) = oneshot::channel();
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<MutationResult> = mocked_collection
        .replace(key, content, ReplaceOptions::default())
        .await;
    assert_eq!(result.unwrap().cas(), 1);
}

#[tokio::test]
async fn get_remove_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Remove(RemoveRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: RemoveOptions::default()
            .durability(DurabilityLevel::Majority)
            .timeout(Duration::from_secs(5)),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Remove(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<MutationResult> = mocked_collection
        .remove(
            key,
            RemoveOptions::default()
                .durability(DurabilityLevel::Majority)
                .timeout(Duration::from_secs(5)),
        )
        .await;
    assert_eq!(result.unwrap().cas(), 1);
}

#[tokio::test]
#[should_panic]
async fn get_remove_panic_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection
        .remove(
            key,
            RemoveOptions::default()
                .durability(DurabilityLevel::Majority)
                .timeout(Duration::from_secs(5)),
        )
        .await;
}

#[tokio::test]
async fn get_remove_does_not_work_observe_based_durability() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Remove(RemoveRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: RemoveOptions::default().durability(DurabilityLevel::ClientVerified(
            ClientVerifiedDurability::default().persist_to(PersistTo::One),
        )),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(0)
        .returning(|x| {
            if let Request::Remove(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<MutationResult> = mocked_collection
        .remove(
            key,
            RemoveOptions::default().durability(DurabilityLevel::ClientVerified(
                ClientVerifiedDurability::default().persist_to(PersistTo::One),
            )),
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn get_touch_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Touch(TouchRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: TouchOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        expiry: Duration::from_secs(10),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Touch(r) = x {
                let _ = r.sender.send(Ok(MutationResult::new(
                    1,
                    Some(MutationToken::new(1, 1, 1, BUCKET.to_string())),
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<MutationResult> = mocked_collection
        .touch(key, Duration::from_secs(10), TouchOptions::default())
        .await;
    assert_eq!(result.unwrap().cas(), 1);
}

#[tokio::test]
#[should_panic]
async fn get_touch_panic_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Get(GetRequest {
        id: key.clone(),
        ty: GetRequestType::Get {
            options: GetOptions::default(),
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"Hello": "Rust!"}"#.as_bytes().to_vec(),
                    0,
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection
        .touch(key, Duration::from_secs(10), TouchOptions::default())
        .await;
}

#[tokio::test]
async fn get_unlock_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Unlock(UnlockRequest {
        id: key.clone(),
        sender,
        bucket: BUCKET.to_string(),
        options: UnlockOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
        cas: 1,
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Unlock(r) = x {
                let _ = r.sender.send(Ok(()));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<()> = mocked_collection
        .unlock(key, 1, UnlockOptions::default())
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
#[should_panic]
async fn get_unlock_panic_for_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Get(GetRequest {
        id: key.clone(),
        ty: GetRequestType::Get {
            options: GetOptions::default(),
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"Hello": "Rust!"}"#.as_bytes().to_vec(),
                    0,
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection
        .unlock(key, 1, UnlockOptions::default())
        .await;
}
#[tokio::test]
async fn lookup_in_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::LookupIn(LookupInRequest {
        id: key.clone(),
        specs: vec![LookupInSpec::get(
            "$document.exptime",
            GetSpecOptions::default().xattr(true),
        )],
        sender,
        bucket: BUCKET.to_string(),
        options: LookupInOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::LookupIn(r) = x {
                let _ = r.sender.send(Ok(LookupInResult::new(
                    vec![
                        SubDocField {
                            status: 0,
                            value: "1713950100".as_bytes().to_vec(),
                        },
                        SubDocField {
                            status: 0,
                            value: "test".as_bytes().to_vec(),
                        },
                    ],
                    1,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<LookupInResult> = mocked_collection
        .lookup_in(
            key,
            vec![LookupInSpec::get(
                "$document.exptime",
                GetSpecOptions::default().xattr(true),
            )],
            LookupInOptions::default(),
        )
        .await;
    assert_eq!(result.unwrap().cas(), 1);
}

#[tokio::test]
#[should_panic]
async fn lookup_in_panic_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Get(GetRequest {
        id: key.clone(),
        ty: GetRequestType::Get {
            options: GetOptions::default(),
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"Hello": "Rust!"}"#.as_bytes().to_vec(),
                    0,
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection
        .lookup_in(
            key,
            vec![LookupInSpec::get(
                "$document.exptime",
                GetSpecOptions::default().xattr(true),
            )],
            LookupInOptions::default(),
        )
        .await;
}

#[tokio::test]
async fn mutate_in_works() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::MutateIn(MutateInRequest {
        id: key.clone(),
        specs: vec![
            MutateInSpec::replace("name", "52-Mile Air", ReplaceSpecOptions::default()).unwrap(),
            MutateInSpec::upsert("foo", "bar", UpsertSpecOptions::default()).unwrap(),
        ],
        sender,
        bucket: BUCKET.to_string(),
        options: MutateInOptions::default(),
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::MutateIn(r) = x {
                let _ = r.sender.send(Ok(MutateInResult::new(
                    vec![
                        SubDocField {
                            status: 0,
                            value: "1713950100".as_bytes().to_vec(),
                        },
                        SubDocField {
                            status: 0,
                            value: "test".as_bytes().to_vec(),
                        },
                    ],
                    1,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    let result: CouchbaseResult<MutateInResult> = mocked_collection
        .mutate_in(
            key,
            vec![
                MutateInSpec::replace("name", "52-Mile Air", ReplaceSpecOptions::default())
                    .unwrap(),
                MutateInSpec::upsert("foo", "bar", UpsertSpecOptions::default()).unwrap(),
            ],
            MutateInOptions::default(),
        )
        .await;
    assert_eq!(result.unwrap().cas(), 1);
}

#[tokio::test]
#[should_panic]
async fn mutate_in_panic_wrong_input() {
    let key = Uuid::new_v4().to_string();
    let (sender, _) = oneshot::channel();
    let request = Request::Get(GetRequest {
        id: key.clone(),
        ty: GetRequestType::Get {
            options: GetOptions::default(),
        },
        bucket: BUCKET.to_string(),
        sender,
        scope: SCOPE.to_string(),
        collection: NAME.to_string(),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Get(r) = x {
                let _ = r.sender.send(Ok(GetResult::new(
                    r#"{"Hello": "Rust!"}"#.as_bytes().to_vec(),
                    0,
                    0,
                )));
            }
            ()
        });
    let mocked_collection = Collection::new(
        Arc::new(mock_core),
        NAME.to_string(),
        SCOPE.to_string(),
        BUCKET.to_string(),
    );
    mocked_collection
        .mutate_in(
            key,
            vec![
                MutateInSpec::replace("name", "52-Mile Air", ReplaceSpecOptions::default())
                    .unwrap(),
                MutateInSpec::upsert("foo", "bar", UpsertSpecOptions::default()).unwrap(),
            ],
            MutateInOptions::default(),
        )
        .await;
}
