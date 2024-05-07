use crate::io::request::{AnalyticsRequest, GetRequest, GetRequestType, QueryRequest, Request};
use crate::tests::mock::{MockCore, BUCKET, NAME, SCOPE};
use crate::{
    AnalyticsMetaData, AnalyticsOptions, AnalyticsResult, Cluster, CouchbaseResult, GetOptions,
    GetResult, QueryMetaData, QueryOptions, QueryResult, Scope,
};
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use mockall::predicate::eq;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn query_works() {
    let (sender, _) = oneshot::channel();
    let request = Request::Query(QueryRequest {
        statement: "select 1=1".to_string(),
        options: QueryOptions::default(),
        sender,
        scope: Some(BUCKET.to_string()),
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Query(r) = x {
                let (mut metadata_sender, metadata_receiver) = mpsc::unbounded::<Vec<u8>>();
                let _ = metadata_sender.send("tes".as_bytes().to_vec());
                let _ = r.sender.send(Ok(QueryResult::new(
                    metadata_receiver,
                    oneshot::channel::<QueryMetaData>().1,
                )));
            }
            ()
        });

    let mocked_scope = Scope::new(Arc::new(mock_core), NAME.to_string(), BUCKET.to_string());

    let result: CouchbaseResult<QueryResult> = mocked_scope
        .query("select 1=1".to_string(), QueryOptions::default())
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
#[should_panic]
async fn query_panic_wrong_input() {
    let (sender, _) = oneshot::channel();
    let request = Request::Get(GetRequest {
        id: Uuid::new_v4().to_string(),
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
                    1,
                    0,
                )));
            }
            ()
        });

    let mocked_scope = Scope::new(Arc::new(mock_core), NAME.to_string(), BUCKET.to_string());

    let _ = mocked_scope
        .query("select 1=1".to_string(), QueryOptions::default())
        .await;
}

#[tokio::test]
async fn analytics_query_works() {
    let (sender, _) = oneshot::channel();
    let request = Request::Analytics(AnalyticsRequest {
        statement: "select 1=1".to_string(),
        options: AnalyticsOptions::default(),
        sender,
        scope: None,
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Analytics(r) = x {
                let (mut metadata_sender, metadata_receiver) = mpsc::unbounded::<Vec<u8>>();
                let _ = metadata_sender.send("tes1".as_bytes().to_vec());
                let _ = r.sender.send(Ok(AnalyticsResult::new(
                    metadata_receiver,
                    oneshot::channel::<AnalyticsMetaData>().1,
                )));
            }
            ()
        });

    let mocked_scope = Scope::new(Arc::new(mock_core), NAME.to_string(), BUCKET.to_string());

    let result: CouchbaseResult<AnalyticsResult> = mocked_scope
        .analytics_query("select 1=1".to_string(), AnalyticsOptions::default())
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
#[should_panic]
async fn analytics_query_panic_wrong_input() {
    let (sender, _) = oneshot::channel();
    let request = Request::Get(GetRequest {
        id: Uuid::new_v4().to_string(),
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
                    1,
                    0,
                )));
            }
            ()
        });

    let mocked_scope = Scope::new(Arc::new(mock_core), NAME.to_string(), BUCKET.to_string());

    let _ = mocked_scope
        .analytics_query("select 1=1".to_string(), AnalyticsOptions::default())
        .await;
}
