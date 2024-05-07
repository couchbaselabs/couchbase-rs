use crate::io::request::{
    AnalyticsRequest, GetRequest, GetRequestType, QueryRequest, Request, SearchRequest,
};
use crate::tests::mock::{MockCore, BUCKET, NAME, SCOPE};
use crate::{
    AnalyticsMetaData, AnalyticsOptions, AnalyticsResult, Cluster, CouchbaseError, CouchbaseResult,
    ErrorContext, GetOptions, GetResult, QueryMetaData, QueryOptions, QueryResult,
    QueryStringQuery, SearchMetaData, SearchOptions, SearchQuery, SearchResult,
};
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use mockall::predicate::eq;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

#[test]
fn bucket_works() {
    let mut mock_core = MockCore::default();

    mock_core
        .expect_open_bucket()
        .with(eq(NAME.to_string()))
        .times(1)
        .returning(|x| ());

    let cluster = Cluster::new(Arc::new(mock_core));

    assert_eq!(cluster.bucket(NAME.to_string()).name(), NAME);
}

#[tokio::test]
async fn query_works() {
    let (sender, _) = oneshot::channel();
    let request = Request::Query(QueryRequest {
        statement: "select 1=1".to_string(),
        options: QueryOptions::default(),
        sender,
        scope: None,
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

    let mocked_cluster = Cluster::new(Arc::new(mock_core));

    let result: CouchbaseResult<QueryResult> = mocked_cluster
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

    let mocked_cluster = Cluster::new(Arc::new(mock_core));

    mocked_cluster
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

    let mocked_cluster = Cluster::new(Arc::new(mock_core));

    let result: CouchbaseResult<AnalyticsResult> = mocked_cluster
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

    let mocked_cluster = Cluster::new(Arc::new(mock_core));

    let _ = mocked_cluster
        .analytics_query("select 1=1".to_string(), AnalyticsOptions::default())
        .await;
}

#[tokio::test]
async fn search_query_works() {
    let (sender, _) = oneshot::channel();
    let query = QueryStringQuery::new(String::from("swanky"));
    let request = Request::Search(SearchRequest {
        index: String::from("test"),
        query: query
            .to_json()
            .map_err(|e| CouchbaseError::EncodingFailure {
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                ctx: ErrorContext::default(),
            })
            .unwrap(),
        options: SearchOptions::default(),
        sender,
    });

    let mut mock_core = MockCore::default();
    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Search(r) = x {
                let (mut metadata_sender, metadata_receiver) = mpsc::unbounded::<Vec<u8>>();
                let _ = metadata_sender.send("tes1".as_bytes().to_vec());
                let _ = r.sender.send(Ok(SearchResult::new(
                    metadata_receiver,
                    oneshot::channel::<SearchMetaData>().1,
                    oneshot::channel::<Value>().1,
                )));
            }
            ()
        });

    let mocked_cluster = Cluster::new(Arc::new(mock_core));

    let result: CouchbaseResult<SearchResult> = mocked_cluster
        .search_query(
            String::from("test"),
            QueryStringQuery::new(String::from("swanky")),
            SearchOptions::default(),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
#[should_panic]
async fn search_query_panic_wrong_input() {
    let (sender, _) = oneshot::channel();
    let query = QueryStringQuery::new(String::from("swanky"));
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

    let mocked_cluster = Cluster::new(Arc::new(mock_core));

    let _ = mocked_cluster
        .search_query(
            String::from("test"),
            QueryStringQuery::new(String::from("swanky")),
            SearchOptions::default(),
        )
        .await;
}
