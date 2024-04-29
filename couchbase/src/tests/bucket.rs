use std::collections::HashMap;
use std::sync::Arc;
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use mockall::predicate::eq;
use crate::{Bucket, CouchbaseResult, EndpointPingReport, ErrorContext, PingOptions, PingResult, ServiceType, ViewMetaData, ViewOptions, ViewResult, ViewRow};
use crate::io::request::{Request, PingRequest, ViewRequest};
use crate::tests::mock::{BUCKET, MockCore, NAME};

#[test]
fn create_custom_collection() {
    let mock_core = MockCore::default();
    let mocked_bucket = Bucket::new(Arc::new(mock_core), BUCKET.to_string());

    assert_eq!(mocked_bucket.collection(NAME.to_string()).name(), NAME);
}

#[test]
fn create_custom_scope() {
    let mock_core = MockCore::default();
    let mocked_bucket = Bucket::new(Arc::new(mock_core), BUCKET.to_string());

    assert_eq!(mocked_bucket.scope(NAME.to_string()).name(), NAME);
}

#[tokio::test]
async fn ping_works() {
    let (sender, _) = oneshot::channel();
    let request = Request::Ping(PingRequest {
        sender,
        options: PingOptions::default(),
    });

    let mut mock_core = MockCore::default();

    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::Ping(r) = x {
                let _ = r
                    .sender
                    .send(Ok(PingResult::new(NAME.to_string(), HashMap::<ServiceType, Vec<EndpointPingReport>>::new())));
            }
            ()
        });

    let mocked_bucket = Bucket::new(Arc::new(mock_core), BUCKET.to_string());

    let result: CouchbaseResult<PingResult> =
        mocked_bucket.ping(PingOptions::default()).await;
    assert_eq!(result.unwrap().id(), NAME);
}

#[tokio::test]
async fn view_query_works() {
    let (sender, _) = oneshot::channel();
    let options = ViewOptions::default();
    let form_data = options.form_data().unwrap();
    let payload = serde_urlencoded::to_string(form_data).unwrap();
    let request = Request::View(ViewRequest {
        design_document: "dev_test_ddoc".into(),
        view_name: "test_view".into(),
        options: payload.into_bytes(),
        sender,
    });

    let mut mock_core = MockCore::default();

    mock_core
        .expect_send()
        .with(eq(request))
        .times(1)
        .returning(|x| {
            if let Request::View(r) = x {
                let (mut metadata_sender, metadata_receiver) = mpsc::unbounded::<ViewRow>();
                let _ = metadata_sender.send(ViewRow{id: Some(NAME.to_string()), key: NAME.as_bytes().to_vec(), value: NAME.as_bytes().to_vec()});
                let _ = r
                    .sender
                    .send(Ok(ViewResult::new(metadata_receiver, oneshot::channel::<ViewMetaData>().1)));
            }
            ()
        });

    let mocked_bucket = Bucket::new(Arc::new(mock_core), BUCKET.to_string());


    let result: CouchbaseResult<ViewResult> =
        mocked_bucket.view_query("dev_test_ddoc",
                                 "test_view",
                                 ViewOptions::default()).await;


    assert!(result.is_ok());
}
