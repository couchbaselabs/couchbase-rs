use couchbase::error::Error;
use log::error;
use std::ops::Add;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

pub async fn try_until<Fut, T>(
    deadline: Instant,
    sleep: tokio::time::Duration,
    fail_msg: impl AsRef<str>,
    mut f: impl FnMut() -> Fut,
) -> T
where
    Fut: std::future::Future<Output = Result<Option<T>, couchbase::error::Error>>,
{
    while Instant::now() < deadline {
        match f().await {
            Ok(Some(r)) => return r,
            Ok(None) => {}
            Err(e) => {
                error!("{:?}", e);
            }
        };

        tokio::time::sleep(sleep).await;
    }
    panic!("{}", fail_msg.as_ref());
}

pub async fn run_with_deadline<Resp, Fut>(deadline: Instant, f: Fut) -> Result<Resp, Error>
where
    Fut: std::future::Future<Output = Result<Resp, Error>>,
{
    timeout_at(deadline, f).await.unwrap()
}

pub async fn run_with_std_kv_deadline<Resp, Fut>(f: Fut) -> Result<Resp, Error>
where
    Fut: std::future::Future<Output = Result<Resp, Error>>,
{
    timeout_at(Instant::now().add(Duration::from_millis(2500)), f)
        .await
        .unwrap()
}

pub async fn run_with_std_mgmt_deadline<Resp, Fut>(f: Fut) -> Result<Resp, Error>
where
    Fut: std::future::Future<Output = Result<Resp, Error>>,
{
    timeout_at(Instant::now().add(Duration::from_millis(10000)), f)
        .await
        .unwrap()
}

pub async fn run_with_std_query_deadline<Resp, Fut>(f: Fut) -> Result<Resp, Error>
where
    Fut: std::future::Future<Output = Result<Resp, Error>>,
{
    timeout_at(Instant::now().add(Duration::from_millis(10000)), f)
        .await
        .unwrap()
}
