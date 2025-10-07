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
