use crate::httpx;
use httpx::error::Result as HttpxResult;
use serde::de::DeserializeOwned;
use crate::httpx::base::ResponseProvider;

pub async fn read_as_json<T>(resp: impl ResponseProvider) -> HttpxResult<T>
where
    T: DeserializeOwned,
{
    resp.read_response_as_json::<T>().await
}
