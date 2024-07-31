use crate::agent::Agent;
use crate::crudoptions::{GetOptions, UpsertOptions};
use crate::crudresults::{GetResult, UpsertResult};
use crate::error::Result;

impl Agent {
    pub async fn upsert(&self, opts: UpsertOptions) -> Result<UpsertResult> {
        self.inner.crud.upsert(opts).await
    }

    pub async fn get(&self, opts: GetOptions) -> Result<GetResult> {
        self.inner.crud.get(opts).await
    }
}
