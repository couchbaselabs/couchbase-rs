use crate::agent::Agent;
use crate::crudoptions::{GetOptions, UpsertOptions};
use crate::crudresults::{GetResult, UpsertResult};
use crate::error::Result;

impl Agent {
    pub async fn upsert<'a>(&self, opts: UpsertOptions<'a>) -> Result<UpsertResult> {
        self.inner.crud.upsert(opts).await
    }

    pub async fn get<'a>(&self, opts: GetOptions<'a>) -> Result<GetResult> {
        self.inner.crud.get(opts).await
    }
}
