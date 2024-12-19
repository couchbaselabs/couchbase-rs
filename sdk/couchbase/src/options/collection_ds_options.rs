#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseListOptions {}

impl CouchbaseListOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseMapOptions {}

impl CouchbaseMapOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseSetOptions {}

impl CouchbaseSetOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseQueueOptions {}

impl CouchbaseQueueOptions {
    pub fn new() -> Self {
        Default::default()
    }
}
