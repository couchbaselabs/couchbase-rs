use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[non_exhaustive]
pub struct Location {
    pub lat: f64,
    pub lon: f64,
}

impl From<Location> for couchbase_core::searchx::query_options::Location {
    fn from(location: Location) -> Self {
        couchbase_core::searchx::query_options::Location::builder()
            .lat(location.lat)
            .lon(location.lon)
            .build()
    }
}
