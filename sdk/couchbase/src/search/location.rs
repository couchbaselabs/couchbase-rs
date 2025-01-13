#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Location {
    pub(crate) lat: f64,
    pub(crate) lon: f64,
}

impl Location {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }

    pub fn lat(&self) -> f64 {
        self.lat
    }

    pub fn lon(&self) -> f64 {
        self.lon
    }
}

impl From<Location> for couchbase_core::searchx::query_options::Location {
    fn from(location: Location) -> Self {
        couchbase_core::searchx::query_options::Location::new(location.lat, location.lon)
    }
}
