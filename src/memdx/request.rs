use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::hello_feature::HelloFeature;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HelloRequest {
    pub client_name: Vec<u8>,
    pub requested_features: Vec<HelloFeature>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetErrorMapRequest {
    pub version: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelectBucketRequest {
    pub bucket_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthRequest {
    pub auth_mechanism: AuthMechanism,
    pub payload: Vec<u8>,
}
