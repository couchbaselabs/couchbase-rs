use crate::memdx::hello_feature::HelloFeature;

pub struct HelloRequest {
    pub client_name: Vec<u8>,
    pub requested_features: Vec<HelloFeature>,
}
