use crate::io::request::Request;

use mockall::mock;

mock!(
    #[derive(Debug)]
    pub Core {
        pub fn new(
            connection_string: String,
            username: Option<String>,
            password: Option<String>,
        ) -> Self ;
        pub fn send(&self, request: Request);
        pub fn open_bucket(&self, name: String);
    }
);

pub const NAME: &str = "default";
pub const SCOPE: &str = "_default";
pub const BUCKET: &str = "default";
