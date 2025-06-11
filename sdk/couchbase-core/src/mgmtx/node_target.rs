#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NodeTarget {
    pub endpoint: String,
    pub username: String,
    pub password: String,
}

impl crate::httpcomponent::NodeTarget for NodeTarget {
    fn new(endpoint: String, username: String, password: String) -> Self {
        Self {
            endpoint,
            username,
            password,
        }
    }
}
