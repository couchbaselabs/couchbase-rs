use crate::bucket::Bucket;

pub struct Cluster {
    connection_string: String,
    username: String,
    password: String,
}

impl Cluster {
    pub fn connect<S>(connection_string: S, username: S, password: S) -> Self
    where
        S: Into<String>,
    {
        Cluster {
            connection_string: connection_string.into(),
            username: username.into(),
            password: password.into(),
        }
    }

    pub fn bucket<S>(&self, name: S) -> Bucket
    where
        S: Into<String>,
    {
        Bucket::new(
            &format!("{}/{}", self.connection_string, name.into()),
            &self.username,
            &self.password,
        )
    }
}
