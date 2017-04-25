use url::{Url, ParseError};
use error::CouchbaseError;

#[derive(Debug)]
pub struct ConnectionString {
    url: Url,
    scheme: UrlScheme,
}

#[derive(Debug,PartialEq)]
pub enum UrlScheme {
    Http,
    Couchbase,
    Couchbases,
}

impl UrlScheme {
    pub fn as_str(&self) -> &str {
        match *self {
            UrlScheme::Http => "http",
            UrlScheme::Couchbase => "couchbase",
            UrlScheme::Couchbases => "couchbases",
        }
    }
}

impl ConnectionString {
    /// Creates a new `ConnectionString` instance from a raw string.
    pub fn new(cs: &str) -> Result<Self, CouchbaseError> {
        let mut url = Url::parse(cs);
        if url.is_err() && url.as_ref().err().unwrap() == &ParseError::RelativeUrlWithoutBase {
            let default_cs = format!("couchbase://{}", cs);
            url = Url::parse(&default_cs);
        }
        if url.is_err() {
            return Err(CouchbaseError::InvalidHostFormat);
        }
        let u = url.unwrap();

        let scheme = match u.scheme() {
            "http" => UrlScheme::Http,
            "couchbase" => UrlScheme::Couchbase,
            "couchbases" => UrlScheme::Couchbases,
            _ => return Err(CouchbaseError::InvalidHostFormat),
        };

        if u.username() != "" {
            warn!("Username must be set on the authenticator directly, not via the connection \
                   string!");
            return Err(CouchbaseError::InvalidHostFormat);
        }

        if u.password().is_some() {
            warn!("Password must be set on the authenticator directly, not via the connection \
                   string!");
            return Err(CouchbaseError::InvalidHostFormat);
        }

        Ok(ConnectionString {
            url: u,
            scheme: scheme,
        })
    }

    pub fn scheme(&self) -> &UrlScheme {
        &self.scheme
    }

    pub fn host(&self) -> &str {
        self.url.host_str().unwrap_or("127.0.0.1")
    }

    pub fn query(&self) -> &str {
        self.url.query().unwrap_or("")
    }

    pub fn export(&self, bucket: &str) -> String {
        format!("{}://{}/{}?{}",
                self.scheme().as_str(),
                self.host(),
                bucket,
                self.query())
    }
}

impl Default for ConnectionString {
    fn default() -> Self {
        ConnectionString::new("couchbase://127.0.0.1").unwrap()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_default() {
        let cs = ConnectionString::default();
        assert_eq!("127.0.0.1", cs.host());
        assert_eq!(UrlScheme::Couchbase, *cs.scheme());
    }

    #[test]
    fn test_bucket_removal() {
        let cs = ConnectionString::new("couchbases://1.2.3.4,5.6.7.8/bucket").unwrap();
        assert_eq!("1.2.3.4,5.6.7.8", cs.host());
        assert_eq!(UrlScheme::Couchbases, *cs.scheme());
        assert_eq!("couchbases://1.2.3.4,5.6.7.8/bla?", cs.export("bla"));
    }

    #[test]
    fn test_with_params() {
        let cs = ConnectionString::new("http://localhost?foo=bar").unwrap();
        assert_eq!("localhost", cs.host());
        assert_eq!(UrlScheme::Http, *cs.scheme());
        assert_eq!("http://localhost/bla?foo=bar", cs.export("bla"));
    }

}
