use url::Url;

use crate::error;
use crate::error::ErrorKind;

pub(crate) fn get_host_from_uri(uri: &str) -> error::Result<Option<String>> {
    let parsed = Url::parse(uri).map_err(|e| ErrorKind::Generic { msg: e.to_string() })?;

    let host = if let Some(host) = parsed.host() {
        if let Some(port) = parsed.port() {
            format!("{}:{}", host, port)
        } else {
            host.to_string()
        }
    } else {
        return Ok(None);
    };

    Ok(Some(host))
}
