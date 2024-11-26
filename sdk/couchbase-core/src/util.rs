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

pub(crate) fn hostname_from_addr_str(addr: &str) -> String {
    let (host, _) = match split_host_port(addr) {
        Ok(hp) => hp,
        Err(_e) => return addr.to_string(),
    };
    host.to_string()
}

fn split_host_port(hostport: &str) -> error::Result<(&str, &str)> {
    const MISSING_PORT: &str = "missing port in address";
    const TOO_MANY_COLONS: &str = "too many colons in address";

    let addr_err = |addr: &str, why: &str| -> error::Result<(&str, &str)> {
        Err(ErrorKind::InvalidArgument {
            msg: format!("invalid address '{}': {}", addr, why),
        }
        .into())
    };

    let i = hostport
        .rfind(':')
        .ok_or_else(|| ErrorKind::InvalidArgument {
            msg: MISSING_PORT.to_string(),
        })?;

    if let Some(stripped) = hostport.strip_prefix('[') {
        let end = hostport
            .find(']')
            .ok_or_else(|| ErrorKind::InvalidArgument {
                msg: "missing ']' in address".to_string(),
            })?;
        if end + 1 == hostport.len() {
            return addr_err(hostport, MISSING_PORT);
        } else if end + 1 != i {
            if hostport.chars().nth(end + 1) == Some(':') {
                return addr_err(hostport, TOO_MANY_COLONS);
            }
            return addr_err(hostport, MISSING_PORT);
        }
        let host = &hostport[1..end];
        let port = &hostport[i + 1..];
        if stripped.contains('[') || hostport[end + 1..].contains(']') {
            return addr_err(hostport, "unexpected '[' or ']' in address");
        }
        Ok((host, port))
    } else {
        let host = &hostport[..i];
        if host.contains(':') {
            return addr_err(hostport, TOO_MANY_COLONS);
        }
        let port = &hostport[i + 1..];
        Ok((host, port))
    }
}
