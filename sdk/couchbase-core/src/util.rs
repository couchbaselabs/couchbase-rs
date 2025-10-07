/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use url::Url;

use crate::error;
use crate::error::Error;

pub(crate) fn get_host_port_from_uri(uri: &str) -> error::Result<String> {
    let parsed = Url::parse(uri)
        .map_err(|e| Error::new_message_error(format!("failed to parse uri: {e}")))?;

    let host = if let Some(host) = parsed.host() {
        if let Some(port) = parsed.port() {
            format!("{host}:{port}")
        } else {
            host.to_string()
        }
    } else {
        return Err(Error::new_message_error(format!("no host in URI {uri}")));
    };

    Ok(host)
}

pub(crate) fn hostname_from_addr_str(addr: &str) -> String {
    let (host, _) = match split_host_port(addr) {
        Ok(hp) => hp,
        Err(_e) => return addr.to_string(),
    };
    host.to_string()
}

pub(crate) fn get_hostname_from_host_port(host_port: &str) -> error::Result<String> {
    let (host, _) = split_host_port(host_port)?;

    if host.contains(':') {
        return Ok(format!("[{host}]"));
    }

    Ok(host.to_string())
}

fn split_host_port(hostport: &str) -> error::Result<(&str, &str)> {
    const MISSING_PORT: &str = "missing port in address";
    const TOO_MANY_COLONS: &str = "too many colons in address";

    let addr_err = |addr: &str, why: &str| -> error::Result<(&str, &str)> {
        Err(Error::new_message_error(format!(
            "invalid address '{addr}': {why}"
        )))
    };

    let i = hostport
        .rfind(':')
        .ok_or_else(|| Error::new_message_error(MISSING_PORT))?;

    if let Some(stripped) = hostport.strip_prefix('[') {
        let end = hostport
            .find(']')
            .ok_or_else(|| Error::new_message_error("missing ']' in address"))?;
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
