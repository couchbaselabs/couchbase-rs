use crate::error;
use crate::error::Error;
use futures::TryStreamExt;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
struct CfgErrMap {
    pub version: i64,
    pub revision: i64,
    pub errors: HashMap<String, CfgErrMapError>,
}

#[derive(Debug, Clone, Deserialize)]
struct CfgErrMapError {
    pub name: String,
    #[serde(rename = "desc")]
    pub description: String,
    #[serde(rename = "attrs")]
    pub attributes: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ErrMap {
    pub version: i64,
    pub revision: i64,
    pub errors: HashMap<u16, ErrMapError>,
}

#[derive(Debug, Clone)]
pub(crate) struct ErrMapError {
    pub name: String,
    pub description: String,
    pub attributes: Vec<String>,
}

pub(crate) fn parse_error_map(map_bytes: &[u8]) -> error::Result<ErrMap> {
    let cfg_err_map: CfgErrMap = serde_json::from_slice(map_bytes)
        .map_err(|e| Error::new_message_error(format!("failed to deserialize error map: {}", e)))?;
    let mut errors = HashMap::new();

    for (code, err) in cfg_err_map.errors {
        let code: u16 = u16::from_str_radix(&code, 16)
            .map_err(|e| Error::new_message_error(format!("failed to parse error code: {}", e)))?;
        errors.insert(
            code,
            ErrMapError {
                name: err.name,
                description: err.description,
                attributes: err.attributes,
            },
        );
    }

    Ok(ErrMap {
        version: cfg_err_map.version,
        revision: cfg_err_map.revision,
        errors,
    })
}
