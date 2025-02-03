use crate::cbconfig::TerseConfig;
use crate::error;
use crate::error::Error;

pub fn parse_terse_config(config: &[u8], source_hostname: &str) -> error::Result<TerseConfig> {
    let mut config = match std::str::from_utf8(config) {
        Ok(v) => v.to_string(),
        Err(e) => {
            return Err(Error::new_message_error(format!(
                "failed to parse terse config: {}",
                e
            )));
        }
    };
    config = config.replace("$HOST", source_hostname);
    let config_out: TerseConfig = serde_json::from_str(&config)
        .map_err(|e| Error::new_message_error(format!("failed to parse terse config: {}", e)))?;
    Ok(config_out)
}
