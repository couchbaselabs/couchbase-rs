use crate::cbconfig::TerseConfig;
use crate::error;
use crate::error::ErrorKind;

pub fn parse_terse_config(config: &[u8], source_hostname: &str) -> error::Result<TerseConfig> {
    let mut config = match std::str::from_utf8(config) {
        Ok(v) => v.to_string(),
        Err(e) => {
            return Err(ErrorKind::Generic { msg: e.to_string() }.into());
        }
    };
    config = config.replace("$HOST", source_hostname);
    let config_out: TerseConfig = serde_json::from_str(&config)?;
    Ok(config_out)
}
