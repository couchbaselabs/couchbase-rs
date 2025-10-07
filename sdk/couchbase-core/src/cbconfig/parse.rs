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

use crate::cbconfig::TerseConfig;
use crate::error;
use crate::error::Error;

pub fn parse_terse_config(config: &[u8], source_hostname: &str) -> error::Result<TerseConfig> {
    let mut config = match std::str::from_utf8(config) {
        Ok(v) => v.to_string(),
        Err(e) => {
            return Err(Error::new_message_error(format!(
                "failed to parse terse config: {e}"
            )));
        }
    };
    config = config.replace("$HOST", source_hostname);
    let config_out: TerseConfig = serde_json::from_str(&config)
        .map_err(|e| Error::new_message_error(format!("failed to parse terse config: {e}")))?;
    Ok(config_out)
}
