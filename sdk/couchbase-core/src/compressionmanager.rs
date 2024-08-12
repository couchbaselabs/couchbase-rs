use std::fmt::Debug;

use snap::raw::Encoder;

use crate::agentoptions::{CompressionConfig, CompressionMode};
use crate::error;
use crate::error::ErrorKind;
use crate::memdx::datatype::DataTypeFlag;

pub(crate) trait CompressionManager: Send + Sync + Debug {
    // This is a bit of a weird signature but it allows us to avoid allocations when no compression occurs.
    fn compress(
        &self,
        connection_supports_snappy: bool,
        datatype: DataTypeFlag,
        input: &[u8],
    ) -> error::Result<Option<(Vec<u8>, u8)>>;
}

#[derive(Debug)]
pub(crate) struct StdCompressionManager {
    compression_enabled: bool,
    compression_min_size: usize,
    compression_min_ratio: f64,
}

impl StdCompressionManager {
    pub fn new(compression_config: CompressionConfig) -> Self {
        let (compression_enabled, compression_min_size, compression_min_ratio) =
            match compression_config.mode {
                CompressionMode::Enabled {
                    min_size,
                    min_ratio,
                } => (true, min_size, min_ratio),
                CompressionMode::Disabled => (false, 0, 0.0),
            };

        Self {
            compression_enabled,
            compression_min_size,
            compression_min_ratio,
        }
    }
}

impl CompressionManager for StdCompressionManager {
    fn compress(
        &self,
        connection_supports_snappy: bool,
        datatype: DataTypeFlag,
        input: &[u8],
    ) -> error::Result<Option<(Vec<u8>, u8)>> {
        if !connection_supports_snappy || !self.compression_enabled {
            return Ok(None);
        }

        let datatype = u8::from(datatype);

        // If the packet is already compressed then we don't want to compress it again.
        if datatype & u8::from(DataTypeFlag::Compressed) != 0 {
            return Ok(None);
        }

        let packet_size = input.len();

        // Only compress values that are large enough to be worthwhile.
        if packet_size <= self.compression_min_size {
            return Ok(None);
        }

        let mut encoder = Encoder::new();
        let compressed_value = encoder
            .compress_vec(input)
            .map_err(|e| ErrorKind::JSONError { msg: e.to_string() })?;

        // Only return the compressed value if the ratio of compressed:original is small enough.
        if compressed_value.len() as f64 / packet_size as f64 > self.compression_min_ratio {
            return Ok(None);
        }

        Ok(Some((
            compressed_value,
            datatype | u8::from(DataTypeFlag::Compressed),
        )))
    }
}
