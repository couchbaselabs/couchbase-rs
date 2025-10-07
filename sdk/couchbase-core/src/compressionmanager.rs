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

use std::fmt::Debug;
use std::marker::PhantomData;

use snap::raw::Encoder;

use crate::error;
use crate::error::ErrorKind;
use crate::memdx::datatype::DataTypeFlag;
use crate::options::agent::{CompressionConfig, CompressionMode};

pub(crate) trait Compressor: Send + Sync + Debug {
    fn new(compression_config: &CompressionConfig) -> Self;
    // This is a bit of a weird signature,  but it allows us to avoid allocations when no compression occurs.
    fn compress<'a>(
        &'a mut self,
        connection_supports_snappy: bool,
        datatype: DataTypeFlag,
        input: &'a [u8],
    ) -> error::Result<(&'a [u8], u8)>;
}

#[derive(Debug)]
pub(crate) struct CompressionManager<C> {
    _phantom: PhantomData<C>,
    compression_config: CompressionConfig,
}

impl<C> CompressionManager<C>
where
    C: Compressor,
{
    pub fn new(compression_config: CompressionConfig) -> Self {
        Self {
            _phantom: Default::default(),
            compression_config,
        }
    }

    pub fn compressor(&self) -> C {
        C::new(&self.compression_config)
    }
}

#[derive(Debug)]
pub(crate) struct StdCompressor {
    compression_enabled: bool,
    compression_min_size: usize,
    compression_min_ratio: f64,

    compressed_value: Vec<u8>,
}

impl StdCompressor {}

impl Compressor for StdCompressor {
    fn new(compression_config: &CompressionConfig) -> Self {
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

            compressed_value: Vec::new(),
        }
    }

    fn compress<'a>(
        &'a mut self,
        connection_supports_snappy: bool,
        datatype: DataTypeFlag,
        input: &'a [u8],
    ) -> error::Result<(&'a [u8], u8)> {
        if !connection_supports_snappy || !self.compression_enabled {
            return Ok((input, u8::from(datatype)));
        }

        let datatype = u8::from(datatype);

        // If the packet is already compressed then we don't want to compress it again.
        if datatype & u8::from(DataTypeFlag::Compressed) != 0 {
            return Ok((input, datatype));
        }

        let packet_size = input.len();

        // Only compress values that are large enough to be worthwhile.
        if packet_size <= self.compression_min_size {
            return Ok((input, datatype));
        }

        let mut encoder = Encoder::new();
        let compressed_value = encoder
            .compress_vec(input)
            .map_err(|e| ErrorKind::Compression { msg: e.to_string() })?;

        // Only return the compressed value if the ratio of compressed:original is small enough.
        if compressed_value.len() as f64 / packet_size as f64 > self.compression_min_ratio {
            return Ok((input, datatype));
        }

        self.compressed_value = compressed_value;

        Ok((
            &self.compressed_value,
            datatype | u8::from(DataTypeFlag::Compressed),
        ))
    }
}
