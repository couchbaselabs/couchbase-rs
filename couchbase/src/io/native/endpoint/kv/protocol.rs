//! Utility functions and statics for interacting with the KV binary protocol

use bytes::{BufMut, Bytes, BytesMut};

pub static HEADER_SIZE: usize = 24;

/// Creates a regular, non-flex request with all fields necessary.
pub fn request(
    opcode: Opcode,
    datatype: u8,
    partition: u16,
    opaque: u32,
    cas: u64,
    key: Option<Bytes>,
    extras: Option<Bytes>,
    body: Option<Bytes>,
) -> BytesMut {
    let key_size = key.as_ref().map(|b| b.len()).unwrap_or_default();
    let extras_size = extras.as_ref().map(|b| b.len()).unwrap_or_default();
    let total_body_size =
        key_size + extras_size + body.as_ref().map(|b| b.len()).unwrap_or_default();

    let mut builder = BytesMut::with_capacity(HEADER_SIZE + total_body_size);
    builder.put_u8(Magic::Request.encoded(false));
    builder.put_u8(opcode.encoded());
    builder.put_u16(key_size as u16);
    builder.put_u8(extras_size as u8);
    builder.put_u8(datatype);
    builder.put_u16(partition);
    builder.put_u32(total_body_size as u32);
    builder.put_u32(opaque);
    builder.put_u64(cas);

    if let Some(extras) = extras {
        builder.put(extras);
    }

    if let Some(key) = key {
        builder.put(key);
    }

    if let Some(body) = body {
        builder.put(body);
    }

    builder
}

// Creates a flexible request with optional framing extras
pub fn flexible_request(
    opcode: Opcode,
    datatype: u8,
    partition: u16,
    opaque: u32,
    cas: u64,
    key: Option<Bytes>,
    framing_extras: Option<Bytes>,
    extras: Option<Bytes>,
    body: Option<Bytes>,
) -> BytesMut {
    let key_size = key.as_ref().map(|b| b.len()).unwrap_or_default();
    let extras_size = extras.as_ref().map(|b| b.len()).unwrap_or_default();
    let framing_extras_size = framing_extras.as_ref().map(|b| b.len()).unwrap_or_default();
    let total_body_size = key_size
        + extras_size
        + framing_extras_size
        + body.as_ref().map(|b| b.len()).unwrap_or_default();

    let mut builder = BytesMut::with_capacity(HEADER_SIZE + total_body_size);
    builder.put_u8(Magic::Request.encoded(true));
    builder.put_u8(opcode.encoded());
    builder.put_u8(framing_extras_size as u8);
    builder.put_u8(key_size as u8);
    builder.put_u8(extras_size as u8);
    builder.put_u8(datatype);
    builder.put_u16(partition);
    builder.put_u32(total_body_size as u32);
    builder.put_u32(opaque);
    builder.put_u64(cas);

    if let Some(framing_extras) = framing_extras {
        builder.put(framing_extras);
    }

    if let Some(extras) = extras {
        builder.put(extras);
    }

    if let Some(key) = key {
        builder.put(key);
    }

    if let Some(body) = body {
        builder.put(body);
    }

    builder
}

/// Creates a regular, non-flex response with all fields necessary.
pub fn response(
    opcode: Opcode,
    datatype: u8,
    status: u16,
    opaque: u32,
    cas: u64,
    key: Option<Bytes>,
    extras: Option<Bytes>,
    body: Option<Bytes>,
) -> BytesMut {
    let key_size = key.as_ref().map(|b| b.len()).unwrap_or_default();
    let extras_size = extras.as_ref().map(|b| b.len()).unwrap_or_default();
    let total_body_size =
        key_size + extras_size + body.as_ref().map(|b| b.len()).unwrap_or_default();

    let mut builder = BytesMut::with_capacity(HEADER_SIZE + total_body_size);
    builder.put_u8(Magic::Response.encoded(false));
    builder.put_u8(opcode.encoded());
    builder.put_u16(key_size as u16);
    builder.put_u8(extras_size as u8);
    builder.put_u8(datatype);
    builder.put_u16(status);
    builder.put_u32(total_body_size as u32);
    builder.put_u32(opaque);
    builder.put_u64(cas);

    if let Some(extras) = extras {
        builder.put(extras);
    }

    if let Some(key) = key {
        builder.put(key);
    }

    if let Some(body) = body {
        builder.put(body);
    }

    builder
}

pub enum Opcode {
    Get,
    Hello,
    Noop,
}

impl Opcode {
    pub fn encoded(&self) -> u8 {
        match self {
            Self::Get => 0x00,
            Self::Hello => 0x1F,
            Self::Noop => 0x0A,
        }
    }
}

pub enum Magic {
    Request,
    Response,
}

impl Magic {
    pub fn encoded(&self, flexible: bool) -> u8 {
        match self {
            Self::Request if flexible => 0x08,
            Self::Request => 0x80,
            Self::Response if flexible => 0x18,
            Self::Response => 0x81,
        }
    }
}
