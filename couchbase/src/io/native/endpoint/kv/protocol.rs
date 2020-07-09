//! Utility functions and statics for interacting with the KV binary protocol

use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    builder.put_u8(Magic::Request.encoded());
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
    builder.put_u8(Magic::FlexibleRequest.encoded());
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
    builder.put_u8(Magic::Response.encoded());
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

/// Takes a full packet and extracts the body as a slice if possible.
pub fn body(input: &Bytes) -> Option<Bytes> {
    let mut slice = input.slice(0..input.len());

    let flexible = Magic::from(slice.get_u8()).is_flexible();

    let flexible_extras_len = if flexible {
        slice.advance(1);
        slice.get_u8()
    } else {
        0
    } as usize;
    let key_len = if flexible {
        slice.get_u8() as u16
    } else {
        slice.advance(1);
        slice.get_u16()
    } as usize;
    let extras_len = slice.get_u8() as usize;
    slice.advance(3);
    let total_body_len = slice.get_u32() as usize;
    let body_len = total_body_len - key_len - extras_len - flexible_extras_len;

    if body_len > 0 {
        Some(input.slice((HEADER_SIZE + flexible_extras_len + extras_len + key_len)..))
    } else {
        None
    }
}

/// Dumps a packet into a easily debuggable string format.
///
/// Note that this is only really suitable when you want to println a full
/// packet, but nonetheless it is helpful for testing.
pub fn dump(input: &Bytes) -> String {
    if input.len() < HEADER_SIZE {
        return "Received less bytes than a KV header, invalid data?".into();
    }

    let mut slice = input.slice(0..input.len());

    let mut output = String::new();
    output.push_str("--- Packet Dump Info --\n");
    let magic = slice.get_u8();
    output.push_str(&format!(
        "     Magic: 0x{:x} ({:?})\n",
        magic,
        Magic::from(magic)
    ));
    let opcode = slice.get_u8();
    output.push_str(&format!(
        "    Opcode: 0x{:x} ({:?})\n",
        opcode,
        Opcode::from(opcode)
    ));
    let key_size = slice.get_u16();
    output.push_str(&format!("   Key Len: {} bytes\n", key_size));
    let extras_size = slice.get_u8();
    output.push_str(&format!("Extras Len: {} bytes\n", extras_size));
    let datatype = slice.get_u8();
    output.push_str(&format!("  Datatype: 0x{:x}\n", datatype));
    let partition = slice.get_u16();
    output.push_str(&format!(" Partition: 0x{:x}\n", partition));

    if let Some(body) = body(&input) {
        output.push_str(&format!("      Body: {:?}\n", body));
    }

    output.push_str("-----------------------\n");

    output
}

#[derive(Debug)]
pub enum Opcode {
    Get,
    Hello,
    Noop,
    Unknown,
}

impl Opcode {
    pub fn encoded(&self) -> u8 {
        match self {
            Self::Get => 0x00,
            Self::Hello => 0x1F,
            Self::Noop => 0x0A,
            Self::Unknown => panic!("Cannot convert unknown opcode"),
        }
    }
}

impl From<u8> for Opcode {
    fn from(input: u8) -> Opcode {
        match input {
            0x00 => Opcode::Get,
            0x1F => Opcode::Hello,
            0x0A => Opcode::Noop,
            _ => Opcode::Unknown,
        }
    }
}

#[derive(Debug)]
pub enum Magic {
    Request,
    FlexibleRequest,
    Response,
    FlexibleResponse,
    Unknown,
}

impl Magic {
    pub fn encoded(&self) -> u8 {
        match self {
            Self::FlexibleRequest => 0x08,
            Self::Request => 0x80,
            Self::FlexibleResponse => 0x18,
            Self::Response => 0x81,
            Self::Unknown => panic!("Cannot convert unknown magic"),
        }
    }

    pub fn is_flexible(&self) -> bool {
        match self {
            Self::FlexibleRequest | Self::FlexibleResponse => true,
            _ => false,
        }
    }
}

impl From<u8> for Magic {
    fn from(input: u8) -> Magic {
        match input {
            0x80 => Magic::Request,
            0x08 => Magic::FlexibleRequest,
            0x81 => Magic::Response,
            0x18 => Magic::FlexibleResponse,
            _ => Magic::Unknown,
        }
    }
}
