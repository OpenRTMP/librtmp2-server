//! E-RTMP v2 multitrack streaming
//!
//! Mirrors `src/ertmp/multitrack.c`.

use crate::types::{ErrorCode, Multitrack, Result};

/// Parse a multitrack descriptor.
pub fn multitrack_parse(mt: &mut Multitrack, data: &[u8]) -> Result<()> {
    if data.len() < 9 {
        return Err(ErrorCode::Io);
    }

    if data[0] != 0x00 {
        return Err(ErrorCode::Protocol);
    }

    let mut type_val: u64 = 0;
    for i in 0..8 {
        type_val = (type_val << 8) | data[1 + i] as u64;
    }

    mt.track_type = match type_val {
        0 => crate::types::MultitrackType::Audio,
        1 => crate::types::MultitrackType::Video,
        _ => crate::types::MultitrackType::Metadata,
    };

    let name_offset = 9;
    if name_offset + 2 > data.len() {
        return Err(ErrorCode::Io);
    }

    let name_len = ((data[name_offset] as usize) << 8) | (data[name_offset + 1] as usize);
    if name_offset + 2 + name_len > data.len() {
        return Err(ErrorCode::Io);
    }

    let copy_len = name_len.min(63);
    mt.track_name[..copy_len].copy_from_slice(&data[name_offset + 2..name_offset + 2 + copy_len]);
    mt.track_name[copy_len] = 0;

    Ok(())
}

/// Write a multitrack descriptor. Returns bytes written.
pub fn multitrack_write(mt: &Multitrack, buf: &mut [u8]) -> usize {
    let name_len = mt.track_name.iter().position(|&b| b == 0).unwrap_or(63);
    let needed = 1 + 8 + 2 + name_len;
    if buf.len() < needed {
        return 0;
    }

    let mut offset = 0;
    // AMF0_NUMBER marker
    buf[offset] = 0x00;
    offset += 1;

    // 8-byte number, big-endian
    let type_val: u64 = mt.track_type as u64;
    for i in (0..8).rev() {
        buf[offset] = ((type_val >> (i * 8)) & 0xFF) as u8;
        offset += 1;
    }

    // AMF0_STRING: 2-byte length + N bytes
    buf[offset] = (name_len >> 8) as u8;
    buf[offset + 1] = name_len as u8;
    offset += 2;
    buf[offset..offset + name_len].copy_from_slice(&mt.track_name[..name_len]);
    offset += name_len;

    offset
}
