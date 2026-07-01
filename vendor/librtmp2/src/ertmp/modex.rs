//! E-RTMP v2 ModEx extension mechanism
//!
//! Mirrors `src/ertmp/modex.c`.

use crate::types::{ErrorCode, Modex, ModexType, Result};

const MODEX_MARKER: u8 = 0x80;

/// Parse a ModEx extension.
pub fn modex_parse(modex: &mut Modex, data: &[u8]) -> Result<()> {
    if data.is_empty() {
        return Err(ErrorCode::Io);
    }

    let marker = data[0];
    if marker & 0x80 == 0 {
        return Err(ErrorCode::Protocol);
    }

    let ty = marker & 0x7F;
    match ty {
        0 => {
            modex.modex_type = ModexType::Nop;
            modex.offset = 0;
        }
        1 => {
            if data.len() < 9 {
                return Err(ErrorCode::Io);
            }
            modex.modex_type = ModexType::Timestamp;
            modex.offset = 0;
            for i in 0..8 {
                modex.offset = (modex.offset << 8) | data[1 + i] as u64;
            }
        }
        _ => {
            // Unknown ModEx type — ignore per §16 graceful-degradation rule
            modex.modex_type = ModexType::Nop;
        }
    }

    Ok(())
}

/// Write a ModEx extension. Returns bytes written, or 0 if the buffer is too small.
pub fn modex_write(modex: &Modex, buf: &mut [u8]) -> usize {
    match modex.modex_type {
        ModexType::Nop => {
            if buf.is_empty() {
                return 0;
            }
            buf[0] = MODEX_MARKER | ModexType::Nop as u8;
            1
        }
        ModexType::Timestamp => {
            if buf.len() < 9 {
                return 0;
            }
            buf[0] = MODEX_MARKER | ModexType::Timestamp as u8;
            let mut tmp = modex.offset;
            for i in (0..8).rev() {
                buf[1 + i] = (tmp & 0xFF) as u8;
                tmp >>= 8;
            }
            9
        }
    }
}
