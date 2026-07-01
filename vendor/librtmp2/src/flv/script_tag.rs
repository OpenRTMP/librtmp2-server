//! FLV script data (metadata) parser
//!
//! Mirrors `src/flv/script_tag.h` and `src/flv/script_tag.c`.

use crate::amf::amf0;
use crate::buffer::Buffer;
use crate::types::{ErrorCode, Result, ScriptTag};

/// Parse an FLV script tag.
pub fn parse(data: &[u8], tag: &mut ScriptTag) -> Result<()> {
    if data.len() < 2 {
        return Err(ErrorCode::Internal);
    }

    let mut buf = Buffer::from_slice(data);

    // First value is usually a string "onMetaData".
    // Peek at the type byte to choose the right reader; the reader functions
    // (read_string, read_long_string, skip_value) each consume the marker
    // themselves, so we must not call read_type() here as that would advance
    // the buffer past the marker before the reader sees it.
    let first_byte = buf.peek().first().copied().ok_or(ErrorCode::Internal)?;
    match first_byte {
        b if b == amf0::Amf0Type::String as u8 => {
            let mut name = [0u8; 64];
            let len = amf0::read_string(&mut buf, &mut name)?;
            tag.name[..len].copy_from_slice(&name[..len]);
        }
        b if b == amf0::Amf0Type::LongString as u8 => {
            let mut name = [0u8; 64];
            let len = amf0::read_long_string(&mut buf, &mut name)?;
            tag.name[..len].copy_from_slice(&name[..len]);
        }
        _ => {
            amf0::skip_value(&mut buf)?;
        }
    }

    // Second value is the metadata
    if let Ok(ty) = amf0::read_type(&mut buf) {
        if ty == amf0::Amf0Type::EcmaArray || ty == amf0::Amf0Type::Object {
            if ty == amf0::Amf0Type::EcmaArray {
                let mut count_bytes = [0u8; 4];
                buf.read(&mut count_bytes).map_err(|_| ErrorCode::Amf)?;
            }
            while !amf0::is_object_end(&mut buf) {
                let mut key = [0u8; 256];
                amf0::read_object_key(&mut buf, &mut key)?;
                amf0::skip_value(&mut buf)?;
            }
            let mut end = [0u8; 3];
            buf.read(&mut end).map_err(|_| ErrorCode::Amf)?;
        }
    }

    tag.data = data.as_ptr();
    tag.size = data.len();
    Ok(())
}
