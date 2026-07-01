//! RTMP command message encoder/decoder
//!
//! Mirrors `src/message/command.h` and `src/message/command.c`.

use crate::amf::amf0;
use crate::buffer::Buffer;
use crate::types::ConnectInfo;
use crate::types::ErrorCode;
use crate::types::Result;

/// Maximum key/value pairs in a connect object
const MAX_CONNECT_OBJECT_KEYS: usize = 256;

/* ── Encoder ── */

/// Build a "connect" command.
pub fn build_connect(
    buf: &mut Buffer,
    app: &str,
    tc_url: &str,
    page_url: &str,
    swf_url: &str,
    flash_ver: &str,
    audio_codecs: i32,
    video_codecs: i32,
) -> Result<()> {
    macro_rules! chk {
        ($expr:expr) => {
            $expr.map_err(|_| ErrorCode::Internal)?
        };
    }

    chk!(amf0::write_string(buf, "connect"));
    chk!(amf0::write_number(buf, 1.0));
    chk!(amf0::write_object_begin(buf));
    chk!(amf0::write_object_key(buf, "app"));
    chk!(amf0::write_string(buf, app));
    chk!(amf0::write_object_key(buf, "type"));
    chk!(amf0::write_string(buf, "nonprivate"));

    chk!(amf0::write_object_key(buf, "tcUrl"));
    chk!(amf0::write_string(buf, tc_url));
    if !page_url.is_empty() {
        chk!(amf0::write_object_key(buf, "pageUrl"));
        chk!(amf0::write_string(buf, page_url));
    }
    if !swf_url.is_empty() {
        chk!(amf0::write_object_key(buf, "swfUrl"));
        chk!(amf0::write_string(buf, swf_url));
    }
    if !flash_ver.is_empty() {
        chk!(amf0::write_object_key(buf, "flashVer"));
        chk!(amf0::write_string(buf, flash_ver));
    }

    chk!(amf0::write_object_key(buf, "audioCodecs"));
    chk!(amf0::write_number(buf, audio_codecs as f64));
    chk!(amf0::write_object_key(buf, "videoCodecs"));
    chk!(amf0::write_number(buf, video_codecs as f64));
    chk!(amf0::write_object_end(buf));

    Ok(())
}

/// Build a "releaseStream" command.
pub fn build_release_stream(buf: &mut Buffer, stream_name: &str) -> Result<()> {
    amf0::write_string(buf, "releaseStream")?;
    amf0::write_number(buf, 2.0)?;
    amf0::write_null(buf)?;
    amf0::write_string(buf, stream_name)?;
    Ok(())
}

/// Build a "createStream" command.
pub fn build_create_stream(buf: &mut Buffer, transaction_id: f64) -> Result<()> {
    amf0::write_string(buf, "createStream")?;
    amf0::write_number(buf, transaction_id)?;
    amf0::write_null(buf)?;
    Ok(())
}

/// Build a "publish" command.
pub fn build_publish(buf: &mut Buffer, stream_name: &str, app: &str) -> Result<()> {
    amf0::write_string(buf, "publish")?;
    amf0::write_number(buf, 0.0)?;
    amf0::write_null(buf)?;
    amf0::write_string(buf, stream_name)?;
    amf0::write_string(buf, app)?;
    Ok(())
}

/// Build a "play" command.
pub fn build_play(buf: &mut Buffer, stream_name: &str) -> Result<()> {
    amf0::write_string(buf, "play")?;
    amf0::write_number(buf, 0.0)?;
    amf0::write_null(buf)?;
    amf0::write_string(buf, stream_name)?;
    Ok(())
}

/// Build a "FCPublish" command.
pub fn build_fcpublish(buf: &mut Buffer, stream_name: &str) -> Result<()> {
    amf0::write_string(buf, "FCPublish")?;
    amf0::write_number(buf, 0.0)?;
    amf0::write_null(buf)?;
    amf0::write_string(buf, stream_name)?;
    Ok(())
}

/// Build a "FCUnpublish" command.
pub fn build_fcunpublish(buf: &mut Buffer, stream_name: &str) -> Result<()> {
    amf0::write_string(buf, "FCUnpublish")?;
    amf0::write_number(buf, 0.0)?;
    amf0::write_null(buf)?;
    amf0::write_string(buf, stream_name)?;
    Ok(())
}

/// Build a "deleteStream" command.
pub fn build_deletestream(buf: &mut Buffer, transaction_id: f64, stream_id: u32) -> Result<()> {
    amf0::write_string(buf, "deleteStream")?;
    amf0::write_number(buf, transaction_id)?;
    amf0::write_null(buf)?;
    amf0::write_number(buf, stream_id as f64)?;
    Ok(())
}

/// Build a createStream _result response.
pub fn build_create_stream_result(
    buf: &mut Buffer,
    transaction_id: f64,
    stream_id: f64,
) -> Result<()> {
    amf0::write_string(buf, "_result")?;
    amf0::write_number(buf, transaction_id)?;
    amf0::write_null(buf)?;
    amf0::write_number(buf, stream_id)?;
    Ok(())
}

/// Build an onStatus command.
pub fn build_onstatus(buf: &mut Buffer, level: &str, code: &str, description: &str) -> Result<()> {
    amf0::write_string(buf, "onStatus")?;
    amf0::write_number(buf, 0.0)?;
    amf0::write_null(buf)?;
    amf0::write_object_begin(buf)?;
    amf0::write_object_key(buf, "level")?;
    amf0::write_string(buf, level)?;
    amf0::write_object_key(buf, "code")?;
    amf0::write_string(buf, code)?;
    amf0::write_object_key(buf, "description")?;
    amf0::write_string(buf, description)?;
    amf0::write_object_end(buf)?;
    Ok(())
}

/* ── Decoder ── */

/// Peek at the command name without consuming it.
pub fn peek_name(buf: &mut Buffer, out: &mut [u8]) -> Result<usize> {
    let saved_pos = buf.read_pos();
    let result = amf0::read_string(buf, out);
    buf.set_read_pos(saved_pos);
    result
}

/// Read a connect command.
pub fn read_connect(buf: &mut Buffer, info: &mut ConnectInfo) -> Result<()> {
    // Read command name
    let mut name = [0u8; 64];
    let name_len = amf0::read_string(buf, &mut name)?;
    info.name[..name_len].copy_from_slice(&name[..name_len]);

    // Read transaction ID
    info.transaction_id = read_number_value(buf)?;

    // Read command object
    amf0::read_object_begin(buf)?;

    // Parse key-value pairs
    let mut keys = 0;
    while !amf0::is_object_end(buf) {
        if keys >= MAX_CONNECT_OBJECT_KEYS {
            return Err(ErrorCode::Amf);
        }
        keys += 1;

        let mut key = [0u8; 256];
        let key_len = amf0::read_object_key(buf, &mut key)?;

        // Peek value type
        let type_pos = buf.read_pos();
        let value_type = amf0::read_type(buf)?;
        buf.set_read_pos(type_pos); // restore

        match value_type {
            amf0::Amf0Type::String => {
                let key_str = std::str::from_utf8(&key[..key_len]).unwrap_or("");
                match key_str {
                    "app" => read_string_trunc(buf, &mut info.app)?,
                    "tcUrl" => read_string_trunc(buf, &mut info.tc_url)?,
                    "pageUrl" => read_string_trunc(buf, &mut info.page_url)?,
                    "swfUrl" => read_string_trunc(buf, &mut info.swf_url)?,
                    "flashVer" => read_string_trunc(buf, &mut info.flash_ver)?,
                    _ => {
                        amf0::skip_value(buf)?;
                    }
                }
            }
            amf0::Amf0Type::Number => {
                let value = read_number_value(buf)?;
                let key_str = std::str::from_utf8(&key[..key_len]).unwrap_or("");
                match key_str {
                    "audioCodecs" => info.audio_codecs = value as i32,
                    "videoCodecs" => info.video_codecs = value as i32,
                    _ => {}
                }
            }
            _ => {
                amf0::skip_value(buf)?;
            }
        }
    }

    // Consume object end marker
    let mut end = [0u8; 3];
    buf.read(&mut end).map_err(|_| ErrorCode::Amf)?;

    Ok(())
}

/// Read a createStream command.
pub fn read_create_stream(buf: &mut Buffer) -> Result<f64> {
    let mut name = [0u8; 64];
    amf0::read_string(buf, &mut name)?;
    let txn = read_number_value(buf)?;
    amf0::skip_value(buf)?;
    Ok(txn)
}

/// Read a publish command.
pub fn read_publish(buf: &mut Buffer, stream_name: &mut [u8], app: &mut [u8]) -> Result<()> {
    let mut name = [0u8; 64];
    amf0::read_string(buf, &mut name)?;
    read_number_value(buf)?; // skip txn
    amf0::skip_value(buf)?;
    read_string_trunc(buf, stream_name)?;

    // The publish type argument is optional in practice. Decode it only when a
    // client actually sent more AMF data; otherwise keep the output buffer empty.
    if buf.available() > 0 {
        let _ = read_string_trunc(buf, app);
    }
    Ok(())
}

/// Read a play command.
pub fn read_play(buf: &mut Buffer, stream_name: &mut [u8]) -> Result<()> {
    let mut name = [0u8; 64];
    amf0::read_string(buf, &mut name)?;
    read_number_value(buf)?; // skip txn
    amf0::skip_value(buf)?;
    read_string_trunc(buf, stream_name)?;
    Ok(())
}

/// Read a connect _result response.
pub fn read_connect_result(buf: &mut Buffer) -> Result<f64> {
    let mut name = [0u8; 64];
    amf0::read_string(buf, &mut name)?;
    let txn = read_number_value(buf)?;
    amf0::skip_value(buf)?;
    amf0::skip_value(buf)?;
    Ok(txn)
}

/// Read a createStream _result response.
pub fn read_create_stream_result(buf: &mut Buffer) -> Result<(f64, f64)> {
    let mut name = [0u8; 64];
    amf0::read_string(buf, &mut name)?;
    let txn = read_number_value(buf)?;
    amf0::skip_value(buf)?;
    let stream_id = read_number_value(buf)?;
    Ok((txn, stream_id))
}

/* ── Helpers ── */

fn read_number_value(buf: &mut Buffer) -> Result<f64> {
    let ty = amf0::read_type(buf)?;
    if ty != amf0::Amf0Type::Number {
        return Err(ErrorCode::Amf);
    }
    amf0::read_number(buf)
}

fn read_string_trunc(buf: &mut Buffer, out: &mut [u8]) -> Result<()> {
    let mut byte = [0u8; 1];
    buf.read(&mut byte).map_err(|_| ErrorCode::Amf)?;
    if byte[0] != amf0::Amf0Type::String as u8 {
        return Err(ErrorCode::Amf);
    }
    let mut lb = [0u8; 2];
    buf.read(&mut lb).map_err(|_| ErrorCode::Amf)?;
    let slen = ((lb[0] as usize) << 8) | (lb[1] as usize);

    if buf.available() < slen {
        return Err(ErrorCode::Io);
    }

    let copy_len = if slen >= out.len() {
        out.len() - 1
    } else {
        slen
    };
    if copy_len > 0 {
        buf.read(&mut out[..copy_len]).map_err(|_| ErrorCode::Amf)?;
    }
    if !out.is_empty() {
        out[copy_len] = 0;
    }
    buf.drain(slen - copy_len);
    Ok(())
}
