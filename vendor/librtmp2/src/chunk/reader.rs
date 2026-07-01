//! Chunk reader
//!
//! Mirrors `src/chunk/chunk_reader.h` and `src/chunk/chunk_reader.c`.

use crate::buffer::Buffer;
use crate::bytes::{hton24, ntoh32};
use crate::chunk::state::{
    ChunkRegistry, ChunkStream, DEFAULT_CHUNK_SIZE, MAX_REASSEMBLY_BYTES_PER_CONN,
};
use crate::types::ErrorCode;
use crate::types::Result;

/// A chunk message (resembled from one or more chunk reads).
#[derive(Debug, Clone, Default)]
pub struct ChunkMessage {
    pub csid: u32,
    pub fmt: u8,
    pub timestamp: u32,
    pub msg_length: u32,
    pub msg_type_id: u8,
    pub msg_stream_id: u32,
    pub is_complete: bool,
}

/// Read a single chunk from the buffer, reassembling into complete messages.
///
/// Returns:
/// - Ok(1) with is_complete=true when a full message is ready
/// - Ok(0) when more data is needed
/// - Err on protocol errors
///
/// Design: the function uses a "peek-first" strategy.  All availability checks
/// happen BEFORE any bytes are consumed from `buf`.  This guarantees that every
/// `Ok(0)` return leaves the cursor exactly where it was on entry, so the next
/// call can retry without corruption.
pub fn chunk_read(
    buf: &mut Buffer,
    reg: &mut ChunkRegistry,
    _unused: Option<&()>,
    msg: &mut ChunkMessage,
    payload: &mut *const u8,
    payload_len: &mut usize,
) -> Result<i32> {
    let available = buf.available();
    if available < 1 {
        return Ok(0);
    }

    // ── Phase 1: parse header structure by peeking (no bytes consumed yet) ──

    let peek = buf.peek();

    let first = peek[0];
    let fmt = first >> 6;
    let csid_low = (first & 0x3F) as u32;

    let (csid, header_size) = match csid_low {
        0 => {
            if available < 2 {
                return Ok(0);
            }
            (peek[1] as u32 + 64, 2usize)
        }
        1 => {
            if available < 3 {
                return Ok(0);
            }
            (((peek[1] as u32) | ((peek[2] as u32) << 8)) + 64, 3usize)
        }
        n => (n, 1usize),
    };

    // Number of message-header bytes this fmt carries
    let msg_field_size: usize = match fmt {
        0 => 11, // timestamp(3) + length(3) + typeid(1) + streamid(4)
        1 => 7,  // timestamp(3) + length(3) + typeid(1)
        2 => 3,  // timestamp(3)
        3 => 0,  // inherited entirely from stream state
        _ => return Err(ErrorCode::Chunk),
    };

    let base_needed = header_size + msg_field_size;
    if available < base_needed {
        return Ok(0);
    }

    // Compressed headers (fmt 1/2/3) inherit fields from prior stream state.
    // A compressed chunk on an unknown CSID is a protocol error.
    if fmt != 0 && reg.get(csid).is_none() {
        return Err(ErrorCode::Chunk);
    }

    // Peek at the 3-byte timestamp field (for fmt 0/1/2) to decide ext_ts
    // without consuming anything.
    let ext_ts_from_header = if fmt <= 2 {
        let off = header_size;
        let ts_raw =
            ((peek[off] as u32) << 16) | ((peek[off + 1] as u32) << 8) | (peek[off + 2] as u32);
        ts_raw >= 0xFFFFFF
    } else {
        false
    };

    // For fmt=3 continuation chunks the writer re-emits the 4-byte extended
    // timestamp whenever the original message had ts >= 0xFFFFFF.  Inherit
    // the flag from the stream's stored state.
    let ext_ts_from_stream = if fmt == 3 {
        reg.get(csid)
            .map(|s| s.type0_ext_ts)
            .ok_or(ErrorCode::Chunk)?
    } else {
        false
    };

    let ext_ts = ext_ts_from_header || ext_ts_from_stream;

    // Total header bytes (basic + message fields + optional ext timestamp)
    let total_header_needed = base_needed + if ext_ts { 4 } else { 0 };
    if available < total_header_needed {
        return Ok(0);
    }

    // Determine effective_length and per-stream chunk_size/reassembly_bytes_read
    // so we can include the payload slice in the upfront availability check.
    let eff_len_for_avail: u32 = match fmt {
        0 | 1 => {
            // message length is peeked from header bytes
            let off = header_size + 3;
            ((peek[off] as u32) << 16) | ((peek[off + 1] as u32) << 8) | (peek[off + 2] as u32)
        }
        _ => reg
            .get(csid)
            .map(|s| s.type0_msg_length)
            .ok_or(ErrorCode::Chunk)?,
    };

    // fmt 0/1 start a new message; treat reassembly as empty for the upfront
    // availability check so we compute the correct first-chunk payload size.
    let (chunk_sz_for_avail, reassembly_read_for_avail) = reg
        .get(csid)
        .map(|s| {
            (
                s.chunk_size as usize,
                if fmt <= 1 {
                    0
                } else {
                    s.reassembly_bytes_read as usize
                },
            )
        })
        .unwrap_or((reg.default_chunk_size as usize, 0));

    let remaining = (eff_len_for_avail as usize).saturating_sub(reassembly_read_for_avail);
    let to_read = remaining.min(chunk_sz_for_avail);

    if available < total_header_needed + to_read {
        return Ok(0);
    }

    // ── Phase 2: all bytes confirmed present — consume them ──

    // Consume basic header
    let mut hdr = vec![0u8; header_size];
    buf.read(&mut hdr).map_err(|_| ErrorCode::Io)?;

    // Consume message header and extract fields
    let timestamp: u32;
    let msg_length: u32;
    let msg_type_id: u8;
    let msg_stream_id: u32;

    match fmt {
        0 => {
            let mut mh = [0u8; 11];
            buf.read(&mut mh).map_err(|_| ErrorCode::Io)?;
            timestamp = ((mh[0] as u32) << 16) | ((mh[1] as u32) << 8) | (mh[2] as u32);
            msg_length = ((mh[3] as u32) << 16) | ((mh[4] as u32) << 8) | (mh[5] as u32);
            msg_type_id = mh[6];
            msg_stream_id = (mh[7] as u32)
                | ((mh[8] as u32) << 8)
                | ((mh[9] as u32) << 16)
                | ((mh[10] as u32) << 24);
        }
        1 => {
            let mut mh = [0u8; 7];
            buf.read(&mut mh).map_err(|_| ErrorCode::Io)?;
            timestamp = ((mh[0] as u32) << 16) | ((mh[1] as u32) << 8) | (mh[2] as u32);
            msg_length = ((mh[3] as u32) << 16) | ((mh[4] as u32) << 8) | (mh[5] as u32);
            msg_type_id = mh[6];
            msg_stream_id = 0; // inherited from stream state
        }
        2 => {
            let mut mh = [0u8; 3];
            buf.read(&mut mh).map_err(|_| ErrorCode::Io)?;
            timestamp = ((mh[0] as u32) << 16) | ((mh[1] as u32) << 8) | (mh[2] as u32);
            msg_length = 0;
            msg_type_id = 0;
            msg_stream_id = 0;
        }
        3 => {
            timestamp = 0;
            msg_length = 0;
            msg_type_id = 0;
            msg_stream_id = 0;
        }
        _ => return Err(ErrorCode::Chunk),
    }

    // Consume extended timestamp if present
    let final_timestamp = if ext_ts {
        let mut ts_buf = [0u8; 4];
        buf.read(&mut ts_buf).map_err(|_| ErrorCode::Io)?;
        ntoh32(&ts_buf)
    } else {
        timestamp
    };

    // ── Phase 3: update stream state and reassemble ──

    // Guard against per-connection reassembly buffer exhaustion before
    // touching any stream state (avoid partial mutation on rejection).
    // fmt=0/1 will immediately discard this CSID's buffer, so exclude its
    // current bytes from the total to avoid rejecting a valid restart near
    // the per-connection limit.
    if to_read > 0 {
        let replaced = if fmt <= 1 {
            reg.get(csid)
                .map(|s| s.reassembly_buf.available())
                .unwrap_or(0)
        } else {
            0
        };
        let total: usize = reg
            .streams
            .iter()
            .filter(|s| s.in_use)
            .map(|s| s.reassembly_buf.available())
            .sum();
        if total.saturating_sub(replaced) + to_read > MAX_REASSEMBLY_BYTES_PER_CONN {
            return Err(ErrorCode::Chunk);
        }
    }

    let stream = reg.get_or_create(csid)?;

    // fmt 0/1 start a fresh message on this CSID; discard any partial
    // reassembly left over from an abandoned prior message.
    if fmt == 0 || fmt == 1 {
        stream.reassembly_bytes_read = 0;
        stream.reassembly_buf.reset();
    }

    match fmt {
        0 => {
            stream.type0_timestamp = final_timestamp;
            stream.type0_msg_length = msg_length;
            stream.type0_msg_type_id = msg_type_id;
            stream.type0_msg_stream_id = msg_stream_id;
            stream.type0_ext_ts = ext_ts;
        }
        1 => {
            stream.type0_timestamp = final_timestamp;
            stream.type0_msg_length = msg_length;
            stream.type0_msg_type_id = msg_type_id;
            stream.type0_ext_ts = ext_ts;
        }
        2 => {
            stream.type0_timestamp = final_timestamp;
            stream.type0_ext_ts = ext_ts;
        }
        _ => {}
    }

    let effective_ts = stream.type0_timestamp;
    let effective_length = stream.type0_msg_length;
    let effective_type_id = stream.type0_msg_type_id;
    let effective_stream_id = stream.type0_msg_stream_id;
    let chunk_size = stream.chunk_size as usize;
    let remaining =
        (effective_length as usize).saturating_sub(stream.reassembly_bytes_read as usize);
    let to_read = remaining.min(chunk_size);

    let mut chunk_data = vec![0u8; to_read];
    buf.read(&mut chunk_data).map_err(|_| ErrorCode::Io)?;
    stream
        .reassembly_buf
        .write(&chunk_data)
        .map_err(|_| ErrorCode::Chunk)?;
    stream.reassembly_bytes_read += to_read as u32;

    if stream.reassembly_bytes_read >= effective_length {
        msg.csid = csid;
        msg.fmt = fmt;
        msg.timestamp = effective_ts;
        msg.msg_length = effective_length;
        msg.msg_type_id = effective_type_id;
        msg.msg_stream_id = effective_stream_id;
        msg.is_complete = true;

        // SAFETY: caller consumes the payload before the next chunk_read call.
        let data = stream.reassembly_buf.peek();
        *payload_len = data.len();
        *payload = data.as_ptr();

        stream.reassembly_bytes_read = 0;
        stream.reassembly_buf.reset();

        Ok(1)
    } else {
        msg.is_complete = false;
        Ok(0)
    }
}
