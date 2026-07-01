//! E-RTMP v2 reconnect mechanism
//!
//! Mirrors `src/ertmp/reconnect.c`.

use crate::types::{ErrorCode, Reconnect, Result};

/// Parse a reconnect request.
pub fn reconnect_parse(rc: &mut Reconnect, data: &[u8]) -> Result<()> {
    if data.len() != 8 {
        return Err(ErrorCode::Io);
    }
    rc.replay = ((data[0] as u32) << 24)
        | ((data[1] as u32) << 16)
        | ((data[2] as u32) << 8)
        | (data[3] as u32);
    rc.limit = ((data[4] as u32) << 24)
        | ((data[5] as u32) << 16)
        | ((data[6] as u32) << 8)
        | (data[7] as u32);
    Ok(())
}

/// Write a reconnect request. Returns bytes written.
pub fn reconnect_write(rc: &Reconnect, buf: &mut [u8]) -> usize {
    if buf.len() < 8 {
        return 0;
    }
    buf[0] = (rc.replay >> 24) as u8;
    buf[1] = (rc.replay >> 16) as u8;
    buf[2] = (rc.replay >> 8) as u8;
    buf[3] = rc.replay as u8;
    buf[4] = (rc.limit >> 24) as u8;
    buf[5] = (rc.limit >> 16) as u8;
    buf[6] = (rc.limit >> 8) as u8;
    buf[7] = rc.limit as u8;
    8
}
