//! E-RTMP v1 fourCcList + E-RTMP v2 capability layer
//!
//! Mirrors `src/ertmp/connect_caps.c`.

use crate::types::{CapsExit, ErrorCode, FourCcList, Result, VideoFourCcInfoMap};

/* ── fourCcList ── */

/// Initialize a FourCC list.
pub fn fourcc_list_init(list: &mut FourCcList) {
    list.count = 0;
}

/// Add a FourCC to the list.
pub fn fourcc_list_add(list: &mut FourCcList, cc: &[u8]) -> Result<()> {
    if list.count >= crate::types::MAX_FOURCCS {
        return Err(ErrorCode::Io);
    }
    list.entries[list.count].cc[..4].copy_from_slice(&cc[..4]);
    list.count += 1;
    Ok(())
}

/// Parse a FourCC list from raw data.
pub fn fourcc_list_parse(list: &mut FourCcList, data: &[u8]) -> Result<usize> {
    if data.len() < 4 {
        return Err(ErrorCode::Io);
    }
    fourcc_list_init(list);

    let count = ((data[0] as u32) << 24)
        | ((data[1] as u32) << 16)
        | ((data[2] as u32) << 8)
        | (data[3] as u32);
    let count = count.min(crate::types::MAX_FOURCCS as u32);

    let mut offset = 4;
    for _ in 0..count {
        if offset + 6 > data.len() {
            break;
        }
        let slen = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
        offset += 2;
        if slen != 4 || offset + 4 > data.len() {
            break;
        }
        list.entries[list.count].cc[..4].copy_from_slice(&data[offset..offset + 4]);
        list.count += 1;
        offset += 4;
    }
    Ok(list.count)
}

/// Write a FourCC list to a buffer. Returns bytes written.
pub fn fourcc_list_write(list: &FourCcList, buf: &mut [u8]) -> usize {
    let needed = 4 + list.count * 6;
    if buf.len() < needed {
        return 0;
    }

    // Write count as big-endian u32 to match fourcc_list_parse's big-endian read.
    let cnt = list.count as u32;
    buf[0] = (cnt >> 24) as u8;
    buf[1] = (cnt >> 16) as u8;
    buf[2] = (cnt >> 8) as u8;
    buf[3] = cnt as u8;

    let mut offset = 4;
    for i in 0..list.count {
        buf[offset] = 0;
        buf[offset + 1] = 4;
        offset += 2;
        buf[offset..offset + 4].copy_from_slice(&list.entries[i].cc[..4]);
        offset += 4;
    }
    offset
}

/* ── E-RTMP v2 capsEx ── */

/// Parse capability negotiation data.
pub fn caps_exit_parse(caps: &mut CapsExit, data: &[u8]) -> Result<()> {
    if data.len() < 8 {
        return Err(ErrorCode::Io);
    }
    caps.version = 1;
    caps.video_codec_32 =
        ((data[0] as u32) << 24 | (data[1] as u32) << 16 | (data[2] as u32) << 8 | data[3] as u32)
            as i32;
    caps.audio_codec_32 =
        ((data[4] as u32) << 24 | (data[5] as u32) << 16 | (data[6] as u32) << 8 | data[7] as u32)
            as i32;
    Ok(())
}

/// Write capability negotiation data. Returns bytes written.
pub fn caps_exit_write(caps: &CapsExit, buf: &mut [u8]) -> usize {
    if buf.len() < 8 {
        return 0;
    }
    let vc = caps.video_codec_32 as u32;
    let ac = caps.audio_codec_32 as u32;
    buf[0] = (vc >> 24) as u8;
    buf[1] = (vc >> 16) as u8;
    buf[2] = (vc >> 8) as u8;
    buf[3] = vc as u8;
    buf[4] = (ac >> 24) as u8;
    buf[5] = (ac >> 16) as u8;
    buf[6] = (ac >> 8) as u8;
    buf[7] = ac as u8;
    8
}

/* ── E-RTMP v2 videoFourCcInfoMap ── */

/// Parse a video FourCC info map.
pub fn video_fourcc_info_map_parse(map: &mut VideoFourCcInfoMap, data: &[u8]) -> Result<usize> {
    if data.len() < 4 {
        return Err(ErrorCode::Io);
    }
    map.count = 0;

    let count = ((data[0] as u32) << 24)
        | ((data[1] as u32) << 16)
        | ((data[2] as u32) << 8)
        | (data[3] as u32);
    let count = count.min(crate::types::MAX_FOURCCS as u32);

    let mut offset = 4;
    for _ in 0..count {
        if offset + 6 > data.len() {
            break;
        }
        let slen = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
        offset += 2;
        if slen != 4 || offset + 4 > data.len() {
            break;
        }
        map.entries[map.count].cc[..4].copy_from_slice(&data[offset..offset + 4]);
        map.count += 1;
        offset += 4;
    }
    Ok(map.count)
}

/// Write a video FourCC info map. Returns bytes written.
pub fn video_fourcc_info_map_write(map: &VideoFourCcInfoMap, buf: &mut [u8]) -> usize {
    let needed = 4 + map.count * 6;
    if buf.len() < needed {
        return 0;
    }

    // Write count as big-endian u32 to match video_fourcc_info_map_parse's big-endian read.
    let cnt = map.count as u32;
    buf[0] = (cnt >> 24) as u8;
    buf[1] = (cnt >> 16) as u8;
    buf[2] = (cnt >> 8) as u8;
    buf[3] = cnt as u8;

    let mut offset = 4;
    for i in 0..map.count {
        buf[offset] = 0;
        buf[offset + 1] = 4;
        offset += 2;
        buf[offset..offset + 4].copy_from_slice(&map.entries[i].cc[..4]);
        offset += 4;
    }
    offset
}
