//! FLV video tag parser
//!
//! Mirrors `src/flv/video_tag.h` and `src/flv/video_tag.c`.

use crate::types::{ErrorCode, Result, VideoCodec, VideoTag};

/// Parse an FLV video tag.
///
/// Returns `Err(ErrorCode::Unsupported)` if `IsExHeader` (bit 7) is set —
/// the caller must route those frames through the E-RTMP enhanced parser instead.
pub fn parse(data: &[u8], tag: &mut VideoTag) -> Result<()> {
    if data.is_empty() {
        return Err(ErrorCode::Internal);
    }

    // Bit 7 set means E-RTMP v1 ExVideoTagHeader: lower nibble is PacketType,
    // not a legacy codec ID. Reject so the caller uses the correct path.
    if data[0] & 0x80 != 0 {
        return Err(ErrorCode::Unsupported);
    }

    tag.frame_type = (data[0] >> 4) & 0x0F;
    tag.codec = match data[0] & 0x0F {
        1 => VideoCodec::Jpeg,
        2 => VideoCodec::Sorenson,
        3 => VideoCodec::Screen,
        4 => VideoCodec::Vp6,
        5 => VideoCodec::Vp6a,
        6 => VideoCodec::Screen2,
        7 => VideoCodec::H264,
        _ => return Err(ErrorCode::Unsupported),
    };

    if data.len() >= 5 && tag.codec == VideoCodec::H264 {
        tag.avc_packet_type = data[1];
        tag.composition_time =
            ((data[2] as u32) << 16) | ((data[3] as u32) << 8) | (data[4] as u32);
    }

    tag.data = data.as_ptr();
    tag.size = data.len();
    Ok(())
}
