//! FLV audio tag parser
//!
//! Mirrors `src/flv/audio_tag.h` and `src/flv/audio_tag.c`.

use crate::types::{AudioCodec, AudioTag, ErrorCode, Result};

/// Parse an FLV audio tag.
pub fn parse(data: &[u8], tag: &mut AudioTag) -> Result<()> {
    if data.is_empty() {
        return Err(ErrorCode::Internal);
    }

    tag.codec = match (data[0] >> 4) & 0x0F {
        0 => AudioCodec::Pcm,
        1 => AudioCodec::Adpcm,
        2 => AudioCodec::Mp3,
        3 => AudioCodec::PcmLe,
        4 => AudioCodec::Nelly16k,
        5 => AudioCodec::Nelly8k,
        6 => AudioCodec::Nelly,
        7 => AudioCodec::G711A,
        8 => AudioCodec::G711U,
        10 => AudioCodec::Aac,
        11 => AudioCodec::Speex,
        14 => AudioCodec::Opus,
        _ => AudioCodec::Aac,
    };
    tag.sample_rate = (data[0] >> 2) & 0x03;
    tag.bit_depth = (data[0] >> 1) & 0x01;
    tag.channels = data[0] & 0x01;

    if tag.codec == AudioCodec::Aac && data.len() >= 2 {
        tag.aac_packet_type = data[1];
    }

    tag.data = data.as_ptr();
    tag.size = data.len();
    Ok(())
}
