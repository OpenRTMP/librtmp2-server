//! Versioned media-plane messages (length-prefixed JSON + binary payloads).

use serde::{Deserialize, Serialize};

pub const MEDIA_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MediaMessage {
    Hello {
        version: u16,
        node_id: u64,
    },
    Auth {
        nonce: Vec<u8>,
        response: String,
    },
    AuthOk,
    AuthFail,
    Subscribe {
        app: String,
        stream: String,
        epoch: u64,
    },
    Unsubscribe {
        app: String,
        stream: String,
    },
    StreamStart {
        app: String,
        stream: String,
        epoch: u64,
        owner_node: u64,
    },
    StreamStop {
        app: String,
        stream: String,
        epoch: u64,
    },
    /// Init-cache dump for a new subscriber.
    InitCache {
        app: String,
        stream: String,
        epoch: u64,
        metadata: Option<Vec<u8>>,
        avc_header: Option<Vec<u8>>,
        aac_header: Option<Vec<u8>>,
        keyframe: Option<(u32, Vec<u8>)>,
    },
    MediaFrame {
        app: String,
        stream: String,
        epoch: u64,
        frame_type: u8,
        timestamp: u32,
        /// Wire timeline timestamp after [`super::timeline`] remapping.
        timeline_ts: u32,
        payload: Vec<u8>,
    },
    Error {
        code: String,
        message: String,
    },
    StatsReq {
        stream_id: String,
    },
    StatsResp {
        stream_id: String,
        body: serde_json::Value,
    },
}

impl MediaMessage {
    pub fn frame_type_from_librtmp2(ft: librtmp2::types::FrameType) -> u8 {
        match ft {
            librtmp2::types::FrameType::Audio => 0,
            librtmp2::types::FrameType::Video => 1,
            librtmp2::types::FrameType::Script => 2,
            librtmp2::types::FrameType::Metadata => 3,
        }
    }

    pub fn frame_type_to_librtmp2(v: u8) -> Option<librtmp2::types::FrameType> {
        match v {
            0 => Some(librtmp2::types::FrameType::Audio),
            1 => Some(librtmp2::types::FrameType::Video),
            2 => Some(librtmp2::types::FrameType::Script),
            3 => Some(librtmp2::types::FrameType::Metadata),
            _ => None,
        }
    }
}
