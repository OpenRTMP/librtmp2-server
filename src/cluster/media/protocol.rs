//! Versioned media-plane messages.
//!
//! The message *types* are version-independent; how they are framed on the
//! wire is chosen per session by the protocol version in `Hello` (see
//! [`super::wire`]): v1 frames every message as JSON, v2 sends `MediaFrame`
//! and `InitCache` as compact binary records and everything else as JSON.

use librtmp2::DeliveryHint;
use serde::{Deserialize, Serialize};

/// Newest media protocol this build speaks (see [`super::wire`] for the
/// per-version framing and the v1 fallback).
pub const MEDIA_PROTOCOL_VERSION: u16 = 2;
/// Oldest media protocol this build still accepts and can fall back to for a
/// rolling upgrade.
pub const MEDIA_PROTOCOL_MIN_VERSION: u16 = 1;
/// `Error.code` when an inbound `Subscribe` is rejected after the gate window.
pub const SUBSCRIBE_DENIED: &str = "subscribe_denied";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MediaMessage {
    Hello {
        version: u16,
        node_id: u64,
    },
    /// Server-issued anti-replay nonce (first frame on a new connection).
    AuthChallenge {
        nonce: Vec<u8>,
    },
    Auth {
        node_id: u64,
        response: String,
    },
    AuthOk,
    AuthFail,
    Subscribe {
        app: String,
        stream: String,
        epoch: u64,
        /// Subscriber-local generation; echoed on `SUBSCRIBE_DENIED`.
        #[serde(default)]
        generation: u64,
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
        /// Codec-neutral congestion class assigned by librtmp2 at export.
        /// Absent in v1 JSON from older nodes, which reads as `Droppable`.
        #[serde(default = "default_hint", with = "hint_serde")]
        hint: DeliveryHint,
        payload: Vec<u8>,
    },
    Error {
        code: String,
        message: String,
        /// Echoed `Subscribe.generation` for `SUBSCRIBE_DENIED` (`0` = legacy).
        #[serde(default)]
        generation: u64,
    },
    StatsReq {
        stream_id: String,
    },
    StatsResp {
        stream_id: String,
        body: serde_json::Value,
    },
}

fn default_hint() -> DeliveryHint {
    DeliveryHint::Droppable
}

/// `DeliveryHint` travels as its one-byte wire code in JSON too.
mod hint_serde {
    use super::DeliveryHint;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(hint: &DeliveryHint, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u8(hint.to_u8())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DeliveryHint, D::Error> {
        let code = u8::deserialize(d)?;
        DeliveryHint::from_u8(code).ok_or_else(|| serde::de::Error::custom("unknown delivery hint"))
    }
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
