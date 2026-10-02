//! Media-plane framing for protocol versions 1 and 2.
//!
//! # Negotiation
//!
//! Authentication (`AuthChallenge`/`Auth`/`AuthOk`/`AuthFail`), the `Hello`
//! and the `Error{VERSION}` rejection are **always v1 frames** (JSON), so any
//! two nodes can read each other's first messages. `Hello.version` is the
//! version the sender will use for every frame after the `Hello`, in both
//! directions of that connection. An acceptor that supports the version
//! confirms it with its own v1-framed `Hello` (only for versions newer than
//! v1, so v1 dialers see nothing new) and keeps the connection open; one that
//! does not support it answers with `Error{code: "VERSION"}` (v1 framed) and
//! closes — a legacy v1 node does exactly that, or just hangs up. A dialer
//! that gets no confirmation falls back to [`MEDIA_PROTOCOL_MIN_VERSION`] for
//! a while and reconnects. A v2 node
//! therefore talks v2 to v2 nodes and v1 to v1 nodes (rolling upgrade); the
//! two encodings are never mixed on one connection, so the same bytes cannot
//! be read two ways.
//!
//! # v1 frame
//!
//! `u32 BE length` + JSON of a [`MediaMessage`].
//!
//! # v2 frame
//!
//! ```text
//! u32 BE  length        bytes after this field (>= 1, <= MAX_FRAME)
//! u8      kind          0 = Control, 1 = MediaFrame, 2 = InitCache
//! ...     body
//! ```
//!
//! * `Control`: the rest is the JSON of any [`MediaMessage`] other than
//!   `MediaFrame` / `InitCache` (rare, small).
//! * `MediaFrame`: `u8 hint, u8 frame_type, u8 flags (0), u64 epoch,
//!   u32 timestamp, u32 timeline_ts, u8 app_len, u16 stream_len,
//!   u32 payload_len`, then `app`, `stream` (UTF-8) and the raw payload.
//!   `length` must equal `1 + 26 + app_len + stream_len + payload_len`.
//! * `InitCache`: `u8 presence (bit0 metadata, bit1 avc, bit2 aac,
//!   bit3 keyframe), u64 epoch, u32 keyframe_ts, u8 app_len, u16 stream_len`,
//!   `app`, `stream`, then for each present part (in that order) `u32 len`
//!   and the bytes.
//!
//! All integers are big endian. Every length is checked against the frame
//! length and a hard cap *before* anything is allocated for it.

use std::io::{Error, ErrorKind};

use librtmp2::DeliveryHint;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::cluster::media::protocol::MediaMessage;
use crate::cluster::media::protocol::{MEDIA_PROTOCOL_MIN_VERSION, MEDIA_PROTOCOL_VERSION};

/// Hard cap on one frame (v1 JSON or v2 binary), also bounds every
/// allocation made from a peer-supplied length.
pub const MAX_FRAME: u32 = 32 * 1024 * 1024;
/// Longest `app` accepted on the wire.
pub const MAX_APP_LEN: usize = 255;
/// Longest `stream` accepted on the wire.
pub const MAX_STREAM_LEN: usize = 1024;

const KIND_CONTROL: u8 = 0;
const KIND_MEDIA_FRAME: u8 = 1;
const KIND_INIT_CACHE: u8 = 2;

/// Fixed part of a v2 `MediaFrame` body after the kind byte.
const MEDIA_FIXED_LEN: usize = 1 + 1 + 1 + 8 + 4 + 4 + 1 + 2 + 4;
/// Fixed part of a v2 `InitCache` body after the kind byte.
const INIT_FIXED_LEN: usize = 1 + 8 + 4 + 1 + 2;
/// Payloads up to this size are copied behind their header so a small frame
/// goes out in one write; larger ones are written straight from the caller's
/// buffer.
const COALESCE_PAYLOAD_MAX: usize = 16 * 1024;

/// Whether this build can speak `version`.
pub fn is_supported_version(version: u16) -> bool {
    (MEDIA_PROTOCOL_MIN_VERSION..=MEDIA_PROTOCOL_VERSION).contains(&version)
}

fn invalid(msg: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, msg)
}

/// Encode and write one frame in the given protocol `version`.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    msg: &MediaMessage,
    version: u16,
) -> Result<(), Error> {
    if version >= 2 {
        write_frame_v2(w, msg).await
    } else {
        write_frame_v1(w, msg).await
    }
}

async fn write_frame_v1<W: AsyncWrite + Unpin>(w: &mut W, msg: &MediaMessage) -> Result<(), Error> {
    let bytes = serde_json::to_vec(msg).map_err(Error::other)?;
    if bytes.len() > MAX_FRAME as usize {
        return Err(Error::other("media frame too large"));
    }
    w.write_u32(bytes.len() as u32).await?;
    w.write_all(&bytes).await?;
    Ok(())
}

async fn write_frame_v2<W: AsyncWrite + Unpin>(w: &mut W, msg: &MediaMessage) -> Result<(), Error> {
    match msg {
        MediaMessage::MediaFrame {
            app,
            stream,
            epoch,
            frame_type,
            timestamp,
            timeline_ts,
            hint,
            payload,
        } => {
            check_names(app, stream)?;
            let body_len = 1 + MEDIA_FIXED_LEN + app.len() + stream.len() + payload.len();
            let body_len = frame_len(body_len)?;
            let inline = payload.len() <= COALESCE_PAYLOAD_MAX;
            let mut head = Vec::with_capacity(
                4 + 1
                    + MEDIA_FIXED_LEN
                    + app.len()
                    + stream.len()
                    + if inline { payload.len() } else { 0 },
            );
            head.extend_from_slice(&body_len.to_be_bytes());
            head.push(KIND_MEDIA_FRAME);
            head.push(hint.to_u8());
            head.push(*frame_type);
            head.push(0); // flags, reserved
            head.extend_from_slice(&epoch.to_be_bytes());
            head.extend_from_slice(&timestamp.to_be_bytes());
            head.extend_from_slice(&timeline_ts.to_be_bytes());
            head.push(app.len() as u8);
            head.extend_from_slice(&(stream.len() as u16).to_be_bytes());
            head.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            head.extend_from_slice(app.as_bytes());
            head.extend_from_slice(stream.as_bytes());
            if inline {
                head.extend_from_slice(payload);
                w.write_all(&head).await
            } else {
                w.write_all(&head).await?;
                w.write_all(payload).await
            }
        }
        MediaMessage::InitCache {
            app,
            stream,
            epoch,
            metadata,
            avc_header,
            aac_header,
            keyframe,
        } => {
            check_names(app, stream)?;
            let parts = [
                metadata.as_deref(),
                avc_header.as_deref(),
                aac_header.as_deref(),
                keyframe.as_ref().map(|(_, p)| p.as_slice()),
            ];
            let mut presence = 0u8;
            let mut body_len = 1 + INIT_FIXED_LEN + app.len() + stream.len();
            for (i, part) in parts.iter().enumerate() {
                if let Some(p) = part {
                    presence |= 1 << i;
                    body_len += 4 + p.len();
                }
            }
            let body_len = frame_len(body_len)?;
            let mut buf = Vec::with_capacity(4 + body_len as usize);
            buf.extend_from_slice(&body_len.to_be_bytes());
            buf.push(KIND_INIT_CACHE);
            buf.push(presence);
            buf.extend_from_slice(&epoch.to_be_bytes());
            buf.extend_from_slice(&keyframe.as_ref().map_or(0, |(ts, _)| *ts).to_be_bytes());
            buf.push(app.len() as u8);
            buf.extend_from_slice(&(stream.len() as u16).to_be_bytes());
            buf.extend_from_slice(app.as_bytes());
            buf.extend_from_slice(stream.as_bytes());
            for p in parts.iter().flatten() {
                buf.extend_from_slice(&(p.len() as u32).to_be_bytes());
                buf.extend_from_slice(p);
            }
            w.write_all(&buf).await
        }
        other => {
            let json = serde_json::to_vec(other).map_err(Error::other)?;
            let body_len = frame_len(1 + json.len())?;
            let mut buf = Vec::with_capacity(5 + json.len());
            buf.extend_from_slice(&body_len.to_be_bytes());
            buf.push(KIND_CONTROL);
            buf.extend_from_slice(&json);
            w.write_all(&buf).await
        }
    }
}

fn check_names(app: &str, stream: &str) -> Result<(), Error> {
    if app.len() > MAX_APP_LEN || stream.len() > MAX_STREAM_LEN {
        return Err(Error::other("media name too long"));
    }
    Ok(())
}

fn frame_len(len: usize) -> Result<u32, Error> {
    if len > MAX_FRAME as usize {
        return Err(Error::other("media frame too large"));
    }
    Ok(len as u32)
}

/// Read one frame in the given protocol `version`. `max` caps the frame
/// length (a smaller cap applies before authentication completes);
/// `reserve` accounts the frame's bytes against the shared read budget and
/// is held until the frame was read.
pub async fn read_frame<R, G>(
    r: &mut R,
    version: u16,
    max: u32,
    reserve: impl FnOnce(usize) -> Result<G, Error>,
) -> Result<MediaMessage, Error>
where
    R: AsyncRead + Unpin,
{
    let len = r.read_u32().await?;
    if len > max {
        return Err(Error::other("media frame too large"));
    }
    let _budget = reserve(len as usize)?;
    if version >= 2 {
        read_body_v2(r, len as usize).await
    } else {
        let buf = read_vec(r, len as usize).await?;
        serde_json::from_slice(&buf).map_err(Error::other)
    }
}

async fn read_body_v2<R: AsyncRead + Unpin>(r: &mut R, len: usize) -> Result<MediaMessage, Error> {
    if len < 1 {
        return Err(invalid("empty v2 frame"));
    }
    let kind = r.read_u8().await?;
    let rest = len - 1;
    match kind {
        KIND_CONTROL => {
            let buf = read_vec(r, rest).await?;
            let msg: MediaMessage = serde_json::from_slice(&buf).map_err(Error::other)?;
            // The binary kinds are the only encoding of these two messages
            // in v2; a JSON copy would give one message two spellings.
            if matches!(
                msg,
                MediaMessage::MediaFrame { .. } | MediaMessage::InitCache { .. }
            ) {
                return Err(invalid("media message sent as control in v2"));
            }
            Ok(msg)
        }
        KIND_MEDIA_FRAME => read_media_frame_v2(r, rest).await,
        KIND_INIT_CACHE => read_init_cache_v2(r, rest).await,
        // A v1 frame is JSON: its body starts with '{' (struct variants) or
        // '"' (unit variants), neither of which is a v2 kind. Reported as
        // `Unsupported` so a dialer can tell "the peer answered in v1" (a
        // legacy acceptor rejecting our version) from a corrupt frame.
        b'{' | b'"' => Err(Error::new(
            ErrorKind::Unsupported,
            "peer answered in v1 framing",
        )),
        _ => Err(invalid("unknown v2 message kind")),
    }
}

async fn read_media_frame_v2<R: AsyncRead + Unpin>(
    r: &mut R,
    rest: usize,
) -> Result<MediaMessage, Error> {
    if rest < MEDIA_FIXED_LEN {
        return Err(invalid("truncated v2 media header"));
    }
    let mut h = [0u8; MEDIA_FIXED_LEN];
    r.read_exact(&mut h).await?;
    let hint = DeliveryHint::from_u8(h[0]).ok_or_else(|| invalid("unknown delivery hint"))?;
    let frame_type = h[1];
    if MediaMessage::frame_type_to_librtmp2(frame_type).is_none() {
        return Err(invalid("unknown frame type"));
    }
    if h[2] != 0 {
        return Err(invalid("unknown v2 media flags"));
    }
    let epoch = u64::from_be_bytes(h[3..11].try_into().map_err(|_| invalid("header"))?);
    let timestamp = u32::from_be_bytes(h[11..15].try_into().map_err(|_| invalid("header"))?);
    let timeline_ts = u32::from_be_bytes(h[15..19].try_into().map_err(|_| invalid("header"))?);
    let app_len = h[19] as usize;
    let stream_len =
        u16::from_be_bytes(h[20..22].try_into().map_err(|_| invalid("header"))?) as usize;
    let payload_len =
        u32::from_be_bytes(h[22..26].try_into().map_err(|_| invalid("header"))?) as usize;
    if stream_len > MAX_STREAM_LEN {
        return Err(invalid("stream name too long"));
    }
    // Checked add: three attacker-chosen lengths must sum to the frame.
    let expected = MEDIA_FIXED_LEN
        .checked_add(app_len)
        .and_then(|n| n.checked_add(stream_len))
        .and_then(|n| n.checked_add(payload_len))
        .ok_or_else(|| invalid("v2 media length overflow"))?;
    if expected != rest {
        return Err(invalid("v2 media length mismatch"));
    }
    let app = read_string(r, app_len).await?;
    let stream = read_string(r, stream_len).await?;
    // Allocated only now that payload_len is proven to fit the (capped,
    // budget-reserved) frame; read straight into the final buffer.
    let payload = read_vec(r, payload_len).await?;
    Ok(MediaMessage::MediaFrame {
        app,
        stream,
        epoch,
        frame_type,
        timestamp,
        timeline_ts,
        hint,
        payload,
    })
}

async fn read_init_cache_v2<R: AsyncRead + Unpin>(
    r: &mut R,
    rest: usize,
) -> Result<MediaMessage, Error> {
    if rest < INIT_FIXED_LEN {
        return Err(invalid("truncated v2 init-cache header"));
    }
    let mut h = [0u8; INIT_FIXED_LEN];
    r.read_exact(&mut h).await?;
    let presence = h[0];
    if presence & !0x0F != 0 {
        return Err(invalid("unknown init-cache parts"));
    }
    let epoch = u64::from_be_bytes(h[1..9].try_into().map_err(|_| invalid("header"))?);
    let keyframe_ts = u32::from_be_bytes(h[9..13].try_into().map_err(|_| invalid("header"))?);
    let app_len = h[13] as usize;
    let stream_len =
        u16::from_be_bytes(h[14..16].try_into().map_err(|_| invalid("header"))?) as usize;
    if stream_len > MAX_STREAM_LEN {
        return Err(invalid("stream name too long"));
    }
    let mut remaining = rest - INIT_FIXED_LEN;
    let names = app_len
        .checked_add(stream_len)
        .filter(|n| *n <= remaining)
        .ok_or_else(|| invalid("truncated v2 init-cache names"))?;
    let app = read_string(r, app_len).await?;
    let stream = read_string(r, stream_len).await?;
    remaining -= names;
    let mut parts: [Option<Vec<u8>>; 4] = [None, None, None, None];
    for (i, slot) in parts.iter_mut().enumerate() {
        if presence & (1 << i) == 0 {
            continue;
        }
        if remaining < 4 {
            return Err(invalid("truncated v2 init-cache part"));
        }
        let len = r.read_u32().await? as usize;
        remaining -= 4;
        if len > remaining {
            return Err(invalid("v2 init-cache part exceeds frame"));
        }
        let buf = read_vec(r, len).await?;
        remaining -= len;
        *slot = Some(buf);
    }
    if remaining != 0 {
        return Err(invalid("trailing bytes in v2 init-cache"));
    }
    let [metadata, avc_header, aac_header, keyframe] = parts;
    Ok(MediaMessage::InitCache {
        app,
        stream,
        epoch,
        metadata,
        avc_header,
        aac_header,
        keyframe: keyframe.map(|p| (keyframe_ts, p)),
    })
}

/// Read exactly `len` bytes. The buffer grows as data actually arrives
/// instead of being sized up front from a peer-supplied length, so a peer
/// that announces a large frame and sends nothing cannot make us allocate it.
async fn read_vec<R: AsyncRead + Unpin>(r: &mut R, len: usize) -> Result<Vec<u8>, Error> {
    const INITIAL_CAP: usize = 64 * 1024;
    let mut buf = Vec::with_capacity(len.min(INITIAL_CAP));
    let got = r.take(len as u64).read_to_end(&mut buf).await?;
    if got != len {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "truncated media frame",
        ));
    }
    Ok(buf)
}

async fn read_string<R: AsyncRead + Unpin>(r: &mut R, len: usize) -> Result<String, Error> {
    let buf = read_vec(r, len).await?;
    String::from_utf8(buf).map_err(|_| invalid("name is not UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_budget(_: usize) -> Result<(), Error> {
        Ok(())
    }

    fn media(payload: Vec<u8>) -> MediaMessage {
        MediaMessage::MediaFrame {
            app: "live".into(),
            stream: "stream-1".into(),
            epoch: 0x0102_0304_0506_0708,
            frame_type: 1,
            timestamp: 4000,
            timeline_ts: 4001,
            hint: DeliveryHint::ResyncPoint,
            payload,
        }
    }

    async fn encode(msg: &MediaMessage, version: u16) -> Vec<u8> {
        let mut out = Vec::new();
        write_frame(&mut out, msg, version).await.unwrap();
        out
    }

    async fn decode(bytes: &[u8], version: u16) -> Result<MediaMessage, Error> {
        let mut r = bytes;
        read_frame(&mut r, version, MAX_FRAME, no_budget).await
    }

    #[tokio::test]
    async fn v2_media_frame_roundtrips_with_raw_payload_and_hint() {
        let payload: Vec<u8> = (0..=255u8).cycle().take(5000).collect();
        let bytes = encode(&media(payload.clone()), 2).await;
        // Raw payload: header overhead is tiny, not a JSON number array.
        assert!(bytes.len() < payload.len() + 80, "len {}", bytes.len());
        assert!(bytes.windows(256).any(|w| w == &payload[..256]));
        match decode(&bytes, 2).await.unwrap() {
            MediaMessage::MediaFrame {
                app,
                stream,
                epoch,
                frame_type,
                timestamp,
                timeline_ts,
                hint,
                payload: got,
            } => {
                assert_eq!((app.as_str(), stream.as_str()), ("live", "stream-1"));
                assert_eq!(epoch, 0x0102_0304_0506_0708);
                assert_eq!((frame_type, timestamp, timeline_ts), (1, 4000, 4001));
                assert_eq!(hint, DeliveryHint::ResyncPoint);
                assert_eq!(got, payload);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn v2_is_much_smaller_than_v1_for_binary_payloads() {
        let msg = media(vec![0xFF; 10_000]);
        let v1 = encode(&msg, 1).await;
        let v2 = encode(&msg, 2).await;
        assert!(v1.len() > 3 * v2.len(), "v1 {} v2 {}", v1.len(), v2.len());
    }

    #[tokio::test]
    async fn large_payload_is_written_without_being_coalesced() {
        let payload = vec![7u8; COALESCE_PAYLOAD_MAX + 1];
        let bytes = encode(&media(payload.clone()), 2).await;
        match decode(&bytes, 2).await.unwrap() {
            MediaMessage::MediaFrame { payload: got, .. } => assert_eq!(got, payload),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn v2_init_cache_roundtrips_every_presence_combination() {
        for mask in 0u8..16 {
            let part = |bit: u8, v: u8| (mask & (1 << bit) != 0).then(|| vec![v; 3 + v as usize]);
            let msg = MediaMessage::InitCache {
                app: "a".into(),
                stream: "s".into(),
                epoch: 9,
                metadata: part(0, 1),
                avc_header: part(1, 2),
                aac_header: part(2, 3),
                keyframe: part(3, 4).map(|p| (777, p)),
            };
            let bytes = encode(&msg, 2).await;
            let MediaMessage::InitCache {
                metadata,
                avc_header,
                aac_header,
                keyframe,
                epoch,
                ..
            } = decode(&bytes, 2).await.unwrap()
            else {
                panic!("not init cache");
            };
            assert_eq!(epoch, 9);
            assert_eq!(metadata, part(0, 1));
            assert_eq!(avc_header, part(1, 2));
            assert_eq!(aac_header, part(2, 3));
            assert_eq!(keyframe, part(3, 4).map(|p| (777, p)));
        }
    }

    #[tokio::test]
    async fn control_messages_use_json_inside_v2() {
        let msg = MediaMessage::Subscribe {
            app: "live".into(),
            stream: "x".into(),
            epoch: 3,
            generation: 4,
        };
        let bytes = encode(&msg, 2).await;
        assert_eq!(bytes[4], KIND_CONTROL);
        assert!(matches!(
            decode(&bytes, 2).await.unwrap(),
            MediaMessage::Subscribe { generation: 4, .. }
        ));
    }

    #[tokio::test]
    async fn v1_roundtrips_and_reads_frames_without_a_hint_as_droppable() {
        let bytes = encode(&media(vec![1, 2, 3]), 1).await;
        assert!(matches!(
            decode(&bytes, 1).await.unwrap(),
            MediaMessage::MediaFrame {
                hint: DeliveryHint::ResyncPoint,
                ..
            }
        ));
        // A v1 frame from a node that predates `hint`.
        let json = br#"{"MediaFrame":{"app":"a","stream":"s","epoch":1,"frame_type":1,"timestamp":2,"timeline_ts":3,"payload":[9]}}"#;
        let mut bytes = (json.len() as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(json);
        match decode(&bytes, 1).await.unwrap() {
            MediaMessage::MediaFrame { hint, payload, .. } => {
                assert_eq!(hint, DeliveryHint::Droppable);
                assert_eq!(payload, vec![9]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_two_versions_do_not_read_each_others_bytes() {
        let v2 = encode(&media(vec![1; 10]), 2).await;
        assert!(decode(&v2, 1).await.is_err(), "v1 reader must reject v2");
        let v1 = encode(&media(vec![1; 10]), 1).await;
        assert!(decode(&v1, 2).await.is_err(), "v2 reader must reject v1");
    }

    #[tokio::test]
    async fn v1_json_read_as_v2_is_reported_as_unsupported_for_fallback() {
        for msg in [
            MediaMessage::Error {
                code: "VERSION".into(),
                message: "x".into(),
                generation: 0,
            },
            MediaMessage::AuthOk,
        ] {
            let v1 = encode(&msg, 1).await;
            let err = decode(&v1, 2).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unsupported, "{msg:?}");
        }
    }

    #[tokio::test]
    async fn truncated_frames_fail_cleanly() {
        let bytes = encode(&media(vec![5; 100]), 2).await;
        for cut in [0, 3, 4, 5, 20, 31, bytes.len() - 1] {
            assert!(decode(&bytes[..cut], 2).await.is_err(), "cut at {cut}");
        }
        let init = encode(
            &MediaMessage::InitCache {
                app: "a".into(),
                stream: "s".into(),
                epoch: 1,
                metadata: Some(vec![1; 8]),
                avc_header: None,
                aac_header: None,
                keyframe: Some((1, vec![2; 8])),
            },
            2,
        )
        .await;
        for cut in 0..init.len() {
            assert!(decode(&init[..cut], 2).await.is_err(), "init cut at {cut}");
        }
    }

    fn frame_with(mutate: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        // length, kind, then the fixed header + "live" + "s" + 4 payload bytes
        let mut body = vec![KIND_MEDIA_FRAME, 2, 1, 0];
        body.extend_from_slice(&1u64.to_be_bytes());
        body.extend_from_slice(&2u32.to_be_bytes());
        body.extend_from_slice(&3u32.to_be_bytes());
        body.push(4); // app_len
        body.extend_from_slice(&1u16.to_be_bytes()); // stream_len
        body.extend_from_slice(&4u32.to_be_bytes()); // payload_len
        body.extend_from_slice(b"lives");
        body.extend_from_slice(&[9, 9, 9, 9]);
        mutate(&mut body);
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    #[tokio::test]
    async fn invalid_lengths_and_fields_are_rejected() {
        assert!(decode(&frame_with(|_| {}), 2).await.is_ok());
        // payload_len larger than the frame
        let big = frame_with(|b| b[23..27].copy_from_slice(&u32::MAX.to_be_bytes()));
        assert!(decode(&big, 2).await.is_err());
        // payload_len smaller than the frame (trailing bytes)
        let small = frame_with(|b| b[23..27].copy_from_slice(&1u32.to_be_bytes()));
        assert!(decode(&small, 2).await.is_err());
        // stream_len over the cap
        let long = frame_with(|b| b[21..23].copy_from_slice(&u16::MAX.to_be_bytes()));
        assert!(decode(&long, 2).await.is_err());
        // unknown hint, frame type, flags
        assert!(decode(&frame_with(|b| b[1] = 9), 2).await.is_err());
        assert!(decode(&frame_with(|b| b[2] = 9), 2).await.is_err());
        assert!(decode(&frame_with(|b| b[3] = 1), 2).await.is_err());
        // names must be UTF-8
        let bad = frame_with(|b| b[27] = 0xFF);
        assert!(decode(&bad, 2).await.is_err());
        // zero-length frame
        assert!(decode(&0u32.to_be_bytes(), 2).await.is_err());
    }

    #[tokio::test]
    async fn oversized_declared_length_is_refused_before_allocation() {
        let bytes = (MAX_FRAME + 1).to_be_bytes();
        let err = decode(&bytes, 2).await.unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
        let mut r: &[u8] = &(100u32).to_be_bytes();
        let err = read_frame(&mut r, 2, 50, no_budget).await.unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
    }

    #[tokio::test]
    async fn read_budget_is_checked_before_the_body_is_read() {
        let mut r: &[u8] = &(1000u32).to_be_bytes();
        let err = read_frame(&mut r, 2, MAX_FRAME, |n| {
            assert_eq!(n, 1000);
            Err::<(), _>(Error::other("budget exceeded"))
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("budget"));
    }

    #[tokio::test]
    async fn unknown_kind_and_media_as_control_are_rejected() {
        let mut bytes = 3u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0x7F, 0, 0]);
        assert!(decode(&bytes, 2).await.is_err());
        // MediaFrame JSON smuggled in a Control record.
        let v1 = encode(&media(vec![1]), 1).await;
        let json = &v1[4..];
        let mut bytes = ((json.len() + 1) as u32).to_be_bytes().to_vec();
        bytes.push(KIND_CONTROL);
        bytes.extend_from_slice(json);
        assert!(decode(&bytes, 2).await.is_err());
    }

    #[tokio::test]
    async fn names_over_the_limits_are_not_written() {
        let mut msg = media(vec![1]);
        if let MediaMessage::MediaFrame { app, .. } = &mut msg {
            *app = "a".repeat(MAX_APP_LEN + 1);
        }
        let mut out = Vec::new();
        assert!(write_frame(&mut out, &msg, 2).await.is_err());
        assert!(
            out.is_empty(),
            "nothing may be written for a rejected frame"
        );
    }

    #[test]
    fn supported_versions_are_one_and_two() {
        assert!(is_supported_version(1));
        assert!(is_supported_version(2));
        assert!(!is_supported_version(0));
        assert!(!is_supported_version(3));
    }
}
