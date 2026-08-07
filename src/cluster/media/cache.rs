//! Init-cache staging for remote subscribe.

use std::collections::HashMap;

use parking_lot::Mutex;

#[derive(Clone, Default)]
pub struct InitCacheEntry {
    pub metadata: Option<Vec<u8>>,
    pub avc_header: Option<Vec<u8>>,
    pub aac_header: Option<Vec<u8>>,
    pub keyframe: Option<(u32, Vec<u8>)>,
    pub epoch: u64,
}

#[derive(Default)]
pub struct InitCacheStore {
    inner: Mutex<HashMap<(String, String), InitCacheEntry>>,
}

impl InitCacheStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&self, app: &str, stream: &str, entry: InitCacheEntry) {
        self.inner
            .lock()
            .insert((app.to_string(), stream.to_string()), entry);
    }

    pub fn get(&self, app: &str, stream: &str) -> Option<InitCacheEntry> {
        self.inner
            .lock()
            .get(&(app.to_string(), stream.to_string()))
            .cloned()
    }

    pub fn update_from_frame(
        &self,
        app: &str,
        stream: &str,
        epoch: u64,
        frame_type: u8,
        timestamp: u32,
        payload: &[u8],
    ) {
        let mut g = self.inner.lock();
        let e = g
            .entry((app.to_string(), stream.to_string()))
            .or_default();
        e.epoch = epoch;
        match frame_type {
            2 | 3 => e.metadata = Some(payload.to_vec()),
            1 => {
                // Heuristic: small early video = header; keyframe flag not available here.
                if payload.len() < 256 && e.avc_header.is_none() {
                    e.avc_header = Some(payload.to_vec());
                } else {
                    e.keyframe = Some((timestamp, payload.to_vec()));
                }
            }
            0 => {
                if payload.len() < 128 && e.aac_header.is_none() {
                    e.aac_header = Some(payload.to_vec());
                }
            }
            _ => {}
        }
    }

    pub fn observe(
        &self,
        app: &str,
        stream: &str,
        epoch: u64,
        frame_type: librtmp2::types::FrameType,
        timestamp: u32,
        payload: &[u8],
    ) {
        let ft = crate::cluster::media::protocol::MediaMessage::frame_type_from_librtmp2(frame_type);
        self.update_from_frame(app, stream, epoch, ft, timestamp, payload);
    }

    pub fn store_snapshot(
        &self,
        app: &str,
        stream: &str,
        epoch: u64,
        snap: &librtmp2::server::StreamInitSnapshot,
    ) {
        self.put(
            app,
            stream,
            InitCacheEntry {
                metadata: snap.metadata.clone(),
                avc_header: snap.avc_header.clone(),
                aac_header: snap.aac_header.clone(),
                keyframe: snap.last_keyframe.clone(),
                epoch,
            },
        );
    }

    pub fn apply_wire(
        &self,
        app: &str,
        stream: &str,
        epoch: u64,
        metadata: Option<Vec<u8>>,
        avc_header: Option<Vec<u8>>,
        aac_header: Option<Vec<u8>>,
        keyframe: Option<(u32, Vec<u8>)>,
    ) {
        self.put(
            app,
            stream,
            InitCacheEntry {
                metadata,
                avc_header,
                aac_header,
                keyframe,
                epoch,
            },
        );
    }
}
