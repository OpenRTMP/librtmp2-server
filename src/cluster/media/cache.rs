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

    pub fn remove(&self, app: &str, stream: &str) {
        self.inner
            .lock()
            .remove(&(app.to_string(), stream.to_string()));
    }

    /// Drop every entry for `stream`, whatever app it was staged under — for
    /// callers that can no longer resolve the stream's app.
    pub fn remove_stream(&self, stream: &str) {
        self.inner.lock().retain(|(_, s), _| s != stream);
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
        let Some(ft) =
            crate::cluster::media::protocol::MediaMessage::frame_type_to_librtmp2(frame_type)
        else {
            return;
        };
        use librtmp2::media::{CacheFrameKind, classify_cache_frame};
        let kind = classify_cache_frame(ft, payload);
        let mut g = self.inner.lock();
        let e = g.entry((app.to_string(), stream.to_string())).or_default();
        // A stale publisher under an old epoch must not wipe the current
        // owner's staged init fields, and a newer epoch must not keep the
        // previous publisher's metadata/headers/keyframe under the new epoch
        // label — subscribers joining mid-handoff (or when the new publisher
        // omits a media type) would otherwise accept incompatible init fields.
        if epoch < e.epoch {
            return;
        }
        if epoch > e.epoch {
            *e = InitCacheEntry {
                epoch,
                ..InitCacheEntry::default()
            };
        }
        match kind {
            CacheFrameKind::VideoSequenceHeader => e.avc_header = Some(payload.to_vec()),
            CacheFrameKind::VideoKeyframe => e.keyframe = Some((timestamp, payload.to_vec())),
            CacheFrameKind::AudioSequenceHeader => e.aac_header = Some(payload.to_vec()),
            CacheFrameKind::LiveOnly => {
                // Metadata/script frames (types 2/3) still stage as metadata.
                if matches!(frame_type, 2 | 3) {
                    e.metadata = Some(payload.to_vec());
                }
            }
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
        let ft =
            crate::cluster::media::protocol::MediaMessage::frame_type_from_librtmp2(frame_type);
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

#[cfg(test)]
mod tests {
    use super::*;
    use librtmp2::types::FrameType;

    const AVC_SEQ: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01];
    const AVC_KEY: &[u8] = &[0x17, 0x01, 0x00, 0x00, 0x00, 0x65];
    const AVC_INTER: &[u8] = &[0x27, 0x01, 0x00, 0x00, 0x00, 0x41];
    const AAC_SEQ: &[u8] = &[0xAF, 0x00, 0x12, 0x10];
    const AAC_RAW: &[u8] = &[0xAF, 0x01, 0x21, 0x00];

    #[test]
    fn put_get_remove_are_keyed_by_app_and_stream() {
        let store = InitCacheStore::new();
        assert!(store.get("live", "s1").is_none());
        store.put(
            "live",
            "s1",
            InitCacheEntry {
                metadata: Some(vec![1]),
                epoch: 3,
                ..InitCacheEntry::default()
            },
        );
        let e = store.get("live", "s1").expect("entry stored");
        assert_eq!(e.metadata, Some(vec![1]));
        assert_eq!(e.epoch, 3);
        assert!(store.get("other", "s1").is_none());
        assert!(store.get("live", "s2").is_none());

        // `put` replaces the whole entry.
        store.put("live", "s1", InitCacheEntry::default());
        assert_eq!(store.get("live", "s1").unwrap().metadata, None);

        store.remove("live", "s1");
        assert!(store.get("live", "s1").is_none());
        // Removing a missing key is a no-op.
        store.remove("live", "missing");
    }

    #[test]
    fn remove_stream_drops_every_app_for_that_stream() {
        let store = InitCacheStore::new();
        store.put(
            "live",
            "s1",
            InitCacheEntry {
                metadata: Some(vec![1]),
                epoch: 1,
                ..InitCacheEntry::default()
            },
        );
        store.put(
            "other",
            "s1",
            InitCacheEntry {
                epoch: 7,
                ..InitCacheEntry::default()
            },
        );
        store.put("live", "s2", InitCacheEntry::default());

        store.remove_stream("s1");
        assert!(store.get("live", "s1").is_none());
        assert!(
            store.get("other", "s1").is_none(),
            "an entry under a different app must be dropped too"
        );
        assert_eq!(
            store.get("live", "s2").unwrap().epoch,
            0,
            "another stream's entries must survive"
        );
        // Removing a stream with no entries is a no-op.
        store.remove_stream("missing");
    }

    #[test]
    fn update_from_frame_stages_each_init_field() {
        let store = InitCacheStore::new();
        store.update_from_frame("live", "s", 1, 1, 0, AVC_SEQ);
        store.update_from_frame("live", "s", 1, 0, 0, AAC_SEQ);
        store.update_from_frame("live", "s", 1, 3, 0, b"meta");
        store.update_from_frame("live", "s", 1, 1, 40, AVC_KEY);
        let e = store.get("live", "s").unwrap();
        assert_eq!(e.epoch, 1);
        assert_eq!(e.avc_header.as_deref(), Some(AVC_SEQ));
        assert_eq!(e.aac_header.as_deref(), Some(AAC_SEQ));
        assert_eq!(e.metadata.as_deref(), Some(&b"meta"[..]));
        assert_eq!(e.keyframe, Some((40, AVC_KEY.to_vec())));

        // Script frames (type 2) also stage as metadata; a newer keyframe
        // replaces the older one.
        store.update_from_frame("live", "s", 1, 2, 0, b"script");
        store.update_from_frame("live", "s", 1, 1, 80, AVC_KEY);
        let e = store.get("live", "s").unwrap();
        assert_eq!(e.metadata.as_deref(), Some(&b"script"[..]));
        assert_eq!(e.keyframe.as_ref().map(|(ts, _)| *ts), Some(80));
    }

    #[test]
    fn update_from_frame_ignores_live_only_and_unknown_types() {
        let store = InitCacheStore::new();
        store.update_from_frame("live", "s", 1, 1, 0, AVC_SEQ);
        // Inter frames and raw audio are live-only: nothing new is staged.
        store.update_from_frame("live", "s", 1, 1, 10, AVC_INTER);
        store.update_from_frame("live", "s", 1, 0, 10, AAC_RAW);
        let e = store.get("live", "s").unwrap();
        assert_eq!(e.avc_header.as_deref(), Some(AVC_SEQ));
        assert!(e.keyframe.is_none());
        assert!(e.aac_header.is_none());
        assert!(e.metadata.is_none());

        // An unknown wire frame type is ignored entirely (no entry created).
        store.update_from_frame("live", "other", 1, 9, 0, AVC_SEQ);
        assert!(store.get("live", "other").is_none());
    }

    #[test]
    fn update_from_frame_fences_stale_epochs_and_resets_on_newer_epoch() {
        let store = InitCacheStore::new();
        store.update_from_frame("live", "s", 5, 1, 0, AVC_SEQ);
        store.update_from_frame("live", "s", 5, 0, 0, AAC_SEQ);

        // A stale publisher (older epoch) must not overwrite current fields.
        store.update_from_frame("live", "s", 4, 3, 0, b"stale-meta");
        let e = store.get("live", "s").unwrap();
        assert_eq!(e.epoch, 5);
        assert!(e.metadata.is_none());

        // A newer epoch wipes the previous publisher's init fields.
        store.update_from_frame("live", "s", 6, 1, 0, AVC_KEY);
        let e = store.get("live", "s").unwrap();
        assert_eq!(e.epoch, 6);
        assert!(e.avc_header.is_none(), "old epoch header must be dropped");
        assert!(e.aac_header.is_none(), "old epoch header must be dropped");
        assert_eq!(e.keyframe, Some((0, AVC_KEY.to_vec())));
    }

    #[test]
    fn observe_maps_librtmp2_frame_types() {
        let store = InitCacheStore::new();
        store.observe("live", "s", 2, FrameType::Video, 0, AVC_SEQ);
        store.observe("live", "s", 2, FrameType::Audio, 0, AAC_SEQ);
        store.observe("live", "s", 2, FrameType::Metadata, 0, b"m");
        let e = store.get("live", "s").unwrap();
        assert_eq!(e.avc_header.as_deref(), Some(AVC_SEQ));
        assert_eq!(e.aac_header.as_deref(), Some(AAC_SEQ));
        assert_eq!(e.metadata.as_deref(), Some(&b"m"[..]));
        store.observe("live", "s", 2, FrameType::Script, 0, b"sc");
        assert_eq!(
            store.get("live", "s").unwrap().metadata.as_deref(),
            Some(&b"sc"[..])
        );
    }

    #[test]
    fn store_snapshot_and_apply_wire_replace_entry() {
        let store = InitCacheStore::new();
        let snap = librtmp2::server::StreamInitSnapshot {
            metadata: Some(b"md".to_vec()),
            avc_header: Some(AVC_SEQ.to_vec()),
            aac_header: Some(AAC_SEQ.to_vec()),
            last_keyframe: Some((7, AVC_KEY.to_vec())),
            ..Default::default()
        };
        store.store_snapshot("live", "s", 9, &snap);
        let e = store.get("live", "s").unwrap();
        assert_eq!(e.epoch, 9);
        assert_eq!(e.metadata.as_deref(), Some(&b"md"[..]));
        assert_eq!(e.avc_header.as_deref(), Some(AVC_SEQ));
        assert_eq!(e.aac_header.as_deref(), Some(AAC_SEQ));
        assert_eq!(e.keyframe, Some((7, AVC_KEY.to_vec())));

        store.apply_wire("live", "s", 10, None, Some(vec![9]), None, None);
        let e = store.get("live", "s").unwrap();
        assert_eq!(e.epoch, 10);
        assert!(e.metadata.is_none());
        assert_eq!(e.avc_header, Some(vec![9]));
        assert!(e.aac_header.is_none());
        assert!(e.keyframe.is_none());
    }
}
