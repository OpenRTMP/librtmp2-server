//! Snapshot receiver bounds (no live cluster required).

#![cfg(feature = "cluster")]

use std::io::SeekFrom;
use std::sync::Arc;

use librtmp2_server::cluster::raft::TypeConfig;
use librtmp2_server::cluster::raft::state_machine::{SnapshotBuf, SqliteStateMachine};
use librtmp2_server::db::Db;
use openraft::alias::SnapshotMetaOf;
use openraft::storage::RaftStateMachine;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// A peer-supplied chunk offset must never extend the receiver buffer by bytes
/// the peer did not send: `Cursor<Vec<u8>>` would zero-fill the gap and abort
/// the process on allocation for a huge offset.
#[tokio::test]
async fn snapshot_buf_rejects_offset_past_received_bytes() {
    let mut buf = SnapshotBuf::default();

    let error = buf
        .seek(SeekFrom::Start(256 * 1024 * 1024 * 1024 * 1024))
        .await
        .expect_err("an offset far past the received bytes must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(buf.get_ref().len(), 0, "a rejected seek must not allocate");
    assert!(
        buf.seek(SeekFrom::End(1)).await.is_err(),
        "a forward End seek must be rejected too"
    );
    assert!(
        buf.seek(SeekFrom::Current(1)).await.is_err(),
        "a forward Current seek must be rejected too"
    );

    buf.write_all(b"abc").await.unwrap();
    assert_eq!(buf.get_ref().len(), 3);

    // A forward seek to the bytes already received is harmless and is what a
    // legitimate retry can request.
    buf.seek(SeekFrom::Start(3)).await.unwrap();
    buf.write_all(b"def").await.unwrap();
    assert_eq!(buf.get_ref().len(), 6);

    // The sender asks for the length with End(0) and may step backwards.
    assert_eq!(buf.seek(SeekFrom::End(0)).await.unwrap(), 6);
    assert_eq!(buf.seek(SeekFrom::Current(-1)).await.unwrap(), 5);

    // A rewind is legitimate (the sender restarts a segment).
    buf.seek(SeekFrom::Start(1)).await.unwrap();
    buf.write_all(b"Z").await.unwrap();

    // `Streaming::finish` shuts the writer down and rewinds it before install.
    buf.flush().await.unwrap();
    buf.shutdown().await.unwrap();
    buf.seek(SeekFrom::Start(0)).await.unwrap();
    let mut out = Vec::new();
    buf.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, b"aZcdef");
    assert_eq!(buf.into_inner(), b"aZcdef");
}

/// The cached-snapshot path must return the same bytes the database load
/// produced, so a second install or send does not switch data types.
#[tokio::test]
async fn get_current_snapshot_reuses_the_cached_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot-cache.db");
    let db = Arc::new(Db::open(path.to_str().unwrap()).unwrap());
    let meta: SnapshotMetaOf<TypeConfig> = SnapshotMetaOf::<TypeConfig> {
        last_log_id: None,
        last_membership: Default::default(),
    };
    let meta_json = serde_json::to_string(&meta).unwrap();
    db.with_conn(|conn| {
        conn.execute(
            "INSERT INTO raft_snapshots(id, meta_json, data) VALUES(1, ?, ?)",
            rusqlite::params![meta_json, b"cached-bytes".to_vec()],
        )
        .unwrap();
    });

    let mut sm = SqliteStateMachine::new(db).unwrap();
    let loaded = sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(loaded.snapshot.get_ref().as_slice(), b"cached-bytes");

    // Second call is served from `current_snapshot`, not the database.
    let cached = sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(cached.snapshot.get_ref().as_slice(), b"cached-bytes");
}
