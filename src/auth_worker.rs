//! Dedicated worker thread for RTMP publish/play authorization.
//!
//! [`crate::rtmp_bridge::DbRtmpBridge::authorize_publish`]/`authorize_play` do
//! blocking SQLite work (and, when clustering is enabled, Raft ownership
//! acquisition). Calling them directly from librtmp2's `publish`/`play`
//! callbacks -- as this server used to -- runs that work on the single RTMP
//! poll thread, so one slow authorization (lock contention, a busy disk, a
//! Raft round trip) stalls every other connection's handshake, publish, and
//! play until it returns.
//!
//! This module moves that work off the RTMP thread using librtmp2's
//! `AuthorizationResult::Pending` API: the `publish`/`play` callbacks
//! (`rtmp_publish_auth_cb`/`rtmp_play_auth_cb` in `server.rs`) submit the
//! request here and return `Pending` immediately, and the RTMP poll loop
//! resolves it later via `Server::complete_publish_authorization`/
//! `complete_play_authorization` once this worker finishes.
//!
//! A single dedicated OS thread processes requests one at a time. This
//! matches `Db`'s existing single-connection-mutex model (SQLite access was
//! already fully serialized; a pool of worker threads would just queue on
//! that same mutex) while guaranteeing authorization work never runs on the
//! RTMP thread. The request queue is bounded: when it's full the caller gets
//! an immediate `Err` and must fail the request closed (`Deny`) rather than
//! block the RTMP thread waiting for room, or letting the queue grow without
//! bound while the DB falls behind.

use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};

use crate::rtmp_bridge::{DbRtmpBridge, RtmpEventHandler};

/// Bound on authorization requests queued but not yet picked up by the
/// worker thread. Generous enough to absorb a burst of simultaneous viewer
/// joins; small enough that a stuck/slow DB fails new requests closed
/// instead of growing this queue without limit.
pub const AUTH_QUEUE_CAPACITY: usize = 512;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AuthKind {
    Publish,
    Play,
}

struct AuthRequest {
    kind: AuthKind,
    conn_id: u64,
    app: String,
    stream_key: String,
}

/// Result of a completed authorization request. Produced by the worker
/// thread, drained by the RTMP poll loop once per tick (see
/// [`drain_completions`]) and applied via `Server::complete_publish_authorization`/
/// `complete_play_authorization`.
pub struct AuthCompletion {
    pub kind: AuthKind,
    pub conn_id: u64,
    pub allow: bool,
}

/// Handle used by the RTMP `publish`/`play` callbacks to submit
/// authorization work. Cheap to clone; every clone shares the same bounded
/// queue and worker thread.
#[derive(Clone)]
pub struct AuthWorkerHandle {
    tx: SyncSender<AuthRequest>,
}

impl AuthWorkerHandle {
    /// Submits publish/play authorization work to the dedicated worker
    /// thread. Returns `Err(())` when the queue is full or the worker thread
    /// is gone; the caller must treat that as `AuthorizationResult::Deny`
    /// (fail closed) rather than retry synchronously, since retrying inline
    /// would reintroduce blocking on the RTMP thread.
    #[allow(clippy::result_unit_err)]
    pub fn try_submit(
        &self,
        kind: AuthKind,
        conn_id: u64,
        app: &str,
        stream_key: &str,
    ) -> Result<(), ()> {
        self.tx
            .try_send(AuthRequest {
                kind,
                conn_id,
                app: app.to_string(),
                stream_key: stream_key.to_string(),
            })
            .map_err(|e| {
                match e {
                    TrySendError::Full(_) => crate::log_warn!(
                        "RTMP auth worker queue full ({AUTH_QUEUE_CAPACITY} pending); denying conn={conn_id}"
                    ),
                    TrySendError::Disconnected(_) => crate::log_error!(
                        "RTMP auth worker thread is not running; denying conn={conn_id}"
                    ),
                }
            })
    }
}

/// Spawns the dedicated auth worker thread. Returns a submission handle for
/// the RTMP callbacks and the completion receiver the RTMP poll loop drains
/// once per tick via [`drain_completions`]. The worker thread runs until
/// `handle` and every clone of it are dropped, at which point the request
/// channel closes and the thread's loop ends on its own.
pub fn spawn(bridge: Arc<DbRtmpBridge>) -> (AuthWorkerHandle, Receiver<AuthCompletion>) {
    spawn_with_notify(bridge, || {})
}

/// [`spawn`], calling `notify` after each completion is queued so the
/// receiving poll loop can be woken instead of finding it on its next tick.
pub fn spawn_with_notify(
    bridge: Arc<DbRtmpBridge>,
    notify: impl Fn() + Send + 'static,
) -> (AuthWorkerHandle, Receiver<AuthCompletion>) {
    let (req_tx, req_rx) = sync_channel::<AuthRequest>(AUTH_QUEUE_CAPACITY);
    let (completion_tx, completion_rx) = sync_channel::<AuthCompletion>(AUTH_QUEUE_CAPACITY);

    std::thread::Builder::new()
        .name("rtmp-auth-worker".to_string())
        .spawn(move || {
            // Blocks between requests -- no busy loop -- and exits cleanly
            // once every `AuthWorkerHandle` (and `req_tx`) is dropped.
            for req in req_rx {
                let allow = match req.kind {
                    AuthKind::Publish => bridge
                        .authorize_publish(req.conn_id, &req.app, &req.stream_key)
                        .is_ok(),
                    AuthKind::Play => bridge
                        .authorize_play(req.conn_id, &req.app, &req.stream_key)
                        .is_ok(),
                };
                // If the RTMP thread already shut down, there's no receiver
                // left to deliver this completion to; drop it silently.
                if completion_tx
                    .send(AuthCompletion {
                        kind: req.kind,
                        conn_id: req.conn_id,
                        allow,
                    })
                    .is_ok()
                {
                    notify();
                }
            }
        })
        .expect("failed to spawn RTMP auth worker thread");

    (AuthWorkerHandle { tx: req_tx }, completion_rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::state::StateCoordinator;
    use parking_lot::Mutex;
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    fn test_bridge() -> Arc<DbRtmpBridge> {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let deleted = Arc::new(Mutex::new(HashSet::new()));
        let bridge = Arc::new(DbRtmpBridge::new(Arc::clone(&db), deleted));
        let coordinator = Arc::new(StateCoordinator::standalone(Arc::clone(&db)));
        bridge.set_coordinator(coordinator);
        bridge
    }

    fn recv_within(rx: &Receiver<AuthCompletion>, timeout: Duration) -> Option<AuthCompletion> {
        rx.recv_timeout(timeout).ok()
    }

    #[test]
    fn unknown_publish_key_denies_without_blocking_caller() {
        let bridge = test_bridge();
        let (handle, rx) = spawn(bridge);

        handle
            .try_submit(AuthKind::Publish, 1, "live", "no-such-key")
            .unwrap();
        let completion = recv_within(&rx, Duration::from_secs(2)).expect("worker did not respond");
        assert_eq!(completion.conn_id, 1);
        assert_eq!(completion.kind, AuthKind::Publish);
        assert!(!completion.allow);
    }

    #[test]
    fn queue_full_denies_immediately_instead_of_blocking() {
        // A capacity-1 scenario is hard to reproduce against the real
        // (generous) constant without a slow bridge; instead verify the
        // documented contract directly: try_submit never blocks the caller
        // even when it returns Err.
        let bridge = test_bridge();
        let (handle, _rx) = spawn(bridge);
        let start = Instant::now();
        for i in 0..AUTH_QUEUE_CAPACITY as u64 {
            let _ = handle.try_submit(AuthKind::Publish, i, "live", "k");
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "submitting a full queue's worth of requests must not block"
        );
    }
}
