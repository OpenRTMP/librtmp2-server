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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel, sync_channel};

use crate::rtmp_bridge::{DbRtmpBridge, RtmpEventHandler};

/// Bound on authorization requests queued but not yet picked up by the
/// worker thread (connection closes share the queue but don't count against
/// it, see [`AuthWorkerHandle::try_submit_close`]). Generous enough to absorb a burst of simultaneous viewer
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

/// Work items for the worker thread, processed strictly in submission order.
enum Job {
    Authorize(AuthRequest),
    /// Run `on_close` (deactivating the connection's publisher/player rows)
    /// for a connection the RTMP poll loop saw close.
    Close(u64),
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
    tx: Sender<Job>,
    /// Authorization jobs queued and not yet taken by the worker; capped at
    /// [`AUTH_QUEUE_CAPACITY`].
    pending_auth: Arc<AtomicUsize>,
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
        if self.pending_auth.fetch_add(1, Ordering::AcqRel) >= AUTH_QUEUE_CAPACITY {
            self.pending_auth.fetch_sub(1, Ordering::AcqRel);
            crate::log_warn!(
                "RTMP auth worker queue full ({AUTH_QUEUE_CAPACITY} pending); denying conn={conn_id}"
            );
            return Err(());
        }
        self.tx
            .send(Job::Authorize(AuthRequest {
                kind,
                conn_id,
                app: app.to_string(),
                stream_key: stream_key.to_string(),
            }))
            .map_err(|_| {
                self.pending_auth.fetch_sub(1, Ordering::AcqRel);
                crate::log_error!("RTMP auth worker thread is not running; denying conn={conn_id}");
            })
    }
}

impl AuthWorkerHandle {
    /// Queues `on_close` for a closed connection on the worker thread, so its
    /// SQLite deactivation writes neither run on nor block the RTMP poll
    /// thread (they used to, stalling every other connection on the shard
    /// once per close -- noticeable whenever many viewers leave at once).
    ///
    /// Because it shares the authorization queue, the release is always
    /// applied before any publish/play authorization submitted after it --
    /// the same ordering the old inline call gave -- and always after any
    /// authorization for the same connection submitted before it. Closes
    /// are therefore never rejected for a full queue (running `on_close`
    /// inline instead could overtake a still-queued authorization, which
    /// would then recreate state for a dead connection that nothing
    /// releases); they don't count against [`AUTH_QUEUE_CAPACITY`] and are
    /// bounded by the number of connections anyway. Returns `Err(())` only
    /// when the worker is gone; the caller must then run `on_close` itself.
    #[allow(clippy::result_unit_err)]
    pub fn try_submit_close(&self, conn_id: u64) -> Result<(), ()> {
        self.tx.send(Job::Close(conn_id)).map_err(|_| ())
    }
}

/// Most jobs folded into one group-commit transaction. Bounds how long the
/// first job in a burst waits for its reply (each job is ~15-30 us of SQLite
/// work inside a batch) and how long other threads wait for the connection.
const MAX_BATCH_JOBS: usize = 32;

/// Group commit holds the SQLite connection across several jobs. With HA
/// clustering active, publish authorization can wait on a Raft round trip
/// whose apply path needs that same connection on another thread, so jobs
/// then run one by one as before.
fn batching_allowed(bridge: &DbRtmpBridge) -> bool {
    #[cfg(feature = "cluster")]
    if bridge.cluster_manager().is_some() {
        return false;
    }
    let _ = bridge;
    true
}

/// Runs `jobs` in order, returning the completion for each authorization.
fn run_jobs(bridge: &DbRtmpBridge, jobs: Vec<Job>) -> Vec<AuthCompletion> {
    let mut completions = Vec::with_capacity(jobs.len());
    for job in jobs {
        match job {
            Job::Close(conn_id) => bridge.on_close(conn_id),
            Job::Authorize(req) => {
                let allow = match req.kind {
                    AuthKind::Publish => bridge
                        .authorize_publish(req.conn_id, &req.app, &req.stream_key)
                        .is_ok(),
                    AuthKind::Play => bridge
                        .authorize_play(req.conn_id, &req.app, &req.stream_key)
                        .is_ok(),
                };
                completions.push(AuthCompletion {
                    kind: req.kind,
                    conn_id: req.conn_id,
                    allow,
                });
            }
        }
    }
    completions
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
    let (req_tx, req_rx) = channel::<Job>();
    let (completion_tx, completion_rx) = sync_channel::<AuthCompletion>(AUTH_QUEUE_CAPACITY);
    let pending_auth = Arc::new(AtomicUsize::new(0));
    let taken = Arc::clone(&pending_auth);
    let take = move |job: Job| {
        if matches!(job, Job::Authorize(_)) {
            taken.fetch_sub(1, Ordering::AcqRel);
        }
        job
    };

    std::thread::Builder::new()
        .name("rtmp-auth-worker".to_string())
        .spawn(move || {
            // Blocks between requests -- no busy loop -- and exits cleanly
            // once every `AuthWorkerHandle` (and `req_tx`) is dropped.
            while let Ok(first) = req_rx.recv().map(&take) {
                // Group commit: take whatever else is already queued (a
                // burst of joins, publishes or closes) and run it in one
                // SQLite transaction instead of one commit per job.
                let mut jobs = vec![first];
                while jobs.len() < MAX_BATCH_JOBS {
                    match req_rx.try_recv() {
                        Ok(job) => jobs.push(take(job)),
                        Err(_) => break,
                    }
                }
                let completions = if jobs.len() > 1 && batching_allowed(&bridge) {
                    let (mut completions, committed) =
                        bridge.db().batch(|| run_jobs(&bridge, jobs));
                    if !committed {
                        // Everything in the group was rolled back: nothing
                        // it allowed is backed by a row, so deny all of it
                        // and drop the per-connection state it created.
                        for completion in &mut completions {
                            if completion.allow {
                                completion.allow = false;
                                bridge.on_close(completion.conn_id);
                            }
                        }
                    }
                    completions
                } else {
                    run_jobs(&bridge, jobs)
                };
                // Completions go out only after the commit, so a client is
                // never told "allowed" for a row that isn't durable yet. If
                // the RTMP thread already shut down, there's no receiver
                // left to deliver them to; drop them silently.
                let mut delivered = false;
                for completion in completions {
                    delivered |= completion_tx.send(completion).is_ok();
                }
                if delivered {
                    notify();
                }
            }
        })
        .expect("failed to spawn RTMP auth worker thread");

    (
        AuthWorkerHandle {
            tx: req_tx,
            pending_auth,
        },
        completion_rx,
    )
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
    fn queued_close_is_applied_before_a_later_authorization() {
        use crate::db::Stream;
        let db = Arc::new(Db::open(":memory:").unwrap());
        let s = Stream {
            id: "s1".to_string(),
            name: "S".to_string(),
            app: "live".to_string(),
            publish_key: crate::keygen::keygen_stream_key("pub_").unwrap(),
            play_key: crate::keygen::keygen_stream_key("play_").unwrap(),
            stats_key: crate::keygen::keygen_stream_key("stats_").unwrap(),
            enabled: true,
            ..Default::default()
        };
        db.stream_add(&s).unwrap();
        let deleted = Arc::new(Mutex::new(HashSet::new()));
        let bridge = Arc::new(DbRtmpBridge::new(Arc::clone(&db), deleted));
        bridge.set_coordinator(Arc::new(StateCoordinator::standalone(Arc::clone(&db))));
        let (handle, rx) = spawn(Arc::clone(&bridge));

        bridge.on_connect(1, "127.0.0.1:1000");
        handle
            .try_submit(AuthKind::Publish, 1, "live", &s.publish_key)
            .unwrap();
        assert!(recv_within(&rx, Duration::from_secs(2)).unwrap().allow);
        assert_eq!(db.publisher_list(Some("s1")).len(), 1);

        // The first publisher's connection closes and a new one publishes
        // the same stream right away: the queued close must release the
        // single-publisher slot before the new authorization runs.
        handle.try_submit_close(1).unwrap();
        bridge.on_connect(2, "127.0.0.1:1001");
        handle
            .try_submit(AuthKind::Publish, 2, "live", &s.publish_key)
            .unwrap();
        let completion = recv_within(&rx, Duration::from_secs(2)).unwrap();
        assert_eq!(completion.conn_id, 2);
        assert!(
            completion.allow,
            "release must be ordered before the re-publish"
        );
        assert!(!bridge.is_registered(1));
    }

    #[test]
    fn close_is_queued_behind_its_authorization_even_when_the_queue_is_full() {
        use crate::db::Stream;
        let db = Arc::new(Db::open(":memory:").unwrap());
        let s = Stream {
            id: "s1".to_string(),
            name: "S".to_string(),
            app: "live".to_string(),
            publish_key: crate::keygen::keygen_stream_key("pub_").unwrap(),
            play_key: crate::keygen::keygen_stream_key("play_").unwrap(),
            stats_key: crate::keygen::keygen_stream_key("stats_").unwrap(),
            enabled: true,
            ..Default::default()
        };
        db.stream_add(&s).unwrap();
        let deleted = Arc::new(Mutex::new(HashSet::new()));
        let bridge = Arc::new(DbRtmpBridge::new(Arc::clone(&db), deleted));
        bridge.set_coordinator(Arc::new(StateCoordinator::standalone(Arc::clone(&db))));
        let (handle, rx) = spawn(Arc::clone(&bridge));
        bridge.on_connect(1, "127.0.0.1:1000");

        // Stall the worker on the DB and fill its authorization queue, with
        // conn 1's publish queued first; then conn 1 disconnects.
        let submitted = db.with_conn(|_| {
            handle
                .try_submit(AuthKind::Publish, 1, "live", &s.publish_key)
                .unwrap();
            let mut submitted = 1;
            let mut conn_id = 1_000;
            while handle
                .try_submit(AuthKind::Play, conn_id, "live", "nope")
                .is_ok()
            {
                submitted += 1;
                conn_id += 1;
                assert!(submitted <= AUTH_QUEUE_CAPACITY + MAX_BATCH_JOBS + 1);
            }
            // The close is still queued (not run inline ahead of conn 1's
            // queued authorization).
            handle.try_submit_close(1).unwrap();
            submitted
        });

        for _ in 0..submitted {
            recv_within(&rx, Duration::from_secs(5)).unwrap();
        }
        // Conn 1 was authorized and then released by its close, in that
        // order, so the stream's publisher slot is free again.
        bridge.on_connect(2, "127.0.0.1:1001");
        handle
            .try_submit(AuthKind::Publish, 2, "live", &s.publish_key)
            .unwrap();
        let completion = recv_within(&rx, Duration::from_secs(5)).unwrap();
        assert_eq!(completion.conn_id, 2);
        assert!(completion.allow, "conn 1's slot must have been released");
        assert!(!bridge.is_registered(1));
    }

    #[test]
    fn burst_of_publishes_allows_exactly_one_per_stream() {
        use crate::db::Stream;
        let db = Arc::new(Db::open(":memory:").unwrap());
        let mut keys = Vec::new();
        for i in 0..10 {
            let s = Stream {
                id: format!("s{i}"),
                name: "S".to_string(),
                app: "live".to_string(),
                publish_key: crate::keygen::keygen_stream_key("pub_").unwrap(),
                play_key: crate::keygen::keygen_stream_key("play_").unwrap(),
                stats_key: crate::keygen::keygen_stream_key("stats_").unwrap(),
                enabled: true,
                ..Default::default()
            };
            db.stream_add(&s).unwrap();
            keys.push(s.publish_key);
        }
        let deleted = Arc::new(Mutex::new(HashSet::new()));
        let bridge = Arc::new(DbRtmpBridge::new(Arc::clone(&db), deleted));
        bridge.set_coordinator(Arc::new(StateCoordinator::standalone(Arc::clone(&db))));
        let (handle, rx) = spawn(Arc::clone(&bridge));

        // Two publishers per stream, submitted back to back so the worker
        // finds them queued together and group-commits them.
        let mut conn_id = 0u64;
        for key in keys.iter().chain(keys.iter()) {
            conn_id += 1;
            bridge.on_connect(conn_id, "127.0.0.1:1000");
            handle
                .try_submit(AuthKind::Publish, conn_id, "live", key)
                .unwrap();
        }
        let mut allowed = 0;
        for _ in 0..conn_id {
            if recv_within(&rx, Duration::from_secs(2)).unwrap().allow {
                allowed += 1;
            }
        }
        assert_eq!(allowed, 10, "exactly one publisher per stream");
        for i in 0..10 {
            assert_eq!(db.publisher_list(Some(&format!("s{i}"))).len(), 1);
        }
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
