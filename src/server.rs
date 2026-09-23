//! Server application lifecycle: wires together the database, the HTTP API,
//! and the RTMP listener(s), then runs until a shutdown signal arrives.

use parking_lot::Mutex;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

use crate::auth_worker::{self, AuthCompletion, AuthKind, AuthWorkerHandle};
use crate::config::ServerConfig;
use crate::db::Db;
use crate::http::{self, AppState};
use crate::media_output::{MediaOutputConfig, MediaOutputManager};
use crate::rtmp_bridge::{DbRtmpBridge, FrameInfo, FrameKind, RtmpEventHandler};
use crate::state::StateCoordinator;
use librtmp2::types::AuthorizationResult;

/// RTMP publish/play callbacks are plain function pointers; the bridge is
/// registered on the RTMP thread before the poll loop starts.
pub(crate) static RTMP_BRIDGE: StdMutex<Option<Arc<DbRtmpBridge>>> = StdMutex::new(None);
/// Submission handle for the dedicated publish/play authorization worker
/// (see `auth_worker`). Set alongside `RTMP_BRIDGE` before the poll loop
/// starts; the `publish`/`play` callbacks below use it to move SQLite/
/// cluster authorization work off the RTMP thread.
pub(crate) static AUTH_WORKER: StdMutex<Option<AuthWorkerHandle>> = StdMutex::new(None);
/// Completions the auth worker has finished but the RTMP poll loop hasn't
/// yet applied via `Server::complete_publish_authorization`/
/// `complete_play_authorization`. Drained once per poll tick in
/// [`drain_auth_completions`].
pub(crate) static AUTH_COMPLETIONS_RX: StdMutex<Option<std::sync::mpsc::Receiver<AuthCompletion>>> =
    StdMutex::new(None);
static PUBLISH_GENERATIONS: LazyLock<StdMutex<HashMap<u64, u64>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn bump_publish_generation(conn_id: u64) -> u64 {
    let mut generations = PUBLISH_GENERATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let generation = generations.entry(conn_id).or_insert(0);
    *generation = generation.saturating_add(1);
    *generation
}

fn publisher_generation(conn_id: u64) -> u64 {
    PUBLISH_GENERATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&conn_id)
        .copied()
        .unwrap_or(0)
}

fn clear_publish_generation(conn_id: u64) {
    PUBLISH_GENERATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&conn_id);
}

thread_local! {
    static RTMP_POLL_SERVER: Cell<Option<*mut librtmp2::server::Server>> = const {
        Cell::new(None)
    };
}

thread_local! {
    /// conn_ids whose callback ran during the current `server.poll()`. A
    /// connection that authorizes publish/play and is then reaped by the
    /// library in the same poll never enters `tracked`, so the poll loop uses
    /// this to close its bridge state instead of leaking an active row.
    static RTMP_POLL_TOUCHED_CONNS: RefCell<HashSet<u64>> = RefCell::new(HashSet::new());
}

/// Pin the active RTMP server for the duration of `poll()` so publish/play
/// callbacks can resolve `remote_addr` before auth rate limiting.
pub(crate) fn set_rtmp_poll_server(server: *mut librtmp2::server::Server) {
    RTMP_POLL_SERVER.with(|cell| cell.set(Some(server)));
}

pub(crate) fn clear_rtmp_poll_server() {
    RTMP_POLL_SERVER.with(|cell| cell.set(None));
}

/// How often the poll loop wakes up to service the RTMP/RTMPS listener(s)
/// once every tracked connection has reached a steady publish/play state.
pub(crate) const POLL_INTERVAL_MS: u64 = 50;

/// Poll interval used instead of `POLL_INTERVAL_MS` while at least one
/// tracked connection is still negotiating (handshake / connect /
/// createStream / publish|play command, including a publish|play command
/// still waiting on the async auth worker) rather than actively publishing
/// or playing, and for one tick right after the async auth worker resolves
/// a publish/play authorization (see `just_authorized` in the poll loop).
/// Handshake and stream-join round trips each wait for the next poll tick
/// before the server's reply goes out, so the fixed 50ms interval alone
/// adds up to tens of milliseconds of avoidable latency per step; polling
/// faster only during this comparatively brief, comparatively rare window
/// keeps that cost low without paying the CPU cost of fast-polling
/// steady-state connections that no longer need it.
pub(crate) const POLL_INTERVAL_FAST_MS: u64 = 1;

/// Block until a listener or tracked connection socket becomes readable, or
/// `timeout_ms` elapses -- whichever comes first.
///
/// This replaces an unconditional `sleep(timeout_ms)` at the end of the poll
/// loop. RTMP's handshake and command exchange (C0/C1/C2, connect,
/// createStream, publish/play) is a long chain of small, sequential round
/// trips, each of which waits for the next poll tick before the server's
/// reply goes out -- a fixed sleep alone adds up to `timeout_ms` of pure
/// waiting to *every one* of those steps, even when the peer's next byte is
/// already sitting in the socket buffer. `poll(2)`'s timeout is still
/// `timeout_ms`, so this can only shorten a tick, never lengthen it -- an
/// async auth completion that arrives via the auth worker's channel with no
/// matching socket activity is still picked up within the same
/// `timeout_ms` bound as before, on the next tick that the timeout (rather
/// than a socket) wakes.
///
/// Best-effort: a peer mid-TLS-handshake (tracked separately by librtmp2,
/// not yet promoted to a `connections` entry) isn't included in the fd set,
/// so a stalled TLS peer with no other socket activity still waits out the
/// full timeout -- no worse than the sleep this replaces, just not sped up
/// by it.
fn wait_for_readiness_or_timeout(server: &librtmp2::server::Server, timeout_ms: u64) {
    let mut fds: Vec<libc::pollfd> = server
        .listener_fds()
        .into_iter()
        .chain(
            server
                .connections
                .iter()
                .map(|conn| conn.client_fd)
                .filter(|&fd| fd >= 0),
        )
        .map(|fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();

    let timeout = timeout_ms.min(i32::MAX as u64) as i32;
    loop {
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
        if rc >= 0 {
            return;
        }
        if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            // Unexpected poll() failure -- e.g. a connection closed and its
            // fd was reused between fd collection and this call. Fall back
            // to the plain sleep this function replaces rather than
            // spinning on a busy error.
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms));
            return;
        }
        // EINTR: a signal interrupted the wait. `poll`'s timeout is
        // relative, so simply retrying restarts the full timeout rather
        // than preserving a deadline -- acceptable for this loop's
        // existing best-effort latency bound (worst case: one timeout
        // window longer under signal pressure, which is rare).
    }
}

/// Normalize a bind string so passing it to librtmp2 cannot fall back to the
/// RTMP library default port. In particular, RTMPS host-only binds such as
/// `0.0.0.0`, `::1`, or `[::1]` must be listened on 1936, not librtmp2's
/// generic RTMP default of 1935.
fn bind_with_default_port(bind: &str, default_port: u16) -> String {
    let bind = bind.trim();

    if let Some(bracket_end) = bind.rfind(']') {
        let suffix = &bind[bracket_end + 1..];
        if suffix
            .strip_prefix(':')
            .and_then(|port| port.parse::<u16>().ok())
            .is_some()
        {
            return bind.to_string();
        }
        return format!("{}:{default_port}", &bind[..=bracket_end]);
    }

    let colon_count = bind.chars().filter(|&c| c == ':').count();
    match colon_count {
        0 => format!("{bind}:{default_port}"),
        1 => match bind.rsplit_once(':') {
            Some((host, port)) if port.parse::<u16>().is_ok() => format!("{host}:{port}"),
            Some((host, _)) => format!("{host}:{default_port}"),
            None => format!("{bind}:{default_port}"),
        },
        _ => format!("[{bind}]:{default_port}"),
    }
}

fn with_rtmp_bridge<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&DbRtmpBridge) -> R,
{
    match RTMP_BRIDGE.lock() {
        Ok(guard) => guard.as_ref().map(|bridge| f(bridge.as_ref())),
        Err(e) => {
            crate::log_error!("RTMP_BRIDGE lock poisoned; rejecting RTMP callback: {e}");
            None
        }
    }
}

/// Register the client IP on the bridge before publish/play auth runs. During
/// `server.poll()` the publish/play callbacks can fire before
/// `process_server_connections` reaches `on_connect`, which would otherwise
/// skip per-IP auth-failure tracking and rate limiting.
fn ensure_conn_registered_for_auth(conn_id: u64) {
    RTMP_POLL_TOUCHED_CONNS.with(|set| {
        set.borrow_mut().insert(conn_id);
    });
    // Skip the pointer walk entirely once the normal `on_connect` pass (or an
    // earlier call from this same function) has already registered the
    // remote IP. Without this check every publish/play attempt on an
    // already-registered connection re-ran `on_connect` and its "new
    // connection" log line, which was both misleading and needless lock
    // contention on the hot path.
    if with_rtmp_bridge(|bridge| bridge.is_registered(conn_id)).unwrap_or(true) {
        return;
    }
    RTMP_POLL_SERVER.with(|cell| {
        let Some(server_ptr) = cell.get() else {
            return;
        };
        if server_ptr.is_null() {
            return;
        }
        // SAFETY: `RTMP_POLL_SERVER` is set only on the RTMP thread for the
        // duration of `server.poll()`, which exclusively owns `server`.
        let server = unsafe { &*server_ptr };
        let Some(conn) = server
            .connections
            .iter()
            .find(|c| c.conn_id == conn_id && c.client_fd >= 0)
        else {
            return;
        };
        with_rtmp_bridge(|bridge| bridge.on_connect(conn_id, &conn.remote_addr));
    });
}

/// Submits publish/play authorization work to the dedicated auth worker
/// (see `auth_worker`) instead of running `DbRtmpBridge::authorize_publish`/
/// `authorize_play` -- blocking SQLite, and cluster ownership acquisition
/// for publish -- directly on the RTMP thread. Fails closed (`Deny`) if the
/// worker is unavailable or its queue is full.
fn submit_auth(kind: AuthKind, conn_id: u64, app: &str, stream_key: &str) -> AuthorizationResult {
    ensure_conn_registered_for_auth(conn_id);
    let submitted = AUTH_WORKER.lock().ok().and_then(|guard| {
        guard
            .as_ref()
            .map(|h| h.try_submit(kind, conn_id, app, stream_key))
    });
    match submitted {
        Some(Ok(())) => AuthorizationResult::Pending,
        _ => AuthorizationResult::Deny,
    }
}

pub(crate) fn rtmp_publish_auth_cb(
    conn_id: u64,
    app: &str,
    stream_key: &str,
) -> AuthorizationResult {
    submit_auth(AuthKind::Publish, conn_id, app, stream_key)
}

pub(crate) fn rtmp_play_auth_cb(conn_id: u64, app: &str, play_key: &str) -> AuthorizationResult {
    submit_auth(AuthKind::Play, conn_id, app, play_key)
}

/// Applies every publish/play authorization the dedicated worker thread has
/// finished since the last call. Called once at the start of every
/// [`process_server_connections`] pass (production and test loops alike) so
/// a `Pending` result the worker resolved between poll ticks turns into the
/// connection's actual publish/play state -- and its `onStatus` reply -- as
/// soon as possible, without ever blocking this thread on the worker.
///
/// Returns whether at least one completion was applied, so the caller can
/// poll again immediately (rather than take the full idle sleep) and pick
/// up whatever the client sends right after receiving that `onStatus` reply.
fn drain_auth_completions(server: &mut librtmp2::server::Server) -> bool {
    let Ok(guard) = AUTH_COMPLETIONS_RX.lock() else {
        return false;
    };
    let Some(rx) = guard.as_ref() else {
        return false;
    };
    let mut any_completed = false;
    while let Ok(completion) = rx.try_recv() {
        any_completed = true;
        match completion.kind {
            AuthKind::Publish => {
                if completion.allow {
                    bump_publish_generation(completion.conn_id);
                }
                let _ = server.complete_publish_authorization(completion.conn_id, completion.allow);
            }
            AuthKind::Play => {
                let _ = server.complete_play_authorization(completion.conn_id, completion.allow);
            }
        }
    }
    any_completed
}

pub(crate) fn rtmp_media_cb(
    conn_id: u64,
    frame_type: librtmp2::types::FrameType,
    codec: Option<&str>,
) -> bool {
    ensure_conn_registered_for_auth(conn_id);
    with_rtmp_bridge(|bridge| {
        let kind = match frame_type {
            librtmp2::types::FrameType::Video => FrameKind::Video,
            librtmp2::types::FrameType::Audio => FrameKind::Audio,
            _ => return true,
        };
        let frame = FrameInfo {
            kind,
            timestamp: 0,
            size: 0,
            codec: codec.unwrap_or("").to_string(),
        };
        bridge.on_frame(conn_id, &frame)
    })
    .unwrap_or(false)
}

/// Per-connection bookkeeping the RTMP poll loop keeps for the lifetime of
/// each connection.
#[derive(Default)]
pub(crate) struct TrackedConn {
    connected: bool,
    publishing: bool,
    playing: bool,
    /// When this connection was first observed (used for pre-auth idle eviction).
    first_seen_at: Option<Instant>,
    /// DB stream id, set after publish/play is fully enabled.
    stream_id: String,
    /// Last detected video codec string from the protocol layer.
    video_codec: String,
    /// Last detected audio codec string from the protocol layer.
    audio_codec: String,
}

/// Stream id used for delete kicks and `deleted_streams` retention. Prefer the
/// bridge (authoritative for live publisher/player rows); fall back to the
/// poll-loop tracker when the bridge has not been synced yet this tick.
#[cfg(test)]
fn eviction_stream_id(rtmp_bridge: &DbRtmpBridge, conn_id: u64, entry: &TrackedConn) -> String {
    let bridge_sid = rtmp_bridge.stream_id_for_conn(conn_id);
    if !bridge_sid.is_empty() {
        return bridge_sid;
    }
    entry.stream_id.clone()
}

pub(crate) fn live_stream_ids_for_deleted_markers(
    tracked: &HashMap<u64, TrackedConn>,
    rtmp_bridge: &DbRtmpBridge,
) -> HashSet<String> {
    let mut live = HashSet::new();
    for (&conn_id, entry) in tracked {
        let bridge_ids = rtmp_bridge.stream_ids_for_conn(conn_id);
        if bridge_ids.is_empty() {
            if !entry.stream_id.is_empty() {
                live.insert(entry.stream_id.clone());
            }
        } else {
            for sid in bridge_ids {
                if !sid.is_empty() {
                    live.insert(sid);
                }
            }
        }
    }
    live
}

/// Drop bridge/library roles whose stream was deleted. Returns `true` when the
/// TCP connection must be kicked (no surviving authorized role).
///
/// Dual-role connections (publish A + play B) only lose the role whose stream
/// was deleted — the other role keeps the socket alive.
fn drain_deleted_stream_roles(
    conn: &mut librtmp2::session::conn::Conn,
    entry: &mut TrackedConn,
    rtmp_bridge: &DbRtmpBridge,
    conn_id: u64,
    deleted_now: &HashSet<String>,
) -> bool {
    let pub_sid = rtmp_bridge.publisher_stream_id_for_conn(conn_id);
    let play_sid = rtmp_bridge.player_stream_id_for_conn(conn_id);
    let pub_hit = pub_sid
        .as_ref()
        .is_some_and(|sid| deleted_now.contains(sid));
    let play_hit = play_sid
        .as_ref()
        .is_some_and(|sid| deleted_now.contains(sid));

    if !pub_hit && !play_hit {
        // Pre-auth / unsynced tracker: fall back to TrackedConn stream id.
        if rtmp_bridge.stream_ids_for_conn(conn_id).is_empty()
            && !entry.stream_id.is_empty()
            && deleted_now.contains(&entry.stream_id)
        {
            crate::log_info!(
                "RTMP: kicking conn={conn_id} from {} — stream '{}' was deleted",
                conn.remote_addr,
                entry.stream_id
            );
            return true;
        }
        return false;
    }

    if pub_hit {
        let deleted_id = pub_sid.as_deref().unwrap_or("");
        crate::log_info!(
            "RTMP: releasing publisher on conn={conn_id} from {} — stream '{deleted_id}' was deleted",
            conn.remote_addr
        );
        rtmp_bridge.release_publisher(conn_id);
        if !rtmp_bridge.has_publisher(conn_id) {
            entry.publishing = false;
            entry.video_codec.clear();
            entry.audio_codec.clear();
            if let Some(stream) = conn.current_stream.as_mut() {
                stream.is_publishing = false;
            }
        }
    }

    if play_hit {
        let deleted_id = play_sid.as_deref().unwrap_or("");
        crate::log_info!(
            "RTMP: releasing player on conn={conn_id} from {} — stream '{deleted_id}' was deleted",
            conn.remote_addr
        );
        rtmp_bridge.release_player(conn_id);
        if !rtmp_bridge.has_player(conn_id) {
            entry.playing = false;
            if let Some(stream) = conn.current_stream.as_mut() {
                stream.is_playing = false;
            }
        }
    }

    let has_pub = rtmp_bridge.has_publisher(conn_id);
    let has_play = rtmp_bridge.has_player(conn_id);
    if has_pub || has_play {
        let sid = rtmp_bridge.stream_id_for_conn(conn_id);
        entry.stream_id = sid.clone();
        // Drop queued frames from the deleted role so they cannot drain onto
        // the surviving publisher/player route after relay_key is rewritten.
        conn.pending_relay.clear();
        // If DB deactivation failed, stream_id_for_conn may still be the
        // deleted stream — never enable relay on a stream being deleted.
        if !sid.is_empty() && !deleted_now.contains(&sid) {
            conn.relay_key = sid;
            conn.relay_enabled = true;
        } else {
            conn.relay_key.clear();
            conn.relay_enabled = false;
        }
        return false;
    }

    let kicked_id = pub_sid
        .filter(|_| pub_hit)
        .or_else(|| play_sid.filter(|_| play_hit))
        .unwrap_or_else(|| entry.stream_id.clone());
    crate::log_info!(
        "RTMP: kicking conn={conn_id} from {} — stream '{kicked_id}' was deleted",
        conn.remote_addr
    );
    true
}

/// Returns true when a connection has no authorized publish/play session and
/// has exceeded the configured pre-auth idle window.
fn should_evict_idle_conn(
    entry: &TrackedConn,
    has_authorized_session: bool,
    now: Instant,
    idle_timeout: Duration,
) -> bool {
    if entry.publishing || entry.playing || has_authorized_session {
        return false;
    }
    entry
        .first_seen_at
        .is_some_and(|at| now.duration_since(at) >= idle_timeout)
}

/// Drive one poll cycle's worth of connection bookkeeping: authorize new
/// publish/play commands, reject connections the bridge doesn't own, kick
/// connections whose stream/play-key was revoked, and flush stats. Returns
/// the conn_ids seen this cycle so the caller can detect connections that
/// disappeared entirely (closed by the peer, rather than rejected here).
///
/// `server` is a single `librtmp2::server::Server` that may have multiple
/// listeners bound (plaintext RTMP and, when TLS is enabled, RTMPS) — they
/// share one `connections` list, so a publisher on one listener is relayed
/// to players on any other listener by the library itself; this function
/// doesn't need to know which listener a given connection came in on.
pub(crate) fn process_server_connections(
    server: &mut librtmp2::server::Server,
    tracked: &mut HashMap<u64, TrackedConn>,
    rtmp_bridge: &Arc<DbRtmpBridge>,
    deleted_now: &HashSet<String>,
    revoked_now: &HashSet<String>,
    idle_timeout: Duration,
) -> (HashSet<u64>, bool) {
    let just_authorized = drain_auth_completions(server);

    let mut current_ids = HashSet::new();
    let mut reject_indices = Vec::new();

    for (idx, conn) in server.connections.iter_mut().enumerate() {
        if conn.client_fd < 0 {
            continue;
        }
        let conn_id = conn.conn_id;
        current_ids.insert(conn_id);
        let entry = tracked.entry(conn_id).or_default();
        if !entry.connected {
            // A publish/play callback may have already run `on_connect` via
            // `ensure_conn_registered_for_auth` earlier this same poll tick;
            // skip the redundant call so the connection isn't logged twice.
            if !rtmp_bridge.is_registered(conn_id) {
                rtmp_bridge.on_connect(conn_id, &conn.remote_addr);
            }
            entry.connected = true;
            entry.first_seen_at = Some(Instant::now());
        } else if entry.first_seen_at.is_none() {
            entry.first_seen_at = Some(Instant::now());
        }

        let has_authorized_session =
            rtmp_bridge.has_publisher(conn_id) || rtmp_bridge.has_player(conn_id);
        if should_evict_idle_conn(entry, has_authorized_session, Instant::now(), idle_timeout) {
            crate::log_info!(
                "RTMP: closing idle conn={conn_id} from {} (no publish/play within {}s)",
                conn.remote_addr,
                idle_timeout.as_secs()
            );
            reject_indices.push(idx);
            continue;
        }

        if rtmp_bridge.take_pending_force_close(conn_id) {
            crate::log_info!(
                "RTMP: force-closing conn={conn_id} from {} after delete-drain timeout",
                conn.remote_addr
            );
            reject_indices.push(idx);
            continue;
        }

        let Some(stream) = conn.current_stream.as_ref() else {
            continue;
        };
        let is_publishing = stream.is_publishing;
        let is_playing = stream.is_playing;

        // Tear down bridge roles when the RTMP session drops publish/play
        // without closing TCP (FCUnpublish / closeStream / role switch).
        if entry.publishing && !is_publishing {
            rtmp_bridge.release_publisher(conn_id);
            // If the DB deactivation failed, release_publisher keeps the
            // row in ConnState for a retry on close -- keep tracking this
            // connection as the active publisher too, so idle eviction
            // doesn't reclaim it while the still-active row blocks others.
            if !rtmp_bridge.has_publisher(conn_id) {
                entry.publishing = false;
                // A future publish session on this connection should start
                // codec detection fresh rather than reporting the just-ended
                // stream's codecs until new detection overwrites them.
                entry.video_codec.clear();
                entry.audio_codec.clear();
                if !is_playing {
                    entry.stream_id.clear();
                    conn.relay_key.clear();
                    conn.relay_enabled = false;
                    conn.pending_relay.clear();
                    // No role survives this teardown -- restart the idle-eviction
                    // window so a client that FCUnpublish'd intending to
                    // republish shortly isn't judged against a first_seen_at
                    // from the original (possibly long-past) TCP connect.
                    entry.first_seen_at = Some(Instant::now());
                } else {
                    let sid = rtmp_bridge.stream_id_for_conn(conn_id);
                    entry.stream_id = sid.clone();
                    conn.relay_key = sid;
                    conn.relay_enabled = true;
                }
            }
        }
        if entry.playing && !is_playing {
            rtmp_bridge.release_player(conn_id);
            if !rtmp_bridge.has_player(conn_id) {
                entry.playing = false;
                if !is_publishing {
                    entry.stream_id.clear();
                    conn.relay_key.clear();
                    conn.relay_enabled = false;
                    conn.pending_relay.clear();
                    entry.first_seen_at = Some(Instant::now());
                } else {
                    let sid = rtmp_bridge.stream_id_for_conn(conn_id);
                    entry.stream_id = sid.clone();
                    conn.relay_key = sid;
                    conn.relay_enabled = true;
                }
            }
        }

        if is_publishing && !entry.publishing {
            if !rtmp_bridge.has_publisher(conn_id) {
                crate::log_warn!(
                    "RTMP: closing unauthorized publisher conn={conn_id} from {} app='{}' key=<redacted>",
                    conn.remote_addr,
                    conn.app
                );
                reject_indices.push(idx);
                continue;
            }
            let stream_id = rtmp_bridge.stream_id_for_conn(conn_id);
            crate::log_info!(
                "RTMP: publisher connected from {} stream='{stream_id}'",
                conn.remote_addr
            );
            entry.publishing = true;
            entry.stream_id = stream_id;
            conn.relay_key = entry.stream_id.clone();
            conn.relay_enabled = true;
        } else if is_publishing && entry.publishing {
            // Same-connection stream switch: bridge already moved the
            // publisher row; keep relay_key / kick targets in sync.
            let sid = rtmp_bridge.stream_id_for_conn(conn_id);
            if !sid.is_empty() && sid != entry.stream_id {
                entry.stream_id = sid.clone();
                conn.relay_key = sid;
            }
        }

        if is_playing && !entry.playing {
            if !rtmp_bridge.has_player(conn_id) {
                crate::log_warn!(
                    "RTMP: closing unauthorized player conn={conn_id} from {} app='{}' key=<redacted>",
                    conn.remote_addr,
                    conn.app
                );
                reject_indices.push(idx);
                continue;
            }
            let stream_id = rtmp_bridge.stream_id_for_conn(conn_id);
            crate::log_info!(
                "RTMP: player connected from {} stream='{stream_id}'",
                conn.remote_addr
            );
            entry.playing = true;
            entry.stream_id = stream_id;
            conn.relay_key = entry.stream_id.clone();
            conn.relay_enabled = true;
        } else if is_playing && entry.playing {
            let sid = rtmp_bridge.stream_id_for_conn(conn_id);
            if !sid.is_empty() && sid != entry.stream_id {
                entry.stream_id = sid.clone();
                conn.relay_key = sid;
            }
        }

        // Tear down roles whose stream was deleted. Dual-role conns keep the
        // surviving role; kick only when nothing authorized remains.
        if drain_deleted_stream_roles(conn, entry, rtmp_bridge, conn_id, deleted_now) {
            reject_indices.push(idx);
            continue;
        }

        let viewer_id = rtmp_bridge.viewer_id_for_conn(conn_id);
        if !viewer_id.is_empty() && revoked_now.contains(&viewer_id) {
            crate::log_info!(
                "RTMP: kicking conn={conn_id} from {} — play key '{viewer_id}' was revoked",
                conn.remote_addr
            );
            reject_indices.push(idx);
            continue;
        }

        // Publisher stats: media bytes only (excludes RTMP control overhead).
        if is_publishing {
            let new_video = conn
                .detected_video_codec
                .as_deref()
                .unwrap_or("")
                .to_string();
            let new_audio = conn
                .detected_audio_codec
                .as_deref()
                .unwrap_or("")
                .to_string();

            if !new_video.is_empty() && new_video != entry.video_codec {
                entry.video_codec = new_video;
            }
            if !new_audio.is_empty() && new_audio != entry.audio_codec {
                entry.audio_codec = new_audio;
            }

            rtmp_bridge.update_publisher_stats(
                conn_id,
                conn.media_bytes_received,
                &entry.video_codec,
                &entry.audio_codec,
                crate::rtmp_bridge::PublisherStreamMetadata {
                    video_width: conn.detected_video_width,
                    video_height: conn.detected_video_height,
                    framerate: conn.detected_video_framerate,
                    audio_sample_rate: conn.detected_audio_sample_rate,
                    audio_channels: conn.detected_audio_channels,
                },
            );
        }

        if is_playing {
            rtmp_bridge.update_player_stats(conn_id, conn.media_bytes_sent);
        }
    }

    for conn in server.connections.iter() {
        if conn.client_fd < 0 {
            continue;
        }
        rtmp_bridge.update_rtt(conn.conn_id, conn.rtt_ms);
    }

    reject_indices.sort_unstable();
    reject_indices.dedup();
    for idx in reject_indices.into_iter().rev() {
        if let Some(conn) = server.connections.get_mut(idx)
            && conn.client_fd >= 0
        {
            let conn_id = conn.conn_id;
            conn.relay_enabled = false;
            conn.relay_key.clear();
            conn.pending_relay.clear();
            tracked.remove(&conn_id);
            rtmp_bridge.on_close(conn_id);
            clear_publish_generation(conn_id);
        }
        server.connections.remove(idx);
    }

    (current_ids, just_authorized)
}

fn is_valid_env_api_token(token: &str) -> bool {
    let token = token.trim();
    token.len() >= 32
        && token.len() <= 256
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
}

fn mask_api_token(token: &str) -> String {
    if token.len() <= 12 {
        return "***".to_string();
    }
    format!("{}...{}", &token[..8], &token[token.len() - 4..])
}

/// Finish any stream deletes that were left half-done (`pending_delete=1`,
/// row still present) by a prior process that crashed or was redeployed
/// mid-delete — see `handle_stream_delete`'s async `202` path in `http.rs`.
/// A fresh process start has no surviving RTMP sessions from before, so it's
/// always safe to finalize these immediately rather than leave them disabled
/// forever. Deliberately keyed on `pending_delete`, not `enabled=0` — a
/// stream can be administratively disabled without being deleted.
fn recover_pending_stream_deletes(db: &Db) {
    for id in db.stream_ids_pending_delete() {
        match db.stream_delete(&id) {
            Some(true) => {
                crate::log_warn!("Recovered abandoned delete for stream '{id}' from a prior run");
            }
            Some(false) => {}
            None => {
                crate::log_error!("Failed to recover abandoned delete for stream '{id}'");
            }
        }
    }
}

#[cfg(feature = "cluster")]
fn recover_pending_stream_deletes_via_coordinator(coordinator: &StateCoordinator) {
    for id in coordinator.db().stream_ids_pending_delete() {
        match coordinator.finalize_delete_stream(&id) {
            Ok(()) => {
                crate::log_warn!(
                    "Recovered abandoned delete for stream '{id}' via Raft from a prior run"
                );
            }
            Err(crate::state::CoordError::NotFound) => {
                // Already gone on leader / concurrent finalize.
            }
            Err(e) => {
                crate::log_error!(
                    "Failed to recover abandoned delete for stream '{id}' via Raft: {e:?}"
                );
            }
        }
    }
}

/// Load the API bearer token from the database, seeding it from `LRTMP2_API_TOKEN`
/// or generating a new value on first startup.
fn resolve_api_token(db: &Db, db_path: &str) -> Result<String, String> {
    if let Some(stored) = db.token_get()? {
        if let Ok(env_token) = std::env::var("LRTMP2_API_TOKEN") {
            let env_token = env_token.trim();
            if !env_token.is_empty() && env_token != stored {
                crate::log_warn!(
                    "LRTMP2_API_TOKEN env differs from database value; using database token ({})",
                    mask_api_token(&stored)
                );
            }
        }
        return Ok(stored);
    }

    if let Ok(env_token) = std::env::var("LRTMP2_API_TOKEN") {
        let env_token = env_token.trim();
        if !env_token.is_empty() {
            if !is_valid_env_api_token(env_token) {
                return Err(
                    "LRTMP2_API_TOKEN must be 32-256 ASCII alphanumeric characters, '-' or '_'"
                        .into(),
                );
            }
            if db.token_set(env_token)? {
                crate::log_info!(
                    "API token loaded from LRTMP2_API_TOKEN (stored in database {db_path})"
                );
            }
            return db
                .token_get()?
                .ok_or_else(|| "API token missing after env seed".to_string());
        }
    }

    let candidate = crate::keygen::keygen_api_token()?;
    if db.token_set(&candidate)? {
        eprintln!(
            "============================================================\n\
             Generated API token (stored in database {db_path}):\n\
             {}\n\
             Set LRTMP2_API_TOKEN in the panel .env to this value.\n\
             ============================================================",
            candidate
        );
        Ok(candidate)
    } else {
        db.token_get()?
            .ok_or_else(|| "API token missing after concurrent insert".to_string())
    }
}

pub struct ServerApp {
    config: ServerConfig,
    db: Arc<Db>,
    coordinator: Arc<StateCoordinator>,
    rtmp_bridge: Arc<DbRtmpBridge>,
    /// Stream IDs deleted via HTTP while connections are live. The RTMP poll
    /// loop reads this set and kicks any connection whose stream_id appears.
    deleted_streams: Arc<Mutex<HashSet<String>>>,
    /// HTTP delete markers that must survive live-session pruning (see
    /// [`crate::http::AppState::sticky_deleted_streams`]).
    sticky_deleted_streams: Arc<Mutex<HashSet<String>>>,
    /// Viewer slot IDs revoked via HTTP while player connections are live.
    revoked_viewers: Arc<Mutex<HashSet<String>>>,
}

impl ServerApp {
    /// Opens the database, loads or auto-generates the API token, and wires
    /// together all server components. Returns an error if the database cannot
    /// be opened or the token cannot be persisted.
    pub fn create(config: ServerConfig) -> Result<ServerApp, String> {
        let db_path = std::env::var("LRTMP2_DB")
            .or_else(|_| std::env::var("LRTMP2_DB_PATH"))
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or("LRTMP2_DB or LRTMP2_DB_PATH environment variable must be set to the SQLite database path")?;

        Self::bootstrap(config, &db_path)
    }

    pub(crate) fn bootstrap(mut config: ServerConfig, db_path: &str) -> Result<ServerApp, String> {
        let db = Arc::new(
            Db::open(db_path).map_err(|e| format!("Failed to open database {db_path}: {e}"))?,
        );

        config.api_token = resolve_api_token(&db, db_path)?;
        // Standalone: finalize abandoned deletes locally. Cluster mode must not
        // mutate SQLite here — recovery runs via Raft after ClusterManager starts.
        #[cfg(feature = "cluster")]
        if !config.cluster.enabled {
            recover_pending_stream_deletes(&db);
        }
        #[cfg(not(feature = "cluster"))]
        recover_pending_stream_deletes(&db);

        let deleted_streams = Arc::new(Mutex::new(HashSet::new()));
        let sticky_deleted_streams = Arc::new(Mutex::new(HashSet::new()));
        let revoked_viewers = Arc::new(Mutex::new(HashSet::new()));

        let coordinator = Arc::new(StateCoordinator::standalone(Arc::clone(&db)));

        let rtmp_bridge = Arc::new(DbRtmpBridge::new(
            Arc::clone(&db),
            Arc::clone(&deleted_streams),
        ));
        rtmp_bridge.set_coordinator(Arc::clone(&coordinator));

        Ok(ServerApp {
            config,
            db,
            coordinator,
            rtmp_bridge,
            deleted_streams,
            sticky_deleted_streams,
            revoked_viewers,
        })
    }

    /// Runs until SIGINT/SIGTERM. Blocks the calling task.
    pub async fn run(&self) -> Result<(), String> {
        crate::log_info!("OpenRTMP librtmp2-server alpha starting...");

        #[cfg(feature = "cluster")]
        let coordinator = {
            if self.config.cluster.enabled {
                let mgr = crate::cluster::ClusterManager::start(
                    self.config.cluster.clone(),
                    Arc::clone(&self.db),
                    tokio::runtime::Handle::current(),
                )
                .await?;
                let coord = Arc::new(StateCoordinator::cluster(mgr));
                self.rtmp_bridge.set_coordinator(Arc::clone(&coord));
                // Pending deletes must go through Raft so all replicas converge.
                recover_pending_stream_deletes_via_coordinator(&coord);
                coord
            } else {
                Arc::clone(&self.coordinator)
            }
        };
        #[cfg(not(feature = "cluster"))]
        let coordinator = Arc::clone(&self.coordinator);

        // After cluster join/snapshot, prefer the replicated API token.
        let mut live_token = self.config.api_token.clone();
        if let Ok(Some(t)) = self.db.token_get() {
            live_token = t;
        }
        let api_token = Arc::new(parking_lot::RwLock::new(live_token));

        #[cfg(feature = "cluster")]
        if let Some(mgr) = coordinator.cluster_manager() {
            let deleted = Arc::clone(&self.deleted_streams);
            let revoked = Arc::clone(&self.revoked_viewers);
            let token = Arc::clone(&api_token);
            let bridge = Arc::clone(&self.rtmp_bridge);
            let bridge_fp = Arc::clone(&bridge);
            let bridge_sessions = Arc::clone(&bridge);
            mgr.register_session_hooks(crate::cluster::SessionHooks {
                deleted_streams: deleted,
                revoked_viewers: revoked,
                api_token: token,
                force_unpublish_stream: Arc::new(move |stream_id: &str| {
                    bridge_fp.force_unpublish_stream(stream_id);
                }),
                local_stream_sessions: Arc::new(move |sid: &str| {
                    bridge_sessions.live_conn_count_for_stream(sid) as u64
                }),
            });
        }

        if self.config.tls_enabled {
            if self.config.tls_cert_file.is_empty() || self.config.tls_key_file.is_empty() {
                return Err("TLS enabled but tls.cert_file / tls.key_file not configured".into());
            }
            crate::log_info!(
                "RTMPS enabled (cert={}) — RTMP and RTMPS will both accept connections",
                self.config.tls_cert_file
            );
        } else {
            crate::log_info!("RTMPS disabled (plaintext RTMP only)");
        }

        let media_output_config = MediaOutputConfig::load(&self.config.config_file);
        if media_output_config.enabled() {
            crate::log_info!(
                "Media outputs enabled — recording={} hls={} push_targets={} exec={}",
                media_output_config.recording_enabled,
                media_output_config.hls_enabled,
                media_output_config.push_targets.len(),
                !media_output_config.exec_publish.is_empty()
                    || !media_output_config.exec_publish_done.is_empty()
            );
        }

        let state = Arc::new(AppState {
            db: Arc::clone(&self.db),
            config: self.config.clone(),
            api_token,
            rtmp_bridge: Arc::clone(&self.rtmp_bridge),
            coordinator: Arc::clone(&coordinator),
            deleted_streams: Arc::clone(&self.deleted_streams),
            sticky_deleted_streams: Arc::clone(&self.sticky_deleted_streams),
            revoked_viewers: Arc::clone(&self.revoked_viewers),
        });
        let mut app = http::router(Arc::clone(&state));
        if media_output_config.hls_enabled {
            let hls_limiter = crate::rate_limit::RateLimiter::new(
                self.config.http_rate_limit_config(),
                self.config.http_trusted_proxies.clone(),
                Arc::clone(&state.api_token),
            );
            #[cfg(feature = "cluster")]
            let remote_viewer_sessions = {
                let coordinator = Arc::clone(&coordinator);
                Some(Arc::new(move |viewer_id: &str| {
                    coordinator
                        .cluster_manager()
                        .map(|mgr| mgr.remote_viewer_session_count_cached(viewer_id))
                        .unwrap_or(0)
                })
                    as crate::media_output::ViewerRemoteSessionCountFn)
            };
            #[cfg(not(feature = "cluster"))]
            let remote_viewer_sessions = None;
            let hls_app = crate::media_output::hls_router(
                media_output_config.hls_path.clone(),
                Arc::clone(&self.db),
                media_output_config.hls_require_key,
                media_output_config.hls_time_secs,
                self.config.http_trusted_proxies.clone(),
                remote_viewer_sessions,
            )
            .layer(axum::middleware::from_fn_with_state(
                hls_limiter,
                crate::rate_limit::middleware,
            ));
            app = app.merge(hls_app);
            crate::log_info!(
                "HLS HTTP enabled at /hls/<stream_id>/index.m3u8 (play-key auth={})",
                media_output_config.hls_require_key
            );
        }

        let http_listener = TcpListener::bind(&self.config.http_bind)
            .await
            .map_err(|e| format!("Failed to bind HTTP on {}: {e}", self.config.http_bind))?;
        crate::log_info!("HTTP listening on {}", self.config.http_bind);

        // Start the RTMP listener(s) in a background thread. librtmp2's Server
        // uses a blocking poll loop, so it lives outside the Tokio runtime.
        // One Server binds both the always-on plaintext listener and, when TLS
        // is enabled, an additional RTMPS listener — they share one
        // connections list, so publish/play work across either listener
        // interchangeably (a publisher on RTMP can be watched over RTMPS and
        // vice versa) and RTMP_MAX_CONNECTIONS / the memory limits apply once,
        // across both listeners combined, rather than doubling per listener.
        let rtmp_bind = self.config.rtmp_bind.clone();
        let rtmps_bind = bind_with_default_port(&self.config.rtmps_bind, self.config.rtmps_port());
        let rtmps_log_bind = rtmps_bind.clone();
        let rtmp_max_conn = self.config.rtmp_max_conn;
        let rtmp_max_connections_per_addr = self.config.rtmp_max_connections_per_addr;
        let rtmp_max_pending_tls_per_addr = self.config.rtmp_max_pending_tls_per_addr;
        let idle_timeout_secs = self.config.rtmp_idle_timeout_secs.clamp(5, 600);
        let rtmp_idle_timeout = Duration::from_secs(idle_timeout_secs);
        let rtmp_resource_limits = self.config.rtmp_resource_limits();
        let rtmp_tls_enabled = self.config.tls_enabled;
        let rtmp_tls_cert = self.config.tls_cert_file.clone();
        let rtmp_tls_key = self.config.tls_key_file.clone();
        let rtmp_bridge = Arc::clone(&self.rtmp_bridge);
        let deleted_streams = Arc::clone(&self.deleted_streams);
        let sticky_deleted_streams = Arc::clone(&self.sticky_deleted_streams);
        let revoked_viewers = Arc::clone(&self.revoked_viewers);
        let rtmp_stop = Arc::new(AtomicBool::new(false));
        let rtmp_stop_clone = Arc::clone(&rtmp_stop);
        let media_export_bytes = if media_output_config.needs_relay_export() {
            media_output_config.export_buffer_bytes()
        } else {
            0
        };
        let media_output_thread_config = media_output_config.clone();
        let media_output_db = Arc::clone(&self.db);
        #[cfg(feature = "cluster")]
        let cluster_enabled = self.config.cluster.enabled;
        #[cfg(feature = "cluster")]
        let cluster_media_queue_mb = self.config.cluster.media_queue_mb;
        #[cfg(feature = "cluster")]
        let relay_export_bytes = {
            let cluster_bytes = if cluster_enabled {
                (cluster_media_queue_mb as usize).saturating_mul(1024 * 1024)
            } else {
                0
            };
            media_export_bytes.max(cluster_bytes)
        };
        #[cfg(not(feature = "cluster"))]
        let relay_export_bytes = media_export_bytes;

        let (rtmp_ready_tx, rtmp_ready_rx) = tokio::sync::oneshot::channel();
        let (rtmp_dead_tx, rtmp_dead_rx) = tokio::sync::oneshot::channel();
        let rtmp_thread = std::thread::spawn(move || {
            use librtmp2::server::Server as RtmpServer;
            use librtmp2::types::ServerConfig as RtmpConfig;

            let cfg = RtmpConfig {
                max_connections: rtmp_max_conn,
                chunk_size: 4096,
                tls_enabled: 0,
                tls_cert_file: std::ptr::null(),
                tls_key_file: std::ptr::null(),
                tls_ca_file: std::ptr::null(),
                tls_insecure: 0,
                max_pending_tls_per_addr: rtmp_max_pending_tls_per_addr,
                max_connections_per_addr: rtmp_max_connections_per_addr,
            };
            let mut server = match RtmpServer::new(cfg) {
                Ok(s) => s,
                Err(e) => {
                    let msg = format!("RTMP server init failed: {e}");
                    crate::log_warn!("{msg}");
                    let _ = rtmp_ready_tx.send(Err(msg));
                    return;
                }
            };
            server.resource_limits = rtmp_resource_limits;
            server.defer_media_relay = true;
            server.on_media_cb = Some(rtmp_media_cb);
            // Pending-capable auth: publish/play requests dispatch to the
            // dedicated auth worker (SQLite + cluster ownership) instead of
            // running that work on this thread. Takes priority over
            // on_publish_cb/on_play_cb, which stay unset here.
            server.on_publish_auth_cb = Some(rtmp_publish_auth_cb);
            server.on_play_auth_cb = Some(rtmp_play_auth_cb);
            if relay_export_bytes > 0 {
                server.enable_relay_export(4096, relay_export_bytes.max(1024 * 1024));
            }
            if let Err(e) = server.listen(&rtmp_bind) {
                let msg = format!("RTMP bind on {rtmp_bind} failed: {e}");
                crate::log_warn!("{msg}");
                let _ = rtmp_ready_tx.send(Err(msg));
                return;
            }
            crate::log_info!("RTMP listening on {rtmp_bind}");

            if rtmp_tls_enabled {
                if let Err(e) = server.listen_tls(&rtmps_bind, &rtmp_tls_cert, &rtmp_tls_key) {
                    let msg = format!("RTMPS bind on {rtmps_bind} failed: {e}");
                    crate::log_warn!("{msg}");
                    let _ = rtmp_ready_tx.send(Err(msg));
                    return;
                }
                crate::log_info!("RTMPS listening on {rtmps_bind}");
            }

            let _ = rtmp_ready_tx.send(Ok(()));
            if let Ok(mut guard) = RTMP_BRIDGE.lock() {
                *guard = Some(Arc::clone(&rtmp_bridge));
            }
            let (auth_worker_handle, auth_completions_rx) =
                auth_worker::spawn(Arc::clone(&rtmp_bridge));
            if let Ok(mut guard) = AUTH_WORKER.lock() {
                *guard = Some(auth_worker_handle);
            }
            if let Ok(mut guard) = AUTH_COMPLETIONS_RX.lock() {
                *guard = Some(auth_completions_rx);
            }

            let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();
            let mut media_outputs =
                MediaOutputManager::new(media_output_thread_config, media_output_db);

            loop {
                if rtmp_stop_clone.load(Ordering::Relaxed) {
                    server.stop();
                    break;
                }

                // Capture the publish generation before entering librtmp2. If the
                // callback accepts a republish during this poll, relay frames already
                // buffered in the same poll cannot be attributed safely to either
                // generation because RelayFrame does not carry that boundary. Such a
                // mixed batch is dropped for media outputs below rather than merging two
                // logical publisher sessions.
                let publish_generations_before_poll: HashMap<u64, u64> = tracked
                    .iter()
                    .filter(|(_, entry)| entry.publishing)
                    .map(|(&conn_id, _)| (conn_id, publisher_generation(conn_id)))
                    .collect();

                set_rtmp_poll_server(&mut server);
                let poll_result = server.poll(0);
                clear_rtmp_poll_server();
                if let Err(e) = poll_result {
                    crate::log_warn!("RTMP polling stopped: {e}");
                    break;
                }

                let deleted_now: HashSet<String> = deleted_streams.lock().iter().cloned().collect();
                let revoked_now: HashSet<String> = revoked_viewers.lock().iter().cloned().collect();

                let (current_ids, just_authorized) = process_server_connections(
                    &mut server,
                    &mut tracked,
                    &rtmp_bridge,
                    &deleted_now,
                    &revoked_now,
                    rtmp_idle_timeout,
                );

                #[cfg(feature = "cluster")]
                if cluster_enabled && let Some(mgr) = rtmp_bridge.cluster_manager() {
                    mgr.poll_side_effects();
                    rtmp_bridge.retry_pending_ownership_releases();
                }

                let exported_frames = if relay_export_bytes > 0 {
                    server.drain_exported_relay_frames()
                } else {
                    Vec::new()
                };

                if media_outputs.enabled() {
                    for frame in &exported_frames {
                        if !tracked
                            .get(&frame.publisher_conn_id)
                            .is_some_and(|entry| entry.publishing)
                        {
                            continue;
                        }
                        let generation = publisher_generation(frame.publisher_conn_id);
                        if publish_generations_before_poll
                            .get(&frame.publisher_conn_id)
                            .is_some_and(|before| *before != generation)
                        {
                            // A same-connection republish happened while this poll was
                            // producing the export batch. RelayFrame has no per-frame
                            // generation marker, so conservatively drop this ambiguous
                            // boundary batch. The reconciliation below starts the new
                            // generation before the next poll, preventing cross-session
                            // recording/HLS/push corruption without guessing by timestamp.
                            continue;
                        }
                        // RelayFrame carries the route active when the frame was
                        // exported. Resolve publish keys before consulting the
                        // connection's current stream so queued frames from stream A
                        // cannot be written into a newly switched B.
                        let frame_stream_id = rtmp_bridge
                            .stream_id_for_publish_route(&frame.stream_name)
                            .unwrap_or_else(|| frame.stream_name.clone());
                        media_outputs.handle_frame(frame, &frame_stream_id, generation);
                    }
                }

                // Reconcile the output session only after draining exported
                // frames. This preserves the tail of an old publish session and
                // still restarts hooks/outputs when the same TCP connection
                // republishes the same stream with a new generation.
                for (&conn_id, entry) in &tracked {
                    if entry.publishing && !entry.stream_id.is_empty() {
                        media_outputs.ensure_publisher(
                            conn_id,
                            &entry.stream_id,
                            publisher_generation(conn_id),
                        );
                    }
                }

                #[cfg(feature = "cluster")]
                if cluster_enabled && let Some(mgr) = rtmp_bridge.cluster_manager() {
                    for frame in exported_frames {
                        let generation = publisher_generation(frame.publisher_conn_id);
                        if publish_generations_before_poll
                            .get(&frame.publisher_conn_id)
                            .is_some_and(|before| *before != generation)
                        {
                            // A republish during this poll makes the buffered frame batch
                            // ambiguous for cluster export as well. Do not stamp old frames
                            // with the new stream/ownership epoch.
                            continue;
                        }
                        let sid = rtmp_bridge.stream_id_for_conn(frame.publisher_conn_id);
                        let stream_id = if sid.is_empty() {
                            rtmp_bridge
                                .stream_id_for_publish_route(&frame.stream_name)
                                .unwrap_or_else(|| frame.stream_name.clone())
                        } else {
                            sid
                        };
                        // Stamp only with this publisher socket's claimed
                        // epoch — durable/current stream epoch can belong
                        // to another node after a local release/failover.
                        let Some(epoch) =
                            rtmp_bridge.ownership_epoch_for_conn(frame.publisher_conn_id)
                        else {
                            continue;
                        };
                        use crate::cluster::media::protocol::MediaMessage;
                        mgr.enqueue_export(crate::cluster::ExportedFrame {
                            app: frame.app.clone(),
                            stream: stream_id,
                            epoch,
                            frame_type: MediaMessage::frame_type_from_librtmp2(frame.frame_type),
                            timestamp: frame.timestamp,
                            payload: frame.payload,
                        });
                    }
                    for inj in mgr.drain_injects() {
                        if let Some(ft) =
                            crate::cluster::media::protocol::MediaMessage::frame_type_to_librtmp2(
                                inj.frame_type,
                            )
                        {
                            // Inject using stream name expected by local players:
                            // resolve play route via stream id → stream.play_key / name.
                            let _ = server.inject_relay_frame(
                                &inj.app,
                                &inj.stream,
                                ft,
                                inj.timestamp,
                                &inj.payload,
                            );
                        }
                    }
                }

                // A conn_id still in `tracked` but absent this cycle was
                // closed by the peer (rather than rejected above) — notify
                // the bridge.
                let closed_ids: Vec<u64> = tracked
                    .keys()
                    .copied()
                    .filter(|id| !current_ids.contains(id))
                    .collect();
                for conn_id in closed_ids {
                    tracked.remove(&conn_id);
                    rtmp_bridge.on_close(conn_id);
                    clear_publish_generation(conn_id);
                }

                // A connection whose publish/play/media callback ran during
                // this poll but whose socket the library reaped in the same
                // `poll(0)` never appears in `current_ids`, so it never entered
                // `tracked` and the `closed_ids` sweep above cannot see it. Its
                // bridge ConnState and any active publisher/player row were
                // created by the callback, so close them explicitly or the row
                // stays active (blocking future publishes) and conn/ownership
                // state leaks. Only ids absent from `current_ids` are touched,
                // so live connections are unaffected.
                let touched: Vec<u64> =
                    RTMP_POLL_TOUCHED_CONNS.with(|set| set.borrow_mut().drain().collect());
                for conn_id in touched {
                    if current_ids.contains(&conn_id)
                        || (!rtmp_bridge.is_registered(conn_id)
                            && !rtmp_bridge.has_publisher(conn_id)
                            && !rtmp_bridge.has_player(conn_id))
                    {
                        continue;
                    }
                    rtmp_bridge.on_close(conn_id);
                    clear_publish_generation(conn_id);
                }

                let live_publishers: HashSet<u64> = tracked
                    .iter()
                    .filter_map(|(&conn_id, entry)| entry.publishing.then_some(conn_id))
                    .collect();
                media_outputs.retain_publishers(&live_publishers);

                // Prune transient drain markers (e.g. ownership force_unpublish)
                // once no local session references them. Keep HTTP sticky
                // markers until finalize/rollback clears them explicitly —
                // otherwise Raft begin_delete timeouts lose rejection cover
                // when the node has no live RTMP sessions for the stream.
                let live_stream_ids = live_stream_ids_for_deleted_markers(&tracked, &rtmp_bridge);
                let sticky = sticky_deleted_streams.lock().clone();
                deleted_streams
                    .lock()
                    .retain(|id| live_stream_ids.contains(id) || sticky.contains(id));

                let live_viewer_ids: HashSet<String> = tracked
                    .keys()
                    .copied()
                    .map(|conn_id| rtmp_bridge.viewer_id_for_conn(conn_id))
                    .filter(|viewer_id| !viewer_id.is_empty())
                    .collect();
                revoked_viewers
                    .lock()
                    .retain(|viewer_id| live_viewer_ids.contains(viewer_id));

                let negotiating = tracked.values().any(|c| !c.publishing && !c.playing);
                let poll_interval_ms = if negotiating || just_authorized {
                    POLL_INTERVAL_FAST_MS
                } else {
                    POLL_INTERVAL_MS
                };
                wait_for_readiness_or_timeout(&server, poll_interval_ms);
            }

            media_outputs.stop_all();

            // Notify the bridge about connections that never got an explicit close event.
            for conn_id in tracked.keys().copied().collect::<Vec<_>>() {
                rtmp_bridge.on_close(conn_id);
                clear_publish_generation(conn_id);
            }

            if !rtmp_stop_clone.load(Ordering::Relaxed) {
                let _ = rtmp_dead_tx.send(());
            }
        });

        rtmp_ready_rx
            .await
            .map_err(|_| "RTMP startup thread exited before reporting readiness".to_string())??;

        crate::log_info!(
            "Server ready — HTTP: {}, RTMP: {}{}",
            self.config.http_bind,
            self.config.rtmp_bind,
            if self.config.tls_enabled {
                format!(", RTMPS: {rtmps_log_bind}")
            } else {
                String::new()
            }
        );

        let http_result = axum::serve(
            http_listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            tokio::select! {
                () = shutdown_signal() => {},
                _ = rtmp_dead_rx => {
                    crate::log_error!(
                        "RTMP thread exited unexpectedly; shutting down HTTP so the process does not keep serving a half-dead API"
                    );
                }
            }
        })
        .await
        .map_err(|e| format!("HTTP server error: {e}"));

        crate::log_info!("Shutting down...");
        // Stop and join the RTMP thread before tearing down Raft: its
        // on_close callbacks release publisher ownership through the
        // coordinator, and running them after `shutdown_blocking()` would
        // have those releases fail against an already-shut-down cluster
        // manager, leaving durable ownership rows behind that block
        // publishers routed to other nodes until the next failure sweep.
        rtmp_stop.store(true, Ordering::Relaxed);
        let _ = rtmp_thread.join();
        crate::log_info!("RTMP thread joined.");
        #[cfg(feature = "cluster")]
        if let Some(mgr) = coordinator.cluster_manager() {
            mgr.shutdown_blocking();
        }
        http_result?;
        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install SIGINT handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AUTH_COMPLETIONS_RX, ServerApp, TrackedConn, bind_with_default_port,
        drain_auth_completions, drain_deleted_stream_roles, eviction_stream_id,
        live_stream_ids_for_deleted_markers, should_evict_idle_conn,
    };
    use crate::auth_worker::{AuthCompletion, AuthKind};
    use crate::config::ServerConfig;
    use crate::db::Db;
    use crate::rtmp_bridge::{DbRtmpBridge, RtmpEventHandler};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::mpsc::sync_channel;
    use std::time::{Duration, Instant};

    fn stale_first_seen(now: Instant) -> Option<Instant> {
        now.checked_sub(Duration::from_secs(120))
    }

    #[test]
    fn idle_eviction_skips_authorized_sessions() {
        let now = Instant::now();
        let entry = TrackedConn {
            first_seen_at: stale_first_seen(now),
            ..Default::default()
        };
        assert!(!should_evict_idle_conn(
            &entry,
            true,
            now,
            Duration::from_secs(30)
        ));

        let publishing_entry = TrackedConn {
            publishing: true,
            first_seen_at: stale_first_seen(now),
            ..Default::default()
        };
        assert!(!should_evict_idle_conn(
            &publishing_entry,
            false,
            now,
            Duration::from_secs(30)
        ));
    }

    #[test]
    fn idle_eviction_targets_stale_pre_auth_connections() {
        let now = Instant::now();
        let entry = TrackedConn {
            first_seen_at: stale_first_seen(now),
            ..Default::default()
        };
        assert!(should_evict_idle_conn(
            &entry,
            false,
            now,
            Duration::from_secs(30)
        ));
    }

    #[test]
    fn drain_auth_completions_reports_when_a_completion_was_applied() {
        let cfg = librtmp2::types::ServerConfig {
            max_connections: 8,
            chunk_size: 4096,
            tls_enabled: 0,
            tls_cert_file: std::ptr::null(),
            tls_key_file: std::ptr::null(),
            tls_ca_file: std::ptr::null(),
            tls_insecure: 0,
            max_pending_tls_per_addr: i32::MAX,
            max_connections_per_addr: i32::MAX,
        };
        let mut server = librtmp2::server::Server::new(cfg).unwrap();

        // Nothing queued: the poll loop must not force a fast follow-up tick.
        assert!(!drain_auth_completions(&mut server));

        // conn_id 999 doesn't exist on this server, so resolving it is a
        // harmless no-op (see `Server::complete_publish_authorization`) --
        // what this test checks is that the drain is still reported, which
        // is what lets the poll loop pick a fast follow-up tick regardless
        // of whether the connection was still around to receive it.
        let (tx, rx) = sync_channel(4);
        tx.send(AuthCompletion {
            kind: AuthKind::Publish,
            conn_id: 999,
            allow: true,
        })
        .unwrap();
        if let Ok(mut guard) = AUTH_COMPLETIONS_RX.lock() {
            *guard = Some(rx);
        }

        assert!(
            drain_auth_completions(&mut server),
            "a drained completion must be reported so the poll loop can skip \
             the idle sleep on this tick"
        );
        assert!(
            !drain_auth_completions(&mut server),
            "the channel is now empty; no fast follow-up is needed"
        );

        if let Ok(mut guard) = AUTH_COMPLETIONS_RX.lock() {
            *guard = None;
        }
    }

    #[test]
    fn bind_with_default_port_leaves_explicit_ports() {
        assert_eq!(bind_with_default_port("0.0.0.0:1936", 1936), "0.0.0.0:1936");
        assert_eq!(bind_with_default_port("[::1]:1936", 1936), "[::1]:1936");
    }

    #[test]
    fn bind_with_default_port_normalizes_host_only_binds() {
        assert_eq!(bind_with_default_port("0.0.0.0", 1936), "0.0.0.0:1936");
        assert_eq!(bind_with_default_port("::1", 1936), "[::1]:1936");
        assert_eq!(bind_with_default_port("[::1]", 1936), "[::1]:1936");
    }

    #[test]
    fn deleted_markers_retain_bridge_stream_id_before_tracker_sync() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let stream = crate::db::Stream {
            id: "s1".to_string(),
            name: "S1".to_string(),
            app: "live".to_string(),
            publish_key: "pub_key_with_sufficient_length_here01".to_string(),
            play_key: "play_key_with_sufficient_length_here01".to_string(),
            stats_key: "stats_key_with_sufficient_length_here01".to_string(),
            enabled: true,
            created_at: crate::db::now_ts(),
        };
        db.stream_add(&stream).unwrap();

        let deleted = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let bridge = DbRtmpBridge::new(Arc::clone(&db), Arc::clone(&deleted));
        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(
            bridge
                .authorize_publish(1, "live", &stream.publish_key)
                .is_ok()
        );

        // Simulate a delete request arriving while the publisher is still live.
        deleted.lock().insert("s1".to_string());

        let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();
        tracked.insert(
            1,
            TrackedConn {
                connected: true,
                ..Default::default()
            },
        );

        let live = live_stream_ids_for_deleted_markers(&tracked, &bridge);
        assert!(
            live.contains("s1"),
            "bridge stream_id must keep deleted marker until RTMP drain even when TrackedConn is not synced"
        );
        assert_eq!(eviction_stream_id(&bridge, 1, &tracked[&1]), "s1");
    }

    #[test]
    fn deleted_markers_retain_dual_role_play_stream() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let s1 = crate::db::Stream {
            id: "s1".to_string(),
            name: "S1".to_string(),
            app: "live".to_string(),
            publish_key: "pub_key_with_sufficient_length_here01".to_string(),
            play_key: "play_key_with_sufficient_length_here01".to_string(),
            stats_key: "stats_key_with_sufficient_length_here01".to_string(),
            enabled: true,
            created_at: crate::db::now_ts(),
        };
        let s2 = crate::db::Stream {
            id: "s2".to_string(),
            name: "S2".to_string(),
            app: "live".to_string(),
            publish_key: "pub_key2_with_sufficient_length_here01".to_string(),
            play_key: "play_key2_with_sufficient_length_here01".to_string(),
            stats_key: "stats_key2_with_sufficient_length_here01".to_string(),
            enabled: true,
            created_at: crate::db::now_ts(),
        };
        db.stream_add(&s1).unwrap();
        db.stream_add(&s2).unwrap();

        let deleted = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let bridge = DbRtmpBridge::new(Arc::clone(&db), Arc::clone(&deleted));
        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", &s1.publish_key).is_ok());
        assert!(bridge.authorize_play(1, "live", &s2.play_key).is_ok());

        deleted.lock().insert("s2".to_string());

        let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();
        tracked.insert(
            1,
            TrackedConn {
                connected: true,
                ..Default::default()
            },
        );

        let live = live_stream_ids_for_deleted_markers(&tracked, &bridge);
        assert!(
            live.contains("s2"),
            "dual-role play stream must retain deleted marker until player drains"
        );
    }

    #[test]
    fn drain_deleted_play_keeps_dual_role_publisher() {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let s1 = crate::db::Stream {
            id: "s1".to_string(),
            name: "S1".to_string(),
            app: "live".to_string(),
            publish_key: "pub_key_with_sufficient_length_here01".to_string(),
            play_key: "play_key_with_sufficient_length_here01".to_string(),
            stats_key: "stats_key_with_sufficient_length_here01".to_string(),
            enabled: true,
            created_at: crate::db::now_ts(),
        };
        let s2 = crate::db::Stream {
            id: "s2".to_string(),
            name: "S2".to_string(),
            app: "live".to_string(),
            publish_key: "pub_key2_with_sufficient_length_here01".to_string(),
            play_key: "play_key2_with_sufficient_length_here01".to_string(),
            stats_key: "stats_key2_with_sufficient_length_here01".to_string(),
            enabled: true,
            created_at: crate::db::now_ts(),
        };
        db.stream_add(&s1).unwrap();
        db.stream_add(&s2).unwrap();

        let deleted = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let bridge = DbRtmpBridge::new(Arc::clone(&db), Arc::clone(&deleted));
        bridge.on_connect(1, "127.0.0.1:1000");
        assert!(bridge.authorize_publish(1, "live", &s1.publish_key).is_ok());
        assert!(bridge.authorize_play(1, "live", &s2.play_key).is_ok());

        deleted.lock().insert("s2".to_string());
        let deleted_now = deleted.lock().clone();

        let mut conn = librtmp2::session::conn::Conn::new();
        conn.conn_id = 1;
        conn.remote_addr = "127.0.0.1:1000".into();
        conn.current_stream = Some(Box::new(librtmp2::session::stream::Stream {
            stream_id: 1,
            is_publishing: true,
            is_playing: true,
            name: "live".into(),
            paused: false,
            receive_audio: true,
            receive_video: true,
        }));
        let mut entry = TrackedConn {
            connected: true,
            publishing: true,
            playing: true,
            stream_id: "s1".into(),
            ..Default::default()
        };

        let kick = drain_deleted_stream_roles(&mut conn, &mut entry, &bridge, 1, &deleted_now);
        assert!(!kick, "publisher on s1 must keep the connection");
        assert!(bridge.has_publisher(1));
        assert!(!bridge.has_player(1));
        assert!(entry.publishing);
        assert!(!entry.playing);
        assert_eq!(entry.stream_id, "s1");
        assert!(!conn.current_stream.as_ref().unwrap().is_playing);
        assert!(
            conn.pending_relay.is_empty(),
            "queued play frames must not survive onto the publisher route"
        );
    }

    #[test]
    fn create_generates_api_token_on_first_start() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("token.db");
        let db_path_str = db_path.to_str().unwrap();

        let config = ServerConfig {
            config_file: String::new(),
            ..Default::default()
        };
        let app = ServerApp::bootstrap(config, db_path_str).expect("ServerApp::bootstrap");

        let db = crate::db::Db::open(db_path_str).expect("reopen db");
        let stored = db.token_get().unwrap().expect("token should be stored");
        assert_eq!(stored.len(), 64, "generated token must be 64 hex chars");
        assert!(
            stored.chars().all(|c| c.is_ascii_hexdigit()),
            "token must be hex"
        );
        assert_eq!(app.config.api_token, stored);
    }

    #[test]
    fn bootstrap_seeds_api_token_from_env_on_first_start() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("env-token.db");
        let db_path_str = db_path.to_str().unwrap();
        let env_token = "c10123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd";

        // SAFETY: test runs single-threaded and restores the env var immediately.
        unsafe {
            std::env::set_var("LRTMP2_API_TOKEN", env_token);
        }
        let app = ServerApp::bootstrap(ServerConfig::default(), db_path_str).expect("bootstrap");
        unsafe {
            std::env::remove_var("LRTMP2_API_TOKEN");
        }

        assert_eq!(app.config.api_token, env_token);
        let db = crate::db::Db::open(db_path_str).expect("reopen db");
        assert_eq!(db.token_get().unwrap().as_deref(), Some(env_token));
    }
}
