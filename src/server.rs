//! Server application lifecycle: wires together the database, the HTTP API,
//! and the RTMP listener(s), then runs until a shutdown signal arrives.

use parking_lot::Mutex;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    /// Per-shard auth-completion receiver, set once by a sharded RTMP thread
    /// (see [`resolve_shard_count`]) instead of the global
    /// `AUTH_COMPLETIONS_RX`. There's still exactly one auth worker and one
    /// underlying completion stream for the whole process; a small
    /// dispatcher thread fans each completion out to the shard that owns its
    /// `conn_id` (see `run`). `None` on the single-shard path, where
    /// `drain_auth_completions` falls back to the legacy global receiver.
    static SHARD_AUTH_COMPLETIONS_RX: RefCell<Option<std::sync::mpsc::Receiver<AuthCompletion>>> =
        const { RefCell::new(None) };
}

pub(crate) fn set_shard_auth_completions_rx(rx: std::sync::mpsc::Receiver<AuthCompletion>) {
    SHARD_AUTH_COMPLETIONS_RX.with(|cell| *cell.borrow_mut() = Some(rx));
}

/// Which shard owns `conn_id`, given `set_conn_id_base(1 + i * SHARD_ID_SPACE)`
/// was used for shard `i` (see [`resolve_shard_count`]).
pub(crate) fn shard_for_conn_id(conn_id: u64) -> usize {
    (conn_id.saturating_sub(1) / SHARD_ID_SPACE) as usize
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

/// Queued publisher/player stats are written to SQLite once per
/// `STATS_FLUSH_TICK * STATS_FLUSH_INTERVAL_TICKS` (1 s, matching the
/// per-connection stats debounce), checking for shutdown every tick.
const STATS_FLUSH_TICK: Duration = Duration::from_millis(100);
const STATS_FLUSH_INTERVAL_TICKS: u32 = 10;

/// Poll interval used instead of `POLL_INTERVAL_MS` while at least one
/// tracked connection is still negotiating (handshake / connect /
/// createStream / publish|play command, including a publish|play command
/// still waiting on the async auth worker) rather than actively publishing
/// or playing; while a connection has started playing but has not yet
/// relayed its first frame (see `TrackedConn::awaiting_first_frame` --
/// otherwise the interval drops back to the slow 50ms the instant a play
/// request is *accepted*, before the viewer has actually received
/// anything, bounding their real join latency by the publisher's frame
/// cadence instead). Right after the async auth worker resolves a
/// publish/play authorization the loop doesn't wait at all (see
/// `just_authorized` in the poll loop). Handshake and stream-join round trips each wait for the
/// next poll tick before the server's reply goes out, so the fixed 50ms
/// interval alone adds up to tens of milliseconds of avoidable latency per
/// step; polling faster only during this comparatively brief, comparatively
/// rare window keeps that cost low without paying the CPU cost of
/// fast-polling steady-state connections that no longer need it.
pub(crate) const POLL_INTERVAL_FAST_MS: u64 = 1;

const FIRST_FRAME_GRACE_MS: u64 = 500;

/// How often the poll loop re-derives `live_publishers`/`live_stream_ids`/
/// `live_viewer_ids` and prunes `deleted_streams`/`revoked_viewers` against
/// them (revocations after a cross-shard grace window). This bookkeeping is
/// pure garbage collection -- it only reclaims markers for connections that
/// are already gone, so it tolerates the same
/// staleness window as the slow poll interval. It is *not* tied to
/// `poll_interval_ms`: that interval drops to `POLL_INTERVAL_FAST_MS` while
/// any connection is negotiating (see above), and each of these derived
/// sets locks `DbRtmpBridge`'s shared connection map once per tracked
/// connection to build. Rebuilding them on every fast tick during a
/// many-viewer join burst turned that lock into a bottleneck contended by
/// every joining connection at once, making the burst slower, not faster --
/// running this at a fixed cadence instead keeps it off the hot path.
pub(crate) const PRUNE_INTERVAL_MS: u64 = POLL_INTERVAL_MS;

pub(crate) const REVOKED_VIEWER_GRACE_MS: u64 = 10_000;

/// Size of the conn_id range reserved for each RTMP shard when sharding is
/// active (see [`resolve_shard_count`]) -- shard `i` gets
/// `set_conn_id_base(1 + i as u64 * SHARD_ID_SPACE)`. Comfortably larger
/// than any realistic connection count per shard, and `MAX_SHARDS *
/// SHARD_ID_SPACE` stays well under librtmp2's reserved external-publisher-id
/// high bit (`1 << 63`).
const SHARD_ID_SPACE: u64 = 1 << 48;
const MAX_SHARDS: usize = 32;

/// Bound on each shard's cross-shard relay inbox (see the broadcast/inject
/// block in the shard loop below). A receiving shard that falls behind the
/// incoming frame rate drops frames via `try_send` past this bound instead
/// of growing the queue without limit, the same fail-fast-under-backpressure
/// choice used for the auth worker's queue (`AUTH_QUEUE_CAPACITY`). Sized to
/// match librtmp2's own per-route pending-relay cap
/// (`MAX_PENDING_RELAY_FRAMES`).
const CROSS_SHARD_RELAY_QUEUE_CAPACITY: usize = 1024;

/// How many independent `librtmp2::server::Server` instances (each on its
/// own OS thread, each with its own disjoint conn_id range, sharing one
/// `SO_REUSEPORT` listener so the kernel load-balances new connections
/// across them) to run the RTMP poll loop across.
///
/// A single dedicated poll thread processes every connection's `recv()`
/// serially each tick -- fine at low concurrency, but it means the whole
/// server runs on one CPU core no matter how many viewers connect.
/// Sharding lets connections that don't need to interact (most of any
/// tick's work) run in parallel across cores.
///
/// Cross-shard relay (a publisher on one shard, viewers on another) reuses
/// librtmp2's existing `inject_relay_frame`/relay-export mechanism built for
/// HA clustering -- broadcasting each shard's exported frames to every other
/// shard in-process, no network involved. Media outputs (recording/HLS/push/
/// exec) and HA clustering are not yet wired through that same cross-shard
/// path -- both assume a single `Server`/`MediaOutputManager` instance -- so
/// sharding is forced off (falls back to the single-thread path, unchanged)
/// whenever either is enabled, rather than risk silently dropping frames or
/// double-processing them.
///
/// Set with `LRTMP2_RTMP_SHARDS` (clamped to `[1, MAX_SHARDS]`). When unset
/// it defaults to the number of available CPUs, capped at
/// [`AUTO_SHARDS_MAX`] -- but only where sharding can't change behaviour:
/// media outputs and HA clustering (above) force a single shard, and so
/// does a configured per-address connection cap, since that cap is
/// enforced per shard (a given IP's connections can land on different
/// shards). The global `max_connections` cap stays exact across shards
/// (see [`ShardConnBudget`]). Setting `LRTMP2_RTMP_SHARDS=1` restores the
/// single-thread loop.
fn resolve_shard_count(
    media_outputs_enabled: bool,
    cluster_enabled: bool,
    per_addr_caps_configured: bool,
) -> usize {
    let explicit = std::env::var("LRTMP2_RTMP_SHARDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    shard_count_for(
        explicit,
        cpus,
        media_outputs_enabled,
        cluster_enabled,
        per_addr_caps_configured,
    )
}

/// [`resolve_shard_count`] without the environment lookups.
fn shard_count_for(
    explicit: Option<usize>,
    cpus: usize,
    media_outputs_enabled: bool,
    cluster_enabled: bool,
    per_addr_caps_configured: bool,
) -> usize {
    let requested = match explicit {
        Some(n) => n,
        None if per_addr_caps_configured => return 1,
        None => cpus.min(AUTO_SHARDS_MAX),
    };
    if requested <= 1 {
        return 1;
    }
    if media_outputs_enabled {
        if explicit.is_some() {
            crate::log_warn!(
                "LRTMP2_RTMP_SHARDS ignored: media outputs (recording/HLS/push/exec) aren't wired through cross-shard relay yet"
            );
        }
        return 1;
    }
    if cluster_enabled {
        if explicit.is_some() {
            crate::log_warn!(
                "LRTMP2_RTMP_SHARDS ignored: HA clustering isn't wired through cross-shard relay yet"
            );
        }
        return 1;
    }
    requested.clamp(1, MAX_SHARDS)
}

/// Default shard count ceiling when `LRTMP2_RTMP_SHARDS` is unset. Every
/// shard receives every other shard's relayed frames, so returns diminish
/// past a handful of shards for a single hot stream.
const AUTO_SHARDS_MAX: usize = 4;

/// One message on a shard's cross-shard relay inbox.
enum ShardRelayMsg {
    /// A frame from a publisher on another shard, to inject for local viewers.
    Frame(librtmp2::RelayFrame),
    /// That publisher is gone: release the inject claim on its route right
    /// away. Without it the receiving shard kept the route claimed for its
    /// external feed until the stale-route timeout (120 s), rejecting a
    /// publisher that reconnected and happened to land on that shard.
    RouteEnded { app: String, stream_name: String },
}

/// Routes (`app`, `stream_name`) this shard's own publishers have relayed to
/// the other shards, with the publishing connection, so the other shards can
/// be told when a route ends.
#[derive(Default)]
struct ExportedRoutes {
    routes: HashMap<(String, String), u64>,
}

impl ExportedRoutes {
    fn record(&mut self, frames: &[librtmp2::RelayFrame]) {
        for frame in frames {
            // Frames injected from elsewhere are never re-broadcast.
            if librtmp2::server::is_external_publisher_id(frame.publisher_conn_id) {
                continue;
            }
            let key = (frame.app.clone(), frame.stream_name.clone());
            if self.routes.get(&key) != Some(&frame.publisher_conn_id) {
                self.routes.insert(key, frame.publisher_conn_id);
            }
        }
    }

    /// Removes and returns the routes whose publisher is no longer among
    /// `publishing` (the ids of this shard's connections still publishing).
    fn take_ended(&mut self, publishing: &HashSet<u64>) -> Vec<(String, String)> {
        let mut ended = Vec::new();
        self.routes.retain(|route, conn_id| {
            let live = publishing.contains(conn_id);
            if !live {
                ended.push(route.clone());
            }
            live
        });
        ended
    }
}

/// librtmp2 `max_connections` for one shard, and whether [`ShardConnBudget`]
/// should adjust it every tick.
///
/// Plaintext only: each shard starts with the full global cap and the
/// budget narrows it to what the other shards leave free. With RTMPS
/// enabled librtmp2 also counts TLS handshakes in progress against
/// `max_connections`, and those aren't visible to the other shards (nor in
/// `Server::connections`), so a per-tick budget could let every shard fill
/// up with stalled handshakes on its own. The cap is then split statically
/// instead (`global / n_shards`, remainder to the first shards): librtmp2
/// enforces each share including pending handshakes, so the shares can't
/// add up past the global cap. `n_shards <= global` is guaranteed by the
/// caller, so no share is 0 (which librtmp2 would read as "unlimited").
fn shard_connection_cap(
    global: i32,
    n_shards: usize,
    shard_index: usize,
    tls_enabled: bool,
) -> (i32, bool) {
    if n_shards <= 1 {
        return (global, false);
    }
    if !tls_enabled {
        return (global, true);
    }
    let n = n_shards as i32;
    let share = global / n + i32::from((shard_index as i32) < global % n);
    (share.max(1), false)
}

/// Keeps the configured global `max_connections` exact when connections are
/// spread over several shards. Each shard publishes its connection count;
/// before each poll a shard's own librtmp2 cap is set to what the others
/// leave free (so it stops accepting once the process-wide total is
/// reached, instead of at a fixed per-shard share that uneven kernel load
/// balancing could fill early), and after the poll any overshoot from two
/// shards accepting in the same instant is trimmed by closing the newest
/// not-yet-authorized connections.
struct ShardConnBudget {
    counts: Arc<Vec<AtomicUsize>>,
    index: usize,
    global: usize,
}

impl ShardConnBudget {
    fn others(&self) -> usize {
        self.counts
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.index)
            .map(|(_, c)| c.load(Ordering::Relaxed))
            .sum()
    }

    fn before_poll(&self, server: &mut librtmp2::server::Server) {
        self.counts[self.index].store(server.connections.len(), Ordering::Relaxed);
        // librtmp2 treats 0 as "unlimited", so never go below 1; the
        // overshoot that can allow is trimmed in `after_poll`.
        let free = self.global.saturating_sub(self.others()).max(1);
        server.config.max_connections = free.min(i32::MAX as usize) as i32;
    }

    fn after_poll(&self, server: &mut librtmp2::server::Server, rtmp_bridge: &DbRtmpBridge) {
        let own = server.connections.len();
        let total = own + self.others();
        let mut trimmed = 0;
        if total > self.global {
            let excess = total - self.global;
            let mut newest: Vec<(u64, usize)> = server
                .connections
                .iter()
                .enumerate()
                .filter(|(_, c)| c.client_fd >= 0 && !rtmp_bridge.has_authorized_session(c.conn_id))
                .map(|(i, c)| (c.conn_id, i))
                .collect();
            newest.sort_unstable_by_key(|&(conn_id, _)| std::cmp::Reverse(conn_id));
            for (_, idx) in newest.into_iter().take(excess) {
                server.connections[idx].disconnect_transport();
                trimmed += 1;
            }
            if trimmed > 0 {
                crate::log_warn!(
                    "RTMP: connection cap {} reached across shards; closed {trimmed} new connection(s)",
                    self.global
                );
            }
        }
        self.counts[self.index].store(own - trimmed, Ordering::Relaxed);
    }
}

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
///
/// Returns the `conn_id`s whose fd came back readable (or errored/hung up --
/// a dead connection must still get a `recv()` attempt to actually notice
/// and close it), for the next call to `Server::poll_ready` to use. `None`
/// means "assume everyone is readable" (the rare `poll(2)`-itself-failed
/// fallback below) -- the caller must fall back to the unfiltered
/// `Server::poll` in that case, never treat it as an empty ready set.
///
/// Beyond gating the sleep, `poll(2)` already tells us exactly which fds
/// are readable; discarding that down to a single yes/no (as this function
/// used to) meant every connection -- including idle players who send
/// nothing after their initial `play` -- got a `recv()` syscall attempted
/// every single tick regardless. At 100 concurrent viewers that is 100
/// wasted syscalls per tick most of the time; returning the actual ready
/// set lets the caller skip them.
fn poll_readiness(server: &librtmp2::server::Server, timeout_ms: u64) -> Option<Wake> {
    let conn_id_by_fd: HashMap<i32, u64> = server
        .connections
        .iter()
        .filter(|conn| conn.client_fd >= 0)
        .map(|conn| (conn.client_fd, conn.conn_id))
        .collect();
    let at_connection_cap = server.config.max_connections > 0
        && server.connections.len() >= server.config.max_connections as usize;
    // A connection with outbound bytes still queued (a player whose socket
    // send buffer filled mid-keyframe, say) also waits for POLLOUT, so the
    // loop wakes to flush the rest as soon as the peer's window opens instead
    // of stalling that viewer's stream for up to a full poll interval. Only
    // requested while bytes are pending, so a drained or idle connection
    // can't turn this into a busy loop.
    let mut fds: Vec<libc::pollfd> = server
        .listener_fds()
        .into_iter()
        .filter(|_| !at_connection_cap)
        .map(|fd| (fd, libc::POLLIN))
        .chain(
            server
                .connections
                .iter()
                .filter(|conn| conn.client_fd >= 0)
                .map(|conn| {
                    let events = if conn.send_buffer.available() > 0 {
                        libc::POLLIN | libc::POLLOUT
                    } else {
                        libc::POLLIN
                    };
                    (conn.client_fd, events)
                }),
        )
        .map(|(fd, events)| libc::pollfd {
            fd,
            events,
            revents: 0,
        })
        .collect();

    let timeout = timeout_ms.min(i32::MAX as u64) as i32;
    loop {
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
        if rc >= 0 {
            const READY_MASK: i16 = libc::POLLIN | libc::POLLERR | libc::POLLHUP | libc::POLLNVAL;
            let ready: HashSet<u64> = fds
                .iter()
                .filter(|pfd| pfd.revents & READY_MASK != 0)
                .filter_map(|pfd| conn_id_by_fd.get(&pfd.fd).copied())
                .collect();
            let listener_ready = fds
                .iter()
                .any(|pfd| pfd.revents & READY_MASK != 0 && !conn_id_by_fd.contains_key(&pfd.fd));
            return Some(Wake {
                ready,
                listener_ready,
            });
        }
        if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            // Unexpected poll() failure -- e.g. a connection closed and its
            // fd was reused between fd collection and this call. Fall back
            // to the plain sleep this function replaces rather than
            // spinning on a busy error, and report "assume everyone ready"
            // so the caller falls back to processing every connection.
            std::thread::sleep(std::time::Duration::from_millis(timeout_ms));
            return None;
        }
        // EINTR: a signal interrupted the wait. `poll`'s timeout is
        // relative, so simply retrying restarts the full timeout rather
        // than preserving a deadline -- acceptable for this loop's
        // existing best-effort latency bound (worst case: one timeout
        // window longer under signal pressure, which is rare).
    }
}

/// Readiness source for the RTMP poll loop: a persistent epoll set on Linux
/// (see `crate::readiness`), falling back to the per-tick `poll(2)` in
/// [`poll_readiness`] elsewhere or if epoll is unavailable.
#[cfg(target_os = "linux")]
use crate::readiness::WakeFd;

/// Stand-in where `eventfd` isn't available: completions are then picked up
/// on the poll loop's next tick, as before wake fds existed.
#[cfg(not(target_os = "linux"))]
struct WakeFd;

#[cfg(not(target_os = "linux"))]
impl WakeFd {
    fn new() -> std::io::Result<Self> {
        Ok(Self)
    }

    fn signal(&self) {}
}

struct ReadinessWaiter {
    source: ReadinessSource,
    /// Newest `conn_id` among `server.connections` when the previous wait
    /// returned with only a listener ready, or `None` if it didn't.
    /// Connection ids are allocated in increasing order, so a newer id
    /// means an accept happened in between -- unlike the connection count,
    /// which stays flat when accepts and closes cancel out under churn.
    listener_only_at: Option<u64>,
}

enum ReadinessSource {
    #[cfg(target_os = "linux")]
    Epoll(crate::readiness::EpollReadiness),
    Poll,
}

/// What one readiness wait reported.
pub(crate) struct Wake {
    /// Connections that are readable (or errored/hung up).
    pub(crate) ready: HashSet<u64>,
    /// A listener had a pending connection.
    pub(crate) listener_ready: bool,
}

impl ReadinessWaiter {
    fn new(wake: Option<Arc<WakeFd>>) -> Self {
        #[cfg(target_os = "linux")]
        match crate::readiness::EpollReadiness::new(wake) {
            Ok(epoll) => return Self::with_source(ReadinessSource::Epoll(epoll)),
            Err(e) => crate::log_warn!("epoll unavailable ({e}); falling back to poll(2)"),
        }
        #[cfg(not(target_os = "linux"))]
        let _ = wake;
        Self::with_source(ReadinessSource::Poll)
    }

    fn with_source(source: ReadinessSource) -> Self {
        Self {
            source,
            listener_only_at: None,
        }
    }

    /// Returns the `conn_id`s that are ready, or `None` for "assume
    /// everyone is ready" (the wait itself failed).
    ///
    /// When only a listener is ready, return at once so the new connection
    /// is accepted without delay. Only if that happens again with no new
    /// connection accepted in between (the server can't service the
    /// listener right now, e.g. per-IP caps, so it stays readable) take a
    /// short bounded sleep, so an unserviceable listener can't spin the
    /// loop. The unconditional sleep this replaces added up to 10 ms to
    /// every single accept.
    fn wait(&mut self, server: &librtmp2::server::Server, timeout_ms: u64) -> Option<HashSet<u64>> {
        let wake = match &mut self.source {
            #[cfg(target_os = "linux")]
            ReadinessSource::Epoll(epoll) => epoll.wait(server, timeout_ms),
            ReadinessSource::Poll => poll_readiness(server, timeout_ms),
        };
        let Some(wake) = wake else {
            self.listener_only_at = None;
            return None;
        };
        if wake.ready.is_empty() && wake.listener_ready {
            let newest = newest_conn_id(server);
            if self.listener_only_at == Some(newest) {
                std::thread::sleep(Duration::from_millis(timeout_ms.min(10)));
            }
            self.listener_only_at = Some(newest);
        } else {
            self.listener_only_at = None;
        }
        Some(wake.ready)
    }
}

/// Highest `conn_id` currently in `server.connections` (0 when empty).
fn newest_conn_id(server: &librtmp2::server::Server) -> u64 {
    server
        .connections
        .iter()
        .map(|conn| conn.conn_id)
        .max()
        .unwrap_or(0)
}

/// `poll(2)` readiness without the waiter's listener backoff (tests).
#[cfg(test)]
fn wait_for_readiness_or_timeout(
    server: &librtmp2::server::Server,
    timeout_ms: u64,
) -> Option<HashSet<u64>> {
    poll_readiness(server, timeout_ms).map(|wake| wake.ready)
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

/// For the IPv4 wildcard bind, derive the matching IPv6 wildcard on the same
/// port. Specific IPv4 addresses stay IPv4-only so an operator binding to
/// loopback or one interface does not unexpectedly expose another address
/// family.
fn ipv6_wildcard_for(bind: &str, default_port: u16) -> Option<String> {
    let normalized = bind_with_default_port(bind, default_port);
    let port = normalized.strip_prefix("0.0.0.0:")?;
    Some(format!("[::]:{port}"))
}

/// Whether the most recently added IPv6 listener also accepts IPv4-mapped
/// connections. Linux commonly defaults IPV6_V6ONLY to 0, while other hosts
/// may default it to 1. Inspect the actual socket instead of assuming either
/// behavior so wildcard RTMP/RTMPS binds stay portable.
fn last_listener_is_ipv6_dual_stack(server: &librtmp2::server::Server) -> Result<bool, String> {
    let fd = *server
        .listener_fds()
        .last()
        .ok_or_else(|| "listener fd missing after successful IPv6 bind".to_string())?;
    let mut only_v6: libc::c_int = 1;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_IPV6,
            libc::IPV6_V6ONLY,
            &mut only_v6 as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return Err(format!(
            "getsockopt(IPV6_V6ONLY) failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(only_v6 == 0)
}

fn bind_one_rtmp_listener(
    server: &mut librtmp2::server::Server,
    bind: &str,
    reuseport: bool,
    tls: Option<(&str, &str)>,
) -> Result<(), String> {
    let result = match (reuseport, tls) {
        (true, Some((cert, key))) => server.listen_tls_reuseport(bind, cert, key),
        (false, Some((cert, key))) => server.listen_tls(bind, cert, key),
        (true, None) => server.listen_reuseport(bind),
        (false, None) => server.listen(bind),
    };
    result.map_err(|e| e.to_string())
}

/// Bind a wildcard RTMP/RTMPS endpoint on IPv6 first. If that socket is
/// dual-stack it already covers IPv4; if the OS marks it v6-only, bind the
/// configured IPv4 wildcard too. Hosts/containers with IPv6 disabled fall
/// back to the original IPv4 listener instead of failing startup.
fn bind_rtmp_listener_set(
    server: &mut librtmp2::server::Server,
    bind: &str,
    default_port: u16,
    reuseport: bool,
    tls: Option<(&str, &str)>,
    label: &str,
    log_fallback: bool,
) -> Result<Vec<String>, String> {
    let primary = bind_with_default_port(bind, default_port);
    let Some(ipv6) = ipv6_wildcard_for(&primary, default_port) else {
        bind_one_rtmp_listener(server, &primary, reuseport, tls)
            .map_err(|e| format!("{label} bind on {primary} failed: {e}"))?;
        return Ok(vec![primary]);
    };

    match bind_one_rtmp_listener(server, &ipv6, reuseport, tls) {
        Ok(()) => {
            let dual_stack = last_listener_is_ipv6_dual_stack(server)
                .map_err(|e| format!("{label} IPv6 listener inspection failed: {e}"))?;
            if dual_stack {
                return Ok(vec![ipv6]);
            }

            bind_one_rtmp_listener(server, &primary, reuseport, tls)
                .map_err(|e| format!("{label} IPv4 bind on {primary} failed: {e}"))?;
            Ok(vec![ipv6, primary])
        }
        Err(ipv6_err) => {
            if log_fallback {
                crate::log_warn!(
                    "{label} IPv6 wildcard bind on {ipv6} unavailable ({ipv6_err}); falling back to IPv4 {primary}"
                );
            }
            bind_one_rtmp_listener(server, &primary, reuseport, tls)
                .map_err(|e| format!("{label} IPv4 fallback bind on {primary} failed: {e}"))?;
            Ok(vec![primary])
        }
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

/// Runs `on_close` for a closed connection on the auth worker thread (see
/// `AuthWorkerHandle::try_submit_close`) instead of inline, in order after
/// any authorization still queued for the same connection; inline only
/// when the worker isn't running. With HA clustering
/// active it stays inline: ownership releases then go through Raft, and
/// shutdown relies on them finishing before the cluster manager stops.
fn close_conn_off_poll_thread(rtmp_bridge: &DbRtmpBridge, conn_id: u64) {
    #[cfg(feature = "cluster")]
    if rtmp_bridge.cluster_manager().is_some() {
        rtmp_bridge.on_close(conn_id);
        return;
    }
    let submitted = AUTH_WORKER
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|h| h.try_submit_close(conn_id)));
    if !matches!(submitted, Some(Ok(()))) {
        rtmp_bridge.on_close(conn_id);
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
fn apply_auth_completion(
    server: &mut librtmp2::server::Server,
    tracked: &HashMap<u64, TrackedConn>,
    completion: &AuthCompletion,
) {
    match completion.kind {
        AuthKind::Publish => {
            if completion.allow && tracked.contains_key(&completion.conn_id) {
                bump_publish_generation(completion.conn_id);
            }
            let _ = server.complete_publish_authorization(completion.conn_id, completion.allow);
        }
        AuthKind::Play => {
            let _ = server.complete_play_authorization(completion.conn_id, completion.allow);
        }
    }
}

fn drain_auth_completions(
    server: &mut librtmp2::server::Server,
    tracked: &HashMap<u64, TrackedConn>,
) -> bool {
    let mut any_completed = false;
    // Sharded RTMP thread: drain this shard's own fan-out receiver instead
    // of the single global one (see `SHARD_AUTH_COMPLETIONS_RX`).
    let handled_locally = SHARD_AUTH_COMPLETIONS_RX.with(|cell| {
        let mut slot = cell.borrow_mut();
        let Some(rx) = slot.as_mut() else {
            return false;
        };
        while let Ok(completion) = rx.try_recv() {
            any_completed = true;
            apply_auth_completion(server, tracked, &completion);
        }
        true
    });
    if handled_locally {
        return any_completed;
    }

    let Ok(guard) = AUTH_COMPLETIONS_RX.lock() else {
        return false;
    };
    let Some(rx) = guard.as_ref() else {
        return false;
    };
    while let Ok(completion) = rx.try_recv() {
        any_completed = true;
        apply_auth_completion(server, tracked, &completion);
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
    /// `Some((baseline, set_at))` from the moment this connection started playing
    /// (baseline = `Conn::media_bytes_sent` at that instant) until at least
    /// one media byte has been queued to it since -- i.e. until its first
    /// relayed frame. Kept alongside `publishing`/`playing` in the poll
    /// loop's fast-interval check: a player that has been accepted but has
    /// not yet received any media is still effectively "negotiating" from
    /// the viewer's perspective, even though its RTMP session already says
    /// `playing`. Cleared after FIRST_FRAME_GRACE_MS even if no media arrives.
    awaiting_first_frame: Option<(u64, Instant)>,
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

/// Whether any tracked connection still needs the fast poll interval: not
/// yet publishing+playing, or playing but its first relayed frame hasn't
/// reached it yet (see `TrackedConn::awaiting_first_frame`).
pub(crate) fn any_negotiating(tracked: &HashMap<u64, TrackedConn>) -> bool {
    tracked
        .values()
        .any(|c| (!c.publishing && !c.playing) || (c.playing && c.awaiting_first_frame.is_some()))
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
            entry.awaiting_first_frame = None;
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
    let just_authorized = drain_auth_completions(server, tracked);

    let mut current_ids = HashSet::new();
    let mut reject_indices = Vec::new();

    for (idx, conn) in server.connections.iter_mut().enumerate() {
        if conn.client_fd < 0 {
            continue;
        }
        let conn_id = conn.conn_id;
        current_ids.insert(conn_id);
        // Folded into this per-connection pass (was a separate full second
        // scan below) so a poll tick locks `rtmp_bridge`'s shared connection
        // map once per connection instead of twice; `update_rtt` debounces
        // internally to at most once per second per connection regardless.
        rtmp_bridge.update_rtt(conn_id, conn.rtt_ms);
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

        let has_authorized_session = rtmp_bridge.has_authorized_session(conn_id);
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
                entry.awaiting_first_frame = None;
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
            entry.awaiting_first_frame = Some((conn.media_bytes_sent, Instant::now()));
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
            if entry
                .awaiting_first_frame
                .is_some_and(|(baseline, set_at)| {
                    conn.media_bytes_sent > baseline
                        || set_at.elapsed() >= Duration::from_millis(FIRST_FRAME_GRACE_MS)
                })
            {
                entry.awaiting_first_frame = None;
            }
            rtmp_bridge.update_player_stats(conn_id, conn.media_bytes_sent);
        }
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
            close_conn_off_poll_thread(rtmp_bridge, conn_id);
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
    revoked_viewers: Arc<Mutex<HashMap<String, Instant>>>,
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
        let revoked_viewers = Arc::new(Mutex::new(HashMap::new()));

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
        // Periodic publisher/player stats are queued in memory by the RTMP
        // poll threads and written here in one transaction per interval,
        // instead of one SQLite transaction per connection per second on
        // the poll threads themselves (which blocked relay for every
        // connection on the shard while it ran).
        let stats_flush_db = Arc::clone(&self.db);
        let stats_flush_stop = Arc::clone(&rtmp_stop);
        let stats_flush_thread = std::thread::Builder::new()
            .name("db-stats-flush".to_string())
            .spawn(move || {
                while !stats_flush_stop.load(Ordering::Relaxed) {
                    for _ in 0..STATS_FLUSH_INTERVAL_TICKS {
                        if stats_flush_stop.load(Ordering::Relaxed) {
                            break;
                        }
                        std::thread::sleep(STATS_FLUSH_TICK);
                    }
                    stats_flush_db.flush_pending_stats();
                }
            })
            .map_err(|e| format!("failed to spawn stats flush thread: {e}"))?;
        let final_stats_flush_db = Arc::clone(&self.db);
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

        let per_addr_caps_configured = rtmp_max_connections_per_addr != i32::MAX
            || self.config.rtmp_max_pending_tls_per_addr != i32::MAX;
        #[cfg(feature = "cluster")]
        let n_shards = resolve_shard_count(
            media_output_config.enabled(),
            cluster_enabled,
            per_addr_caps_configured,
        );
        #[cfg(not(feature = "cluster"))]
        let n_shards = resolve_shard_count(
            media_output_config.enabled(),
            false,
            per_addr_caps_configured,
        );
        // Never shard past the configured connection cap: `rtmp_max_conn` is
        // always >= 1 (see `parse_max_connections`), so this also guarantees
        // `per_shard_max_conn` below floors to at least 1 without needing to
        // bump it back up -- more shards than `rtmp_max_conn` would otherwise
        // silently raise the effective global cap from `rtmp_max_conn` to
        // `n_shards` (each shard's own floor-of-1 minimum summing past it).
        let n_shards = n_shards.min(rtmp_max_conn as usize).max(1);
        if n_shards > 1 {
            crate::log_info!("RTMP sharding enabled: {n_shards} worker threads");
        }
        let shard_conn_counts: Arc<Vec<AtomicUsize>> =
            Arc::new((0..n_shards).map(|_| AtomicUsize::new(0)).collect());
        // At n_shards > 1 relay export must always be on (cross-shard relay
        // depends on it, regardless of media outputs/clustering), sized
        // generously since it's now also the sole path getting frames to
        // viewers on other shards.
        let relay_export_bytes = if n_shards > 1 {
            relay_export_bytes.max(4 * 1024 * 1024)
        } else {
            relay_export_bytes
        };

        // The auth worker and the two globals its callbacks read
        // (`RTMP_BRIDGE`, `AUTH_WORKER`) are set up once, before any shard
        // thread starts -- there is exactly one auth worker for the whole
        // process regardless of shard count, since it just does SQLite/
        // cluster-ownership work keyed by conn_id, which is already unique
        // across shards (see `SHARD_ID_SPACE`).
        if let Ok(mut guard) = RTMP_BRIDGE.lock() {
            *guard = Some(Arc::clone(&rtmp_bridge));
        }
        // One wake fd per shard: whoever delivers that shard's auth
        // completions signals it, so the shard's poll loop picks a
        // publish/play decision up at once instead of on its next tick.
        let shard_wakes: Vec<Option<Arc<WakeFd>>> = (0..n_shards)
            .map(|_| match WakeFd::new() {
                Ok(wake) => Some(Arc::new(wake)),
                Err(e) => {
                    crate::log_warn!("RTMP: auth wake fd unavailable ({e}); using poll ticks");
                    None
                }
            })
            .collect();
        let worker_wake = if n_shards == 1 {
            shard_wakes[0].clone()
        } else {
            None
        };
        let (auth_worker_handle, auth_completions_rx) =
            auth_worker::spawn_with_notify(Arc::clone(&rtmp_bridge), move || {
                if let Some(wake) = &worker_wake {
                    wake.signal();
                }
            });
        if let Ok(mut guard) = AUTH_WORKER.lock() {
            *guard = Some(auth_worker_handle);
        }
        // Each shard may only apply a completion via its own `Server`, from
        // its own thread (librtmp2's single-thread-per-`Conn` rule), so with
        // more than one shard a small dispatcher thread fans the one
        // completion stream out to each shard's own receiver by
        // `shard_for_conn_id`. With exactly one shard this is just the
        // legacy global `AUTH_COMPLETIONS_RX` -- no dispatcher needed.
        let mut shard_auth_rxs: Vec<Option<std::sync::mpsc::Receiver<AuthCompletion>>> =
            (0..n_shards).map(|_| None).collect();
        let mut auth_dispatch_thread = None;
        if n_shards == 1 {
            if let Ok(mut guard) = AUTH_COMPLETIONS_RX.lock() {
                *guard = Some(auth_completions_rx);
            }
        } else {
            let (shard_auth_txs, shard_auth_rx_vec): (Vec<_>, Vec<_>) = (0..n_shards)
                .map(|_| std::sync::mpsc::channel::<AuthCompletion>())
                .unzip();
            for (i, rx) in shard_auth_rx_vec.into_iter().enumerate() {
                shard_auth_rxs[i] = Some(rx);
            }
            let dispatch_wakes = shard_wakes.clone();
            auth_dispatch_thread = Some(
                std::thread::Builder::new()
                    .name("rtmp-auth-dispatch".to_string())
                    .spawn(move || {
                        for completion in auth_completions_rx {
                            let shard =
                                shard_for_conn_id(completion.conn_id).min(shard_auth_txs.len() - 1);
                            if shard_auth_txs[shard].send(completion).is_ok()
                                && let Some(wake) = &dispatch_wakes[shard]
                            {
                                wake.signal();
                            }
                        }
                    })
                    .expect("failed to spawn auth-completion dispatcher thread"),
            );
        }

        // Cross-shard media relay: each shard broadcasts the frames its own
        // local publishers produced (the same relay-export buffer used for
        // media outputs/clustering) to every other shard's inbox, which
        // injects them into its own local relay/player fan-out via
        // `inject_relay_frame` -- the same mechanism librtmp2 already uses
        // to relay frames arriving from another HA-cluster node, just over
        // an in-process channel instead of the network. A publisher and its
        // viewers can land on different shards since SO_REUSEPORT
        // load-balances by connection, not by route (the route isn't known
        // until after the `publish`/`play` command, long after accept).
        let mut relay_rxs: Vec<Option<std::sync::mpsc::Receiver<ShardRelayMsg>>> =
            (0..n_shards).map(|_| None).collect();
        let relay_txs: Option<Arc<Vec<std::sync::mpsc::SyncSender<ShardRelayMsg>>>> =
            if n_shards > 1 {
                let mut txs = Vec::with_capacity(n_shards);
                for slot in relay_rxs.iter_mut() {
                    let (tx, rx) = std::sync::mpsc::sync_channel::<ShardRelayMsg>(
                        CROSS_SHARD_RELAY_QUEUE_CAPACITY,
                    );
                    txs.push(tx);
                    *slot = Some(rx);
                }
                Some(Arc::new(txs))
            } else {
                None
            };

        let (rtmp_dead_tx, mut rtmp_dead_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let mut ready_rxs = Vec::with_capacity(n_shards);
        let mut shard_threads = Vec::with_capacity(n_shards);

        // Every shard can wake every other one: cross-shard relay frames
        // land in the receiving shard's inbox and must be fanned out to its
        // viewers right away, not on that shard's next poll timeout.
        let all_shard_wakes: Arc<Vec<Option<Arc<WakeFd>>>> = Arc::new(shard_wakes.clone());
        for (shard_index, (shard_auth_rx, shard_wake)) in
            shard_auth_rxs.into_iter().zip(shard_wakes).enumerate()
        {
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            ready_rxs.push(ready_rx);
            let (shard_max_connections, dynamic_budget) =
                shard_connection_cap(rtmp_max_conn, n_shards, shard_index, rtmp_tls_enabled);
            let conn_budget = dynamic_budget.then(|| ShardConnBudget {
                counts: Arc::clone(&shard_conn_counts),
                index: shard_index,
                global: rtmp_max_conn.max(1) as usize,
            });
            let relay_rx = relay_rxs[shard_index].take();
            let relay_txs = relay_txs.clone();
            let all_shard_wakes = Arc::clone(&all_shard_wakes);
            let conn_id_base = (n_shards > 1).then(|| 1 + shard_index as u64 * SHARD_ID_SPACE);
            let rtmp_bind = rtmp_bind.clone();
            let rtmps_bind = rtmps_bind.clone();
            let rtmp_tls_cert = rtmp_tls_cert.clone();
            let rtmp_tls_key = rtmp_tls_key.clone();
            let rtmp_bridge = Arc::clone(&rtmp_bridge);
            let deleted_streams = Arc::clone(&deleted_streams);
            let sticky_deleted_streams = Arc::clone(&sticky_deleted_streams);
            let revoked_viewers = Arc::clone(&revoked_viewers);
            let rtmp_stop_clone = Arc::clone(&rtmp_stop);
            let media_output_thread_config = media_output_thread_config.clone();
            let media_output_db = Arc::clone(&media_output_db);
            let rtmp_dead_tx = rtmp_dead_tx.clone();
            #[cfg(feature = "cluster")]
            let cluster_enabled = cluster_enabled;

            let shard_thread = std::thread::Builder::new()
                .name(format!("rtmp-shard-{shard_index}"))
                .spawn(move || {
                    use librtmp2::server::Server as RtmpServer;
                    use librtmp2::types::ServerConfig as RtmpConfig;

                    if let Some(rx) = shard_auth_rx {
                        set_shard_auth_completions_rx(rx);
                    }

                    let cfg = RtmpConfig {
                        max_connections: shard_max_connections,
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
                            let _ = ready_tx.send(Err(msg));
                            return;
                        }
                    };
                    if let Some(base) = conn_id_base {
                        server.set_conn_id_base(base);
                    }
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
                    let reuseport = conn_id_base.is_some();
                    let rtmp_listeners = match bind_rtmp_listener_set(
                        &mut server,
                        &rtmp_bind,
                        1935,
                        reuseport,
                        None,
                        "RTMP",
                        shard_index == 0,
                    ) {
                        Ok(listeners) => listeners,
                        Err(msg) => {
                            crate::log_warn!("{msg}");
                            let _ = ready_tx.send(Err(msg));
                            return;
                        }
                    };
                    crate::log_info!(
                        "RTMP listening on {} (shard {shard_index})",
                        rtmp_listeners.join(", ")
                    );

                    if rtmp_tls_enabled {
                        let rtmps_listeners = match bind_rtmp_listener_set(
                            &mut server,
                            &rtmps_bind,
                            1936,
                            reuseport,
                            Some((&rtmp_tls_cert, &rtmp_tls_key)),
                            "RTMPS",
                            shard_index == 0,
                        ) {
                            Ok(listeners) => listeners,
                            Err(msg) => {
                                crate::log_warn!("{msg}");
                                let _ = ready_tx.send(Err(msg));
                                return;
                            }
                        };
                        crate::log_info!(
                            "RTMPS listening on {} (shard {shard_index})",
                            rtmps_listeners.join(", ")
                        );
                    }

                    let _ = ready_tx.send(Ok(()));

                    let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();
                    let mut media_outputs =
                        MediaOutputManager::new(media_output_thread_config, media_output_db);
                    let mut last_prune = Instant::now();
                    // `None` until the first `wait_for_readiness_or_timeout` call
                    // below reports which connections are actually readable --
                    // until then (and whenever it falls back to "assume everyone
                    // ready"), `Server::poll` processes every connection, same as
                    // before this readiness-aware path existed.
                    let mut readable: Option<HashSet<u64>> = None;
                    let mut readiness = ReadinessWaiter::new(shard_wake);
                    let mut exported_routes = ExportedRoutes::default();
                    // Route-end notices that didn't fit a full inbox yet:
                    // (target shard, app, stream_name). Unlike frames these
                    // must not be dropped, so they're retried every tick.
                    let mut pending_route_ends: Vec<(usize, String, String)> = Vec::new();

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
                        // logical publisher sessions. Only media outputs and clustering consult
                        // this map, so skip building it (and the per-publisher global-mutex
                        // lock in `publisher_generation`) on every poll tick for a plain
                        // deployment that has neither configured.
                        #[cfg(feature = "cluster")]
                        let need_publish_generations = media_outputs.enabled() || cluster_enabled;
                        #[cfg(not(feature = "cluster"))]
                        let need_publish_generations = media_outputs.enabled();
                        let publish_generations_before_poll: HashMap<u64, u64> =
                            if need_publish_generations {
                                tracked
                                    .iter()
                                    .filter(|(_, entry)| entry.publishing)
                                    .map(|(&conn_id, _)| (conn_id, publisher_generation(conn_id)))
                                    .collect()
                            } else {
                                HashMap::new()
                            };

                        if let Some(budget) = &conn_budget {
                            budget.before_poll(&mut server);
                        }
                        set_rtmp_poll_server(&mut server);
                        let poll_result = match &readable {
                            Some(r) => server.poll_ready(0, r),
                            None => server.poll(0),
                        };
                        clear_rtmp_poll_server();
                        if let Err(e) = poll_result {
                            crate::log_warn!("RTMP polling stopped: {e}");
                            break;
                        }
                        if let Some(budget) = &conn_budget {
                            budget.after_poll(&mut server, &rtmp_bridge);
                        }

                        let deleted_now: HashSet<String> =
                            deleted_streams.lock().iter().cloned().collect();
                        let revoked_now: HashSet<String> =
                            revoked_viewers.lock().keys().cloned().collect();

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

                        // Cross-shard relay: broadcast this shard's exported frames
                        // to every other shard's inbox (`relay_txs` is only `Some`
                        // when n_shards > 1 -- see its setup above `run`'s shard
                        // loop). Frame generation isn't consulted here the way it is
                        // for media outputs/clustering below: an unattributable
                        // republish batch is still fine to relay to viewers on
                        // another shard (unlike recording/HLS, which must not splice
                        // two publish sessions into one file/segment sequence), so
                        // there's no ambiguity to guard against.
                        //
                        // Each inbox is bounded (`CROSS_SHARD_RELAY_QUEUE_CAPACITY`)
                        // and this is a non-blocking `try_send`: a receiving shard
                        // that falls behind drops frames past that bound rather than
                        // this shard's poll tick blocking on a slow/stuck peer, which
                        // would stall every connection on *this* shard too.
                        if let Some(txs) = relay_txs.as_ref() {
                            exported_routes.record(&exported_frames);
                            let publishing: HashSet<u64> = server
                                .connections
                                .iter()
                                .filter(|c| c.state == librtmp2::types::ConnState::Publishing)
                                .map(|c| c.conn_id)
                                .collect();
                            for (app, stream_name) in exported_routes.take_ended(&publishing) {
                                for i in (0..txs.len()).filter(|&i| i != shard_index) {
                                    pending_route_ends.push((i, app.clone(), stream_name.clone()));
                                }
                            }
                            for (i, tx) in txs.iter().enumerate() {
                                if i == shard_index {
                                    continue;
                                }
                                let mut sent = false;
                                for frame in &exported_frames {
                                    sent |=
                                        tx.try_send(ShardRelayMsg::Frame(frame.clone())).is_ok();
                                }
                                // After this tick's frames, so the route is
                                // released only once its last frames are in.
                                pending_route_ends.retain(|(target, app, stream_name)| {
                                    if *target != i {
                                        return true;
                                    }
                                    let msg = ShardRelayMsg::RouteEnded {
                                        app: app.clone(),
                                        stream_name: stream_name.clone(),
                                    };
                                    let queued = tx.try_send(msg).is_ok();
                                    sent |= queued;
                                    !queued
                                });
                                if sent && let Some(wake) = &all_shard_wakes[i] {
                                    wake.signal();
                                }
                            }
                        }
                        // Frames injected here are only fanned out to this
                        // shard's viewers inside the next `server.poll`, so
                        // re-poll immediately (below) rather than holding them
                        // for a tick.
                        let mut injected_relay = false;
                        if let Some(rx) = relay_rx.as_ref() {
                            while let Ok(msg) = rx.try_recv() {
                                injected_relay = true;
                                match msg {
                                    ShardRelayMsg::Frame(frame) => {
                                        let _ = server.inject_relay_frame(
                                            &frame.app,
                                            &frame.stream_name,
                                            frame.frame_type,
                                            frame.timestamp,
                                            &frame.payload,
                                        );
                                    }
                                    ShardRelayMsg::RouteEnded { app, stream_name } => {
                                        server.release_injected_route(&app, &stream_name);
                                    }
                                }
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
                                    frame_type: MediaMessage::frame_type_from_librtmp2(
                                        frame.frame_type,
                                    ),
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
                        for &conn_id in &closed_ids {
                            tracked.remove(&conn_id);
                            close_conn_off_poll_thread(&rtmp_bridge, conn_id);
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
                            // Closed just above: its (possibly still queued)
                            // on_close is already on the way.
                            if closed_ids.contains(&conn_id)
                                || current_ids.contains(&conn_id)
                                || (!rtmp_bridge.is_registered(conn_id)
                                    && !rtmp_bridge.has_publisher(conn_id)
                                    && !rtmp_bridge.has_player(conn_id))
                            {
                                continue;
                            }
                            close_conn_off_poll_thread(&rtmp_bridge, conn_id);
                            clear_publish_generation(conn_id);
                        }

                        // This bookkeeping only reclaims markers for connections that
                        // are already gone, so -- unlike the poll interval itself --
                        // it does not need to run on every fast tick. Each of these
                        // derived sets locks `rtmp_bridge`'s shared connection map
                        // once per tracked connection; rebuilding them at the full
                        // 1ms negotiating cadence during a many-viewer join burst
                        // turns that lock into a bottleneck the joining connections
                        // contend on, which is counterproductive. See
                        // `PRUNE_INTERVAL_MS`.
                        if last_prune.elapsed() >= Duration::from_millis(PRUNE_INTERVAL_MS) {
                            last_prune = Instant::now();

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
                            let live_stream_ids =
                                live_stream_ids_for_deleted_markers(&tracked, &rtmp_bridge);
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
                            revoked_viewers.lock().retain(|viewer_id, inserted_at| {
                                live_viewer_ids.contains(viewer_id)
                                    || inserted_at.elapsed()
                                        < Duration::from_millis(REVOKED_VIEWER_GRACE_MS)
                            });
                        }

                        let negotiating = any_negotiating(&tracked);
                        // An authorization applied this tick (e.g. a play just
                        // got Play.Start) has follow-up work that only runs
                        // inside the next `server.poll`: replaying the cached
                        // codec headers and keyframe to the new player. Its
                        // reply is usually flushed already, so nothing would
                        // wake the wait early -- re-poll immediately instead of
                        // sleeping a tick before the viewer's first frame.
                        let poll_interval_ms = if just_authorized || injected_relay {
                            0
                        } else if negotiating {
                            POLL_INTERVAL_FAST_MS
                        } else {
                            POLL_INTERVAL_MS
                        };
                        readable = readiness.wait(&server, poll_interval_ms);
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
                })
                .expect("failed to spawn RTMP shard thread");
            shard_threads.push(shard_thread);
        }

        let mut startup_error: Option<String> = None;
        for ready_rx in ready_rxs {
            match ready_rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    startup_error = Some(e);
                    break;
                }
                Err(_) => {
                    startup_error =
                        Some("RTMP startup thread exited before reporting readiness".to_string());
                    break;
                }
            }
        }
        if let Some(e) = startup_error {
            // A later shard failed to bind/init after earlier shards already
            // started listening and polling. Stop and join every shard
            // thread spawned so far -- each checks `rtmp_stop` at the top of
            // its poll loop (see the `loop { if rtmp_stop_clone.load(...) `
            // above) -- rather than returning this error with those shards'
            // listeners, auth routing, and relay channels left running.
            rtmp_stop.store(true, Ordering::Relaxed);
            for shard_thread in shard_threads.drain(..) {
                let _ = shard_thread.join();
            }

            // Tear down the auth pipeline as part of failed startup too.
            // Dropping the submission handle closes the worker request
            // channel; once the worker exits, its completion channel closes
            // and the dispatcher can be joined instead of being left
            // detached after run returns an error.
            if let Ok(mut guard) = AUTH_COMPLETIONS_RX.lock() {
                guard.take();
            }
            if let Ok(mut guard) = AUTH_WORKER.lock() {
                guard.take();
            }
            if let Some(dispatch_thread) = auth_dispatch_thread.take() {
                let _ = dispatch_thread.join();
            }
            if let Ok(mut guard) = RTMP_BRIDGE.lock() {
                guard.take();
            }
            return Err(e);
        }

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
        .with_graceful_shutdown(async move {
            tokio::select! {
                () = shutdown_signal() => {},
                _ = rtmp_dead_rx.recv() => {
                    crate::log_error!(
                        "An RTMP shard thread exited unexpectedly; shutting down HTTP so the process does not keep serving a half-dead API"
                    );
                }
            }
        })
        .await
        .map_err(|e| format!("HTTP server error: {e}"));

        crate::log_info!("Shutting down...");
        // Stop and join every RTMP shard thread before tearing down Raft:
        // their on_close callbacks release publisher ownership through the
        // coordinator, and running them after `shutdown_blocking()` would
        // have those releases fail against an already-shut-down cluster
        // manager, leaving durable ownership rows behind that block
        // publishers routed to other nodes until the next failure sweep.
        rtmp_stop.store(true, Ordering::Relaxed);
        for shard_thread in shard_threads {
            let _ = shard_thread.join();
        }
        crate::log_info!("RTMP shard threads joined.");
        let _ = stats_flush_thread.join();
        // Stats the shards queued while shutting down.
        final_stats_flush_db.flush_pending_stats();
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
        AUTH_COMPLETIONS_RX, ReadinessSource, ReadinessWaiter, ServerApp, TrackedConn,
        any_negotiating, bind_rtmp_listener_set, bind_with_default_port, drain_auth_completions,
        drain_deleted_stream_roles, eviction_stream_id, ipv6_wildcard_for,
        live_stream_ids_for_deleted_markers, newest_conn_id, should_evict_idle_conn,
        wait_for_readiness_or_timeout,
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
    fn negotiating_stays_true_while_playing_conn_awaits_its_first_frame() {
        let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();
        tracked.insert(
            1,
            TrackedConn {
                connected: true,
                publishing: true,
                playing: true,
                awaiting_first_frame: Some((0, Instant::now())),
                ..Default::default()
            },
        );
        assert!(
            any_negotiating(&tracked),
            "a viewer that is playing but hasn't received its first relayed \
             frame yet must still keep the poll loop on the fast interval"
        );
    }

    #[test]
    fn negotiating_clears_once_every_conn_is_settled() {
        let mut tracked: HashMap<u64, TrackedConn> = HashMap::new();
        tracked.insert(
            1,
            TrackedConn {
                connected: true,
                publishing: true,
                playing: true,
                awaiting_first_frame: None,
                ..Default::default()
            },
        );
        assert!(
            !any_negotiating(&tracked),
            "a fully settled publish+play connection with no pending first \
             frame must let the poll loop fall back to the slow interval"
        );
    }

    fn free_local_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    fn unbound_rtmp_server(max_connections: i32) -> librtmp2::server::Server {
        let cfg = librtmp2::types::ServerConfig {
            max_connections,
            chunk_size: 4096,
            tls_enabled: 0,
            tls_cert_file: std::ptr::null(),
            tls_key_file: std::ptr::null(),
            tls_ca_file: std::ptr::null(),
            tls_insecure: 0,
            max_pending_tls_per_addr: i32::MAX,
            max_connections_per_addr: i32::MAX,
        };
        librtmp2::server::Server::new(cfg).unwrap()
    }

    fn listening_rtmp_server(max_connections: i32, port: u16) -> librtmp2::server::Server {
        let mut server = unbound_rtmp_server(max_connections);
        server.listen(&format!("127.0.0.1:{port}")).unwrap();
        server
    }

    /// A listener-only wake returns at once (so the connection is accepted
    /// without delay); a repeat with no accept in between backs off.
    fn assert_listener_backoff(mut waiter: ReadinessWaiter) {
        let port = free_local_port();
        let server = listening_rtmp_server(0, port);
        let _pending = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let start = Instant::now();
        assert_eq!(waiter.wait(&server, 50).map(|ids| ids.len()), Some(0));
        assert!(
            start.elapsed() < Duration::from_millis(5),
            "a new connection must not wait out a backoff before accept"
        );

        // Not accepted (nothing polled the server): still readable.
        let start = Instant::now();
        assert_eq!(waiter.wait(&server, 50).map(|ids| ids.len()), Some(0));
        assert!(
            start.elapsed() >= Duration::from_millis(5),
            "an unserviceable listener must not turn the wait into a busy loop"
        );
    }

    #[test]
    fn shard_count_defaults_to_cpus_only_where_behaviour_is_unchanged() {
        use super::shard_count_for;
        // Unset: CPUs, capped.
        assert_eq!(shard_count_for(None, 2, false, false, false), 2);
        assert_eq!(shard_count_for(None, 16, false, false, false), 4);
        assert_eq!(shard_count_for(None, 1, false, false, false), 1);
        // Features that aren't shard-aware, or per-address caps, keep one.
        assert_eq!(shard_count_for(None, 8, true, false, false), 1);
        assert_eq!(shard_count_for(None, 8, false, true, false), 1);
        assert_eq!(shard_count_for(None, 8, false, false, true), 1);
        // Explicit setting wins (clamped), except where sharding can't work.
        assert_eq!(shard_count_for(Some(1), 8, false, false, false), 1);
        assert_eq!(shard_count_for(Some(6), 2, false, false, true), 6);
        assert_eq!(shard_count_for(Some(1000), 2, false, false, false), 32);
        assert_eq!(shard_count_for(Some(4), 8, true, false, false), 1);
    }

    #[test]
    fn exported_routes_end_when_their_publisher_stops() {
        use super::ExportedRoutes;
        let frame = |conn_id: u64, stream: &str| librtmp2::RelayFrame {
            frame_type: librtmp2::types::FrameType::Video,
            timestamp: 0,
            payload: Vec::new(),
            cache_payload: None,
            app: "live".to_string(),
            stream_name: stream.to_string(),
            publisher_conn_id: conn_id,
        };
        let mut routes = ExportedRoutes::default();
        routes.record(&[
            frame(1, "a"),
            frame(2, "b"),
            // Injected from another shard: never announced back.
            frame(1 << 63 | 5, "c"),
        ]);
        assert!(routes.take_ended(&HashSet::from([1, 2])).is_empty());
        // Publisher 2 disconnected: only its route ends, and only once.
        assert_eq!(
            routes.take_ended(&HashSet::from([1])),
            vec![("live".to_string(), "b".to_string())]
        );
        assert!(routes.take_ended(&HashSet::from([1])).is_empty());
        let mut ended = routes.take_ended(&HashSet::new());
        ended.sort();
        assert_eq!(ended, vec![("live".to_string(), "a".to_string())]);
    }

    #[test]
    fn tls_shards_split_the_connection_cap_statically() {
        use super::shard_connection_cap;
        // Single shard: the cap as configured, no budget.
        assert_eq!(shard_connection_cap(10, 1, 0, true), (10, false));
        assert_eq!(shard_connection_cap(10, 1, 0, false), (10, false));
        // Plaintext: full cap per shard, narrowed by the per-tick budget.
        assert_eq!(shard_connection_cap(10, 4, 3, false), (10, true));
        // RTMPS: fixed shares that add up to exactly the global cap, since
        // pending TLS handshakes are invisible to the other shards.
        let shares: Vec<i32> = (0..4)
            .map(|i| shard_connection_cap(10, 4, i, true).0)
            .collect();
        assert_eq!(shares, vec![3, 3, 2, 2]);
        assert_eq!(shares.iter().sum::<i32>(), 10);
        assert!((0..4).all(|i| !shard_connection_cap(10, 4, i, true).1));
        let shares: Vec<i32> = (0..4)
            .map(|i| shard_connection_cap(4, 4, i, true).0)
            .collect();
        assert_eq!(shares, vec![1, 1, 1, 1]);
    }

    #[test]
    fn shard_conn_budget_keeps_the_global_cap_exact() {
        use super::ShardConnBudget;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counts: Arc<Vec<AtomicUsize>> = Arc::new((0..2).map(|_| AtomicUsize::new(0)).collect());
        let db = Arc::new(Db::open(":memory:").unwrap());
        let bridge = DbRtmpBridge::new(db, Arc::new(parking_lot::Mutex::new(HashSet::new())));
        let budget = ShardConnBudget {
            counts: Arc::clone(&counts),
            index: 0,
            global: 3,
        };
        let mut server = unbound_rtmp_server(3);

        // The other shard holds 2: this one may take only 1 more.
        counts[1].store(2, Ordering::Relaxed);
        budget.before_poll(&mut server);
        assert_eq!(server.config.max_connections, 1);

        // Both shards accepted at once and overshot: this shard now has 3
        // new connections, 5 in total. The 2 newest are closed.
        for id in 1..=3u64 {
            let mut conn = librtmp2::session::conn::Conn::new();
            conn.conn_id = id;
            conn.client_fd = 1000 + id as i32;
            server.connections.push(conn);
        }
        budget.after_poll(&mut server, &bridge);
        let open: Vec<u64> = server
            .connections
            .iter()
            .filter(|c| c.client_fd >= 0)
            .map(|c| c.conn_id)
            .collect();
        assert_eq!(open, vec![1]);
        assert_eq!(counts[0].load(Ordering::Relaxed), 1);

        // Everything else full: librtmp2's 0 means unlimited, so the cap
        // floors at 1 and relies on the trim above.
        counts[1].store(3, Ordering::Relaxed);
        budget.before_poll(&mut server);
        assert_eq!(server.config.max_connections, 1);
    }

    #[test]
    fn listener_backoff_counts_accepts_even_when_closes_keep_the_count_flat() {
        let port = free_local_port();
        let mut server = listening_rtmp_server(0, port);
        let mut waiter = ReadinessWaiter::with_source(ReadinessSource::Poll);
        let first = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        accept_one(&mut server);

        let _second = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(waiter.wait(&server, 50).map(|r| r.len()), Some(0));

        // Accept the second connection and reap the first, so the count is
        // back to 1 even though an accept happened.
        drop(first);
        for _ in 0..200 {
            server.poll(0).unwrap();
            if server.connections.len() == 1 && newest_conn_id(&server) > 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(server.connections.len(), 1);

        let _third = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let start = Instant::now();
        assert_eq!(waiter.wait(&server, 50).map(|r| r.len()), Some(0));
        assert!(
            start.elapsed() < Duration::from_millis(5),
            "an accept since the last listener-only wake must not trigger the backoff"
        );
    }

    #[test]
    fn listener_only_readiness_backs_off_only_without_accept_progress() {
        assert_listener_backoff(ReadinessWaiter::with_source(ReadinessSource::Poll));
    }

    #[test]
    fn pending_outbound_bytes_wake_the_wait_on_writability() {
        let port = free_local_port();
        let mut server = listening_rtmp_server(0, port);
        let _client = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        for _ in 0..50 {
            server.poll(0).unwrap();
            if !server.connections.is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(server.connections.len(), 1);

        // Nothing queued and nothing to read: the wait honors its timeout.
        let start = Instant::now();
        let ready = wait_for_readiness_or_timeout(&server, 100);
        assert_eq!(ready.map(|ids| ids.len()), Some(0));
        assert!(start.elapsed() >= Duration::from_millis(50));

        // Queued outbound bytes on a writable socket must end the wait
        // right away so they get flushed, without marking the connection
        // recv-ready (it sent nothing).
        server.connections[0]
            .send_buffer
            .write(b"queued media")
            .unwrap();
        let start = Instant::now();
        let ready = wait_for_readiness_or_timeout(&server, 1000);
        assert_eq!(ready.map(|ids| ids.len()), Some(0));
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "a writable connection with queued bytes must not wait out the poll interval"
        );
    }

    #[cfg(target_os = "linux")]
    fn epoll_waiter() -> ReadinessWaiter {
        let waiter = ReadinessWaiter::new(None);
        assert!(matches!(waiter.source, ReadinessSource::Epoll(_)));
        waiter
    }

    fn accept_one(server: &mut librtmp2::server::Server) {
        let before = server.connections.len();
        for _ in 0..200 {
            server.poll(0).unwrap();
            if server.connections.len() > before {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("connection was not accepted");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn epoll_reports_readable_connections_and_honors_timeout() {
        use std::io::Write;
        let port = free_local_port();
        let mut server = listening_rtmp_server(0, port);
        let mut waiter = epoll_waiter();
        let mut client = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        accept_one(&mut server);
        let conn_id = server.connections[0].conn_id;

        let start = Instant::now();
        assert_eq!(waiter.wait(&server, 100).map(|r| r.len()), Some(0));
        assert!(start.elapsed() >= Duration::from_millis(50));

        client.write_all(&[3u8]).unwrap();
        let start = Instant::now();
        let ready = waiter.wait(&server, 1000).unwrap();
        assert!(ready.contains(&conn_id));
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn epoll_wakes_on_writability_only_while_bytes_are_queued() {
        let port = free_local_port();
        let mut server = listening_rtmp_server(0, port);
        let mut waiter = epoll_waiter();
        let _client = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        accept_one(&mut server);

        server.connections[0]
            .send_buffer
            .write(b"queued media")
            .unwrap();
        let start = Instant::now();
        assert_eq!(waiter.wait(&server, 1000).map(|r| r.len()), Some(0));
        assert!(start.elapsed() < Duration::from_millis(500));

        // Drained again: EPOLLOUT interest must be dropped, or a writable
        // idle socket would spin the loop.
        let queued = server.connections[0].send_buffer.available();
        server.connections[0].send_buffer.drain(queued);
        let start = Instant::now();
        assert_eq!(waiter.wait(&server, 100).map(|r| r.len()), Some(0));
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn epoll_tracks_connections_replaced_on_a_reused_fd() {
        use std::io::Write;
        let port = free_local_port();
        let mut server = listening_rtmp_server(0, port);
        let mut waiter = epoll_waiter();
        let first = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        accept_one(&mut server);
        let old_fd = server.connections[0].client_fd;
        assert_eq!(waiter.wait(&server, 20).map(|r| r.len()), Some(0));

        // Close the first connection server-side, then accept a second one,
        // which the kernel typically hands the same fd number.
        drop(first);
        for _ in 0..200 {
            server.poll(0).unwrap();
            if server.connections.is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(server.connections.is_empty());
        let mut second = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        accept_one(&mut server);
        let new_conn = &server.connections[0];
        let (new_fd, new_id) = (new_conn.client_fd, new_conn.conn_id);

        second.write_all(&[3u8]).unwrap();
        let ready = waiter.wait(&server, 1000).unwrap();
        assert!(
            ready.contains(&new_id),
            "fd {new_fd} (previously {old_fd}) must report the new connection's id"
        );
    }

    #[cfg(target_os = "linux")]
    #[cfg(target_os = "linux")]
    #[test]
    fn epoll_listener_only_readiness_backs_off_only_without_accept_progress() {
        assert_listener_backoff(epoll_waiter());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wake_fd_cuts_the_wait_short_and_rearms() {
        let port = free_local_port();
        let server = listening_rtmp_server(0, port);
        let wake = Arc::new(super::WakeFd::new().unwrap());
        let mut waiter = ReadinessWaiter::new(Some(Arc::clone(&wake)));
        assert!(matches!(waiter.source, ReadinessSource::Epoll(_)));

        let signaller = Arc::clone(&wake);
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            signaller.signal();
        });
        let start = Instant::now();
        assert_eq!(waiter.wait(&server, 2000).map(|r| r.len()), Some(0));
        let woke_after = start.elapsed();
        t.join().unwrap();
        assert!(
            woke_after < Duration::from_millis(1000),
            "a signal must end the wait early, took {woke_after:?}"
        );

        // Drained: with no new signal the next wait honors its timeout.
        let start = Instant::now();
        assert_eq!(waiter.wait(&server, 100).map(|r| r.len()), Some(0));
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn connection_cap_drops_listeners_from_the_poll_set() {
        let port = free_local_port();
        let mut server = listening_rtmp_server(1, port);
        let _active = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        server.poll(0).unwrap();
        assert_eq!(
            server.connections.len(),
            1,
            "the first client must be accepted so the cap is reached"
        );
        let _queued = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let start = Instant::now();
        let ready = wait_for_readiness_or_timeout(&server, 50);

        assert_eq!(ready.map(|ids| ids.len()), Some(0));
        assert!(
            start.elapsed() >= Duration::from_millis(25),
            "at the connection cap the listener must be excluded so the wait \
             honors its timeout instead of spinning"
        );
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
        let tracked: HashMap<u64, TrackedConn> = HashMap::new();

        // Nothing queued: the poll loop must not force a fast follow-up tick.
        assert!(!drain_auth_completions(&mut server, &tracked));

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
            drain_auth_completions(&mut server, &tracked),
            "a drained completion must be reported so the poll loop can skip \
             the idle sleep on this tick"
        );
        assert!(
            !drain_auth_completions(&mut server, &tracked),
            "the channel is now empty; no fast follow-up is needed"
        );
        assert_eq!(
            super::publisher_generation(999),
            0,
            "an untracked connection's completion must not re-create generation state"
        );

        // A completion for a connection that is still tracked bumps its
        // publish generation (the line the untracked case above must skip).
        let mut tracked_with_conn: HashMap<u64, TrackedConn> = HashMap::new();
        tracked_with_conn.entry(999).or_default();
        let (tx2, rx2) = sync_channel(4);
        tx2.send(AuthCompletion {
            kind: AuthKind::Publish,
            conn_id: 999,
            allow: true,
        })
        .unwrap();
        if let Ok(mut guard) = AUTH_COMPLETIONS_RX.lock() {
            *guard = Some(rx2);
        }
        assert!(drain_auth_completions(&mut server, &tracked_with_conn));
        assert_eq!(super::publisher_generation(999), 1);
        super::clear_publish_generation(999);

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
    fn ipv6_wildcard_is_added_only_for_ipv4_wildcard_binds() {
        assert_eq!(
            ipv6_wildcard_for("0.0.0.0:1935", 1935),
            Some("[::]:1935".to_string())
        );
        assert_eq!(
            ipv6_wildcard_for("0.0.0.0", 1936),
            Some("[::]:1936".to_string())
        );
        assert_eq!(ipv6_wildcard_for("127.0.0.1:1935", 1935), None);
        assert_eq!(ipv6_wildcard_for("[::1]:1935", 1935), None);
    }

    #[test]
    fn wildcard_listener_accepts_ipv4_and_ipv6_when_available() {
        let port_probe = std::net::TcpListener::bind("[::]:0")
            .or_else(|_| std::net::TcpListener::bind("0.0.0.0:0"))
            .expect("reserve wildcard test port");
        let port = port_probe.local_addr().unwrap().port();
        drop(port_probe);

        let mut server = unbound_rtmp_server(8);
        let listeners = bind_rtmp_listener_set(
            &mut server,
            &format!("0.0.0.0:{port}"),
            port,
            false,
            None,
            "RTMP",
            false,
        )
        .expect("bind wildcard listener set");

        assert!(
            std::net::TcpStream::connect(("127.0.0.1", port)).is_ok(),
            "IPv4 loopback must reach a wildcard RTMP listener"
        );

        if std::net::TcpListener::bind("[::1]:0").is_ok() {
            assert!(
                listeners.iter().any(|bind| bind.starts_with("[::]")),
                "IPv6-capable hosts must create an IPv6 wildcard listener"
            );
            assert!(
                std::net::TcpStream::connect(("::1", port)).is_ok(),
                "IPv6 loopback must reach the same wildcard RTMP endpoint"
            );
        }
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
            awaiting_first_frame: Some((0, Instant::now())),
            ..Default::default()
        };

        let kick = drain_deleted_stream_roles(&mut conn, &mut entry, &bridge, 1, &deleted_now);
        assert!(!kick, "publisher on s1 must keep the connection");
        assert!(bridge.has_publisher(1));
        assert!(!bridge.has_player(1));
        assert!(entry.publishing);
        assert!(!entry.playing);
        assert!(
            entry.awaiting_first_frame.is_none(),
            "losing the playing role must clear the pending first-frame flag so \
             the poll loop can fall back to the slow interval"
        );
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
