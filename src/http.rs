//! HTTP server (axum) — REST API + stats endpoints.
//!
//! Endpoints:
//!   GET    /api/v1/health                       public `status`; details with Bearer
//!   GET    /api/v1/streams                      Bearer token (includes keys)
//!   POST   /api/v1/streams                      Bearer token, returns keys
//!   DELETE /api/v1/streams/:id                   Bearer token
//!
//!   GET    /stats?key=<stats_key>               flat JSON stats (no stream ids)
//!   GET    /api/v1/streams/:id/stats            Bearer = full JSON; key = flat public JSON
//!   GET    /stats-nginx?key=<stats_key>         XML (nginx-rtmp compatible)

use axum::extract::{ConnectInfo, DefaultBodyLimit, FromRequestParts, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, request::Parts};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::ServerConfig;
use crate::db::{Db, DbLookup, Stream, StreamAddError, StreamViewer};
use crate::keygen::keygen_stream_key;
use crate::rate_limit::{self, RateLimiter};
use crate::rtmp_bridge::DbRtmpBridge;

pub struct AppState {
    pub db: Arc<Db>,
    pub config: ServerConfig,
    pub rtmp_bridge: Arc<DbRtmpBridge>,
    /// Stream IDs deleted via this API while RTMP connections are active.
    /// The RTMP poll loop reads this set and evicts matching connections.
    pub deleted_streams: Arc<Mutex<HashSet<String>>>,
    /// Viewer slot IDs revoked via HTTP while RTMP player sessions are active.
    pub revoked_viewers: Arc<Mutex<HashSet<String>>>,
}

/// Build the Axum router, wiring all HTTP handlers to the shared application state.
pub fn router(state: Arc<AppState>) -> Router {
    let limiter = RateLimiter::new(
        state.config.http_rate_limit_config(),
        state.config.http_trusted_proxies.clone(),
    );
    Router::new()
        .route("/api/v1/health", get(handle_health))
        .route("/stats", get(handle_stats_json))
        .route("/stats-nginx", get(handle_stats_nginx))
        .route("/stat.xsl", get(handle_stat_xsl))
        .route(
            "/api/v1/streams",
            get(handle_streams_list).post(handle_stream_create),
        )
        .route("/api/v1/streams/{id}", delete(handle_stream_delete))
        .route("/api/v1/streams/{id}/stats", get(handle_stream_stats))
        .route(
            "/api/v1/streams/{id}/players",
            get(handle_stream_players_list).post(handle_stream_player_create),
        )
        .route(
            "/api/v1/streams/{id}/players/{player_id}",
            delete(handle_stream_player_delete),
        )
        .layer(DefaultBodyLimit::max(state.config.http_max_body_bytes))
        .layer(middleware::from_fn_with_state(
            limiter,
            rate_limit::middleware,
        ))
        .with_state(state)
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn http_peer(state: &AppState, addr: ClientAddr, headers: &HeaderMap) -> String {
    // Without a real peer address (e.g. `ConnectInfo` missing from a
    // non-standard embedding), there is no basis for deciding whether the
    // peer is a trusted proxy, so X-Forwarded-For must not be honored.
    let Some(peer) = addr.0 else {
        return "unknown".to_string();
    };
    rate_limit::resolve_client_ip(
        peer,
        headers.get("X-Forwarded-For"),
        &state.config.http_trusted_proxies,
    )
    .to_string()
}

/// Optional peer address for access logs. Missing in unit tests that use
/// `oneshot` without `ConnectInfo`; production always has it via
/// `into_make_service_with_connect_info`.
struct ClientAddr(Option<std::net::IpAddr>);

impl<S> FromRequestParts<S> for ClientAddr
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(ClientAddr(
            parts
                .extensions
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ConnectInfo(addr)| addr.ip()),
        ))
    }
}

fn log_http_access(method: &str, path: &str, peer: &str, status: StatusCode, detail: &str) {
    let code = status.as_u16();
    if detail.is_empty() {
        crate::log_info!("HTTP: {method} {path} from {peer} → {code}");
    } else {
        crate::log_info!("HTTP: {method} {path} from {peer} → {code} {detail}");
    }
}

// ---------- errors ----------

fn err_json(status: StatusCode, code: &str, msg: &str) -> Response {
    (
        status,
        Json(json!({"error": {"code": code, "message": msg}})),
    )
        .into_response()
}

fn err_xml(status: StatusCode, msg: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<rtmp><error>{}</error></rtmp>\n",
        xml_escape(msg)
    );
    xml_response(status, body)
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (
        status,
        [("Content-Type", "application/xml; charset=utf-8")],
        body,
    )
        .into_response()
}

fn public_stats_text(status: StatusCode, msg: &str) -> Response {
    (
        status,
        [("Content-Type", "text/plain; charset=utf-8")],
        msg.to_string(),
    )
        .into_response()
}

/// XML 1.0 forbids most control characters; the rest of the five reserved
/// characters are escaped so attacker-controlled strings (RTMP `app`,
/// stream names) can't inject markup into the stats document.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\t' | '\n' | '\r' => out.push(c),
            c if (c as u32) < 0x20 => {}
            c => out.push(c),
        }
    }
    out
}

// ---------- auth ----------

/// Constant-time string equality so token validation does not leak the
/// secret one byte at a time via response timing.
fn ct_str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let n = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..n {
        let ca = a.get(i).copied().unwrap_or(0);
        let cb = b.get(i).copied().unwrap_or(0);
        diff |= ca ^ cb;
    }
    diff == 0
}

fn bearer_ok(state: &AppState, headers: &HeaderMap) -> bool {
    if state.config.api_token.is_empty() {
        return false;
    }
    let Some(hdr) = headers.get("Authorization").and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some(tok) = hdr.strip_prefix("Bearer ") else {
        return false;
    };
    ct_str_eq(tok.trim(), &state.config.api_token)
}

fn stats_key_lookup(
    state: &AppState,
    key: &str,
    stream_id: Option<&str>,
) -> Option<crate::db::Stream> {
    if key.is_empty() {
        return None;
    }
    match state.db.stream_find_by_stats_key(key) {
        DbLookup::Ok(s) if stream_id.is_none_or(|id| s.id == id) => Some(s),
        DbLookup::Ok(_) | DbLookup::Missing | DbLookup::Failed => None,
    }
}

const STATS_MIN_RESPONSE: Duration = Duration::from_millis(50);

async fn pace_public_stats(start: Instant, response: Response) -> Response {
    if let Some(remaining) = STATS_MIN_RESPONSE.checked_sub(start.elapsed()) {
        tokio::time::sleep(remaining).await;
    }
    response
}

#[derive(Deserialize, Default)]
pub struct KeyQuery {
    #[serde(default)]
    key: String,
}

fn is_valid_stream_key_part(value: &str) -> bool {
    if value.is_empty() || value.len() > 63 {
        return false;
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Publish/play/stats keys: safe ASCII, no slashes, minimum entropy via length.
fn is_valid_access_key(value: &str) -> bool {
    crate::keygen::is_valid_access_key(value)
}

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

enum AccessKeyFieldError {
    Invalid,
    GenerationFailed,
}

fn resolve_or_generate_access_key(
    provided: Option<String>,
    prefix: &str,
) -> Result<String, AccessKeyFieldError> {
    match trim_optional_string(provided) {
        Some(key) => {
            if !is_valid_access_key(&key) {
                return Err(AccessKeyFieldError::Invalid);
            }
            Ok(key)
        }
        None => keygen_stream_key(prefix).map_err(|_| AccessKeyFieldError::GenerationFailed),
    }
}

const ACCESS_KEY_VALIDATION_MSG: &str = crate::keygen::ACCESS_KEY_VALIDATION_MSG;

fn access_keys_must_be_unique(keys: &[&str]) -> bool {
    let mut seen = HashSet::with_capacity(keys.len());
    keys.iter().all(|k| seen.insert(*k))
}

fn is_valid_display_name(value: &str) -> bool {
    !value.is_empty() && value.chars().count() <= 128 && !value.chars().any(char::is_control)
}

fn viewer_to_json(v: &StreamViewer) -> Value {
    json!({
        "id": v.id,
        "name": v.name,
        "play_key": v.play_key,
        "enabled": v.enabled,
        "created_at": v.created_at,
    })
}

fn stream_to_json(db: &Db, s: &Stream) -> Value {
    let players: Vec<Value> = db.viewer_list(&s.id).iter().map(viewer_to_json).collect();
    json!({
        "id": s.id,
        "name": s.name,
        "app": s.app,
        "publish_key": s.publish_key,
        "play_key": s.play_key,
        "stats_key": s.stats_key,
        "players": players,
        "enabled": s.enabled,
        "created_at": s.created_at,
    })
}

fn create_viewer_row(
    stream_id: &str,
    name: &str,
    play_key: &str,
    created_at: i64,
) -> Option<StreamViewer> {
    let viewer_id = keygen_stream_key(crate::keygen::PREFIX_VIEWER_ID).ok()?;
    Some(StreamViewer {
        id: viewer_id,
        stream_id: stream_id.to_string(),
        name: name.to_string(),
        play_key: play_key.to_string(),
        enabled: true,
        created_at,
    })
}

// ---------- JSON stats builder ----------

fn build_json_stats(db: &Db, stream_id: Option<&str>) -> Value {
    let (pubs, players) = match stream_id {
        Some(id) => (db.publisher_list(Some(id)), db.player_list(Some(id))),
        None => (db.publisher_list_all(), db.player_list_all()),
    };

    let now = now_ts();

    let streams: Vec<Value> = pubs
        .iter()
        .map(|p| {
            json!({
                "id": p.stream_id,
                "name": p.stream_name,
                "app": p.app,
                "uptime": (now - p.connected_at).max(0),
                "bitrate_kbps": p.bitrate_kbps,
                "rtt_ms": p.rtt_ms,
                "bytes_in": p.bytes_in,
                "video": {
                    "codec": p.video_codec,
                    "width": p.video_width,
                    "height": p.video_height,
                    "fps": p.fps,
                },
                "audio": { "codec": p.audio_codec },
            })
        })
        .collect();

    let players_json: Vec<Value> = players
        .iter()
        .map(|pl| {
            json!({
                "id": pl.id,
                "stream_name": pl.stream_name,
                "app": pl.app,
                "uptime": (now - pl.connected_at).max(0),
                "bitrate_kbps": pl.bitrate_kbps,
                "rtt_ms": pl.rtt_ms,
                "bytes_out": pl.bytes_out,
            })
        })
        .collect();

    json!({
        "streams": streams,
        "players": players_json,
        "summary": {
            "publishers": pubs.len(),
            "players": players.len(),
            "total_clients": pubs.len() + players.len(),
        },
    })
}

/// Key-protected public stats: flat JSON while live; `None` when offline.
fn build_public_json_stats(db: &Db, stream_id: &str) -> Option<Value> {
    let pubs = db.publisher_list(Some(stream_id));
    let p = pubs.first()?;
    let now = now_ts();

    Some(json!({
        "uptime": (now - p.connected_at).max(0),
        "bitrate_kbps": p.bitrate_kbps,
        "rtt_ms": p.rtt_ms,
        "bytes_in": p.bytes_in,
        "video": {
            "codec": p.video_codec,
            "width": p.video_width,
            "height": p.video_height,
            "fps": p.fps,
        },
        "audio": { "codec": p.audio_codec },
    }))
}

// ---------- XML stats (nginx-rtmp compatible) ----------

fn build_nginx_xml(db: &Db, stream_id: Option<&str>, redact_identifiers: bool) -> String {
    let (pubs, players) = match stream_id {
        Some(id) => (db.publisher_list(Some(id)), db.player_list(Some(id))),
        None => (db.publisher_list_all(), db.player_list_all()),
    };

    let now = now_ts();
    let app_name = if redact_identifiers {
        "live"
    } else {
        pubs.first()
            .map(|p| p.app.as_str())
            .or_else(|| players.first().map(|pl| pl.app.as_str()))
            .unwrap_or("live")
    };
    let mut out = String::with_capacity(8192);
    out.push_str(&format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
         <?xml-stylesheet type=\"text/xsl\" href=\"/stat.xsl\"?>\n<rtmp>\n  <server>\n\
         \x20\x20\x20\x20<application>\n      <name>{}</name>\n      <live>\n",
        xml_escape(app_name),
    ));

    // nginx-rtmp represents a stream as one <stream> element per stream name,
    // with one <client> child per connected session (publisher and players
    // alike). Emitting a separate <stream> per publisher/player — as this
    // used to — makes a viewer session shadow the publisher's bitrate under
    // the same (possibly redacted) name, since consumers like NOALBS match
    // on stream name and take the last hit.
    struct StreamGroup {
        label: String,
        uptime_ms: i64,
        bw_in: i64,
        bytes_in: u64,
        bw_out: i64,
        bytes_out: u64,
        publishing: bool,
        video: Option<(u32, u32, f64, String)>,
        audio: Option<(String, u32, u32)>,
        clients: String,
    }

    fn find_group<'g>(groups: &'g mut Vec<StreamGroup>, label: &str) -> &'g mut StreamGroup {
        if !groups.iter().any(|g| g.label == label) {
            groups.push(StreamGroup {
                label: label.to_string(),
                uptime_ms: 0,
                bw_in: 0,
                bytes_in: 0,
                bw_out: 0,
                bytes_out: 0,
                publishing: false,
                video: None,
                audio: None,
                clients: String::new(),
            });
        }
        groups.iter_mut().find(|g| g.label == label).unwrap()
    }

    let mut groups: Vec<StreamGroup> = Vec::new();

    for p in &pubs {
        let uptime_ms = (now - p.connected_at).max(0) * 1000;
        // librtmp2-server tracks one combined bitrate per publisher, not separate
        // audio/video bandwidth like nginx-rtmp does, so bw_video mirrors bw_in —
        // nginx-rtmp-compatible consumers (e.g. NOALBS) read bw_video for switching.
        let bw_in = (p.bitrate_kbps * 1000.0) as i64;
        let stream_label = if redact_identifiers {
            "stream"
        } else {
            p.stream_name.as_str()
        };

        let group = find_group(&mut groups, stream_label);
        group.uptime_ms = uptime_ms;
        group.bw_in = bw_in;
        group.bytes_in = p.bytes_in;
        group.publishing = true;
        if !p.video_codec.is_empty() {
            group.video = Some((p.video_width, p.video_height, p.fps, p.video_codec.clone()));
        }
        if !p.audio_codec.is_empty() {
            group.audio = Some((p.audio_codec.clone(), p.audio_sample_rate, p.audio_channels));
        }
        group.clients.push_str(&format!(
            "          <client>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<flashver>FMLE/3.0</flashver>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<dropped>0</dropped>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<avsync>0</avsync>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<timestamp>{uptime_ms}</timestamp>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<active>1</active>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<publisher>1</publisher>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20</client>\n",
        ));
    }

    for pl in &players {
        let uptime_ms = (now - pl.connected_at).max(0) * 1000;
        let bw_out = (pl.bitrate_kbps * 1000.0) as i64;
        let stream_label = if redact_identifiers {
            "stream"
        } else {
            pl.stream_name.as_str()
        };

        let group = find_group(&mut groups, stream_label);
        if group.clients.is_empty() {
            group.uptime_ms = uptime_ms;
        }
        group.bw_out += bw_out;
        group.bytes_out += pl.bytes_out;
        group.clients.push_str(&format!(
            "          <client>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<flashver>FMLE/3.0</flashver>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<dropped>0</dropped>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<avsync>0</avsync>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<timestamp>{uptime_ms}</timestamp>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<active>1</active>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<publisher>0</publisher>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20</client>\n",
        ));
    }

    for g in &groups {
        out.push_str(&format!(
            "        <stream>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<name>{}</name>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<time>{}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_in>{}</bw_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_in>{}</bytes_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_out>{}</bw_out>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_out>{}</bytes_out>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_audio>0</bw_audio>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_video>{}</bw_video>\n",
            xml_escape(&g.label),
            g.uptime_ms,
            g.bw_in,
            g.bytes_in,
            g.bw_out,
            g.bytes_out,
            g.bw_in,
        ));

        if g.publishing {
            out.push_str("        <publishing/>\n        <active/>\n");
        }

        if g.video.is_some() || g.audio.is_some() {
            // NOALBS's Nginx provider models <meta> as requiring both <video>
            // and <audio> children (neither is optional in its Rust struct),
            // so a <meta> with only one of them fails to deserialize and the
            // whole stream reads as unparseable — i.e. offline. Always emit
            // both; an empty self-closing element is valid since every field
            // inside Video/Audio on the NOALBS side is itself optional.
            out.push_str("          <meta>\n");

            if let Some((width, height, fps, codec)) = &g.video {
                out.push_str(&format!(
                    "            <video>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<width>{width}</width>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<height>{height}</height>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<frame_rate>{fps:.1}</frame_rate>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<codec>{}</codec>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<profile>baseline</profile>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<level>3.1</level>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20</video>\n",
                    xml_escape(codec),
                ));
            } else {
                out.push_str("            <video/>\n");
            }

            if let Some((codec, sample_rate, channels)) = &g.audio {
                out.push_str(&format!(
                    "            <audio>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<codec>{}</codec>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<sample_rate>{sample_rate}</sample_rate>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<channels>{channels}</channels>\n\
                     \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20</audio>\n",
                    xml_escape(codec),
                ));
            } else {
                out.push_str("            <audio/>\n");
            }

            out.push_str("          </meta>\n");
        }

        out.push_str(&g.clients);
        out.push_str("        </stream>\n");
    }

    out.push_str(&format!(
        "        <nclients>{}</nclients>\n      </live>\n    </application>\n  </server>\n</rtmp>\n",
        pubs.len() + players.len()
    ));

    out
}

// ---------- handlers ----------

async fn handle_health(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if bearer_ok(&state, &headers) {
        return Json(json!({
            "status": "ok",
            "timestamp": now_ts(),
            "rtmp_port": state.config.rtmp_port(),
            "rtmps_enabled": state.config.tls_enabled,
            "rtmps_port": state.config.rtmps_port(),
        }))
        .into_response();
    }
    Json(json!({"status": "ok"})).into_response()
}

async fn handle_stats_json(
    State(state): State<Arc<AppState>>,
    addr: ClientAddr,
    headers: HeaderMap,
    Query(q): Query<KeyQuery>,
) -> Response {
    let peer = http_peer(&state, addr, &headers);
    let start = Instant::now();
    if q.key.is_empty() {
        let status = StatusCode::UNAUTHORIZED;
        log_http_access("GET", "/stats", &peer, status, "stats_key required");
        return pace_public_stats(start, public_stats_text(status, "stats_key required")).await;
    }
    let Some(s) = stats_key_lookup(&state, &q.key, None) else {
        let status = StatusCode::FORBIDDEN;
        log_http_access("GET", "/stats", &peer, status, "invalid stats key");
        return pace_public_stats(start, public_stats_text(status, "Invalid stats key")).await;
    };
    let (response, detail) = match build_public_json_stats(&state.db, &s.id) {
        Some(body) => (Json(body).into_response(), format!("stream='{}'", s.id)),
        None => (
            public_stats_text(StatusCode::OK, "Stream offline"),
            format!("stream='{}' offline", s.id),
        ),
    };
    log_http_access("GET", "/stats", &peer, StatusCode::OK, &detail);
    pace_public_stats(start, response).await
}

/// Dark-themed nginx-rtmp-compatible XSLT stylesheet for `/stats-nginx`. The
/// XML response links here via an `<?xml-stylesheet?>` processing
/// instruction, so browsers render the raw XML as an HTML table instead —
/// same idea as `nginx-rtmp-module`'s classic `stat.xsl`, just dark.
const STAT_XSL: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="html" encoding="utf-8" indent="yes" doctype-system="about:legacy-compat"/>
<xsl:template match="/rtmp">
<html>
<head>
<title>librtmp2-server stats</title>
<meta charset="utf-8"/>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body {
    background: #0d1117; color: #c9d1d9; margin: 1rem;
    font-family: Roboto, -apple-system, "Segoe UI", sans-serif;
  }
  table { border-collapse: collapse; width: 100%; background: #161b22; border: 1px solid #21262d; }
  th, td { padding: 0.35rem 0.6rem; border-bottom: 1px solid #21262d; border-right: 1px solid #21262d; text-align: left; font-size: 0.85rem; }
  th:last-child, td:last-child { border-right: none; }
  tr:last-child td { border-bottom: none; }
  th { background: #0d1117; color: #8b949e; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.05em; }
  tbody tr:hover { background: #1c2128; }
  .state-live { color: #3fb950; font-weight: 600; }
  .state-off { color: #f85149; font-weight: 600; }
  .section { background: #21262d; font-weight: 600; }
  .clients { margin: 0; padding: 0.5rem 0.6rem 0.6rem; background: #0d1117; }
  .clients table { background: transparent; border: none; }
  .clients th, .clients td { font-size: 0.78rem; padding: 0.3rem 0.6rem; border-right: none; border-bottom: 1px solid #1c2128; }
  details summary { cursor: pointer; color: #58a6ff; font-size: 0.8rem; list-style: none; }
  details summary::-webkit-details-marker { display: none; }
  details summary::before { content: "▸ "; }
  details[open] summary::before { content: "▾ "; }
  .empty { color: #484f58; font-style: italic; padding: 0.6rem; }
</style>
</head>
<body>
<xsl:for-each select="server/application">
  <table>
    <thead>
      <tr>
        <th rowspan="2">RTMP</th>
        <th rowspan="2">#clients</th>
        <th colspan="4">Video</th>
        <th colspan="4">Audio</th>
        <th rowspan="2">In bytes</th>
        <th rowspan="2">Out bytes</th>
        <th rowspan="2">In bits/s</th>
        <th rowspan="2">Out bits/s</th>
        <th rowspan="2">State</th>
        <th rowspan="2">Time</th>
      </tr>
      <tr>
        <th>codec</th><th>bits/s</th><th>size</th><th>fps</th>
        <th>codec</th><th>bits/s</th><th>freq</th><th>chan</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td colspan="15">Accepted: <xsl:value-of select="live/nclients"/></td>
      </tr>
      <tr class="section">
        <td colspan="15"><xsl:value-of select="name"/></td>
      </tr>
      <xsl:choose>
        <xsl:when test="live/stream">
          <xsl:for-each select="live/stream">
          <tr>
            <td><xsl:value-of select="name"/></td>
            <td><xsl:value-of select="count(client)"/></td>
            <td><xsl:value-of select="meta/video/codec"/></td>
            <td><xsl:value-of select="round(bw_video div 1000)"/>K</td>
            <td><xsl:value-of select="meta/video/width"/>x<xsl:value-of select="meta/video/height"/></td>
            <td><xsl:value-of select="meta/video/frame_rate"/></td>
            <td><xsl:value-of select="meta/audio/codec"/></td>
            <td><xsl:value-of select="round(bw_audio div 1000)"/>K</td>
            <td><xsl:value-of select="meta/audio/sample_rate"/></td>
            <td><xsl:value-of select="meta/audio/channels"/></td>
            <td><xsl:value-of select="bytes_in"/></td>
            <td><xsl:value-of select="bytes_out"/></td>
            <td><xsl:value-of select="round(bw_in div 1000)"/>Kb/s</td>
            <td><xsl:value-of select="round(bw_out div 1000)"/>Kb/s</td>
            <td>
              <xsl:choose>
                <xsl:when test="active"><span class="state-live">LIVE</span></xsl:when>
                <xsl:otherwise><span class="state-off">OFFLINE</span></xsl:otherwise>
              </xsl:choose>
            </td>
            <td><xsl:value-of select="round(time div 1000)"/>s</td>
          </tr>
          <tr>
            <td colspan="16" style="padding: 0;">
              <details>
                <summary style="padding: 0.3rem 0.6rem;">
                  <xsl:value-of select="count(client)"/> client(s)
                </summary>
                <div class="clients">
                  <table>
                    <thead>
                      <tr><th>Role</th><th>Time</th><th>Dropped</th></tr>
                    </thead>
                    <tbody>
                      <xsl:for-each select="client">
                      <tr>
                        <td>
                          <xsl:choose>
                            <xsl:when test="publisher = 1">publisher</xsl:when>
                            <xsl:otherwise>player</xsl:otherwise>
                          </xsl:choose>
                        </td>
                        <td><xsl:value-of select="round(time div 1000)"/>s</td>
                        <td><xsl:value-of select="dropped"/></td>
                      </tr>
                      </xsl:for-each>
                    </tbody>
                  </table>
                </div>
              </details>
            </td>
          </tr>
          </xsl:for-each>
        </xsl:when>
        <xsl:otherwise>
          <tr><td colspan="16" class="empty">live streams: 0</td></tr>
        </xsl:otherwise>
      </xsl:choose>
    </tbody>
  </table>
</xsl:for-each>
</body>
</html>
</xsl:template>
</xsl:stylesheet>
"#;

async fn handle_stat_xsl(
    addr: ClientAddr,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Response {
    let peer = http_peer(&state, addr, &headers);
    log_http_access("GET", "/stat.xsl", &peer, StatusCode::OK, "");
    (
        StatusCode::OK,
        [("Content-Type", "text/xsl; charset=utf-8")],
        STAT_XSL,
    )
        .into_response()
}

async fn handle_stats_nginx(
    State(state): State<Arc<AppState>>,
    addr: ClientAddr,
    headers: HeaderMap,
    Query(q): Query<KeyQuery>,
) -> Response {
    let peer = http_peer(&state, addr, &headers);
    let start = Instant::now();
    if q.key.is_empty() {
        let status = StatusCode::UNAUTHORIZED;
        log_http_access("GET", "/stats-nginx", &peer, status, "stats_key required");
        return pace_public_stats(start, err_xml(status, "Missing stats key")).await;
    }
    let Some(s) = stats_key_lookup(&state, &q.key, None) else {
        let status = StatusCode::FORBIDDEN;
        log_http_access("GET", "/stats-nginx", &peer, status, "invalid stats key");
        return pace_public_stats(start, err_xml(status, "Invalid stats key")).await;
    };
    log_http_access(
        "GET",
        "/stats-nginx",
        &peer,
        StatusCode::OK,
        &format!("stream='{}'", s.id),
    );
    let response = xml_response(
        StatusCode::OK,
        build_nginx_xml(&state.db, Some(&s.id), true),
    );
    pace_public_stats(start, response).await
}

async fn handle_streams_list(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !bearer_ok(&state, &headers) {
        return err_json(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Missing or invalid token",
        );
    }
    // Bearer-authenticated admin view — includes keys for panels like librtmp2-server-panel.
    let list: Vec<Value> = state
        .db
        .stream_list()
        .iter()
        .map(|s| stream_to_json(&state.db, s))
        .collect();
    Json(list).into_response()
}

#[derive(Deserialize, Default)]
struct CreateStreamRequest {
    id: Option<String>,
    name: Option<String>,
    app: Option<String>,
    publish_key: Option<String>,
    play_key: Option<String>,
    stats_key: Option<String>,
}

async fn handle_stream_create(
    State(state): State<Arc<AppState>>,
    addr: ClientAddr,
    headers: HeaderMap,
    body: Option<Json<CreateStreamRequest>>,
) -> Response {
    const PATH: &str = "/api/v1/streams";
    let peer = http_peer(&state, addr, &headers);
    if !bearer_ok(&state, &headers) {
        log_http_access(
            "POST",
            PATH,
            &peer,
            StatusCode::UNAUTHORIZED,
            "missing or invalid token",
        );
        return err_json(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Missing or invalid token",
        );
    }

    let req = body.map(|Json(r)| r).unwrap_or_default();
    let Some(id) = req
        .id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    else {
        log_http_access(
            "POST",
            PATH,
            &peer,
            StatusCode::BAD_REQUEST,
            "missing 'id' field",
        );
        return err_json(StatusCode::BAD_REQUEST, "BAD_REQUEST", "Missing 'id' field");
    };
    let app = req
        .app
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "live".to_string());
    let name = req
        .name
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| id.clone());

    if !is_valid_stream_key_part(&id) {
        log_http_access(
            "POST",
            PATH,
            &peer,
            StatusCode::BAD_REQUEST,
            "invalid stream id",
        );
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "Stream id must be 1-63 characters and use only letters, numbers, dots, underscores, or hyphens",
        );
    }
    if !is_valid_stream_key_part(&app) {
        log_http_access("POST", PATH, &peer, StatusCode::BAD_REQUEST, "invalid app");
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "App must be 1-63 characters and use only letters, numbers, dots, underscores, or hyphens",
        );
    }
    if !is_valid_display_name(&name) {
        log_http_access("POST", PATH, &peer, StatusCode::BAD_REQUEST, "invalid name");
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "Name must be 1-128 characters and must not contain control characters",
        );
    }
    if state.deleted_streams.lock().contains(&id) {
        log_http_access(
            "POST",
            PATH,
            &peer,
            StatusCode::CONFLICT,
            "stream is being deleted",
        );
        return err_json(
            StatusCode::CONFLICT,
            "CONFLICT",
            "Stream is being deleted; try again shortly",
        );
    }

    let publish_key =
        match resolve_or_generate_access_key(req.publish_key, crate::keygen::PREFIX_PUBLISH_KEY) {
            Ok(k) => k,
            Err(AccessKeyFieldError::Invalid) => {
                log_http_access(
                    "POST",
                    PATH,
                    &peer,
                    StatusCode::BAD_REQUEST,
                    "invalid publish_key",
                );
                return err_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("publish_key: {ACCESS_KEY_VALIDATION_MSG}"),
                );
            }
            Err(AccessKeyFieldError::GenerationFailed) => {
                crate::log_error!("publish key generation failed");
                log_http_access(
                    "POST",
                    PATH,
                    &peer,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "publish key generation failed",
                );
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Key generation failed",
                );
            }
        };
    let play_key =
        match resolve_or_generate_access_key(req.play_key, crate::keygen::PREFIX_PLAY_KEY) {
            Ok(k) => k,
            Err(AccessKeyFieldError::Invalid) => {
                log_http_access(
                    "POST",
                    PATH,
                    &peer,
                    StatusCode::BAD_REQUEST,
                    "invalid play_key",
                );
                return err_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("play_key: {ACCESS_KEY_VALIDATION_MSG}"),
                );
            }
            Err(AccessKeyFieldError::GenerationFailed) => {
                crate::log_error!("play key generation failed");
                log_http_access(
                    "POST",
                    PATH,
                    &peer,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "play key generation failed",
                );
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Key generation failed",
                );
            }
        };
    let stats_key =
        match resolve_or_generate_access_key(req.stats_key, crate::keygen::PREFIX_STATS_KEY) {
            Ok(k) => k,
            Err(AccessKeyFieldError::Invalid) => {
                log_http_access(
                    "POST",
                    PATH,
                    &peer,
                    StatusCode::BAD_REQUEST,
                    "invalid stats_key",
                );
                return err_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("stats_key: {ACCESS_KEY_VALIDATION_MSG}"),
                );
            }
            Err(AccessKeyFieldError::GenerationFailed) => {
                crate::log_error!("stats key generation failed");
                log_http_access(
                    "POST",
                    PATH,
                    &peer,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "stats key generation failed",
                );
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Key generation failed",
                );
            }
        };
    if !access_keys_must_be_unique(&[&publish_key, &play_key, &stats_key]) {
        log_http_access(
            "POST",
            PATH,
            &peer,
            StatusCode::BAD_REQUEST,
            "duplicate access keys",
        );
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "publish_key, play_key, and stats_key must be distinct",
        );
    }
    let s = Stream {
        id: id.clone(),
        name,
        app,
        publish_key,
        play_key,
        stats_key,
        enabled: true,
        created_at: now_ts(),
    };

    match state.db.stream_add(&s) {
        Ok(()) => {}
        Err(StreamAddError::Duplicate) => {
            log_http_access(
                "POST",
                PATH,
                &peer,
                StatusCode::CONFLICT,
                "stream id or access key already exists",
            );
            return err_json(
                StatusCode::CONFLICT,
                "CONFLICT",
                "Stream ID or access key already exists",
            );
        }
        Err(StreamAddError::Db) => {
            log_http_access(
                "POST",
                PATH,
                &peer,
                StatusCode::INTERNAL_SERVER_ERROR,
                "db error",
            );
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to create stream",
            );
        }
    }

    crate::log_info!(
        "HTTP: POST /api/v1/streams from {peer} → 201 stream created id={} app={}",
        s.id,
        s.app
    );

    (StatusCode::CREATED, Json(stream_to_json(&state.db, &s))).into_response()
}

async fn finalize_stream_delete(state: &Arc<AppState>, id: &str) -> Result<(), ()> {
    match state.db.stream_delete(id) {
        Some(true) => {
            state.deleted_streams.lock().remove(id);
            Ok(())
        }
        Some(false) => {
            let _ = state.db.stream_set_enabled(id, true);
            state.deleted_streams.lock().remove(id);
            Err(())
        }
        None => {
            let _ = state.db.stream_set_enabled(id, true);
            state.deleted_streams.lock().remove(id);
            Err(())
        }
    }
}

async fn wait_and_finalize_stream_delete(state: Arc<AppState>, id: String) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while state.rtmp_bridge.live_conn_count_for_stream(&id) > 0 {
        if std::time::Instant::now() >= deadline {
            // Leave the stream disabled (`pending_delete=1`) so a failed drain
            // does not silently re-enable publish/play keys. Operators can
            // retry the delete once RTMP sessions drop.
            state.deleted_streams.lock().remove(&id);
            crate::log_warn!(
                "Timed out deleting stream '{id}' — stream stays disabled; \
                 active RTMP sessions remained"
            );
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    match finalize_stream_delete(&state, &id).await {
        Ok(()) => crate::log_info!("HTTP: stream '{id}' deleted after RTMP drain"),
        Err(()) => crate::log_error!("HTTP: failed to finalize delete for stream '{id}'"),
    }
}

async fn handle_stream_delete(
    State(state): State<Arc<AppState>>,
    addr: ClientAddr,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let peer = http_peer(&state, addr, &headers);
    if !bearer_ok(&state, &headers) {
        log_http_access(
            "DELETE",
            "/api/v1/streams/:id",
            &peer,
            StatusCode::UNAUTHORIZED,
            "missing or invalid token",
        );
        return err_json(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Missing or invalid token",
        );
    }
    if !is_valid_stream_key_part(&id) {
        // `id` is not yet validated here and may contain control characters
        // decoded from the URL, so it must not be interpolated into the log.
        log_http_access(
            "DELETE",
            "/api/v1/streams/<invalid>",
            &peer,
            StatusCode::BAD_REQUEST,
            "invalid stream id",
        );
        return err_json(StatusCode::BAD_REQUEST, "BAD_REQUEST", "Invalid stream id");
    }
    let path = format!("/api/v1/streams/{id}");
    if state.deleted_streams.lock().contains(&id) {
        log_http_access(
            "DELETE",
            &path,
            &peer,
            StatusCode::CONFLICT,
            "stream is being deleted",
        );
        return err_json(StatusCode::CONFLICT, "CONFLICT", "Stream is being deleted");
    }
    match state.db.stream_get(&id) {
        DbLookup::Missing => {
            log_http_access(
                "DELETE",
                &path,
                &peer,
                StatusCode::NOT_FOUND,
                "stream not found",
            );
            return err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Stream not found");
        }
        DbLookup::Failed => {
            log_http_access(
                "DELETE",
                &path,
                &peer,
                StatusCode::INTERNAL_SERVER_ERROR,
                "db error",
            );
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to load stream",
            );
        }
        DbLookup::Ok(_) => {}
    }

    state.deleted_streams.lock().insert(id.clone());
    if state.db.stream_disable(&id).is_none() {
        state.deleted_streams.lock().remove(&id);
        log_http_access(
            "DELETE",
            &path,
            &peer,
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to disable stream",
        );
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Failed to disable stream",
        );
    }

    if state.rtmp_bridge.live_conn_count_for_stream(&id) == 0 {
        return match finalize_stream_delete(&state, &id).await {
            Ok(()) => {
                crate::log_info!(
                    "HTTP: DELETE /api/v1/streams/{id} from {peer} → 200 stream deleted"
                );
                Json(json!({"status": "deleted"})).into_response()
            }
            Err(()) => {
                log_http_access(
                    "DELETE",
                    &path,
                    &peer,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to delete stream",
                );
                err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Failed to delete stream",
                )
            }
        };
    }

    crate::log_info!(
        "HTTP: DELETE /api/v1/streams/{id} from {peer} → 202 deleting (draining RTMP)"
    );
    let state_bg = Arc::clone(&state);
    let id_bg = id.clone();
    tokio::spawn(async move {
        wait_and_finalize_stream_delete(state_bg, id_bg).await;
    });
    (StatusCode::ACCEPTED, Json(json!({"status": "deleting"}))).into_response()
}

async fn handle_stream_players_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !bearer_ok(&state, &headers) {
        return err_json(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Missing or invalid token",
        );
    }
    if !is_valid_stream_key_part(&id) {
        return err_json(StatusCode::BAD_REQUEST, "BAD_REQUEST", "Invalid stream id");
    }
    match state.db.stream_get(&id) {
        DbLookup::Missing => {
            return err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Stream not found");
        }
        DbLookup::Failed => {
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to load stream",
            );
        }
        DbLookup::Ok(_) => {}
    }
    let list: Vec<Value> = state
        .db
        .viewer_list(&id)
        .iter()
        .map(viewer_to_json)
        .collect();
    Json(list).into_response()
}

#[derive(Deserialize, Default)]
struct CreatePlayerRequest {
    name: Option<String>,
    play_key: Option<String>,
}

async fn handle_stream_player_create(
    State(state): State<Arc<AppState>>,
    addr: ClientAddr,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Option<Json<CreatePlayerRequest>>,
) -> Response {
    let peer = http_peer(&state, addr, &headers);
    if !bearer_ok(&state, &headers) {
        log_http_access(
            "POST",
            "/api/v1/streams/:id/players",
            &peer,
            StatusCode::UNAUTHORIZED,
            "missing or invalid token",
        );
        return err_json(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Missing or invalid token",
        );
    }
    if !is_valid_stream_key_part(&id) {
        // `id` is not yet validated here and may contain control characters
        // decoded from the URL, so it must not be interpolated into the log.
        log_http_access(
            "POST",
            "/api/v1/streams/<invalid>/players",
            &peer,
            StatusCode::BAD_REQUEST,
            "invalid stream id",
        );
        return err_json(StatusCode::BAD_REQUEST, "BAD_REQUEST", "Invalid stream id");
    }
    let path = format!("/api/v1/streams/{id}/players");
    let stream = match state.db.stream_get(&id) {
        DbLookup::Ok(s) => s,
        DbLookup::Missing => {
            log_http_access(
                "POST",
                &path,
                &peer,
                StatusCode::NOT_FOUND,
                "stream not found",
            );
            return err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Stream not found");
        }
        DbLookup::Failed => {
            log_http_access(
                "POST",
                &path,
                &peer,
                StatusCode::INTERNAL_SERVER_ERROR,
                "db error",
            );
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to load stream",
            );
        }
    };

    let req = body.map(|Json(r)| r).unwrap_or_default();
    let slot = state.db.viewer_list(&id).len() + 1;
    let name = req
        .name
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("Player {slot}"));
    if !is_valid_display_name(&name) {
        log_http_access(
            "POST",
            &path,
            &peer,
            StatusCode::BAD_REQUEST,
            "invalid name",
        );
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "Name must be 1-128 characters and must not contain control characters",
        );
    }

    let play_key =
        match resolve_or_generate_access_key(req.play_key, crate::keygen::PREFIX_PLAY_KEY) {
            Ok(k) => k,
            Err(AccessKeyFieldError::Invalid) => {
                log_http_access(
                    "POST",
                    &path,
                    &peer,
                    StatusCode::BAD_REQUEST,
                    "invalid play_key",
                );
                return err_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("play_key: {ACCESS_KEY_VALIDATION_MSG}"),
                );
            }
            Err(AccessKeyFieldError::GenerationFailed) => {
                crate::log_error!("play key generation failed");
                log_http_access(
                    "POST",
                    &path,
                    &peer,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "play key generation failed",
                );
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Key generation failed",
                );
            }
        };
    if play_key == stream.publish_key
        || play_key == stream.stats_key
        || play_key == stream.play_key
        || state.db.access_key_globally_in_use(&play_key)
        || state
            .db
            .viewer_list(&id)
            .iter()
            .any(|v| v.play_key == play_key)
    {
        log_http_access(
            "POST",
            &path,
            &peer,
            StatusCode::CONFLICT,
            "play_key already in use",
        );
        return err_json(
            StatusCode::CONFLICT,
            "CONFLICT",
            "play_key already in use for this stream or conflicts with another access key",
        );
    }
    let Some(viewer) = create_viewer_row(&stream.id, &name, &play_key, now_ts()) else {
        crate::log_error!("viewer id generation failed");
        log_http_access(
            "POST",
            &path,
            &peer,
            StatusCode::INTERNAL_SERVER_ERROR,
            "viewer id generation failed",
        );
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Key generation failed",
        );
    };
    if !state.db.viewer_add(&viewer) {
        log_http_access(
            "POST",
            &path,
            &peer,
            StatusCode::INTERNAL_SERVER_ERROR,
            "db error",
        );
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Failed to create play key",
        );
    }

    crate::log_info!(
        "HTTP: POST /api/v1/streams/{}/players from {peer} → 201 play key created name={}",
        stream.id,
        viewer.name
    );
    (StatusCode::CREATED, Json(viewer_to_json(&viewer))).into_response()
}

async fn handle_stream_player_delete(
    State(state): State<Arc<AppState>>,
    addr: ClientAddr,
    headers: HeaderMap,
    Path((id, player_id)): Path<(String, String)>,
) -> Response {
    let peer = http_peer(&state, addr, &headers);
    if !bearer_ok(&state, &headers) {
        log_http_access(
            "DELETE",
            "/api/v1/streams/:id/players/:player_id",
            &peer,
            StatusCode::UNAUTHORIZED,
            "missing or invalid token",
        );
        return err_json(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Missing or invalid token",
        );
    }
    if !is_valid_stream_key_part(&id) {
        // `id` is not yet validated here and may contain control characters
        // decoded from the URL, so it must not be interpolated into the log.
        log_http_access(
            "DELETE",
            "/api/v1/streams/<invalid>/players/:player_id",
            &peer,
            StatusCode::BAD_REQUEST,
            "invalid stream id",
        );
        return err_json(StatusCode::BAD_REQUEST, "BAD_REQUEST", "Invalid stream id");
    }
    if !is_valid_access_key(&player_id) {
        // `player_id` is not yet validated here and may contain control
        // characters decoded from the URL, so it must not be interpolated
        // into the log.
        log_http_access(
            "DELETE",
            &format!("/api/v1/streams/{id}/players/<invalid>"),
            &peer,
            StatusCode::BAD_REQUEST,
            "invalid player id",
        );
        return err_json(StatusCode::BAD_REQUEST, "BAD_REQUEST", "Invalid player id");
    }
    let path = format!("/api/v1/streams/{id}/players/{player_id}");
    match state.db.stream_get(&id) {
        DbLookup::Missing => {
            log_http_access(
                "DELETE",
                &path,
                &peer,
                StatusCode::NOT_FOUND,
                "stream not found",
            );
            return err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Stream not found");
        }
        DbLookup::Failed => {
            log_http_access(
                "DELETE",
                &path,
                &peer,
                StatusCode::INTERNAL_SERVER_ERROR,
                "db error",
            );
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to load stream",
            );
        }
        DbLookup::Ok(_) => {}
    }
    match state.db.viewer_get(&id, &player_id) {
        DbLookup::Missing => {
            log_http_access(
                "DELETE",
                &path,
                &peer,
                StatusCode::NOT_FOUND,
                "player not found",
            );
            return err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Player not found");
        }
        DbLookup::Failed => {
            log_http_access(
                "DELETE",
                &path,
                &peer,
                StatusCode::INTERNAL_SERVER_ERROR,
                "db error",
            );
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to load player",
            );
        }
        DbLookup::Ok(_) => {}
    }
    if state.db.viewer_list(&id).len() <= 1 {
        log_http_access(
            "DELETE",
            &path,
            &peer,
            StatusCode::BAD_REQUEST,
            "cannot delete last play key",
        );
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "Cannot delete the last play key for a stream",
        );
    }
    match state.db.viewer_delete(&id, &player_id) {
        Some(true) => {
            state.revoked_viewers.lock().insert(player_id.clone());
            state.db.players_deactivate_for_viewer(&player_id);
            crate::log_info!(
                "HTTP: DELETE /api/v1/streams/{id}/players/{player_id} from {peer} → 200 play key revoked"
            );
            Json(json!({"status": "deleted"})).into_response()
        }
        Some(false) => {
            log_http_access(
                "DELETE",
                &path,
                &peer,
                StatusCode::NOT_FOUND,
                "player not found",
            );
            err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Player not found")
        }
        None => {
            log_http_access(
                "DELETE",
                &path,
                &peer,
                StatusCode::INTERNAL_SERVER_ERROR,
                "db error",
            );
            err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to delete play key",
            )
        }
    }
}

async fn handle_stream_stats(
    State(state): State<Arc<AppState>>,
    addr: ClientAddr,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<KeyQuery>,
) -> Response {
    let peer = http_peer(&state, addr, &headers);
    let bearer = bearer_ok(&state, &headers);
    if !is_valid_stream_key_part(&id) {
        // `id` is not yet validated here and may contain control characters
        // decoded from the URL, so it must not be interpolated into the log.
        const PATH: &str = "/api/v1/streams/<invalid>/stats";
        if bearer {
            log_http_access(
                "GET",
                PATH,
                &peer,
                StatusCode::BAD_REQUEST,
                "invalid stream id",
            );
            return err_json(StatusCode::BAD_REQUEST, "BAD_REQUEST", "Invalid stream id");
        }
        log_http_access(
            "GET",
            PATH,
            &peer,
            StatusCode::FORBIDDEN,
            "invalid stream id",
        );
        return public_stats_text(StatusCode::FORBIDDEN, "Invalid stats key");
    }
    let path = format!("/api/v1/streams/{id}/stats");
    if bearer {
        match state.db.stream_get(&id) {
            DbLookup::Ok(_) => {
                log_http_access("GET", &path, &peer, StatusCode::OK, "bearer");
                return Json(build_json_stats(&state.db, Some(&id))).into_response();
            }
            DbLookup::Missing => {
                log_http_access(
                    "GET",
                    &path,
                    &peer,
                    StatusCode::NOT_FOUND,
                    "stream not found",
                );
                return err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Stream not found");
            }
            DbLookup::Failed => {
                log_http_access(
                    "GET",
                    &path,
                    &peer,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "db error",
                );
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Failed to load stream",
                );
            }
        }
    }
    let start = Instant::now();
    if stats_key_lookup(&state, &q.key, Some(&id)).is_none() {
        log_http_access(
            "GET",
            &path,
            &peer,
            StatusCode::FORBIDDEN,
            "invalid stats key",
        );
        return pace_public_stats(
            start,
            public_stats_text(StatusCode::FORBIDDEN, "Invalid stats key"),
        )
        .await;
    }
    let (response, detail) = match build_public_json_stats(&state.db, &id) {
        Some(body) => (Json(body).into_response(), format!("stream='{id}'")),
        None => (
            public_stats_text(StatusCode::OK, "Stream offline"),
            format!("stream='{id}' offline"),
        ),
    };
    log_http_access("GET", &path, &peer, StatusCode::OK, &detail);
    pace_public_stats(start, response).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtmp_bridge::RtmpEventHandler;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state(api_token: &str) -> Arc<AppState> {
        test_state_with_config(ServerConfig {
            api_token: api_token.to_string(),
            ..Default::default()
        })
    }

    fn test_state_with_config(config: ServerConfig) -> Arc<AppState> {
        let db = Arc::new(Db::open(":memory:").unwrap());
        let deleted_streams = Arc::new(Mutex::new(HashSet::new()));
        let rtmp_bridge = Arc::new(DbRtmpBridge::new(
            Arc::clone(&db),
            Arc::clone(&deleted_streams),
        ));
        Arc::new(AppState {
            db,
            config,
            rtmp_bridge,
            deleted_streams,
            revoked_viewers: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    fn bearer(token: &str) -> (&'static str, String) {
        ("Authorization", format!("Bearer {token}"))
    }

    #[tokio::test]
    async fn empty_api_token_always_denies() {
        let app = router(test_state(""));
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn ct_str_eq_matches_and_differs() {
        assert!(ct_str_eq("abc", "abc"));
        assert!(!ct_str_eq("abc", "abd"));
        assert!(!ct_str_eq("abc", "abcd"));
    }

    #[test]
    fn xml_escape_handles_reserved_chars() {
        assert_eq!(xml_escape("<a&b>\"'"), "&lt;a&amp;b&gt;&quot;&apos;");
    }

    #[test]
    fn stream_create_validation_helpers_reject_unsafe_values() {
        assert!(is_valid_access_key("live.main_1_with_sufficient_length_ok"));
        assert!(is_valid_access_key("custom_pub_key_with_enough_chars"));
        assert!(is_valid_access_key(&"a".repeat(63)));
        assert!(!is_valid_access_key(""));
        assert!(!is_valid_access_key("too_short"));
        assert!(!is_valid_access_key(
            "-starts-with-hyphen-but-long-enough-here"
        ));
        assert!(!is_valid_access_key("bad/id"));
        assert!(!is_valid_access_key(&"a".repeat(64)));

        assert!(is_valid_stream_key_part("live.main_1"));
        assert!(!is_valid_stream_key_part("bad/id"));

        assert!(is_valid_display_name("Main Stream"));
        assert!(!is_valid_display_name("bad\nname"));

        assert!(access_keys_must_be_unique(&["a", "b", "c"]));
        assert!(!access_keys_must_be_unique(&["a", "a"]));
    }

    #[tokio::test]
    async fn health_requires_no_auth() {
        let app = router(test_state("a-strong-random-secret-value"));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_reports_rtmps_capability_with_bearer() {
        let config = ServerConfig {
            api_token: "a-strong-random-secret-value".to_string(),
            rtmp_bind: "0.0.0.0:1935".to_string(),
            tls_enabled: true,
            rtmps_bind: "0.0.0.0:1936".to_string(),
            ..Default::default()
        };
        let db = Arc::new(Db::open(":memory:").unwrap());
        let deleted_streams = Arc::new(Mutex::new(HashSet::new()));
        let rtmp_bridge = Arc::new(DbRtmpBridge::new(
            Arc::clone(&db),
            Arc::clone(&deleted_streams),
        ));
        let state = Arc::new(AppState {
            db,
            config,
            rtmp_bridge,
            deleted_streams,
            revoked_viewers: Arc::new(Mutex::new(HashSet::new())),
        });
        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["rtmp_port"], 1935);
        assert_eq!(json["rtmps_enabled"], true);
        assert_eq!(json["rtmps_port"], 1936);
    }

    #[tokio::test]
    async fn health_public_response_is_minimal() {
        let app = router(test_state("a-strong-random-secret-value"));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert!(json.get("rtmp_port").is_none());
    }

    #[tokio::test]
    async fn streams_list_requires_bearer_token() {
        let app = router(test_state("a-strong-random-secret-value"));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn create_stream_rejects_invalid_fields() {
        let app = router(test_state("a-strong-random-secret-value"));

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"id":"bad/id","app":"live"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"id":"ok","app":"bad/app"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_stream_accepts_custom_keys() {
        let app = router(test_state("a-strong-random-secret-value"));

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"id":"customkeys","publish_key":"my_pub_key_with_sufficient_length_01","play_key":"my_play_key_with_sufficient_length_01","stats_key":"my_stats_key_with_sufficient_length_01"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["publish_key"], "my_pub_key_with_sufficient_length_01");
        assert_eq!(json["play_key"], "my_play_key_with_sufficient_length_01");
        assert_eq!(json["stats_key"], "my_stats_key_with_sufficient_length_01");
    }

    #[tokio::test]
    async fn create_stream_rejects_duplicate_custom_keys() {
        let app = router(test_state("a-strong-random-secret-value"));

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"id":"dupkeys","publish_key":"same_key_with_sufficient_length_here","play_key":"same_key_with_sufficient_length_here"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_stream_rejects_invalid_custom_key() {
        let app = router(test_state("a-strong-random-secret-value"));

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"id":"badkey","publish_key":"bad/key"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_player_accepts_custom_play_key() {
        let state = test_state("a-strong-random-secret-value");
        let app = router(state.clone());

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"id":"playerkeys"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams/playerkeys/players")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"name":"Guest","play_key":"guest_play_key_with_sufficient_len01"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["play_key"], "guest_play_key_with_sufficient_len01");
    }

    #[tokio::test]
    async fn create_and_list_stream_with_valid_token() {
        let state = test_state("a-strong-random-secret-value");
        let app = router(state.clone());

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"id":"mystream"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn create_stream_rejects_play_key_that_matches_another_streams_publish_key() {
        let state = test_state("a-strong-random-secret-value");
        let app = router(state.clone());

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"id":"victim","publish_key":"pub_victim_key_with_sufficient_length01","play_key":"play_victim_key_with_sufficient_len01","stats_key":"stats_victim_key_with_sufficient_len01"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"id":"attacker","publish_key":"pub_attacker_key_with_sufficient_len01","play_key":"pub_victim_key_with_sufficient_length01","stats_key":"stats_attacker_key_with_sufficient_len01"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn create_player_rejects_play_key_that_matches_another_streams_publish_key() {
        let state = test_state("a-strong-random-secret-value");
        let app = router(state.clone());

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"id":"victim","publish_key":"pub_victim_key_with_sufficient_length01","play_key":"play_victim_key_with_sufficient_len01","stats_key":"stats_victim_key_with_sufficient_len01"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"id":"other","publish_key":"pub_other_key_with_sufficient_length01","play_key":"play_other_key_with_sufficient_length01","stats_key":"stats_other_key_with_sufficient_length01"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams/other/players")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"name":"Guest","play_key":"pub_victim_key_with_sufficient_length01"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn create_stream_rejects_short_custom_key() {
        let app = router(test_state("a-strong-random-secret-value"));

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header("Authorization", "Bearer a-strong-random-secret-value")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"id":"shortkey","publish_key":"tiny"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn public_stats_nginx_omits_identifiers() {
        use crate::db::Publisher;

        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "pubstream".to_string(),
            name: "Secret Name".to_string(),
            app: "live".to_string(),
            publish_key: "pub_test_key_with_sufficient_length_here".to_string(),
            play_key: "play_test_key_with_sufficient_length_here".to_string(),
            stats_key: "st_test_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        state.db.publisher_try_acquire(&Publisher {
            id: "pub_sess".to_string(),
            stream_id: "pubstream".to_string(),
            app: "live".to_string(),
            stream_name: "Secret Name".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        });

        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/stats-nginx?key=st_test_key_with_sufficient_length_here")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();
        assert!(!xml.contains("Secret Name"));
        assert!(xml.contains("<name>stream</name>"));
    }

    #[tokio::test]
    async fn public_stats_nginx_merges_publisher_and_player_into_one_stream() {
        use crate::db::{Player, Publisher};

        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "pubstream".to_string(),
            name: "Secret Name".to_string(),
            app: "live".to_string(),
            publish_key: "pub_test_key_with_sufficient_length_here".to_string(),
            play_key: "play_test_key_with_sufficient_length_here".to_string(),
            stats_key: "st_test_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        state.db.publisher_try_acquire(&Publisher {
            id: "pub_sess".to_string(),
            stream_id: "pubstream".to_string(),
            app: "live".to_string(),
            stream_name: "Secret Name".to_string(),
            active: true,
            connected_at: now_ts(),
            bitrate_kbps: 2500.0,
            ..Default::default()
        });
        state.db.player_try_acquire(&Player {
            id: "player_sess".to_string(),
            stream_id: "pubstream".to_string(),
            viewer_id: "viewer1".to_string(),
            app: "live".to_string(),
            stream_name: "Secret Name".to_string(),
            active: true,
            connected_at: now_ts(),
            bitrate_kbps: 2500.0,
            ..Default::default()
        });

        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/stats-nginx?key=st_test_key_with_sufficient_length_here")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();

        // A viewer session must not shadow the publisher's bitrate under the
        // same (redacted) stream name — there must be exactly one <stream>
        // block, carrying the publisher's bw_video, not 0.
        assert_eq!(xml.matches("<stream>").count(), 1);
        assert!(xml.contains("<bw_video>2500000</bw_video>"));
        assert_eq!(xml.matches("<client>").count(), 2);
    }

    #[tokio::test]
    async fn public_stats_nginx_meta_always_has_both_video_and_audio_siblings() {
        use crate::db::Publisher;

        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "pubstream".to_string(),
            name: "Secret Name".to_string(),
            app: "live".to_string(),
            publish_key: "pub_test_key_with_sufficient_length_here".to_string(),
            play_key: "play_test_key_with_sufficient_length_here".to_string(),
            stats_key: "st_test_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        // Video-only publisher: audio_codec is never set (e.g. no audio
        // track, or the codec hasn't been detected yet).
        state.db.publisher_try_acquire(&Publisher {
            id: "pub_sess".to_string(),
            stream_id: "pubstream".to_string(),
            app: "live".to_string(),
            stream_name: "Secret Name".to_string(),
            active: true,
            connected_at: now_ts(),
            bitrate_kbps: 2500.0,
            video_codec: "h264".to_string(),
            video_width: 1920,
            video_height: 1080,
            fps: 60.0,
            ..Default::default()
        });

        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/stats-nginx?key=st_test_key_with_sufficient_length_here")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();

        // NOALBS's Nginx provider models <meta> as requiring both <video>
        // and <audio> children — a <meta> with only <video> fails to
        // deserialize there and the whole stream reads as offline. Both
        // must be present, even if <audio/> carries no data.
        assert!(xml.contains("<meta>"));
        assert!(xml.contains("<video>"));
        assert!(xml.contains("<audio/>"));
    }

    #[tokio::test]
    async fn public_stats_nginx_emits_real_metadata_values() {
        use crate::db::Publisher;

        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "pubstream".to_string(),
            name: "Secret Name".to_string(),
            app: "live".to_string(),
            publish_key: "pub_test_key_with_sufficient_length_here".to_string(),
            play_key: "play_test_key_with_sufficient_length_here".to_string(),
            stats_key: "st_test_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        state.db.publisher_try_acquire(&Publisher {
            id: "pub_sess".to_string(),
            stream_id: "pubstream".to_string(),
            app: "live".to_string(),
            stream_name: "Secret Name".to_string(),
            active: true,
            connected_at: now_ts(),
            bitrate_kbps: 2500.0,
            video_codec: "h264".to_string(),
            audio_codec: "aac".to_string(),
            video_width: 1920,
            video_height: 1080,
            fps: 29.97,
            audio_sample_rate: 48000,
            audio_channels: 2,
            ..Default::default()
        });

        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/stats-nginx?key=st_test_key_with_sufficient_length_here")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();

        assert!(xml.contains("<width>1920</width>"));
        assert!(xml.contains("<height>1080</height>"));
        assert!(xml.contains("<frame_rate>30.0</frame_rate>"));
        assert!(xml.contains("<sample_rate>48000</sample_rate>"));
        assert!(xml.contains("<channels>2</channels>"));
        assert!(!xml.contains("<sample_rate>44100</sample_rate>"));
    }

    #[tokio::test]
    async fn public_stats_nginx_missing_audio_metadata_defaults_to_zero() {
        use crate::db::Publisher;

        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "pubstream".to_string(),
            name: "Secret Name".to_string(),
            app: "live".to_string(),
            publish_key: "pub_test_key_with_sufficient_length_here".to_string(),
            play_key: "play_test_key_with_sufficient_length_here".to_string(),
            stats_key: "st_test_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        state.db.publisher_try_acquire(&Publisher {
            id: "pub_sess".to_string(),
            stream_id: "pubstream".to_string(),
            app: "live".to_string(),
            stream_name: "Secret Name".to_string(),
            active: true,
            connected_at: now_ts(),
            bitrate_kbps: 2500.0,
            video_codec: "h264".to_string(),
            audio_codec: "aac".to_string(),
            ..Default::default()
        });

        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/stats-nginx?key=st_test_key_with_sufficient_length_here")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();

        assert!(xml.contains("<sample_rate>0</sample_rate>"));
        assert!(xml.contains("<channels>0</channels>"));
    }

    #[tokio::test]
    async fn public_stats_nginx_player_without_publisher_is_not_marked_active() {
        use crate::db::Player;

        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "pubstream".to_string(),
            name: "Secret Name".to_string(),
            app: "live".to_string(),
            publish_key: "pub_test_key_with_sufficient_length_here".to_string(),
            play_key: "play_test_key_with_sufficient_length_here".to_string(),
            stats_key: "st_test_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        // A lingering viewer with no active publisher — e.g. the broadcaster
        // dropped but the player connection hasn't been torn down yet.
        state.db.player_try_acquire(&Player {
            id: "player_sess".to_string(),
            stream_id: "pubstream".to_string(),
            viewer_id: "viewer1".to_string(),
            app: "live".to_string(),
            stream_name: "Secret Name".to_string(),
            active: true,
            connected_at: now_ts(),
            bitrate_kbps: 2500.0,
            ..Default::default()
        });

        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/stats-nginx?key=st_test_key_with_sufficient_length_here")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();

        // Without an active publisher the stream-level <active/> marker must
        // be absent, or nginx-rtmp-compatible consumers (e.g. NOALBS, which
        // treats "active present + bw_video=0" as "keep the previous scene"
        // rather than "offline") never notice the broadcaster is gone.
        assert!(!xml.contains("<active/>"));
        assert!(!xml.contains("<publishing/>"));
    }

    #[tokio::test]
    async fn public_stats_live_json_omits_identifiers() {
        use crate::db::Publisher;

        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "pubstream".to_string(),
            name: "Secret Name".to_string(),
            app: "live".to_string(),
            publish_key: "pub_test_key_with_sufficient_length_here".to_string(),
            play_key: "play_test_key_with_sufficient_length_here".to_string(),
            stats_key: "st_test_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        state.db.publisher_try_acquire(&Publisher {
            id: "pub_sess".to_string(),
            stream_id: "pubstream".to_string(),
            app: "live".to_string(),
            stream_name: "Secret Name".to_string(),
            active: true,
            connected_at: now_ts(),
            ..Default::default()
        });

        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/stats?key=st_test_key_with_sufficient_length_here")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("streams").is_none());
        assert!(json.get("players").is_none());
        assert!(json.get("summary").is_none());
        assert!(json.get("id").is_none());
        assert!(json.get("name").is_none());
        assert!(json.get("app").is_none());
        assert!(json.get("uptime").is_some());
    }

    #[tokio::test]
    async fn public_stats_offline_and_errors_are_plain_text() {
        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "offstream".to_string(),
            name: "Offline".to_string(),
            app: "live".to_string(),
            publish_key: "pub_off_key_with_sufficient_length_here".to_string(),
            play_key: "play_off_key_with_sufficient_length_here".to_string(),
            stats_key: "st_off_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();

        let app = router(state);

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/stats?key=st_off_key_with_sufficient_length_here")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, "Stream offline");

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/stats?key=wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, "Invalid stats key");

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, "stats_key required");
    }

    #[tokio::test]
    async fn invalid_bearer_token_denies_protected_routes() {
        let app = router(test_state("correct-token-value-here"));
        let (header, value) = bearer("wrong-token-value-here");
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/streams")
                    .header(header, value)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn api_rate_limit_returns_429_when_exceeded() {
        let state = test_state_with_config(ServerConfig {
            api_token: "token".to_string(),
            http_rate_limit_api: 3,
            ..Default::default()
        });
        let app = router(state);

        for _ in 0..3 {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/v1/health")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn stats_rate_limit_returns_429_when_exceeded() {
        let state = test_state_with_config(ServerConfig {
            api_token: "token".to_string(),
            http_rate_limit_stats: 2,
            ..Default::default()
        });
        let stream = Stream {
            id: "ratestream".to_string(),
            name: "Rate".to_string(),
            app: "live".to_string(),
            publish_key: "pub_rate_key_with_sufficient_length_here".to_string(),
            play_key: "play_rate_key_with_sufficient_length_here".to_string(),
            stats_key: "st_rate_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        let app = router(state);
        let uri = "/stats?key=st_rate_key_with_sufficient_length_here";

        for _ in 0..2 {
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let resp = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn oversized_request_body_is_rejected() {
        let state = test_state_with_config(ServerConfig {
            api_token: "a-strong-random-secret-value".to_string(),
            http_max_body_bytes: 1024,
            ..Default::default()
        });
        let app = router(state);
        let huge = "x".repeat(2048);
        let (header, value) = bearer("a-strong-random-secret-value");
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/streams")
                    .header(header, value)
                    .header("Content-Type", "application/json")
                    .body(Body::from(format!(r#"{{"id":"big","name":"{huge}"}}"#)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn delete_stream_without_live_connections_removes_stream() {
        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "gone".to_string(),
            name: "Gone".to_string(),
            app: "live".to_string(),
            publish_key: "pub_gone_key_with_sufficient_length_here".to_string(),
            play_key: "play_gone_key_with_sufficient_length_here".to_string(),
            stats_key: "st_gone_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        let app = router(state.clone());
        let (header, value) = bearer("a-strong-random-secret-value");

        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/streams/gone")
                    .header(header, value)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "deleted");
        assert!(matches!(state.db.stream_get("gone"), DbLookup::Missing));
        assert!(!state.deleted_streams.lock().contains("gone"));
    }

    #[tokio::test]
    async fn delete_stream_with_live_connections_returns_accepted() {
        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "busy".to_string(),
            name: "Busy".to_string(),
            app: "live".to_string(),
            publish_key: "pub_busy_key_with_sufficient_length_here".to_string(),
            play_key: "play_busy_key_with_sufficient_length_here".to_string(),
            stats_key: "st_busy_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        state.rtmp_bridge.on_connect(1, "127.0.0.1:1000");
        assert!(
            state
                .rtmp_bridge
                .authorize_publish(1, "live", "pub_busy_key_with_sufficient_length_here")
                .is_ok()
        );

        let app = router(state.clone());
        let (header, value) = bearer("a-strong-random-secret-value");
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/streams/busy")
                    .header(header, value)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "deleting");
        assert!(state.deleted_streams.lock().contains("busy"));
        assert_eq!(
            state.db.stream_ids_pending_delete(),
            vec!["busy".to_string()]
        );
        let DbLookup::Ok(st) = state.db.stream_get("busy") else {
            panic!("stream should remain until RTMP drain completes");
        };
        assert!(!st.enabled);
    }

    #[tokio::test]
    async fn delete_stream_finalizes_after_rtmp_disconnect() {
        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "drain".to_string(),
            name: "Drain".to_string(),
            app: "live".to_string(),
            publish_key: "pub_drain_key_with_sufficient_length_here".to_string(),
            play_key: "play_drain_key_with_sufficient_length_here".to_string(),
            stats_key: "st_drain_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        state.rtmp_bridge.on_connect(1, "127.0.0.1:1000");
        assert!(
            state
                .rtmp_bridge
                .authorize_publish(1, "live", "pub_drain_key_with_sufficient_length_here")
                .is_ok()
        );

        let app = router(state.clone());
        let (header, value) = bearer("a-strong-random-secret-value");
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/streams/drain")
                    .header(header, value)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        state.rtmp_bridge.on_close(1);
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(matches!(state.db.stream_get("drain"), DbLookup::Missing));
        assert!(!state.deleted_streams.lock().contains("drain"));
        assert!(state.db.stream_ids_pending_delete().is_empty());
    }

    #[tokio::test]
    async fn delete_stream_conflict_while_already_deleting() {
        let state = test_state("a-strong-random-secret-value");
        let stream = Stream {
            id: "pending".to_string(),
            name: "Pending".to_string(),
            app: "live".to_string(),
            publish_key: "pub_pend_key_with_sufficient_length_here".to_string(),
            play_key: "play_pend_key_with_sufficient_length_here".to_string(),
            stats_key: "st_pend_key_with_sufficient_length_here".to_string(),
            enabled: true,
            created_at: now_ts(),
        };
        state.db.stream_add(&stream).unwrap();
        state.deleted_streams.lock().insert("pending".to_string());

        let app = router(state);
        let (header, value) = bearer("a-strong-random-secret-value");
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/streams/pending")
                    .header(header, value)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }
}
