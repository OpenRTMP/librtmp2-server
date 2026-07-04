//! HTTP server (axum) — REST API + stats endpoints.
//!
//! Endpoints:
//!   GET    /api/v1/health                       no auth
//!   GET    /api/v1/streams                      Bearer token (includes keys)
//!   POST   /api/v1/streams                      Bearer token, returns keys
//!   DELETE /api/v1/streams/:id                   Bearer token
//!
//!   GET    /stats?key=<stats_key>               flat JSON stats (no stream ids)
//!   GET    /api/v1/streams/:id/stats            Bearer = full JSON; key = flat public JSON
//!   GET    /stats-nginx?key=<stats_key>         XML (nginx-rtmp compatible)

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    let limiter = RateLimiter::new(state.config.http_trusted_proxies.clone());
    Router::new()
        .route("/api/v1/health", get(handle_health))
        .route("/stats", get(handle_stats_json))
        .route("/stats-nginx", get(handle_stats_nginx))
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

fn stats_key_ok(state: &AppState, key: &str, stream_id: Option<&str>) -> bool {
    if key.is_empty() {
        return false;
    }
    match state.db.stream_find_by_stats_key(key) {
        DbLookup::Ok(s) => stream_id.is_none_or(|id| s.id == id),
        DbLookup::Missing | DbLookup::Failed => false,
    }
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

/// Minimum length for operator-supplied publish/play/stats keys. Shorter custom
/// keys are trivially brute-forced over the unrate-limited RTMP auth path.
const MIN_ACCESS_KEY_LEN: usize = 32;

/// Publish/play/stats keys: safe ASCII, no slashes, minimum entropy via length.
fn is_valid_access_key(value: &str) -> bool {
    if value.len() < MIN_ACCESS_KEY_LEN || value.len() > 63 {
        return false;
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
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

const ACCESS_KEY_VALIDATION_MSG: &str =
    "Key must be 32-63 characters and use only letters, numbers, dots, underscores, or hyphens";

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
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<rtmp>\n  <server>\n\
         \x20\x20\x20\x20<application>\n      <name>{}</name>\n      <live>\n",
        xml_escape(app_name),
    ));

    for p in &pubs {
        let uptime_ms = (now - p.connected_at).max(0) * 1000;
        let bw_in = (p.bitrate_kbps * 1000.0) as i64;
        let stream_label = if redact_identifiers {
            "stream"
        } else {
            p.stream_name.as_str()
        };
        out.push_str(&format!(
            "        <stream>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<name>{}</name>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_in>{bw_in}</bw_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_in>{}</bytes_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_out>0</bw_out>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_out>0</bytes_out>\n",
            xml_escape(stream_label),
            p.bytes_in,
        ));

        if !p.video_codec.is_empty() {
            out.push_str(&format!(
                "          <video>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<width>{}</width>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<height>{}</height>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<frame_rate>{:.1}</frame_rate>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<codec>{}</codec>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<profile>baseline</profile>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<level>3.1</level>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20</video>\n",
                p.video_width,
                p.video_height,
                p.fps,
                xml_escape(&p.video_codec),
            ));
        }

        if !p.audio_codec.is_empty() {
            out.push_str(&format!(
                "          <audio>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<codec>{}</codec>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<sample_rate>44100</sample_rate>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<channels>2</channels>\n\
                 \x20\x20\x20\x20\x20\x20\x20\x20</audio>\n",
                xml_escape(&p.audio_codec),
            ));
        }

        out.push_str(&format!(
            "          <client>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<flashver>FMLE/3.0</flashver>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<dropped>0</dropped>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<avsync>0</avsync>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<timestamp>{uptime_ms}</timestamp>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<active>1</active>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<publisher>1</publisher>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20</client>\n        </stream>\n",
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
        out.push_str(&format!(
            "        <stream>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<name>{}</name>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_in>0</bw_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_in>0</bytes_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_out>{bw_out}</bw_out>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_out>{}</bytes_out>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<client>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<flashver>FMLE/3.0</flashver>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<dropped>0</dropped>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<avsync>0</avsync>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<timestamp>{uptime_ms}</timestamp>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<active>1</active>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<publisher>0</publisher>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20</client>\n        </stream>\n",
            xml_escape(stream_label),
            pl.bytes_out,
        ));
    }

    out.push_str(&format!(
        "        <nclients>{}</nclients>\n      </live>\n    </application>\n  </server>\n</rtmp>\n",
        pubs.len() + players.len()
    ));

    out
}

// ---------- handlers ----------

async fn handle_health(State(state): State<Arc<AppState>>) -> Response {
    Json(json!({
        "status": "ok",
        "timestamp": now_ts(),
        "rtmp_port": state.config.rtmp_port(),
        "rtmps_enabled": state.config.tls_enabled,
        "rtmps_port": state.config.rtmps_port(),
    }))
    .into_response()
}

async fn handle_stats_json(
    State(state): State<Arc<AppState>>,
    Query(q): Query<KeyQuery>,
) -> Response {
    if q.key.is_empty() {
        return public_stats_text(StatusCode::UNAUTHORIZED, "stats_key required");
    }
    let DbLookup::Ok(s) = state.db.stream_find_by_stats_key(&q.key) else {
        return public_stats_text(StatusCode::FORBIDDEN, "Invalid stats key");
    };
    match build_public_json_stats(&state.db, &s.id) {
        Some(body) => Json(body).into_response(),
        None => public_stats_text(StatusCode::OK, "Stream offline"),
    }
}

async fn handle_stats_nginx(
    State(state): State<Arc<AppState>>,
    Query(q): Query<KeyQuery>,
) -> Response {
    if q.key.is_empty() {
        return err_xml(StatusCode::UNAUTHORIZED, "Missing stats key");
    }
    let DbLookup::Ok(s) = state.db.stream_find_by_stats_key(&q.key) else {
        return err_xml(StatusCode::FORBIDDEN, "Invalid stats key");
    };
    xml_response(
        StatusCode::OK,
        build_nginx_xml(&state.db, Some(&s.id), true),
    )
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
    headers: HeaderMap,
    body: Option<Json<CreateStreamRequest>>,
) -> Response {
    if !bearer_ok(&state, &headers) {
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
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "Stream id must be 1-63 characters and use only letters, numbers, dots, underscores, or hyphens",
        );
    }
    if !is_valid_stream_key_part(&app) {
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "App must be 1-63 characters and use only letters, numbers, dots, underscores, or hyphens",
        );
    }
    if !is_valid_display_name(&name) {
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "Name must be 1-128 characters and must not contain control characters",
        );
    }
    if state.deleted_streams.lock().contains(&id) {
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
                return err_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("publish_key: {ACCESS_KEY_VALIDATION_MSG}"),
                );
            }
            Err(AccessKeyFieldError::GenerationFailed) => {
                crate::log_error!("publish key generation failed");
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
                return err_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("play_key: {ACCESS_KEY_VALIDATION_MSG}"),
                );
            }
            Err(AccessKeyFieldError::GenerationFailed) => {
                crate::log_error!("play key generation failed");
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
                return err_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("stats_key: {ACCESS_KEY_VALIDATION_MSG}"),
                );
            }
            Err(AccessKeyFieldError::GenerationFailed) => {
                crate::log_error!("stats key generation failed");
                return err_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Key generation failed",
                );
            }
        };
    if !access_keys_must_be_unique(&[&publish_key, &play_key, &stats_key]) {
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
            return err_json(
                StatusCode::CONFLICT,
                "CONFLICT",
                "Stream ID or access key already exists",
            );
        }
        Err(StreamAddError::Db) => {
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to create stream",
            );
        }
    }

    crate::log_info!("Stream created: id={} app={}", s.id, s.app);

    (StatusCode::CREATED, Json(stream_to_json(&state.db, &s))).into_response()
}

async fn handle_stream_delete(
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
    if state.deleted_streams.lock().contains(&id) {
        return err_json(StatusCode::CONFLICT, "CONFLICT", "Stream is being deleted");
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

    state.deleted_streams.lock().insert(id.clone());
    if state.db.stream_disable(&id).is_none() {
        state.deleted_streams.lock().remove(&id);
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Failed to disable stream",
        );
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while state.rtmp_bridge.live_conn_count_for_stream(&id) > 0 {
        if std::time::Instant::now() >= deadline {
            let _ = state.db.stream_set_enabled(&id, true);
            state.deleted_streams.lock().remove(&id);
            return err_json(
                StatusCode::SERVICE_UNAVAILABLE,
                "UNAVAILABLE",
                "Timed out waiting for active RTMP sessions to close",
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    match state.db.stream_delete(&id) {
        Some(true) => {
            crate::log_info!("Stream deleted: {id}");
            state.deleted_streams.lock().remove(&id);
            Json(json!({"status": "deleted"})).into_response()
        }
        Some(false) => {
            let _ = state.db.stream_set_enabled(&id, true);
            state.deleted_streams.lock().remove(&id);
            err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Stream not found")
        }
        None => {
            let _ = state.db.stream_set_enabled(&id, true);
            state.deleted_streams.lock().remove(&id);
            err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to delete stream",
            )
        }
    }
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
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Option<Json<CreatePlayerRequest>>,
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
    let stream = match state.db.stream_get(&id) {
        DbLookup::Ok(s) => s,
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
    };

    let req = body.map(|Json(r)| r).unwrap_or_default();
    let slot = state.db.viewer_list(&id).len() + 1;
    let name = req
        .name
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("Player {slot}"));
    if !is_valid_display_name(&name) {
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
                return err_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("play_key: {ACCESS_KEY_VALIDATION_MSG}"),
                );
            }
            Err(AccessKeyFieldError::GenerationFailed) => {
                crate::log_error!("play key generation failed");
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
        || state
            .db
            .viewer_list(&id)
            .iter()
            .any(|v| v.play_key == play_key)
    {
        return err_json(
            StatusCode::CONFLICT,
            "CONFLICT",
            "play_key already in use for this stream or conflicts with publish_key/stats_key",
        );
    }
    let Some(viewer) = create_viewer_row(&stream.id, &name, &play_key, now_ts()) else {
        crate::log_error!("viewer id generation failed");
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Key generation failed",
        );
    };
    if !state.db.viewer_add(&viewer) {
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Failed to create play key",
        );
    }

    crate::log_info!(
        "Play key created: stream={} name={}",
        stream.id,
        viewer.name
    );
    (StatusCode::CREATED, Json(viewer_to_json(&viewer))).into_response()
}

async fn handle_stream_player_delete(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((id, player_id)): Path<(String, String)>,
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
    match state.db.viewer_get(&id, &player_id) {
        DbLookup::Missing => {
            return err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Player not found");
        }
        DbLookup::Failed => {
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Failed to load player",
            );
        }
        DbLookup::Ok(_) => {}
    }
    if state.db.viewer_list(&id).len() <= 1 {
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "Cannot delete the last play key for a stream",
        );
    }
    state.revoked_viewers.lock().insert(player_id.clone());
    match state.db.viewer_delete(&id, &player_id) {
        Some(true) => {
            state.db.players_deactivate_for_viewer(&player_id);
            Json(json!({"status": "deleted"})).into_response()
        }
        Some(false) => err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Player not found"),
        None => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Failed to delete play key",
        ),
    }
}

async fn handle_stream_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<KeyQuery>,
) -> Response {
    let bearer = bearer_ok(&state, &headers);
    if !is_valid_stream_key_part(&id) {
        if bearer {
            return err_json(StatusCode::BAD_REQUEST, "BAD_REQUEST", "Invalid stream id");
        }
        return public_stats_text(StatusCode::FORBIDDEN, "Invalid stats key");
    }
    if bearer {
        match state.db.stream_get(&id) {
            DbLookup::Ok(_) => return Json(build_json_stats(&state.db, Some(&id))).into_response(),
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
        }
    }
    if !stats_key_ok(&state, &q.key, Some(&id)) {
        return public_stats_text(StatusCode::FORBIDDEN, "Invalid stats key");
    }
    match build_public_json_stats(&state.db, &id) {
        Some(body) => Json(body).into_response(),
        None => public_stats_text(StatusCode::OK, "Stream offline"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state(api_token: &str) -> Arc<AppState> {
        let config = ServerConfig {
            api_token: api_token.to_string(),
            ..Default::default()
        };
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
    async fn health_reports_rtmps_capability() {
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
}
