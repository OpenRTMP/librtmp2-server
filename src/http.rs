//! HTTP server (axum) — REST API + stats endpoints.
//!
//! Endpoints:
//!   GET    /api/v1/health                       no auth
//!   GET    /api/v1/streams                      Bearer token
//!   POST   /api/v1/streams                      Bearer token, returns keys
//!   DELETE /api/v1/streams/:id                   Bearer token
//!
//!   GET    /stats?key=<stats_key>               JSON stats (modern)
//!   GET    /api/v1/streams/:id/stats?key=<sk>   JSON per-stream stats
//!   GET    /stats-nginx?key=<stats_key>         XML (nginx-rtmp compatible)

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::ServerConfig;
use crate::db::{Db, Stream, StreamAddError};
use crate::keygen::keygen_secret;

pub struct AppState {
    pub db: Arc<Db>,
    pub config: ServerConfig,
    /// Stream IDs deleted via this API while RTMP connections are active.
    /// The RTMP poll loop reads this set and evicts matching connections.
    pub deleted_streams: Arc<Mutex<HashSet<String>>>,
}

/// Build the Axum router, wiring all HTTP handlers to the shared application state.
pub fn router(state: Arc<AppState>) -> Router {
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
        Some(s) => stream_id.is_none_or(|id| s.id == id),
        None => false,
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

fn is_valid_display_name(value: &str) -> bool {
    !value.is_empty() && value.chars().count() <= 128 && !value.chars().any(char::is_control)
}

fn is_valid_allowed_codecs(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.split(',').all(|codec| {
            !codec.is_empty()
                && codec.len() <= 32
                && codec
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
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
                "bytes_in": p.bytes_in,
                "video": {
                    "codec": p.video_codec,
                    "width": p.video_width,
                    "height": p.video_height,
                    "fps": p.fps,
                },
                "audio": { "codec": p.audio_codec },
                "client": { "address": p.remote_addr, "publisher": true },
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
                "bytes_out": pl.bytes_out,
                "client": { "address": pl.remote_addr },
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

// ---------- XML stats (nginx-rtmp compatible) ----------

fn build_nginx_xml(db: &Db, stream_id: Option<&str>) -> String {
    let (pubs, players) = match stream_id {
        Some(id) => (db.publisher_list(Some(id)), db.player_list(Some(id))),
        None => (db.publisher_list_all(), db.player_list_all()),
    };

    let now = now_ts();
    let app_name = pubs
        .first()
        .map(|p| p.app.as_str())
        .or_else(|| players.first().map(|pl| pl.app.as_str()))
        .unwrap_or("live");
    let mut out = String::with_capacity(8192);
    out.push_str(&format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<rtmp>\n  <server>\n\
         \x20\x20\x20\x20<application>\n      <name>{}</name>\n      <live>\n",
        xml_escape(app_name),
    ));

    for p in &pubs {
        let uptime_ms = (now - p.connected_at).max(0) * 1000;
        let bw_in = (p.bitrate_kbps * 1000.0) as i64;
        out.push_str(&format!(
            "        <stream>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<name>{}</name>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_in>{bw_in}</bw_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_in>{}</bytes_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_out>0</bw_out>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_out>0</bytes_out>\n",
            xml_escape(&p.stream_name),
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
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<address>{}</address>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<flashver>FMLE/3.0</flashver>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<dropped>0</dropped>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<avsync>0</avsync>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<timestamp>{uptime_ms}</timestamp>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<active>1</active>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<publisher>1</publisher>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20</client>\n        </stream>\n",
            xml_escape(&p.remote_addr),
        ));
    }

    for pl in &players {
        let uptime_ms = (now - pl.connected_at).max(0) * 1000;
        let bw_out = (pl.bitrate_kbps * 1000.0) as i64;
        out.push_str(&format!(
            "        <stream>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<name>{}</name>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_in>0</bw_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_in>0</bytes_in>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bw_out>{bw_out}</bw_out>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<bytes_out>{}</bytes_out>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<client>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<address>{}</address>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<time>{uptime_ms}</time>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<flashver>FMLE/3.0</flashver>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<dropped>0</dropped>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<avsync>0</avsync>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<timestamp>{uptime_ms}</timestamp>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<active>1</active>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20<publisher>0</publisher>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20</client>\n        </stream>\n",
            xml_escape(&pl.stream_name),
            pl.bytes_out,
            xml_escape(&pl.remote_addr),
        ));
    }

    out.push_str(&format!(
        "        <nclients>{}</nclients>\n      </live>\n    </application>\n  </server>\n</rtmp>\n",
        pubs.len() + players.len()
    ));

    out
}

// ---------- handlers ----------

async fn handle_health() -> Response {
    Json(json!({"status": "ok", "timestamp": now_ts()})).into_response()
}

async fn handle_stats_json(
    State(state): State<Arc<AppState>>,
    Query(q): Query<KeyQuery>,
) -> Response {
    if q.key.is_empty() {
        return err_json(
            StatusCode::UNAUTHORIZED,
            "MISSING_KEY",
            "stats_key required",
        );
    }
    let Some(s) = state.db.stream_find_by_stats_key(&q.key) else {
        return err_json(StatusCode::FORBIDDEN, "INVALID_KEY", "Invalid stats key");
    };
    Json(build_json_stats(&state.db, Some(&s.id))).into_response()
}

async fn handle_stats_nginx(
    State(state): State<Arc<AppState>>,
    Query(q): Query<KeyQuery>,
) -> Response {
    if q.key.is_empty() {
        return err_xml(StatusCode::UNAUTHORIZED, "Missing stats key");
    }
    let Some(s) = state.db.stream_find_by_stats_key(&q.key) else {
        return err_xml(StatusCode::FORBIDDEN, "Invalid stats key");
    };
    xml_response(StatusCode::OK, build_nginx_xml(&state.db, Some(&s.id)))
}

async fn handle_streams_list(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !bearer_ok(&state, &headers) {
        return err_json(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Missing or invalid token",
        );
    }
    // Never expose keys in list view.
    let list: Vec<Value> = state
        .db
        .stream_list()
        .into_iter()
        .map(|s| {
            json!({
                "id": s.id,
                "name": s.name,
                "app": s.app,
                "enabled": s.enabled,
                "created_at": s.created_at,
            })
        })
        .collect();
    Json(list).into_response()
}

#[derive(Deserialize, Default)]
struct CreateStreamRequest {
    id: Option<String>,
    name: Option<String>,
    app: Option<String>,
    allowed_codecs: Option<String>,
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
    let allowed_codecs = req
        .allowed_codecs
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "avc1,hvc1,av01,mp4a".to_string());

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
    if !is_valid_allowed_codecs(&allowed_codecs) {
        return err_json(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "allowed_codecs must be a comma-separated list of codec tokens",
        );
    }

    // Cryptographically unpredictable keys — never derive from stream id/time.
    let publish_key = match keygen_secret("pub_") {
        Ok(k) => k,
        Err(e) => {
            crate::log_error!("publish key generation failed: {e}");
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Key generation failed",
            );
        }
    };
    let play_key = match keygen_secret("pl_") {
        Ok(k) => k,
        Err(e) => {
            crate::log_error!("play key generation failed: {e}");
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Key generation failed",
            );
        }
    };
    let stats_key = match keygen_secret("st_") {
        Ok(k) => k,
        Err(e) => {
            crate::log_error!("stats key generation failed: {e}");
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Key generation failed",
            );
        }
    };
    let s = Stream {
        id: id.clone(),
        name,
        app,
        publish_key,
        play_key,
        stats_key,
        enabled: true,
        allowed_codecs,
        created_at: now_ts(),
    };

    match state.db.stream_add(&s) {
        Ok(()) => {}
        Err(StreamAddError::Duplicate) => {
            return err_json(StatusCode::CONFLICT, "CONFLICT", "Stream ID already exists");
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

    (
        StatusCode::CREATED,
        Json(json!({
            "id": s.id,
            "name": s.name,
            "app": s.app,
            "publish_key": s.publish_key,
            "play_key": s.play_key,
            "stats_key": s.stats_key,
            "enabled": true,
            "created_at": s.created_at,
        })),
    )
        .into_response()
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
    match state.db.stream_delete(&id) {
        Some(true) => {
            crate::log_info!("Stream deleted: {id}");
            // Signal the RTMP poll loop to evict any active connections for
            // this stream. The set is drained once no connections remain.
            state.deleted_streams.lock().unwrap().insert(id);
            Json(json!({"status": "deleted"})).into_response()
        }
        Some(false) => err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Stream not found"),
        None => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Failed to delete stream",
        ),
    }
}

async fn handle_stream_stats(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(q): Query<KeyQuery>,
) -> Response {
    if !stats_key_ok(&state, &q.key, Some(&id)) {
        return err_json(StatusCode::FORBIDDEN, "FORBIDDEN", "Invalid stats key");
    }
    if state.db.stream_get(&id).is_none() {
        return err_json(StatusCode::NOT_FOUND, "NOT_FOUND", "Stream not found");
    }
    Json(build_json_stats(&state.db, Some(&id))).into_response()
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
        Arc::new(AppState {
            db: Arc::new(Db::open(":memory:").unwrap()),
            config,
            deleted_streams: Arc::new(Mutex::new(HashSet::new())),
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
        assert!(is_valid_stream_key_part("live.main_1"));
        assert!(is_valid_stream_key_part(&"a".repeat(63)));
        assert!(!is_valid_stream_key_part(""));
        assert!(!is_valid_stream_key_part("-starts-with-hyphen"));
        assert!(!is_valid_stream_key_part("bad/id"));
        assert!(!is_valid_stream_key_part(&"a".repeat(64)));

        assert!(is_valid_display_name("Main Stream"));
        assert!(!is_valid_display_name("bad\nname"));
        assert!(is_valid_allowed_codecs("avc1,hvc1,av01,mp4a.40.2"));
        assert!(!is_valid_allowed_codecs("avc1,,hvc1"));
        assert!(!is_valid_allowed_codecs("avc1,../../oops"));
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
        assert_eq!(
            state.db.stream_get("mystream").unwrap().allowed_codecs,
            "avc1,hvc1,av01,mp4a"
        );

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
}
