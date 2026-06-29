//! JSON configuration file parsing.

use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// RTMP listener, e.g. "0.0.0.0:1935"
    pub rtmp_bind: String,
    pub rtmp_max_conn: i32,
    pub rtmp_chunk_size: i32,

    /// RTMPS (TLS) — off by default.
    pub tls_enabled: bool,
    pub tls_cert_file: String,
    pub tls_key_file: String,

    /// HTTP API + UI, e.g. "0.0.0.0:8080"
    pub http_bind: String,

    pub api_token: String,
    pub require_stream_key: bool,

    pub web_root: String,
    /// Path the config was loaded from, kept for diagnostics/reload support.
    #[allow(dead_code)]
    pub config_file: String,

    /// 0=error, 1=warn, 2=info, 3=debug
    pub log_level: i32,
    pub log_file: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            rtmp_bind: "0.0.0.0:1935".to_string(),
            rtmp_max_conn: 100,
            rtmp_chunk_size: 4096,
            tls_enabled: false,
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            http_bind: "0.0.0.0:8080".to_string(),
            // Left empty by default — the server refuses to start with
            // protected endpoints unless a real token is configured.
            api_token: String::new(),
            require_stream_key: true,
            web_root: "./web".to_string(),
            config_file: String::new(),
            log_level: 2,
            log_file: String::new(),
        }
    }
}

/// Returns false for empty, placeholder, or other known-weak API tokens.
pub fn config_api_token_usable(token: &str) -> bool {
    const MIN_TOKEN_LEN: usize = 16;
    if token.is_empty() || token.len() < MIN_TOKEN_LEN {
        return false;
    }

    const WEAK_TOKENS: &[&str] = &[
        "<replace-with-random-token>",
        "changeme",
        "secret",
        "password",
        "api_token",
        "test-token",
        "test-token-123",
        "admin",
        "administrator",
        "123456",
        "12345678",
        "letmein",
        "default",
    ];

    !WEAK_TOKENS.iter().any(|w| w.eq_ignore_ascii_case(token))
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawConfig {
    api_token: Option<String>,
    require_stream_key: Option<bool>,
    log_level: Option<i32>,
    log_file: Option<String>,
    web_root: Option<String>,
    rtmp: Option<RawRtmp>,
    tls: Option<RawTls>,
    http: Option<RawHttp>,
    auth: Option<RawAuth>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawRtmp {
    bind: Option<String>,
    max_connections: Option<i32>,
    chunk_size: Option<i32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawTls {
    enabled: Option<bool>,
    cert_file: Option<String>,
    key_file: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawHttp {
    bind: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RawAuth {
    api_token: Option<String>,
    require_stream_key: Option<bool>,
}

/// Load config from a JSON file, starting from defaults.
pub fn config_load(path: &str) -> Result<ServerConfig, String> {
    let mut config = ServerConfig::default();

    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("Cannot open config file: {path} ({e})"))?;

    let raw: RawConfig =
        serde_json::from_str(&text).map_err(|e| format!("Invalid config JSON in {path}: {e}"))?;

    if let Some(v) = raw.api_token {
        config.api_token = v;
    }
    if let Some(v) = raw.require_stream_key {
        config.require_stream_key = v;
    }
    if let Some(v) = raw.log_level {
        config.log_level = v;
    }
    if let Some(v) = raw.log_file {
        config.log_file = v;
    }
    if let Some(v) = raw.web_root {
        config.web_root = v;
    }

    if let Some(rtmp) = raw.rtmp {
        if let Some(v) = rtmp.bind {
            config.rtmp_bind = v;
        }
        if let Some(v) = rtmp.max_connections {
            config.rtmp_max_conn = v;
        }
        if let Some(v) = rtmp.chunk_size {
            config.rtmp_chunk_size = v;
        }
    }

    if let Some(tls) = raw.tls {
        if let Some(v) = tls.enabled {
            config.tls_enabled = v;
        }
        if let Some(v) = tls.cert_file {
            config.tls_cert_file = v;
        }
        if let Some(v) = tls.key_file {
            config.tls_key_file = v;
        }
    }

    if let Some(http) = raw.http {
        if let Some(v) = http.bind {
            config.http_bind = v;
        }
    }

    if let Some(auth) = raw.auth {
        if let Some(v) = auth.api_token {
            config.api_token = v;
        }
        if let Some(v) = auth.require_stream_key {
            config.require_stream_key = v;
        }
    }

    crate::log_info!("Config loaded from {path}");
    crate::log_debug!(
        "RTMP bind={}, HTTP bind={}",
        config.rtmp_bind,
        config.http_bind
    );

    Ok(config)
}

/// Environment variables override config file values.
pub fn config_apply_env(config: &mut ServerConfig) {
    if let Ok(v) = std::env::var("LRTMP2_API_TOKEN") {
        if !v.is_empty() {
            config.api_token = v;
        }
    }

    if let Ok(v) = std::env::var("LRTMP2_RTMP_BIND") {
        if !v.is_empty() {
            config.rtmp_bind = v;
        }
    }

    if let Ok(v) = std::env::var("LRTMP2_HTTP_BIND") {
        if !v.is_empty() {
            config.http_bind = v;
        }
    }

    if let Ok(v) = std::env::var("LRTMP2_TLS_ENABLED") {
        if !v.is_empty() {
            // Only recognized values flip the flag; a typo or unsupported
            // form leaves the configured value untouched rather than
            // silently downgrading to plaintext.
            match v.as_str() {
                "1" | "true" => config.tls_enabled = true,
                "0" | "false" => config.tls_enabled = false,
                _ => crate::log_warn!(
                    "Ignoring invalid LRTMP2_TLS_ENABLED value '{v}' (expected 1/0/true/false)"
                ),
            }
        }
    }

    if let Ok(v) = std::env::var("LRTMP2_TLS_CERT_FILE") {
        if !v.is_empty() {
            config.tls_cert_file = v;
        }
    }

    if let Ok(v) = std::env::var("LRTMP2_TLS_KEY_FILE") {
        if !v.is_empty() {
            config.tls_key_file = v;
        }
    }

    if let Ok(v) = std::env::var("LRTMP2_LOG_LEVEL") {
        match v.parse::<i32>() {
            Ok(lvl) if (0..=3).contains(&lvl) => config.log_level = lvl,
            _ => crate::log_warn!("Ignoring invalid LRTMP2_LOG_LEVEL value '{v}' (expected 0-3)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Environment variable tests mutate global process state, so they must
    // not run concurrently with each other.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn defaults() {
        let config = ServerConfig::default();
        assert_eq!(config.rtmp_bind, "0.0.0.0:1935");
        assert_eq!(config.http_bind, "0.0.0.0:8080");
        assert_eq!(config.rtmp_max_conn, 100);
        assert_eq!(config.log_level, 2);
        assert!(!config.tls_enabled);
        assert!(config.tls_cert_file.is_empty());
        assert!(config.tls_key_file.is_empty());
    }

    #[test]
    fn load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "rtmp": {"bind": "127.0.0.1:1936", "max_connections": 50},
                "tls": {"enabled": true, "cert_file": "/etc/ssl/cert.pem", "key_file": "/etc/ssl/key.pem"},
                "http": {"bind": "127.0.0.1:8081"},
                "auth": {"api_token": "test-token-123"},
                "log_level": 3
            }"#,
        )
        .unwrap();

        let config = config_load(path.to_str().unwrap()).expect("config_load");
        assert_eq!(config.rtmp_bind, "127.0.0.1:1936");
        assert_eq!(config.rtmp_max_conn, 50);
        assert_eq!(config.http_bind, "127.0.0.1:8081");
        assert_eq!(config.api_token, "test-token-123");
        assert_eq!(config.log_level, 3);
        assert!(config.tls_enabled);
        assert_eq!(config.tls_cert_file, "/etc/ssl/cert.pem");
        assert_eq!(config.tls_key_file, "/etc/ssl/key.pem");
    }

    #[test]
    fn load_missing_file_fails() {
        assert!(config_load("/nonexistent/path.json").is_err());
    }

    #[test]
    fn env_overrides_tls() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LRTMP2_TLS_ENABLED", "1");
        std::env::set_var("LRTMP2_TLS_CERT_FILE", "/env/cert.pem");
        std::env::set_var("LRTMP2_TLS_KEY_FILE", "/env/key.pem");

        let mut config = ServerConfig::default();
        config_apply_env(&mut config);

        assert!(config.tls_enabled);
        assert_eq!(config.tls_cert_file, "/env/cert.pem");
        assert_eq!(config.tls_key_file, "/env/key.pem");

        // An invalid value must not silently flip TLS off.
        std::env::set_var("LRTMP2_TLS_ENABLED", "yesplease");
        // pretend the JSON enabled it
        let mut config = ServerConfig {
            tls_enabled: true,
            ..Default::default()
        };
        config_apply_env(&mut config);
        assert!(
            config.tls_enabled,
            "invalid value should leave TLS unchanged"
        );

        std::env::remove_var("LRTMP2_TLS_ENABLED");
        std::env::remove_var("LRTMP2_TLS_CERT_FILE");
        std::env::remove_var("LRTMP2_TLS_KEY_FILE");
    }

    #[test]
    fn weak_tokens_rejected() {
        assert!(!config_api_token_usable(""));
        assert!(!config_api_token_usable("<replace-with-random-token>"));
        assert!(config_api_token_usable("a-strong-random-secret-value"));
    }
}
