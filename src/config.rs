//! `.env`-style configuration file parsing.

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
            api_token: String::new(),
            require_stream_key: true,
            web_root: "./web".to_string(),
            config_file: String::new(),
            log_level: 2,
            log_file: String::new(),
        }
    }
}

/// Parse a single `.env` line into a (key, value) pair, skipping comments and blanks.
fn parse_env_line(line: &str) -> Option<(String, String)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (key, val) = line.split_once('=')?;
    let key = key.trim().to_string();
    let val = val.trim();
    // Strip optional surrounding quotes (single or double).
    let val = if (val.starts_with('"') && val.ends_with('"'))
        || (val.starts_with('\'') && val.ends_with('\''))
    {
        &val[1..val.len() - 1]
    } else {
        val
    };
    Some((key, val.to_string()))
}

/// Apply a key/value pair from the config file to `config`.
fn apply_kv(config: &mut ServerConfig, key: &str, val: &str) {
    match key {
        "RTMP_BIND" => config.rtmp_bind = val.to_string(),
        "RTMP_MAX_CONNECTIONS" => {
            if let Ok(v) = val.parse::<i32>() {
                config.rtmp_max_conn = v;
            }
        }
        "RTMP_CHUNK_SIZE" => {
            if let Ok(v) = val.parse::<i32>() {
                config.rtmp_chunk_size = v;
            }
        }
        "TLS_ENABLED" => match val {
            "1" | "true" => config.tls_enabled = true,
            "0" | "false" => config.tls_enabled = false,
            _ => {}
        },
        "TLS_CERT_FILE" => config.tls_cert_file = val.to_string(),
        "TLS_KEY_FILE" => config.tls_key_file = val.to_string(),
        "HTTP_BIND" => config.http_bind = val.to_string(),
        "API_TOKEN" => config.api_token = val.to_string(),
        "REQUIRE_STREAM_KEY" => match val {
            "1" | "true" => config.require_stream_key = true,
            "0" | "false" => config.require_stream_key = false,
            _ => {}
        },
        "WEB_ROOT" => config.web_root = val.to_string(),
        "LOG_LEVEL" => {
            if let Ok(v) = val.parse::<i32>() {
                if (0..=3).contains(&v) {
                    config.log_level = v;
                }
            }
        }
        "LOG_FILE" => config.log_file = val.to_string(),
        _ => {} // Unknown keys are silently ignored.
    }
}

/// Load config from a `.env` file, starting from defaults.
pub fn config_load(path: &str) -> Result<ServerConfig, String> {
    let mut config = ServerConfig::default();

    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("Cannot open config file: {path} ({e})"))?;

    for line in text.lines() {
        if let Some((key, val)) = parse_env_line(line) {
            apply_kv(&mut config, &key, &val);
        }
    }

    config.config_file = path.to_string();
    crate::log_info!("Config loaded from {path}");
    crate::log_debug!(
        "RTMP bind={}, HTTP bind={}",
        config.rtmp_bind,
        config.http_bind
    );

    Ok(config)
}

/// Write or update `API_TOKEN=<token>` in the given `.env` file.
///
/// If the file already contains an `API_TOKEN` line it is replaced in-place;
/// otherwise the line is appended so the rest of the config is untouched.
pub fn config_write_token(path: &str, token: &str) -> Result<(), String> {
    let line = format!("API_TOKEN={token}");

    let existing = std::fs::read_to_string(path).unwrap_or_default();
    let mut found = false;
    let mut lines: Vec<String> = existing
        .lines()
        .map(|l| {
            if let Some((k, _)) = parse_env_line(l) {
                if k == "API_TOKEN" {
                    found = true;
                    return line.clone();
                }
            }
            l.to_string()
        })
        .collect();

    if !found {
        if !lines.is_empty() && !lines.last().map(|l| l.is_empty()).unwrap_or(false) {
            lines.push(String::new());
        }
        lines.push(line);
    }

    let mut out = lines.join("\n");
    if !out.ends_with('\n') {
        out.push('\n');
    }

    std::fs::write(path, out).map_err(|e| format!("Cannot write config file {path}: {e}"))
}

/// Environment variables override config file values (except API_TOKEN,
/// which is always auto-generated and managed by the server).
pub fn config_apply_env(config: &mut ServerConfig) {
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
    fn load_from_env_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.env");
        std::fs::write(
            &path,
            "# test config\n\
             RTMP_BIND=127.0.0.1:1936\n\
             RTMP_MAX_CONNECTIONS=50\n\
             TLS_ENABLED=true\n\
             TLS_CERT_FILE=/etc/ssl/cert.pem\n\
             TLS_KEY_FILE=/etc/ssl/key.pem\n\
             HTTP_BIND=127.0.0.1:8081\n\
             API_TOKEN=auto-generated-test-token-xyz\n\
             LOG_LEVEL=3\n",
        )
        .unwrap();

        let config = config_load(path.to_str().unwrap()).expect("config_load");
        assert_eq!(config.rtmp_bind, "127.0.0.1:1936");
        assert_eq!(config.rtmp_max_conn, 50);
        assert_eq!(config.http_bind, "127.0.0.1:8081");
        assert_eq!(config.api_token, "auto-generated-test-token-xyz");
        assert_eq!(config.log_level, 3);
        assert!(config.tls_enabled);
        assert_eq!(config.tls_cert_file, "/etc/ssl/cert.pem");
        assert_eq!(config.tls_key_file, "/etc/ssl/key.pem");
    }

    #[test]
    fn load_missing_file_fails() {
        assert!(config_load("/nonexistent/path.env").is_err());
    }

    #[test]
    fn parse_env_line_handles_comments_and_blanks() {
        assert!(parse_env_line("").is_none());
        assert!(parse_env_line("  ").is_none());
        assert!(parse_env_line("# a comment").is_none());
        assert_eq!(
            parse_env_line("KEY=value"),
            Some(("KEY".to_string(), "value".to_string()))
        );
        assert_eq!(
            parse_env_line(r#"KEY="quoted""#),
            Some(("KEY".to_string(), "quoted".to_string()))
        );
        assert_eq!(
            parse_env_line("KEY=val=with=equals"),
            Some(("KEY".to_string(), "val=with=equals".to_string()))
        );
    }

    #[test]
    fn write_token_appends_and_updates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.env");
        std::fs::write(&path, "HTTP_BIND=0.0.0.0:8080\n").unwrap();

        // First write: appends.
        config_write_token(path.to_str().unwrap(), "first-token-aabbccdd11223344").unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("API_TOKEN=first-token-aabbccdd11223344"));
        assert!(content.contains("HTTP_BIND=0.0.0.0:8080"));

        // Second write: replaces in-place without duplicating.
        config_write_token(path.to_str().unwrap(), "second-token-aabbccdd11223344").unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("API_TOKEN=second-token-aabbccdd11223344"));
        assert!(!content.contains("first-token"));
        assert_eq!(content.matches("API_TOKEN=").count(), 1);
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

        std::env::set_var("LRTMP2_TLS_ENABLED", "yesplease");
        let mut config = ServerConfig {
            tls_enabled: true,
            ..Default::default()
        };
        config_apply_env(&mut config);
        assert!(config.tls_enabled, "invalid value should leave TLS unchanged");

        std::env::remove_var("LRTMP2_TLS_ENABLED");
        std::env::remove_var("LRTMP2_TLS_CERT_FILE");
        std::env::remove_var("LRTMP2_TLS_KEY_FILE");
    }
}
