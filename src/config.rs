//! `.env`-style configuration file parsing.

use std::net::IpAddr;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// RTMP listener, e.g. "0.0.0.0:1935". Always active.
    pub rtmp_bind: String,
    pub rtmp_max_conn: i32,

    /// Per-connection reassembly buffer cap (megabytes).
    pub rtmp_max_reassembly_mb: u32,
    /// Server-wide stream codec cache cap (megabytes).
    pub rtmp_max_cache_mb: u32,
    /// Per-publisher relay queue cap (megabytes).
    pub rtmp_max_relay_queue_mb: u32,

    /// RTMPS (TLS) — off by default. When enabled, a second RTMPS listener
    /// is started on `rtmps_bind` *alongside* the plaintext `rtmp_bind`
    /// listener — both accept connections at the same time.
    pub tls_enabled: bool,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    /// RTMPS listener, e.g. "0.0.0.0:1936". Only bound when `tls_enabled`.
    pub rtmps_bind: String,

    /// HTTP API + UI, e.g. "0.0.0.0:8080"
    pub http_bind: String,
    /// When the TCP peer is one of these addresses, use `X-Forwarded-For` for rate limiting.
    pub http_trusted_proxies: Vec<IpAddr>,

    /// Populated at startup from the database, never from the config file.
    pub api_token: String,

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
            rtmp_max_reassembly_mb: 32,
            rtmp_max_cache_mb: 64,
            rtmp_max_relay_queue_mb: 8,
            tls_enabled: false,
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            rtmps_bind: "0.0.0.0:1936".to_string(),
            http_bind: "0.0.0.0:8080".to_string(),
            http_trusted_proxies: Vec::new(),
            api_token: String::new(),
            config_file: String::new(),
            log_level: 2,
            log_file: String::new(),
        }
    }
}

impl ServerConfig {
    /// Memory limits passed into the librtmp2 server instance.
    pub fn rtmp_resource_limits(&self) -> librtmp2::ResourceLimits {
        librtmp2::ResourceLimits {
            max_stream_cache_bytes: mb_to_bytes(self.rtmp_max_cache_mb),
            max_reassembly_bytes: mb_to_bytes(self.rtmp_max_reassembly_mb),
            max_pending_relay_bytes: mb_to_bytes(self.rtmp_max_relay_queue_mb),
        }
    }

    /// Port parsed from `rtmp_bind` ("host:port"), or 0 if unparsable.
    pub fn rtmp_port(&self) -> u16 {
        port_of(&self.rtmp_bind)
    }

    /// Port parsed from `rtmps_bind` ("host:port"), or 0 if unparsable.
    pub fn rtmps_port(&self) -> u16 {
        port_of(&self.rtmps_bind)
    }
}

/// Extract the port number from a "host:port" string, mirroring how
/// `librtmp2::net::split_host_port` (the actual bind-time parser) tells a
/// port apart from a bare IPv6 host:
/// - `"[v6addr]:port"` / `"[v6addr]"` — bracketed IPv6; port must immediately
///   follow the closing `]`, or there is none.
/// - exactly one `:` — `"host:port"`.
/// - zero or 2+ `:` with no brackets — no port (a bare IPv6 literal like
///   `"::1"` has 2+ colons and *no* port of its own; naively splitting on the
///   last `:` would misparse its final hextet as a port).
fn port_of(bind: &str) -> u16 {
    if let Some(bracket_end) = bind.rfind(']') {
        return bind[bracket_end + 1..]
            .strip_prefix(':')
            .and_then(|port| port.parse::<u16>().ok())
            .unwrap_or(0);
    }
    if bind.chars().filter(|&c| c == ':').count() != 1 {
        return 0;
    }
    bind.rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .unwrap_or(0)
}

fn mb_to_bytes(mb: u32) -> usize {
    (mb as usize).saturating_mul(1024 * 1024)
}

fn clamp_u32(val: u32, min: u32, max: u32) -> u32 {
    val.clamp(min, max)
}

fn parse_mb(val: &str, default: u32, min: u32, max: u32, key: &str) -> u32 {
    match val.parse::<u32>() {
        Ok(v) => clamp_u32(v, min, max),
        Err(_) => {
            eprintln!("Config: ignoring invalid {key} value '{val}'");
            default
        }
    }
}

fn parse_max_connections(val: &str) -> i32 {
    match val.parse::<i32>() {
        Ok(v) => v.clamp(1, 10_000),
        Err(_) => {
            eprintln!("Config: ignoring invalid RTMP_MAX_CONNECTIONS value '{val}'");
            100
        }
    }
}

fn parse_ip_list(val: &str) -> Vec<IpAddr> {
    val.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .filter_map(|part| match part.parse::<IpAddr>() {
            Ok(ip) => Some(ip),
            Err(_) => {
                eprintln!("Config: ignoring invalid HTTP_TRUSTED_PROXIES entry '{part}'");
                None
            }
        })
        .collect()
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
    // Strip optional surrounding quotes (single or double), but only when both
    // the opening and closing character are present and the string is at least
    // two characters long — a bare `KEY="` would otherwise panic on the slice.
    let is_quoted = val.len() >= 2
        && ((val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\'')));
    let val = if is_quoted {
        &val[1..val.len() - 1]
    } else {
        val
    };
    Some((key, val.to_string()))
}

/// Apply a key/value pair from the config file to `config`.
/// `API_TOKEN` is intentionally not handled here; the token is managed
/// exclusively by the database layer and cannot be set via the config file.
fn apply_kv(config: &mut ServerConfig, key: &str, val: &str) {
    match key {
        "RTMP_BIND" => config.rtmp_bind = val.to_string(),
        "RTMP_MAX_CONNECTIONS" => config.rtmp_max_conn = parse_max_connections(val),
        "RTMP_MAX_REASSEMBLY_MB" => {
            config.rtmp_max_reassembly_mb = parse_mb(val, 32, 1, 256, key);
        }
        "RTMP_MAX_CACHE_MB" => {
            config.rtmp_max_cache_mb = parse_mb(val, 64, 1, 512, key);
        }
        "RTMP_MAX_RELAY_QUEUE_MB" => {
            config.rtmp_max_relay_queue_mb = parse_mb(val, 8, 1, 128, key);
        }
        "TLS_ENABLED" => match val {
            "1" | "true" => config.tls_enabled = true,
            "0" | "false" => config.tls_enabled = false,
            _ => eprintln!(
                "Config: ignoring invalid TLS_ENABLED value '{val}' (expected 1/0/true/false)"
            ),
        },
        "TLS_CERT_FILE" => config.tls_cert_file = val.to_string(),
        "TLS_KEY_FILE" => config.tls_key_file = val.to_string(),
        "RTMPS_BIND" => config.rtmps_bind = val.to_string(),
        "HTTP_BIND" => config.http_bind = val.to_string(),
        "HTTP_TRUSTED_PROXIES" => config.http_trusted_proxies = parse_ip_list(val),
        "LOG_LEVEL" => match val.parse::<i32>() {
            Ok(v) if (0..=3).contains(&v) => config.log_level = v,
            _ => eprintln!("Config: ignoring invalid LOG_LEVEL value '{val}' (expected 0-3)"),
        },
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

/// Environment variables override config file values.
/// The API token is managed exclusively by the database layer and cannot be
/// set via environment or config file.
pub fn config_apply_env(config: &mut ServerConfig) {
    config_apply_env_from(config, |key| std::env::var(key).ok());
}

fn config_apply_env_from<F>(config: &mut ServerConfig, mut get: F)
where
    F: FnMut(&str) -> Option<String>,
{
    if let Some(v) = get("LRTMP2_RTMP_BIND")
        && !v.is_empty()
    {
        config.rtmp_bind = v;
    }

    if let Some(v) = get("LRTMP2_HTTP_BIND")
        && !v.is_empty()
    {
        config.http_bind = v;
    }

    if let Some(v) = get("LRTMP2_RTMP_MAX_CONNECTIONS")
        && !v.is_empty()
    {
        config.rtmp_max_conn = parse_max_connections(&v);
    }

    if let Some(v) = get("LRTMP2_RTMP_MAX_REASSEMBLY_MB")
        && !v.is_empty()
    {
        config.rtmp_max_reassembly_mb = parse_mb(&v, 32, 1, 256, "LRTMP2_RTMP_MAX_REASSEMBLY_MB");
    }

    if let Some(v) = get("LRTMP2_RTMP_MAX_CACHE_MB")
        && !v.is_empty()
    {
        config.rtmp_max_cache_mb = parse_mb(&v, 64, 1, 512, "LRTMP2_RTMP_MAX_CACHE_MB");
    }

    if let Some(v) = get("LRTMP2_RTMP_MAX_RELAY_QUEUE_MB")
        && !v.is_empty()
    {
        config.rtmp_max_relay_queue_mb = parse_mb(&v, 8, 1, 128, "LRTMP2_RTMP_MAX_RELAY_QUEUE_MB");
    }

    if let Some(v) = get("LRTMP2_TLS_ENABLED")
        && !v.is_empty()
    {
        match v.as_str() {
            "1" | "true" => config.tls_enabled = true,
            "0" | "false" => config.tls_enabled = false,
            _ => crate::log_warn!(
                "Ignoring invalid LRTMP2_TLS_ENABLED value '{v}' (expected 1/0/true/false)"
            ),
        }
    }

    if let Some(v) = get("LRTMP2_TLS_CERT_FILE")
        && !v.is_empty()
    {
        config.tls_cert_file = v;
    }

    if let Some(v) = get("LRTMP2_TLS_KEY_FILE")
        && !v.is_empty()
    {
        config.tls_key_file = v;
    }

    if let Some(v) = get("LRTMP2_RTMPS_BIND")
        && !v.is_empty()
    {
        config.rtmps_bind = v;
    }

    if let Some(v) = get("LRTMP2_HTTP_TRUSTED_PROXIES")
        && !v.is_empty()
    {
        config.http_trusted_proxies = parse_ip_list(&v);
    }

    if let Some(v) = get("LRTMP2_LOG_LEVEL") {
        match v.parse::<i32>() {
            Ok(lvl) if (0..=3).contains(&lvl) => config.log_level = lvl,
            _ => crate::log_warn!("Ignoring invalid LRTMP2_LOG_LEVEL value '{v}' (expected 0-3)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn defaults() {
        let config = ServerConfig::default();
        assert_eq!(config.rtmp_bind, "0.0.0.0:1935");
        assert_eq!(config.http_bind, "0.0.0.0:8080");
        assert_eq!(config.rtmp_max_conn, 100);
        assert_eq!(config.rtmp_max_reassembly_mb, 32);
        assert_eq!(config.rtmp_max_cache_mb, 64);
        assert_eq!(config.rtmp_max_relay_queue_mb, 8);
        assert_eq!(config.log_level, 2);
        assert!(!config.tls_enabled);
        assert!(config.tls_cert_file.is_empty());
        assert!(config.tls_key_file.is_empty());
        assert_eq!(config.rtmps_bind, "0.0.0.0:1936");
        assert!(config.http_trusted_proxies.is_empty());
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
             RTMP_MAX_REASSEMBLY_MB=16\n\
             RTMP_MAX_CACHE_MB=32\n\
             RTMP_MAX_RELAY_QUEUE_MB=4\n\
             TLS_ENABLED=true\n\
             TLS_CERT_FILE=/etc/ssl/cert.pem\n\
             TLS_KEY_FILE=/etc/ssl/key.pem\n\
             RTMPS_BIND=127.0.0.1:1937\n\
             HTTP_BIND=127.0.0.1:8081\n\
             HTTP_TRUSTED_PROXIES=127.0.0.1,10.0.0.1\n\
             LOG_LEVEL=3\n",
        )
        .unwrap();

        let config = config_load(path.to_str().unwrap()).expect("config_load");
        assert_eq!(config.rtmp_bind, "127.0.0.1:1936");
        assert_eq!(config.rtmp_max_conn, 50);
        assert_eq!(config.rtmp_max_reassembly_mb, 16);
        assert_eq!(config.rtmp_max_cache_mb, 32);
        assert_eq!(config.rtmp_max_relay_queue_mb, 4);
        assert_eq!(config.http_bind, "127.0.0.1:8081");
        assert!(
            config.api_token.is_empty(),
            "api_token must not be set from config file"
        );
        assert_eq!(config.log_level, 3);
        assert!(config.tls_enabled);
        assert_eq!(config.tls_cert_file, "/etc/ssl/cert.pem");
        assert_eq!(config.tls_key_file, "/etc/ssl/key.pem");
        assert_eq!(config.rtmps_bind, "127.0.0.1:1937");
        assert_eq!(config.http_trusted_proxies.len(), 2);
    }

    #[test]
    fn api_token_ignored_in_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.env");
        std::fs::write(&path, "API_TOKEN=should-be-ignored\n").unwrap();
        let config = config_load(path.to_str().unwrap()).unwrap();
        assert!(
            config.api_token.is_empty(),
            "API_TOKEN in config file must be ignored"
        );
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
        // Single-char quote edge cases must not panic.
        assert_eq!(
            parse_env_line(r#"KEY=""#),
            Some(("KEY".to_string(), "\"".to_string()))
        );
        assert_eq!(
            parse_env_line("KEY='"),
            Some(("KEY".to_string(), "'".to_string()))
        );
    }

    #[test]
    fn env_overrides_tls() {
        let env = HashMap::from([
            ("LRTMP2_TLS_ENABLED", "1"),
            ("LRTMP2_TLS_CERT_FILE", "/env/cert.pem"),
            ("LRTMP2_TLS_KEY_FILE", "/env/key.pem"),
            ("LRTMP2_RTMPS_BIND", "0.0.0.0:9443"),
        ]);

        let mut config = ServerConfig::default();
        config_apply_env_from(&mut config, |key| env.get(key).map(|v| v.to_string()));

        assert!(config.tls_enabled);
        assert_eq!(config.tls_cert_file, "/env/cert.pem");
        assert_eq!(config.tls_key_file, "/env/key.pem");
        assert_eq!(config.rtmps_bind, "0.0.0.0:9443");

        let env = HashMap::from([("LRTMP2_TLS_ENABLED", "yesplease")]);
        let mut config = ServerConfig {
            tls_enabled: true,
            ..Default::default()
        };
        config_apply_env_from(&mut config, |key| env.get(key).map(|v| v.to_string()));
        assert!(
            config.tls_enabled,
            "invalid value should leave TLS unchanged"
        );
    }

    #[test]
    fn max_connections_are_clamped() {
        assert_eq!(parse_max_connections("0"), 1);
        assert_eq!(parse_max_connections("99999"), 10_000);
    }

    #[test]
    fn port_helpers_parse_bind_strings() {
        let config = ServerConfig {
            rtmp_bind: "0.0.0.0:1935".to_string(),
            rtmps_bind: "0.0.0.0:1936".to_string(),
            ..Default::default()
        };
        assert_eq!(config.rtmp_port(), 1935);
        assert_eq!(config.rtmps_port(), 1936);
    }

    #[test]
    fn port_of_handles_bracketed_ipv6() {
        assert_eq!(port_of("[::1]:1935"), 1935);
        assert_eq!(port_of("[2001:db8::1]:8080"), 8080);
        // Portless bracketed IPv6 must not misparse the literal's own colons
        // as a "host:port" split — there is no port here.
        assert_eq!(port_of("[::1]"), 0);
        assert_eq!(port_of("not-a-bind"), 0);
    }

    #[test]
    fn port_of_treats_bare_unbracketed_ipv6_as_portless() {
        // Matches librtmp2::net::split_host_port: an unbracketed literal with
        // 2+ colons is a bare IPv6 host with no port of its own, not
        // "host:port". Naively splitting on the last ':' would misparse the
        // literal's final hextet ("1") as the port.
        assert_eq!(port_of("::1"), 0);
        assert_eq!(port_of("fe80::1"), 0);
        assert_eq!(port_of("2001:db8::1"), 0);
    }
}
