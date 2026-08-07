//! Cluster configuration from `.env` + `LRTMP2_CLUSTER_*` overrides.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// How [`measure_interface_utilization`](super::admission::measure_interface_utilization)
/// combines RX and TX byte counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BandwidthMode {
    /// Transmit only (default).
    #[default]
    Tx,
    /// Receive only.
    Rx,
    /// `max(rx, tx)`.
    Max,
    /// `rx + tx` (can double-count relay traffic).
    Sum,
}

impl BandwidthMode {
    pub fn parse(val: &str) -> Option<Self> {
        match val.trim().to_ascii_lowercase().as_str() {
            "tx" => Some(Self::Tx),
            "rx" => Some(Self::Rx),
            "max" => Some(Self::Max),
            "sum" => Some(Self::Sum),
            _ => None,
        }
    }

    pub fn combine(self, rx_bps: f64, tx_bps: f64) -> f64 {
        match self {
            Self::Tx => tx_bps,
            Self::Rx => rx_bps,
            Self::Max => rx_bps.max(tx_bps),
            Self::Sum => rx_bps + tx_bps,
        }
    }
}

/// Parsed cluster settings. Validated only when `enabled` is true.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: u64,
    pub bind: String,
    pub media_bind: String,
    pub bootstrap: bool,
    pub join: Option<String>,
    pub secret: String,
    pub tls_enabled: bool,
    pub tls_cert_file: PathBuf,
    pub tls_key_file: PathBuf,
    pub tls_ca_file: PathBuf,
    pub heartbeat: Duration,
    pub capacity: f64,
    pub drain_threshold: f64,
    pub resume_threshold: f64,
    pub bandwidth_interface: String,
    /// How to combine RX/TX when measuring interface utilization (`tx` default).
    pub bandwidth_mode: BandwidthMode,
    pub bandwidth_max_mbps: f64,
    pub media_replicas: u32,
    pub media_queue_mb: u32,
    pub advertise_addr: Option<String>,
    pub media_advertise_addr: Option<String>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 0,
            bind: "0.0.0.0:1940".to_string(),
            media_bind: "0.0.0.0:1941".to_string(),
            bootstrap: false,
            join: None,
            secret: String::new(),
            tls_enabled: false,
            tls_cert_file: PathBuf::new(),
            tls_key_file: PathBuf::new(),
            tls_ca_file: PathBuf::new(),
            heartbeat: Duration::from_millis(500),
            capacity: 1.0,
            drain_threshold: 0.85,
            resume_threshold: 0.70,
            bandwidth_interface: String::new(),
            bandwidth_mode: BandwidthMode::Tx,
            bandwidth_max_mbps: 0.0,
            media_replicas: 0,
            media_queue_mb: 64,
            advertise_addr: None,
            media_advertise_addr: None,
        }
    }
}

impl ClusterConfig {
    /// Load from an already-parsed `.env` key map plus process environment.
    pub fn load_from_kv<F>(mut get_file: F) -> Result<Self, String>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let mut cfg = ClusterConfig::default();
        let mut drain_at_mbps: Option<f64> = None;
        let mut resume_at_mbps: Option<f64> = None;

        let mut apply = |key: &str, val: &str| match key {
            "CLUSTER_DRAIN_AT_MBPS" => {
                if let Ok(mbps) = val.parse::<f64>() {
                    drain_at_mbps = Some(mbps);
                }
            }
            "CLUSTER_RESUME_AT_MBPS" => {
                if let Ok(mbps) = val.parse::<f64>() {
                    resume_at_mbps = Some(mbps);
                }
            }
            _ => apply_cluster_kv(&mut cfg, key, val),
        };

        for key in CLUSTER_FILE_KEYS {
            if let Some(val) = get_file(key) {
                apply(key, &val);
            }
        }

        // Process env overrides (LRTMP2_CLUSTER_*).
        for (env_key, file_key) in CLUSTER_ENV_OVERRIDES {
            if let Ok(val) = std::env::var(env_key)
                && !val.is_empty()
            {
                apply(file_key, &val);
            }
        }

        normalize_absolute_bandwidth_thresholds(&mut cfg, drain_at_mbps, resume_at_mbps);

        if cfg.enabled {
            cfg.validate()?;
        }
        Ok(cfg)
    }

    /// Apply `LRTMP2_CLUSTER_*` process overrides on top of an already-loaded
    /// cluster block (file / previous defaults). Does not rebuild from
    /// [`ClusterConfig::default`], so unspecified keys keep file values.
    pub fn apply_env_overrides_from<F>(&mut self, mut get: F) -> Result<(), String>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let mut drain_at_mbps: Option<f64> = None;
        let mut resume_at_mbps: Option<f64> = None;
        for (env_key, file_key) in CLUSTER_ENV_OVERRIDES {
            if let Some(val) = get(env_key).filter(|v| !v.is_empty()) {
                match *file_key {
                    "CLUSTER_DRAIN_AT_MBPS" => {
                        if let Ok(mbps) = val.parse::<f64>() {
                            drain_at_mbps = Some(mbps);
                        }
                    }
                    "CLUSTER_RESUME_AT_MBPS" => {
                        if let Ok(mbps) = val.parse::<f64>() {
                            resume_at_mbps = Some(mbps);
                        }
                    }
                    _ => apply_cluster_kv(self, file_key, &val),
                }
            }
        }
        normalize_absolute_bandwidth_thresholds(self, drain_at_mbps, resume_at_mbps);
        if self.enabled {
            self.validate()?;
        }
        Ok(())
    }

    /// Load from a config file path (same format as server `.env`).
    pub fn load(path: &str) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("Cannot open config file: {path} ({e})"))?;
        let mut map = std::collections::HashMap::new();
        for line in text.lines() {
            if let Some((k, v)) = crate::config::parse_env_line_public(line) {
                map.insert(k, v);
            }
        }
        Self::load_from_kv(|k| map.get(k).cloned())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.node_id == 0 {
            return Err(
                "CLUSTER_NODE_ID must be a positive integer when CLUSTER_ENABLED=true".into(),
            );
        }
        if self.secret.len() < 16 {
            return Err(
                "CLUSTER_SECRET must be at least 16 characters when CLUSTER_ENABLED=true".into(),
            );
        }
        parse_bind(&self.bind).map_err(|e| format!("CLUSTER_BIND: {e}"))?;
        parse_bind(&self.media_bind).map_err(|e| format!("CLUSTER_MEDIA_BIND: {e}"))?;
        if self.bootstrap && self.join.is_some() {
            return Err("CLUSTER_BOOTSTRAP and CLUSTER_JOIN are mutually exclusive".into());
        }
        if !self.bootstrap && self.join.is_none() {
            return Err(
                "When CLUSTER_ENABLED=true set CLUSTER_BOOTSTRAP=true or CLUSTER_JOIN=<addr>"
                    .into(),
            );
        }
        if self.tls_enabled {
            if self.tls_cert_file.as_os_str().is_empty()
                || self.tls_key_file.as_os_str().is_empty()
                || self.tls_ca_file.as_os_str().is_empty()
            {
                return Err(
                    "CLUSTER_TLS_ENABLED requires CLUSTER_TLS_CERT_FILE, CLUSTER_TLS_KEY_FILE, and CLUSTER_TLS_CA_FILE"
                        .into(),
                );
            }
            for (label, path) in [
                ("CLUSTER_TLS_CERT_FILE", &self.tls_cert_file),
                ("CLUSTER_TLS_KEY_FILE", &self.tls_key_file),
                ("CLUSTER_TLS_CA_FILE", &self.tls_ca_file),
            ] {
                if !path.is_file() {
                    return Err(format!(
                        "{label} does not exist or is not a file: {}",
                        path.display()
                    ));
                }
            }
        }
        if !self.drain_threshold.is_finite() || !(0.0..=1.0).contains(&self.drain_threshold) {
            return Err("CLUSTER_DRAIN_THRESHOLD must be a finite value in 0.0..=1.0".into());
        }
        if !self.resume_threshold.is_finite() || !(0.0..=1.0).contains(&self.resume_threshold) {
            return Err("CLUSTER_RESUME_THRESHOLD must be a finite value in 0.0..=1.0".into());
        }
        if self.drain_threshold <= self.resume_threshold {
            return Err(
                "CLUSTER_DRAIN_THRESHOLD must be greater than CLUSTER_RESUME_THRESHOLD".into(),
            );
        }
        if !self.capacity.is_finite() || !(0.0..=1.0).contains(&self.capacity) {
            return Err("CLUSTER_CAPACITY must be a finite value between 0.0 and 1.0".into());
        }
        if self.media_queue_mb == 0 || self.media_queue_mb > 1024 {
            return Err("CLUSTER_MEDIA_QUEUE_MB must be 1–1024".into());
        }
        Ok(())
    }

    pub fn control_socket_addr(&self) -> Result<SocketAddr, String> {
        parse_bind(&self.bind)
    }

    pub fn media_socket_addr(&self) -> Result<SocketAddr, String> {
        parse_bind(&self.media_bind)
    }

    pub fn advertise_control(&self) -> String {
        self.advertise_addr
            .clone()
            .unwrap_or_else(|| rewrite_wildcard_to_loopback(&self.bind))
    }

    pub fn advertise_media(&self) -> String {
        self.media_advertise_addr
            .clone()
            .unwrap_or_else(|| rewrite_wildcard_to_loopback(&self.media_bind))
    }
}

const CLUSTER_FILE_KEYS: &[&str] = &[
    "CLUSTER_ENABLED",
    "CLUSTER_NODE_ID",
    "CLUSTER_NODE_NAME",
    "CLUSTER_BIND",
    "CLUSTER_MEDIA_BIND",
    "CLUSTER_BOOTSTRAP",
    "CLUSTER_JOIN",
    "CLUSTER_SECRET",
    "CLUSTER_TLS_ENABLED",
    "CLUSTER_TLS_CERT_FILE",
    "CLUSTER_TLS_KEY_FILE",
    "CLUSTER_TLS_CA_FILE",
    "CLUSTER_HEARTBEAT_MS",
    "CLUSTER_HEARTBEAT_INTERVAL_MS",
    "CLUSTER_NODE_TIMEOUT_MS",
    "CLUSTER_CAPACITY",
    "CLUSTER_CAPACITY_MBPS",
    "CLUSTER_DRAIN_THRESHOLD",
    "CLUSTER_DRAIN_AT_MBPS",
    "CLUSTER_RESUME_THRESHOLD",
    "CLUSTER_RESUME_AT_MBPS",
    "CLUSTER_BANDWIDTH_INTERFACE",
    "CLUSTER_BANDWIDTH_MODE",
    "CLUSTER_BANDWIDTH_MAX_MBPS",
    "CLUSTER_MEDIA_REPLICAS",
    "CLUSTER_MEDIA_QUEUE_MB",
    "CLUSTER_ADVERTISE_ADDR",
    "CLUSTER_MEDIA_ADVERTISE_ADDR",
];

const CLUSTER_ENV_OVERRIDES: &[(&str, &str)] = &[
    ("LRTMP2_CLUSTER_ENABLED", "CLUSTER_ENABLED"),
    ("LRTMP2_CLUSTER_NODE_ID", "CLUSTER_NODE_ID"),
    ("LRTMP2_CLUSTER_NODE_NAME", "CLUSTER_NODE_NAME"),
    ("LRTMP2_CLUSTER_BIND", "CLUSTER_BIND"),
    ("LRTMP2_CLUSTER_MEDIA_BIND", "CLUSTER_MEDIA_BIND"),
    ("LRTMP2_CLUSTER_BOOTSTRAP", "CLUSTER_BOOTSTRAP"),
    ("LRTMP2_CLUSTER_JOIN", "CLUSTER_JOIN"),
    ("LRTMP2_CLUSTER_SECRET", "CLUSTER_SECRET"),
    ("LRTMP2_CLUSTER_TLS_ENABLED", "CLUSTER_TLS_ENABLED"),
    ("LRTMP2_CLUSTER_TLS_CERT_FILE", "CLUSTER_TLS_CERT_FILE"),
    ("LRTMP2_CLUSTER_TLS_KEY_FILE", "CLUSTER_TLS_KEY_FILE"),
    ("LRTMP2_CLUSTER_TLS_CA_FILE", "CLUSTER_TLS_CA_FILE"),
    ("LRTMP2_CLUSTER_HEARTBEAT_MS", "CLUSTER_HEARTBEAT_MS"),
    (
        "LRTMP2_CLUSTER_HEARTBEAT_INTERVAL_MS",
        "CLUSTER_HEARTBEAT_INTERVAL_MS",
    ),
    ("LRTMP2_CLUSTER_NODE_TIMEOUT_MS", "CLUSTER_NODE_TIMEOUT_MS"),
    ("LRTMP2_CLUSTER_CAPACITY", "CLUSTER_CAPACITY"),
    ("LRTMP2_CLUSTER_CAPACITY_MBPS", "CLUSTER_CAPACITY_MBPS"),
    ("LRTMP2_CLUSTER_DRAIN_THRESHOLD", "CLUSTER_DRAIN_THRESHOLD"),
    ("LRTMP2_CLUSTER_DRAIN_AT_MBPS", "CLUSTER_DRAIN_AT_MBPS"),
    (
        "LRTMP2_CLUSTER_RESUME_THRESHOLD",
        "CLUSTER_RESUME_THRESHOLD",
    ),
    ("LRTMP2_CLUSTER_RESUME_AT_MBPS", "CLUSTER_RESUME_AT_MBPS"),
    (
        "LRTMP2_CLUSTER_BANDWIDTH_INTERFACE",
        "CLUSTER_BANDWIDTH_INTERFACE",
    ),
    ("LRTMP2_CLUSTER_BANDWIDTH_MODE", "CLUSTER_BANDWIDTH_MODE"),
    (
        "LRTMP2_CLUSTER_BANDWIDTH_MAX_MBPS",
        "CLUSTER_BANDWIDTH_MAX_MBPS",
    ),
    ("LRTMP2_CLUSTER_MEDIA_REPLICAS", "CLUSTER_MEDIA_REPLICAS"),
    ("LRTMP2_CLUSTER_MEDIA_QUEUE_MB", "CLUSTER_MEDIA_QUEUE_MB"),
    ("LRTMP2_CLUSTER_ADVERTISE_ADDR", "CLUSTER_ADVERTISE_ADDR"),
    (
        "LRTMP2_CLUSTER_MEDIA_ADVERTISE_ADDR",
        "CLUSTER_MEDIA_ADVERTISE_ADDR",
    ),
];

/// Process-env keys that may override a file-loaded [`ClusterConfig`].
pub const CLUSTER_ENV_OVERRIDE_KEYS: &[&str] = &[
    "LRTMP2_CLUSTER_ENABLED",
    "LRTMP2_CLUSTER_NODE_ID",
    "LRTMP2_CLUSTER_NODE_NAME",
    "LRTMP2_CLUSTER_BIND",
    "LRTMP2_CLUSTER_MEDIA_BIND",
    "LRTMP2_CLUSTER_BOOTSTRAP",
    "LRTMP2_CLUSTER_JOIN",
    "LRTMP2_CLUSTER_SECRET",
    "LRTMP2_CLUSTER_TLS_ENABLED",
    "LRTMP2_CLUSTER_TLS_CERT_FILE",
    "LRTMP2_CLUSTER_TLS_KEY_FILE",
    "LRTMP2_CLUSTER_TLS_CA_FILE",
    "LRTMP2_CLUSTER_HEARTBEAT_MS",
    "LRTMP2_CLUSTER_HEARTBEAT_INTERVAL_MS",
    "LRTMP2_CLUSTER_NODE_TIMEOUT_MS",
    "LRTMP2_CLUSTER_CAPACITY",
    "LRTMP2_CLUSTER_CAPACITY_MBPS",
    "LRTMP2_CLUSTER_DRAIN_THRESHOLD",
    "LRTMP2_CLUSTER_DRAIN_AT_MBPS",
    "LRTMP2_CLUSTER_RESUME_THRESHOLD",
    "LRTMP2_CLUSTER_RESUME_AT_MBPS",
    "LRTMP2_CLUSTER_BANDWIDTH_INTERFACE",
    "LRTMP2_CLUSTER_BANDWIDTH_MODE",
    "LRTMP2_CLUSTER_BANDWIDTH_MAX_MBPS",
    "LRTMP2_CLUSTER_MEDIA_REPLICAS",
    "LRTMP2_CLUSTER_MEDIA_QUEUE_MB",
    "LRTMP2_CLUSTER_ADVERTISE_ADDR",
    "LRTMP2_CLUSTER_MEDIA_ADVERTISE_ADDR",
];

fn apply_cluster_kv(cfg: &mut ClusterConfig, key: &str, val: &str) {
    match key {
        "CLUSTER_ENABLED" => cfg.enabled = parse_bool(val, false),
        "CLUSTER_NODE_ID" => {
            if let Ok(v) = val.parse::<u64>() {
                cfg.node_id = v;
            }
        }
        "CLUSTER_BIND" => cfg.bind = val.to_string(),
        "CLUSTER_MEDIA_BIND" => cfg.media_bind = val.to_string(),
        "CLUSTER_BOOTSTRAP" => cfg.bootstrap = parse_bool(val, false),
        "CLUSTER_JOIN" => {
            cfg.join = if val.is_empty() {
                None
            } else {
                Some(val.to_string())
            };
        }
        "CLUSTER_SECRET" => cfg.secret = val.to_string(),
        "CLUSTER_TLS_ENABLED" => cfg.tls_enabled = parse_bool(val, false),
        "CLUSTER_TLS_CERT_FILE" => cfg.tls_cert_file = PathBuf::from(val),
        "CLUSTER_TLS_KEY_FILE" => cfg.tls_key_file = PathBuf::from(val),
        "CLUSTER_TLS_CA_FILE" => cfg.tls_ca_file = PathBuf::from(val),
        "CLUSTER_HEARTBEAT_MS" | "CLUSTER_HEARTBEAT_INTERVAL_MS" => {
            if let Ok(ms) = val.parse::<u64>() {
                cfg.heartbeat = Duration::from_millis(ms.clamp(50, 10_000));
            }
        }
        "CLUSTER_NODE_TIMEOUT_MS" => {
            // Reserved for peer DOWN detection; currently derived as 5× heartbeat.
            let _ = val;
        }
        "CLUSTER_NODE_NAME" => {
            // Display name optional; node_id remains authoritative.
            let _ = val;
        }
        "CLUSTER_CAPACITY" => {
            if let Ok(v) = val.parse::<f64>() {
                cfg.capacity = v;
            }
        }
        "CLUSTER_CAPACITY_MBPS" => {
            if let Ok(v) = val.parse::<f64>() {
                cfg.bandwidth_max_mbps = v;
                if cfg.capacity <= 0.0 {
                    cfg.capacity = 1.0;
                }
            }
        }
        "CLUSTER_DRAIN_THRESHOLD" => {
            if let Ok(v) = val.parse::<f64>() {
                cfg.drain_threshold = v;
            }
        }
        "CLUSTER_DRAIN_AT_MBPS" => {
            // Handled in a second pass after BANDWIDTH_MAX_MBPS is known.
            let _ = val;
        }
        "CLUSTER_RESUME_THRESHOLD" => {
            if let Ok(v) = val.parse::<f64>() {
                cfg.resume_threshold = v;
            }
        }
        "CLUSTER_RESUME_AT_MBPS" => {
            // Handled in a second pass after BANDWIDTH_MAX_MBPS is known.
            let _ = val;
        }
        "CLUSTER_BANDWIDTH_MODE" => {
            if let Some(mode) = BandwidthMode::parse(val) {
                cfg.bandwidth_mode = mode;
            }
        }
        "CLUSTER_BANDWIDTH_INTERFACE" => cfg.bandwidth_interface = val.to_string(),
        "CLUSTER_BANDWIDTH_MAX_MBPS" => {
            if let Ok(v) = val.parse::<f64>() {
                cfg.bandwidth_max_mbps = v;
            }
        }
        "CLUSTER_MEDIA_REPLICAS" => {
            if let Ok(v) = val.parse::<u32>() {
                cfg.media_replicas = v.min(8);
            }
        }
        "CLUSTER_MEDIA_QUEUE_MB" => {
            if let Ok(v) = val.parse::<u32>() {
                cfg.media_queue_mb = v;
            }
        }
        "CLUSTER_ADVERTISE_ADDR" => cfg.advertise_addr = Some(val.to_string()),
        "CLUSTER_MEDIA_ADVERTISE_ADDR" => cfg.media_advertise_addr = Some(val.to_string()),
        _ => {}
    }
}

fn parse_bool(val: &str, default: bool) -> bool {
    match val.trim() {
        "1" | "true" | "TRUE" | "yes" | "YES" => true,
        "0" | "false" | "FALSE" | "no" | "NO" => false,
        _ => default,
    }
}

/// Convert absolute Mbps thresholds once `bandwidth_max_mbps` is finalized.
fn normalize_absolute_bandwidth_thresholds(
    cfg: &mut ClusterConfig,
    drain_at_mbps: Option<f64>,
    resume_at_mbps: Option<f64>,
) {
    if cfg.bandwidth_max_mbps <= 0.0 {
        return;
    }
    if let Some(mbps) = drain_at_mbps {
        cfg.drain_threshold = (mbps / cfg.bandwidth_max_mbps).clamp(0.0, 1.0);
    }
    if let Some(mbps) = resume_at_mbps {
        cfg.resume_threshold = (mbps / cfg.bandwidth_max_mbps).clamp(0.0, 1.0);
    }
}

fn parse_bind(bind: &str) -> Result<SocketAddr, String> {
    bind.parse::<SocketAddr>()
        .or_else(|_| format!("127.0.0.1:{bind}").parse())
        .map_err(|e| format!("invalid bind '{bind}': {e}"))
}

fn rewrite_wildcard_to_loopback(bind: &str) -> String {
    if let Ok(addr) = bind.parse::<SocketAddr>() {
        if addr.ip().is_unspecified() {
            return format!("127.0.0.1:{}", addr.port());
        }
    }
    bind.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn disabled_by_default() {
        let cfg = ClusterConfig::load_from_kv(|_| None).unwrap();
        assert!(!cfg.enabled);
    }

    #[test]
    fn enabled_requires_secret_and_node_id() {
        let map = HashMap::from([
            ("CLUSTER_ENABLED", "true"),
            ("CLUSTER_NODE_ID", "1"),
            ("CLUSTER_BOOTSTRAP", "true"),
            ("CLUSTER_SECRET", "short"),
        ]);
        let err =
            ClusterConfig::load_from_kv(|k| map.get(k).map(|s| (*s).to_string())).unwrap_err();
        assert!(err.contains("CLUSTER_SECRET"));
    }

    #[test]
    fn valid_bootstrap_config() {
        let map = HashMap::from([
            ("CLUSTER_ENABLED", "true"),
            ("CLUSTER_NODE_ID", "1"),
            ("CLUSTER_BOOTSTRAP", "true"),
            ("CLUSTER_SECRET", "0123456789abcdef"),
            ("CLUSTER_BIND", "127.0.0.1:1940"),
            ("CLUSTER_MEDIA_BIND", "127.0.0.1:1941"),
        ]);
        let cfg = ClusterConfig::load_from_kv(|k| map.get(k).map(|s| (*s).to_string())).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.node_id, 1);
        assert!(cfg.bootstrap);
    }

    #[test]
    fn absolute_bandwidth_thresholds_normalize_after_max() {
        let map = HashMap::from([
            ("CLUSTER_ENABLED", "true"),
            ("CLUSTER_NODE_ID", "1"),
            ("CLUSTER_BOOTSTRAP", "true"),
            ("CLUSTER_SECRET", "0123456789abcdef"),
            ("CLUSTER_DRAIN_AT_MBPS", "800"),
            ("CLUSTER_RESUME_AT_MBPS", "500"),
            ("CLUSTER_BANDWIDTH_MAX_MBPS", "1000"),
        ]);
        let cfg = ClusterConfig::load_from_kv(|k| map.get(k).map(|s| (*s).to_string())).unwrap();
        assert!((cfg.drain_threshold - 0.8).abs() < f64::EPSILON);
        assert!((cfg.resume_threshold - 0.5).abs() < f64::EPSILON);
    }
}
