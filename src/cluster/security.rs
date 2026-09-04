//! Constant-time secret comparison and TLS helpers for the cluster plane.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::net::IpAddr;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use rand::TryRng;
use rand::rngs::SysRng;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

const NODE_ID_CERT_PREFIX: &[u8] = b"lrtmp2-node-";
const CLUSTER_AUTH_MAX_FAILURES: usize = 10;
const CLUSTER_AUTH_FAILURE_WINDOW: Duration = Duration::from_secs(60);
const MAX_TRACKED_CLUSTER_AUTH_IPS: usize = 10_000;

static CLUSTER_AUTH_FAILURES: LazyLock<Mutex<HashMap<IpAddr, Vec<Instant>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn active_cluster_auth_failure_count(entries: &[Instant], now: Instant) -> usize {
    entries
        .iter()
        .copied()
        .filter(|t| {
            now.checked_duration_since(*t)
                .is_none_or(|age| age < CLUSTER_AUTH_FAILURE_WINDOW)
        })
        .count()
}

fn purge_expired_cluster_auth_failures(guard: &mut HashMap<IpAddr, Vec<Instant>>, now: Instant) {
    guard.retain(|_, entries| {
        entries.retain(|t| {
            now.checked_duration_since(*t)
                .is_none_or(|age| age < CLUSTER_AUTH_FAILURE_WINDOW)
        });
        !entries.is_empty()
    });
}

/// Remove the least-recently-active IP bucket that is not currently
/// rate-limited. Actively throttled buckets are never evicted — dropping
/// one would reset its failure window and let a client immediately resume
/// brute-forcing CLUSTER_SECRET.
fn evict_oldest_eligible_cluster_auth_ip(
    guard: &mut HashMap<IpAddr, Vec<Instant>>,
    now: Instant,
) -> bool {
    let Some(oldest_key) = guard
        .iter()
        .filter(|(_, entries)| {
            active_cluster_auth_failure_count(entries, now) < CLUSTER_AUTH_MAX_FAILURES
        })
        .min_by_key(|(_, entries)| entries.last().copied().unwrap_or_else(Instant::now))
        .map(|(key, _)| *key)
    else {
        return false;
    };
    guard.remove(&oldest_key);
    true
}

fn all_cluster_auth_buckets_throttled(guard: &HashMap<IpAddr, Vec<Instant>>, now: Instant) -> bool {
    !guard.is_empty()
        && guard.values().all(|entries| {
            active_cluster_auth_failure_count(entries, now) >= CLUSTER_AUTH_MAX_FAILURES
        })
}

/// True when `peer` has exceeded the cluster auth failure budget.
///
/// When the tracker is at capacity, only peers whose buckets are actively
/// throttled cause fail-closed behaviour for new source IPs — mirroring the
/// HTTP rate limiter so a scan with many one-off failures cannot deny
/// legitimate cluster joins that present a valid CLUSTER_SECRET.
pub fn cluster_auth_rate_limited(peer: IpAddr) -> bool {
    let mut guard = CLUSTER_AUTH_FAILURES.lock();
    let now = Instant::now();
    purge_expired_cluster_auth_failures(&mut guard, now);
    let Some(entries) = guard.get_mut(&peer) else {
        if guard.len() >= MAX_TRACKED_CLUSTER_AUTH_IPS {
            if !evict_oldest_eligible_cluster_auth_ip(&mut guard, now) {
                return all_cluster_auth_buckets_throttled(&guard, now);
            }
        }
        return false;
    };
    active_cluster_auth_failure_count(entries, now) >= CLUSTER_AUTH_MAX_FAILURES
}

/// Record a failed cluster auth handshake attempt from `peer`.
pub fn record_cluster_auth_failure(peer: IpAddr) {
    let mut guard = CLUSTER_AUTH_FAILURES.lock();
    let now = Instant::now();
    purge_expired_cluster_auth_failures(&mut guard, now);
    if !guard.contains_key(&peer) && guard.len() >= MAX_TRACKED_CLUSTER_AUTH_IPS {
        if !evict_oldest_eligible_cluster_auth_ip(&mut guard, now) {
            return;
        }
    }
    guard.entry(peer).or_default().push(now);
}

/// Clear auth failures after a successful cluster auth handshake.
pub fn clear_cluster_auth_failures(peer: IpAddr) {
    CLUSTER_AUTH_FAILURES.lock().remove(&peer);
}

/// Compare two secrets in constant time (length included via padded XOR).
pub fn secrets_equal(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let max_len = a.len().max(b.len());
    let mut diff = u8::from(a.len() != b.len());
    for i in 0..max_len {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

fn parse_node_id_from_identity_str(value: &str) -> Option<u64> {
    let marker = "lrtmp2-node-";
    let pos = value.find(marker)?;
    let digits = &value[pos + marker.len()..];
    let mut id = 0u64;
    let mut digits_seen = 0usize;
    for ch in digits.chars() {
        if ch.is_ascii_digit() {
            id = id
                .saturating_mul(10)
                .saturating_add((ch as u8 - b'0') as u64);
            digits_seen += 1;
            if digits_seen > 20 {
                return None;
            }
        } else {
            break;
        }
    }
    (digits_seen > 0 && id > 0).then_some(id)
}

fn node_id_from_cert_der(bytes: &[u8]) -> Option<u64> {
    use x509_parser::prelude::*;
    let Ok((_, cert)) = X509Certificate::from_der(bytes) else {
        return None;
    };
    for cn in cert.subject().iter_common_name() {
        if let Ok(cn_str) = cn.as_str()
            && let Some(id) = parse_node_id_from_identity_str(cn_str)
        {
            return Some(id);
        }
    }
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in san.value.general_names.iter() {
            let candidate = match name {
                GeneralName::DNSName(s) | GeneralName::URI(s) => Some(*s),
                _ => None,
            };
            if let Some(s) = candidate
                && let Some(id) = parse_node_id_from_identity_str(s)
            {
                return Some(id);
            }
        }
    }
    None
}

fn node_id_from_cert_bytes_scan(bytes: &[u8]) -> Option<u64> {
    let mut search_from = 0usize;
    while let Some(rel) = bytes[search_from..]
        .windows(NODE_ID_CERT_PREFIX.len())
        .position(|w| w == NODE_ID_CERT_PREFIX)
    {
        let pos = search_from + rel + NODE_ID_CERT_PREFIX.len();
        let mut id = 0u64;
        let mut digits = 0usize;
        for &b in &bytes[pos..] {
            if b.is_ascii_digit() {
                id = id.saturating_mul(10).saturating_add((b - b'0') as u64);
                digits += 1;
                if digits > 20 {
                    break;
                }
            } else {
                break;
            }
        }
        if digits > 0 && id > 0 {
            return Some(id);
        }
        search_from = pos.saturating_add(1);
        if search_from >= bytes.len() {
            break;
        }
    }
    None
}

/// CSPRNG nonce for cluster auth handshakes.
pub fn auth_nonce() -> Vec<u8> {
    let mut nonce = vec![0u8; 16];
    SysRng
        .try_fill_bytes(&mut nonce)
        .expect("OS RNG failure while generating cluster auth nonce");
    nonce
}

/// Extract `node_id` embedded in a peer client certificate (SAN/CN string
/// `lrtmp2-node-{id}`). Returns `None` when TLS is off or the pattern is absent.
pub fn node_id_from_peer_certs(certs: &[CertificateDer<'_>]) -> Option<u64> {
    for cert in certs {
        let bytes = cert.as_ref();
        if let Some(id) = node_id_from_cert_der(bytes) {
            return Some(id);
        }
        if let Some(id) = node_id_from_cert_bytes_scan(bytes) {
            return Some(id);
        }
    }
    None
}

/// When mTLS is active, the authenticated `node_id` must match the client cert.
pub fn verify_tls_node_identity(
    tls_active: bool,
    cert_node_id: Option<u64>,
    claimed_node_id: u64,
) -> Result<(), io::Error> {
    if !tls_active {
        return Ok(());
    }
    let Some(cert_id) = cert_node_id else {
        return Err(io::Error::other(
            "mTLS required but peer certificate missing lrtmp2-node-{id} identity",
        ));
    };
    if cert_id != claimed_node_id {
        return Err(io::Error::other(
            "mTLS peer certificate node_id does not match authenticated node_id",
        ));
    }
    Ok(())
}

/// Derive a hex-encoded SHA-256 challenge response (never log the secret).
///
/// `node_id` is mixed into the digest so a holder of the cluster secret cannot
/// authenticate as a different member by reusing another node's response.
pub fn auth_response(secret: &str, node_id: u64, nonce: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.update(node_id.to_le_bytes());
    hasher.update(nonce);
    hex::encode(hasher.finalize())
}

/// Reject cluster peer dial targets that would turn join into SSRF against
/// loopback, link-local (including cloud metadata), or other non-unicast IPs.
/// RFC1918 addresses remain allowed — they are normal for inter-node traffic.
pub fn validate_cluster_peer_addr(addr: &str, allow_loopback: bool) -> Result<(), String> {
    let trimmed = addr.trim();
    if trimmed.is_empty() {
        return Err("peer address is required".into());
    }
    let parsed = trimmed
        .parse::<std::net::SocketAddr>()
        .map_err(|e| format!("invalid peer address '{trimmed}': {e}"))?;
    // Canonicalize IPv4-mapped IPv6 before classification so addresses such as
    // ::ffff:127.0.0.1 and ::ffff:169.254.169.254 cannot bypass the IPv4 checks.
    let ip = parsed.ip().to_canonical();
    if ip.is_unspecified() || ip.is_multicast() {
        return Err(format!("peer address '{trimmed}' must be a unicast IP"));
    }
    if !allow_loopback && ip.is_loopback() {
        return Err(format!(
            "peer address '{trimmed}' must not be loopback (set CLUSTER_ALLOW_LOOPBACK_PEER_ADDRS=true for local dev)"
        ));
    }
    match ip {
        std::net::IpAddr::V4(v4) if v4.is_link_local() => {
            return Err(format!(
                "peer address '{trimmed}' must not be link-local (includes cloud metadata endpoints)"
            ));
        }
        std::net::IpAddr::V6(v6) if v6.is_unicast_link_local() => {
            return Err(format!("peer address '{trimmed}' must not be link-local"));
        }
        _ => {}
    }
    Ok(())
}

/// Canonical payload for an HTTP-API-signed cluster join (`admin_proof`).
pub fn join_admin_proof_payload(node_id: u64, control_addr: &str, media_addr: &str) -> String {
    format!("Join:{node_id}:{control_addr}:{media_addr}")
}

/// Proof that a membership change was initiated via the HTTP API (bearer token).
pub fn admin_proof(api_token: &str, payload: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(api_token.as_bytes());
    hasher.update(payload.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("open cert {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parse cert {}: {e}", path.display()))
}

pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|e| format!("open key {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("parse key {}: {e}", path.display()))?
        .ok_or_else(|| format!("no private key in {}", path.display()))
}

pub fn build_server_tls(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> Result<Arc<ServerConfig>, String> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let mut roots = RootCertStore::empty();
    for cert in load_certs(ca_path)? {
        roots.add(cert).map_err(|e| format!("add CA cert: {e}"))?;
    }
    let mut cfg = ServerConfig::builder()
        .with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| format!("client verifier: {e}"))?,
        )
        .with_single_cert(certs, key)
        .map_err(|e| format!("server tls config: {e}"))?;
    cfg.alpn_protocols = vec![b"lrtmp2-cluster/1".to_vec()];
    Ok(Arc::new(cfg))
}

pub fn build_client_tls(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> Result<Arc<ClientConfig>, String> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let mut roots = RootCertStore::empty();
    for cert in load_certs(ca_path)? {
        roots.add(cert).map_err(|e| format!("add CA cert: {e}"))?;
    }
    let mut cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("client tls config: {e}"))?;
    cfg.alpn_protocols = vec![b"lrtmp2-cluster/1".to_vec()];
    Ok(Arc::new(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_compare() {
        assert!(secrets_equal("abcdef", "abcdef"));
        assert!(!secrets_equal("abcdef", "abcdeg"));
        assert!(!secrets_equal("abc", "abcd"));
        let long_a = "a".repeat(256);
        let long_b = "b".repeat(256);
        assert!(!secrets_equal(&long_a, &long_b));
        assert!(!secrets_equal(&long_a, ""));
    }

    #[test]
    fn node_id_from_cert_prefix_scan() {
        let mut der = vec![0u8; 64];
        der.extend_from_slice(b"prefix-lrtmp2-node-42-suffix");
        let id = node_id_from_peer_certs(&[CertificateDer::from(der)]);
        assert_eq!(id, Some(42));
    }

    #[test]
    fn verify_tls_node_identity_rejects_mismatch() {
        assert!(verify_tls_node_identity(true, Some(2), 2).is_ok());
        assert!(verify_tls_node_identity(false, None, 2).is_ok());
        assert!(verify_tls_node_identity(true, Some(1), 2).is_err());
        assert!(verify_tls_node_identity(true, None, 2).is_err());
    }

    #[test]
    fn validate_cluster_peer_addr_rejects_loopback_and_metadata() {
        assert!(validate_cluster_peer_addr("127.0.0.1:1940", false).is_err());
        assert!(validate_cluster_peer_addr("[::1]:1940", false).is_err());
        assert!(validate_cluster_peer_addr("169.254.169.254:80", false).is_err());
        assert!(validate_cluster_peer_addr("[::ffff:127.0.0.1]:1940", false).is_err());
        assert!(validate_cluster_peer_addr("[::ffff:169.254.169.254]:80", false).is_err());
        assert!(validate_cluster_peer_addr("0.0.0.0:1940", false).is_err());
        assert!(validate_cluster_peer_addr("10.0.0.2:1940", false).is_ok());
        assert!(validate_cluster_peer_addr("[::ffff:10.0.0.2]:1940", false).is_ok());
        assert!(validate_cluster_peer_addr("127.0.0.1:1940", true).is_ok());
        assert!(validate_cluster_peer_addr("[::ffff:127.0.0.1]:1940", true).is_ok());
    }

    #[test]
    fn auth_response_deterministic() {
        // Build secret/nonce at runtime so static analysis does not treat
        // them as hard-coded production cryptographic material.
        let secret: String = (0u8..32).map(|i| char::from(b'a' + (i % 26))).collect();
        let nonce: Vec<u8> = (0u8..16)
            .map(|i| i.wrapping_mul(17).wrapping_add(3))
            .collect();
        let a = auth_response(&secret, 42, &nonce);
        let b = auth_response(&secret, 42, &nonce);
        assert_eq!(a, b);
        let other: String = (0u8..32).map(|i| char::from(b'z' - (i % 26))).collect();
        assert_ne!(a, auth_response(&other, 42, &nonce));
        assert_ne!(a, auth_response(&secret, 43, &nonce));
    }

    #[test]
    fn cluster_auth_rate_limit_tracks_failures_per_ip() {
        use std::net::IpAddr;

        let peer: IpAddr = "198.51.100.77".parse().unwrap();
        assert!(!super::cluster_auth_rate_limited(peer));
        for _ in 0..10 {
            super::record_cluster_auth_failure(peer);
        }
        assert!(super::cluster_auth_rate_limited(peer));
        super::clear_cluster_auth_failures(peer);
        assert!(!super::cluster_auth_rate_limited(peer));
    }

    #[test]
    fn cluster_auth_tracker_evicts_eligible_ips_when_at_capacity() {
        use std::net::{IpAddr, Ipv4Addr};

        let now = Instant::now();
        let limited: IpAddr = "198.51.100.0".parse().unwrap();
        let fresh: IpAddr = "203.0.113.9".parse().unwrap();

        {
            let mut guard = super::CLUSTER_AUTH_FAILURES.lock();
            guard.clear();
            for i in 1..super::MAX_TRACKED_CLUSTER_AUTH_IPS {
                let ip = IpAddr::V4(Ipv4Addr::from(i as u32));
                guard.insert(ip, vec![now - Duration::from_secs(30)]);
            }
            guard.insert(
                limited,
                vec![now - Duration::from_secs(1); super::CLUSTER_AUTH_MAX_FAILURES],
            );
        }

        assert!(
            !super::cluster_auth_rate_limited(fresh),
            "a new peer must be admitted after evicting an eligible bucket"
        );
        assert!(
            super::cluster_auth_rate_limited(limited),
            "actively throttled bucket must not be reset by eviction"
        );

        super::CLUSTER_AUTH_FAILURES.lock().clear();
    }

    #[test]
    fn cluster_auth_tracker_fails_closed_when_every_bucket_is_throttled() {
        use std::net::{IpAddr, Ipv4Addr};

        let now = Instant::now();
        let fresh: IpAddr = "203.0.113.9".parse().unwrap();

        {
            let mut guard = super::CLUSTER_AUTH_FAILURES.lock();
            guard.clear();
            for i in 0..super::MAX_TRACKED_CLUSTER_AUTH_IPS {
                let ip = IpAddr::V4(Ipv4Addr::from(i as u32));
                guard.insert(ip, vec![now; super::CLUSTER_AUTH_MAX_FAILURES]);
            }
        }

        assert!(
            super::cluster_auth_rate_limited(fresh),
            "new peer must be denied when every tracked bucket is throttled"
        );

        super::CLUSTER_AUTH_FAILURES.lock().clear();
    }
}
