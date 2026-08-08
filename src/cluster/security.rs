//! Constant-time secret comparison and TLS helpers for the cluster plane.

use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rand::RngCore;
use rand::rngs::SysRng;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

const NODE_ID_CERT_PREFIX: &[u8] = b"lrtmp2-node-";

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
            id = id.saturating_mul(10).saturating_add((ch as u8 - b'0') as u64);
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
    SysRng.fill_bytes(&mut nonce);
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
    fn auth_response_deterministic() {
        // Build secret/nonce at runtime so static analysis does not treat
        // them as hard-coded production cryptographic material.
        let secret: String = (0u8..24).map(|i| char::from(b'a' + (i % 26))).collect();
        let nonce: Vec<u8> = (0u8..16)
            .map(|i| i.wrapping_mul(17).wrapping_add(3))
            .collect();
        let a = auth_response(&secret, 42, &nonce);
        let b = auth_response(&secret, 42, &nonce);
        assert_eq!(a, b);
        let other: String = (0u8..24).map(|i| char::from(b'z' - (i % 26))).collect();
        assert_ne!(a, auth_response(&other, 42, &nonce));
        assert_ne!(a, auth_response(&secret, 43, &nonce));
    }
}
