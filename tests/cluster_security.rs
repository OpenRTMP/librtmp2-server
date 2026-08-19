//! Control-plane security unit tests (no live cluster required).

#![cfg(feature = "cluster")]

use librtmp2_server::cluster::command::ClusterCommand;
use librtmp2_server::cluster::membership::verify_cluster_identity;
use librtmp2_server::cluster::security::{
    admin_proof, auth_response, join_admin_proof_payload, node_id_from_peer_certs, secrets_equal,
    validate_cluster_peer_addr, verify_tls_node_identity,
};

#[test]
fn auth_response_binds_node_id() {
    let secret = "cluster-secret-at-least-16";
    let nonce = b"nonce-1234567890";
    let a = auth_response(secret, 1, nonce);
    let b = auth_response(secret, 2, nonce);
    assert_ne!(a, b);
    assert_eq!(a, auth_response(secret, 1, nonce));
}

#[test]
fn client_write_admin_commands_require_proof() {
    assert!(ClusterCommand::SetApiToken { token: "x".into() }.requires_admin_proof());
    assert!(
        ClusterCommand::AcquireStreamOwner {
            stream_id: "s".into(),
            node_id: 1,
            epoch: 1,
            acquired_at: 0,
        }
        .requires_admin_proof()
    );
    assert!(ClusterCommand::ReleaseOwnersForNode { node_id: 2 }.requires_admin_proof());
}

#[test]
fn admin_proof_changes_with_payload() {
    let token = "api-token-for-tests-only";
    let a = admin_proof(token, r#"{"AddVoterIds":[2]}"#);
    let b = admin_proof(token, r#"{"AddVoterIds":[3]}"#);
    assert_ne!(a, b);
}

#[test]
fn join_peer_addr_validation_blocks_ssrf_targets() {
    assert!(validate_cluster_peer_addr("127.0.0.1:1940", false).is_err());
    assert!(validate_cluster_peer_addr("169.254.169.254:80", false).is_err());
    assert!(validate_cluster_peer_addr("10.0.0.5:1941", false).is_ok());
}

#[test]
fn join_admin_proof_binds_node_addresses() {
    let token = "api-token-for-tests-only";
    let a = admin_proof(
        token,
        &join_admin_proof_payload(2, "127.0.0.1:1940", "127.0.0.1:1941"),
    );
    let b = admin_proof(
        token,
        &join_admin_proof_payload(3, "127.0.0.1:1940", "127.0.0.1:1941"),
    );
    assert_ne!(a, b);
}

#[test]
fn empty_client_write_proof_does_not_match_signed_payload() {
    let token = "api-token-for-tests-only";
    let payload = r#"{"SetApiToken":{"token":"stolen"}}"#;
    let expected = admin_proof(token, payload);
    assert!(!secrets_equal("", &expected));
    assert!(!secrets_equal("deadbeef", &expected));
    assert!(secrets_equal(&expected, &admin_proof(token, payload)));
}

#[test]
fn verify_cluster_identity_empty_remote_fails() {
    assert!(verify_cluster_identity(Some("cid"), "").is_err());
}

#[test]
fn secrets_equal_rejects_length_pairs_that_overflow_u8() {
    let a = "x".repeat(256);
    assert!(!secrets_equal(&a, ""));
    assert!(!secrets_equal("", &a));
}

#[test]
fn admin_proof_is_deterministic_so_replay_cache_is_required() {
    let token = "api-token-for-tests-only";
    let payload = r#"{"SetApiToken":{"token":"stolen"}}"#;
    let a = admin_proof(token, payload);
    let b = admin_proof(token, payload);
    assert_eq!(
        a, b,
        "identical payloads must yield identical proofs so nodes need replay tracking"
    );
}

#[test]
fn tls_identity_requires_cert_marker_when_tls_on() {
    let mut der = Vec::new();
    der.extend_from_slice(b"noise-lrtmp2-node-99-trailer");
    let cert_id = node_id_from_peer_certs(&[rustls::pki_types::CertificateDer::from(der)]);
    assert_eq!(cert_id, Some(99));
    assert!(verify_tls_node_identity(true, cert_id, 99).is_ok());
    assert!(verify_tls_node_identity(true, cert_id, 1).is_err());
    assert!(verify_tls_node_identity(true, None, 1).is_err());
}
