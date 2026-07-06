//! OS-backed cryptographically secure key material.

use rand::TryRng;
use rand::rngs::SysRng;

/// Stream keys and internal session ids: 16 bytes = 128 bits → 32 hex chars.
pub const STREAM_KEY_ENTROPY_BYTES: usize = 16;

/// API bearer token: 32 bytes = 256 bits → 64 hex chars.
pub const API_TOKEN_ENTROPY_BYTES: usize = 32;

/// Minimum length for operator-supplied API tokens persisted at bootstrap.
pub const MIN_API_TOKEN_LEN: usize = 16;

/// Known weak placeholders rejected for `LRTMP2_API_TOKEN` (case-insensitive).
const WEAK_API_TOKEN_PLACEHOLDERS: &[&str] = &[
    "admin",
    "123456",
    "letmein",
    "default",
    "changeme",
    "password",
    "secret",
    "token",
    "replace-with-random-token",
    "<replace-with-random-token>",
];

/// Prefix for RTMP publish keys (OBS stream key) and publisher session row ids.
pub const PREFIX_PUBLISH_KEY: &str = "live_";
/// Prefix for RTMP play keys (URL path segment) and player session row ids.
pub const PREFIX_PLAY_KEY: &str = "play_";
/// Prefix for stats URL keys (`?key=...`).
pub const PREFIX_STATS_KEY: &str = "sts_";
/// Prefix for configured viewer-slot row ids (panel-managed play access).
pub const PREFIX_VIEWER_ID: &str = "vi_";

fn keygen_with_entropy(prefix: &str, entropy_bytes: usize) -> Result<String, String> {
    let mut rnd = vec![0u8; entropy_bytes];
    SysRng
        .try_fill_bytes(&mut rnd)
        .map_err(|e| format!("OS RNG failure: {e}"))?;

    let hex_len = entropy_bytes * 2;
    let mut out = String::with_capacity(prefix.len() + hex_len);
    out.push_str(prefix);
    for b in rnd {
        out.push_str(&format!("{b:02x}"));
    }
    Ok(out)
}

/// Stream/play/stats keys (128-bit entropy).
pub fn keygen_stream_key(prefix: &str) -> Result<String, String> {
    keygen_with_entropy(prefix, STREAM_KEY_ENTROPY_BYTES)
}

/// API bearer token (256-bit entropy, no prefix).
pub fn keygen_api_token() -> Result<String, String> {
    keygen_with_entropy("", API_TOKEN_ENTROPY_BYTES)
}

/// Reject short or commonly guessed operator-supplied API tokens before they
/// are persisted and used for Bearer authentication.
pub fn is_valid_api_token(token: &str) -> bool {
    let token = token.trim();
    if token.len() < MIN_API_TOKEN_LEN {
        return false;
    }
    !WEAK_API_TOKEN_PLACEHOLDERS
        .iter()
        .any(|placeholder| token.eq_ignore_ascii_case(placeholder))

pub fn api_token_validation_error() -> String {
    format!(
        "LRTMP2_API_TOKEN must be at least {MIN_API_TOKEN_LEN} characters and must not be a known weak placeholder; omit it to auto-generate a secure token"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_keys_are_unique_and_well_formed() {
        let a = keygen_stream_key(PREFIX_PUBLISH_KEY).unwrap();
        let b = keygen_stream_key(PREFIX_PUBLISH_KEY).unwrap();
        assert_ne!(a, b, "duplicate stream keys generated");
        assert!(a.starts_with(PREFIX_PUBLISH_KEY));
        assert_eq!(
            a.len(),
            PREFIX_PUBLISH_KEY.len() + STREAM_KEY_ENTROPY_BYTES * 2
        );
    }

    #[test]
    fn api_token_has_no_prefix_and_256_bit_entropy() {
        let token = keygen_api_token().unwrap();
        assert_eq!(token.len(), API_TOKEN_ENTROPY_BYTES * 2);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn api_token_validation_rejects_short_and_placeholder_values() {
        assert!(!is_valid_api_token(""));
        assert!(!is_valid_api_token("admin"));
        assert!(!is_valid_api_token("ADMIN"));
        assert!(!is_valid_api_token("123456"));
        assert!(!is_valid_api_token("replace-with-random-token"));
        assert!(!is_valid_api_token("short"));
        assert!(is_valid_api_token(
            "env_api_token_for_first_start_tests_only_value_0123456789ab"
        ));
    }
}
