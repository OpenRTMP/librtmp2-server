//! OS-backed cryptographically secure key material.

use rand::TryRng;
use rand::rngs::SysRng;

/// Stream keys and internal session ids: 16 bytes = 128 bits → 32 hex chars.
pub const STREAM_KEY_ENTROPY_BYTES: usize = 16;

/// API bearer token: 32 bytes = 256 bits → 64 hex chars.
pub const API_TOKEN_ENTROPY_BYTES: usize = 32;

/// Prefix for RTMP publish keys (OBS stream key) and publisher session row ids.
pub const PREFIX_PUBLISH_KEY: &str = "live_";
/// Prefix for RTMP play keys (URL path segment) and player session row ids.
pub const PREFIX_PLAY_KEY: &str = "play_";
/// Prefix for stats URL keys (`?key=...`).
pub const PREFIX_STATS_KEY: &str = "sts_";
/// Prefix for configured viewer-slot row ids (panel-managed play access).
pub const PREFIX_VIEWER_ID: &str = "vi_";

/// Minimum length for publish/play/stats keys used at runtime. Shorter keys stored
/// in legacy databases are rejected on RTMP and public stats paths.
pub const MIN_ACCESS_KEY_LEN: usize = 32;

/// Publish/play/stats keys: safe ASCII, no slashes, minimum entropy via length.
pub fn is_valid_access_key(value: &str) -> bool {
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

pub const ACCESS_KEY_VALIDATION_MSG: &str =
    "Key must be 32-63 characters, start with a letter or number, and use only letters, numbers, dots, underscores, or hyphens";

#[cfg(test)]
pub fn test_pad_access_key(value: &str) -> String {
    if is_valid_access_key(value) {
        return value.to_string();
    }
    let mut padded = value.to_string();
    while padded.len() < MIN_ACCESS_KEY_LEN {
        padded.push('x');
    }
    padded.chars().take(63).collect()
}

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
    fn access_key_validation_enforces_minimum_length() {
        assert!(is_valid_access_key("live.main_1_with_sufficient_length_ok"));
        assert!(!is_valid_access_key("too_short"));
        assert!(!is_valid_access_key("a"));
    }
}
