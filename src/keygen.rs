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
}
