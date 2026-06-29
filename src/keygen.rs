//! OS-backed cryptographically secure key material.

use rand::rngs::SysRng;
use rand::TryRng;

/// `prefix` followed by 32 hex chars (16 bytes / 128 bits of entropy).
pub fn keygen_secret(prefix: &str) -> String {
    let mut rnd = [0u8; 16];
    SysRng.try_fill_bytes(&mut rnd).expect("OS RNG failure");

    let mut out = String::with_capacity(prefix.len() + 32);
    out.push_str(prefix);
    for b in rnd {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_unique_and_well_formed() {
        let a = keygen_secret("pub_");
        let b = keygen_secret("pub_");
        assert_ne!(a, b, "duplicate stream keys generated");
        assert!(a.starts_with("pub_"));
        assert_eq!(a.len(), 4 + 32);
    }
}
