//! Comparison for shared secrets presented by control-plane callers.

use sha2::Digest;
use sha2::Sha256;
use subtle::ConstantTimeEq;

/// Reports whether a presented secret matches the configured one.
///
/// Both values are reduced to fixed-width SHA-256 digests before comparison so
/// neither a partial match nor either secret's length affects comparison time.
/// A blank configured secret never authorizes a caller.
#[must_use]
pub fn matches(configured: &str, presented: &str) -> bool {
    if configured.trim().is_empty() {
        return false;
    }

    let configured_digest = Sha256::digest(configured.as_bytes());
    let presented_digest = Sha256::digest(presented.as_bytes());

    bool::from(configured_digest.ct_eq(&presented_digest))
}

#[cfg(test)]
mod tests {
    use super::matches;

    #[test]
    fn exact_secrets_match() {
        // Given: a configured control-plane secret.
        let configured = "control-secret";

        // When: the caller presents the same value.
        let accepted = matches(configured, "control-secret");

        // Then: the caller is authorized.
        assert!(accepted);
    }

    #[test]
    fn near_misses_are_rejected() {
        // Given: a configured secret and guesses around it.
        let configured = "control-secret";

        // When: each guess is compared.
        let results = [
            matches(configured, "control-secre"),
            matches(configured, "control-secret-extra"),
            matches(configured, "different-secret"),
        ];

        // Then: prefixes, extensions, and unrelated values are rejected.
        assert_eq!(results, [false, false, false]);
    }

    #[test]
    fn blank_configured_secrets_authorize_nobody() {
        // Given: an unset or whitespace-only configured secret.
        // When: callers present blank or non-blank values.
        let results = [
            matches("", ""),
            matches("   ", "   "),
            matches("", "presented"),
        ];

        // Then: the unconfigured endpoint stays closed.
        assert_eq!(results, [false, false, false]);
    }
}
