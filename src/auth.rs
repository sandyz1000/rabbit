//! Auth implementation — copied from bore and adapted for HTTP transport.
//! TCP handshake methods removed; core HMAC logic kept as-is.
//! Expanded with fingerprint() for multi-tenant namespace isolation.

use anyhow::{ensure, Result};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Wrapper around a MAC used for authenticating clients that have a secret.
pub struct Authenticator {
    mac: Hmac<Sha256>,
    /// SHA256(secret) — stable tenant namespace identifier.
    /// Stored separately because Hmac<Sha256> does not expose its key bytes.
    fingerprint: [u8; 32],
}

impl Authenticator {
    /// Generate an authenticator from a secret.
    pub fn new(secret: &str) -> Self {
        let hash = Sha256::new().chain_update(secret).finalize();
        let fingerprint: [u8; 32] = hash.into();
        Self {
            mac: Hmac::new_from_slice(&fingerprint).expect("HMAC accepts any key size"),
            fingerprint,
        }
    }

    /// SHA256(secret) — used to scope service visibility to callers sharing the same secret.
    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    /// Generate a reply to a challenge (bore compatibility).
    #[allow(dead_code)]
    pub fn answer(&self, challenge: &Uuid) -> String {
        let mut hmac = self.mac.clone();
        hmac.update(challenge.as_bytes());
        hex::encode(hmac.finalize().into_bytes())
    }

    /// Generate an HMAC tag over arbitrary bytes (used for timestamp-based auth).
    pub fn sign(&self, data: &[u8]) -> String {
        let mut hmac = self.mac.clone();
        hmac.update(data);
        hex::encode(hmac.finalize().into_bytes())
    }

    /// Validate an HMAC tag over arbitrary bytes.
    pub fn verify(&self, data: &[u8], tag: &str) -> Result<()> {
        let expected = self.sign(data);
        ensure!(expected == tag, "invalid secret");
        Ok(())
    }

    /// Validate a reply to a UUID challenge (bore compatibility).
    #[allow(dead_code)]
    pub fn validate(&self, challenge: &Uuid, tag: &str) -> bool {
        if let Ok(tag_bytes) = hex::decode(tag) {
            let mut hmac = self.mac.clone();
            hmac.update(challenge.as_bytes());
            hmac.verify_slice(&tag_bytes).is_ok()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_roundtrip() {
        let auth = Authenticator::new("test-secret");
        let data = b"hello world";
        let tag = auth.sign(data);
        assert!(auth.verify(data, &tag).is_ok());
    }

    #[test]
    fn verify_rejects_wrong_tag() {
        let auth = Authenticator::new("test-secret");
        let result = auth.verify(b"data", "not-a-valid-hmac");
        assert!(result.is_err());
    }

    #[test]
    fn sign_is_deterministic() {
        let auth = Authenticator::new("secret");
        let ts: u64 = 1_700_000_000;
        let tag1 = auth.sign(&ts.to_le_bytes());
        let tag2 = auth.sign(&ts.to_le_bytes());
        assert_eq!(tag1, tag2);
    }

    #[test]
    fn different_secrets_produce_different_tags() {
        let a = Authenticator::new("secret-a");
        let b = Authenticator::new("secret-b");
        let data = b"some-data";
        assert_ne!(a.sign(data), b.sign(data));
    }

    #[test]
    fn different_secrets_produce_different_fingerprints() {
        let a = Authenticator::new("secret-a");
        let b = Authenticator::new("secret-b");
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn same_secret_produces_same_fingerprint() {
        let a = Authenticator::new("shared-secret");
        let b = Authenticator::new("shared-secret");
        assert_eq!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn answer_validate_roundtrip() {
        let auth = Authenticator::new("my-secret");
        let challenge = Uuid::new_v4();
        let reply = auth.answer(&challenge);
        assert!(auth.validate(&challenge, &reply));
    }
}
