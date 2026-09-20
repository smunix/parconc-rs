//! Cryptographic primitives for End-to-End Encryption (E2EE).
//!
//! Provides:
//! - X25519 Diffie-Hellman key exchange for identity and ephemeral keys.
//! - HKDF-SHA256 key derivation for deriving symmetric encryption keys from DH shared secrets.
//! - ChaCha20-Poly1305 AEAD symmetric authenticated encryption.
//! - Base64 wire encoding and decoding for transmission over text streams.

use anyhow::{Result, anyhow};
use base64::prelude::*;
use chacha20poly1305::{
    ChaCha20Poly1305, Nonce,
    aead::{Aead, KeyInit},
};
use hkdf::Hkdf;
use rand::{RngCore, rngs::OsRng};
use sha2::Sha256;
pub use x25519_dalek::{EphemeralSecret, PublicKey, StaticSecret};

/// Info string for HKDF key derivation
const HKDF_INFO: &[u8] = b"distrib-chat-e2ee-v1";

/// Generates a static (long-term) X25519 identity keypair for a client.
pub fn generate_identity_keypair() -> (StaticSecret, PublicKey) {
    let secret = StaticSecret::random_from_rng(OsRng);
    let public = PublicKey::from(&secret);
    (secret, public)
}

/// Encodes a 32-byte public key into a standard base64 string.
pub fn encode_pubkey(pk: &PublicKey) -> String {
    BASE64_STANDARD.encode(pk.as_bytes())
}

/// Decodes a base64 string into a 32-byte X25519 public key.
pub fn decode_pubkey(encoded: &str) -> Result<PublicKey> {
    let bytes = BASE64_STANDARD.decode(encoded.trim())?;
    if bytes.len() != 32 {
        return Err(anyhow!(
            "Invalid public key length: expected 32 bytes, got {}",
            bytes.len()
        ));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(PublicKey::from(arr))
}

/// Derives a 32-byte symmetric AEAD key from a Diffie-Hellman shared secret using HKDF-SHA256.
fn derive_symmetric_key(shared_secret: &x25519_dalek::SharedSecret) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());
    let mut okm = [0u8; 32];
    hk.expand(HKDF_INFO, &mut okm)
        .expect("32 bytes is a valid output length for HKDF-SHA256");
    okm
}

/// Encrypts plaintext destined for a recipient with `recipient_pubkey`.
///
/// Security properties:
/// - Uses a fresh ephemeral secret for Ephemeral Diffie-Hellman (forward secrecy).
/// - Encrypts with ChaCha20-Poly1305 AEAD (confidentiality and authentication).
///
/// Wire format packed before Base64 encoding:
/// `[ ephemeral_public_key (32 bytes) | nonce (12 bytes) | ciphertext + tag (variable) ]`
pub fn encrypt_message(recipient_pubkey: &PublicKey, plaintext: &str) -> Result<String> {
    // 1. Generate fresh ephemeral keypair
    let ephemeral_secret = EphemeralSecret::random_from_rng(OsRng);
    let ephemeral_public = PublicKey::from(&ephemeral_secret);

    // 2. Perform Diffie-Hellman key agreement
    let shared_secret = ephemeral_secret.diffie_hellman(recipient_pubkey);

    // 3. Derive symmetric key
    let sym_key = derive_symmetric_key(&shared_secret);

    // 4. Generate random 12-byte nonce
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    // 5. Encrypt plaintext with ChaCha20-Poly1305 AEAD
    let cipher = ChaCha20Poly1305::new_from_slice(&sym_key)
        .map_err(|e| anyhow!("Cipher init error: {e}"))?;
    let ciphertext_with_tag = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| anyhow!("Encryption error: {e}"))?;

    // 6. Pack payload: ephemeral_public (32) + nonce (12) + ciphertext
    let mut payload = Vec::with_capacity(32 + 12 + ciphertext_with_tag.len());
    payload.extend_from_slice(ephemeral_public.as_bytes());
    payload.extend_from_slice(&nonce_bytes);
    payload.extend_from_slice(&ciphertext_with_tag);

    // 7. Base64 encode
    Ok(BASE64_STANDARD.encode(&payload))
}

/// Decrypts a base64-encoded E2EE message using the recipient's private `StaticSecret`.
pub fn decrypt_message(my_secret: &StaticSecret, base64_payload: &str) -> Result<String> {
    let payload = BASE64_STANDARD
        .decode(base64_payload.trim())
        .map_err(|e| anyhow!("Base64 decode error: {e}"))?;

    // Minimum length: 32 (pubkey) + 12 (nonce) + 16 (Poly1305 tag) = 60 bytes
    if payload.len() < 60 {
        return Err(anyhow!("Payload too short for E2EE message"));
    }

    let ephemeral_pub_bytes: [u8; 32] = payload[..32]
        .try_into()
        .map_err(|_| anyhow!("Failed to slice ephemeral public key"))?;
    let nonce_bytes: [u8; 12] = payload[32..44]
        .try_into()
        .map_err(|_| anyhow!("Failed to slice nonce"))?;
    let ciphertext_with_tag = &payload[44..];

    let ephemeral_public = PublicKey::from(ephemeral_pub_bytes);

    // 1. Perform Diffie-Hellman key agreement: my_secret * sender_ephemeral_public
    let shared_secret = my_secret.diffie_hellman(&ephemeral_public);

    // 2. Derive identical symmetric key
    let sym_key = derive_symmetric_key(&shared_secret);

    // 3. Decrypt with ChaCha20-Poly1305
    let cipher = ChaCha20Poly1305::new_from_slice(&sym_key)
        .map_err(|e| anyhow!("Cipher init error: {e}"))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let plaintext_bytes = cipher
        .decrypt(nonce, ciphertext_with_tag)
        .map_err(|e| anyhow!("Decryption authentication failed: {e}"))?;

    String::from_utf8(plaintext_bytes).map_err(|e| anyhow!("UTF-8 decode error: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_e2ee_roundtrip() {
        let (alice_secret, _alice_public) = generate_identity_keypair();
        let (bob_secret, bob_public) = generate_identity_keypair();

        let message = "Hello Bob! This is top secret via E2EE.";

        // Alice encrypts for Bob
        let encrypted = encrypt_message(&bob_public, message).expect("encryption succeeds");
        assert_ne!(encrypted, message);

        // Bob decrypts
        let decrypted = decrypt_message(&bob_secret, &encrypted).expect("decryption succeeds");
        assert_eq!(decrypted, message);

        // Alice cannot decrypt Bob's message with Alice's secret
        assert!(decrypt_message(&alice_secret, &encrypted).is_err());
    }

    #[test]
    fn test_pubkey_encoding() {
        let (_secret, public) = generate_identity_keypair();
        let encoded = encode_pubkey(&public);
        let decoded = decode_pubkey(&encoded).expect("valid decode");
        assert_eq!(public.as_bytes(), decoded.as_bytes());
    }
}
