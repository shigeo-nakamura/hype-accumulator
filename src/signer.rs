//! KMS envelope-decryption for the Hyperliquid signing key.
//!
//! Mirrors the existing `debot-utils::decrypt_data_with_kms` pattern already
//! used by the other bots in this fleet (see `pairtrade/src/config.rs`): a
//! KMS-wrapped AES data key in `ENCRYPTED_DATA_KEY` unwraps a locally
//! AES-256-CBC-encrypted secret. This module never logs, echoes, persists,
//! or returns the decrypted key through any `Debug`/`Display` path; callers
//! must scope its lifetime tightly and never write it to disk, a durable
//! journal, or a metric/status field.

use crate::config::Environment;
use debot_utils::decrypt_data_with_kms;
use thiserror::Error;

const ENCRYPTED_DATA_KEY_ENV: &str = "ENCRYPTED_DATA_KEY";

#[derive(Debug, Error)]
pub enum SignerError {
    #[error("{ENCRYPTED_DATA_KEY_ENV} is not set")]
    MissingDataKey,
    #[error("signing-key environment variable name is empty")]
    EmptyKeyName,
    #[error("signing-key environment variable is not set")]
    MissingSecret,
    #[error("KMS decrypt failed: {0}")]
    Decrypt(String),
    #[error("decrypted signing key is not valid UTF-8")]
    InvalidPlaintext,
}

/// Resolves the Hyperliquid signer's private key by decrypting the
/// ciphertext named by `signing_key_env` with the KMS-wrapped data key in
/// `ENCRYPTED_DATA_KEY`. Returns the raw hex private key with no `0x`
/// prefix; `dex_connector`'s wallet parser accepts it either way.
///
/// # Errors
///
/// Returns an error when either environment variable is missing or empty,
/// or when the KMS decrypt/AES-unwrap fails. [`SignerError::Decrypt`] carries
/// the underlying KMS/base64/AES failure's message (an AWS error code or a
/// generic "bad decrypt"/encoding failure) for operator diagnosis; none of
/// `debot_utils::decrypt_data_with_kms`'s failure paths include the
/// ciphertext or plaintext key material in that message.
pub async fn resolve_signer_private_key<E: Environment>(
    env: &E,
    signing_key_env: &str,
) -> Result<String, SignerError> {
    let signing_key_env = signing_key_env.trim();
    if signing_key_env.is_empty() {
        return Err(SignerError::EmptyKeyName);
    }
    let encrypted_data_key = env
        .get(ENCRYPTED_DATA_KEY_ENV)
        .filter(|value| !value.trim().is_empty())
        .ok_or(SignerError::MissingDataKey)?;
    let ciphertext = env
        .get(signing_key_env)
        .filter(|value| !value.trim().is_empty())
        .ok_or(SignerError::MissingSecret)?;
    let key_hex = decrypt_data_with_kms(&encrypted_data_key, ciphertext, true)
        .await
        .map_err(|error| SignerError::Decrypt(error.to_string()))?;
    String::from_utf8(key_hex).map_err(|_| SignerError::InvalidPlaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_with(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[tokio::test]
    async fn rejects_an_empty_signing_key_variable_name() {
        let env = env_with(&[(ENCRYPTED_DATA_KEY_ENV, "anything")]);
        assert!(matches!(
            resolve_signer_private_key(&env, "  ").await,
            Err(SignerError::EmptyKeyName)
        ));
    }

    #[tokio::test]
    async fn rejects_a_missing_data_key() {
        let env = env_with(&[("HYPE_SIGNING_KEY", "ciphertext")]);
        assert!(matches!(
            resolve_signer_private_key(&env, "HYPE_SIGNING_KEY").await,
            Err(SignerError::MissingDataKey)
        ));
    }

    #[tokio::test]
    async fn rejects_a_blank_data_key() {
        let env = env_with(&[
            (ENCRYPTED_DATA_KEY_ENV, "   "),
            ("HYPE_SIGNING_KEY", "ciphertext"),
        ]);
        assert!(matches!(
            resolve_signer_private_key(&env, "HYPE_SIGNING_KEY").await,
            Err(SignerError::MissingDataKey)
        ));
    }

    #[tokio::test]
    async fn rejects_a_missing_secret() {
        let env = env_with(&[(ENCRYPTED_DATA_KEY_ENV, "wrapped-key")]);
        assert!(matches!(
            resolve_signer_private_key(&env, "HYPE_SIGNING_KEY").await,
            Err(SignerError::MissingSecret)
        ));
    }

    #[tokio::test]
    async fn rejects_a_blank_secret() {
        let env = env_with(&[
            (ENCRYPTED_DATA_KEY_ENV, "wrapped-key"),
            ("HYPE_SIGNING_KEY", "  "),
        ]);
        assert!(matches!(
            resolve_signer_private_key(&env, "HYPE_SIGNING_KEY").await,
            Err(SignerError::MissingSecret)
        ));
    }
}
