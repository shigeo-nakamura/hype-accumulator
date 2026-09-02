//! Feature-gated bridge from a durably prepared workflow order to one exact
//! Hyperliquid IOC submission.
//!
//! This module deliberately has no scheduler, config loader, secret loader, or
//! retry loop. A caller must first obtain [`ExternalAction::SubmitOrder`] from
//! [`DurableWorkflow::prepare_order`](crate::workflow::DurableWorkflow::prepare_order),
//! which fsyncs the exact CLOID, nonce, and expiry. Once this method is invoked,
//! every error is reconciliation-only: the caller must query by CLOID and must
//! never call `submit` again for the same prepared action.

use crate::{
    pacing::UsdcMicros,
    workflow::{ExternalAction, HypeAtoms},
};
use chrono::{DateTime, Utc};
use dex_connector::{
    DexError, HyperliquidConnector, HyperliquidL1ActionEnvelope, HyperliquidOrderReconciliation,
    OrderSide,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};
use thiserror::Error;

const USDC_MICROS_PER_USDC: u64 = 1_000_000;
const HYPE_SPOT_MARKET: &str = "HYPE/USDC";
const EXECUTION_IDENTITY_DOMAIN: &[u8] = b"hype-accumulator/execution-account-identity/v1";
const SIGNER_IDENTITY_DOMAIN: &[u8] = b"hype-accumulator/api-wallet-identity/v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveProbeBinding {
    pub symbol: String,
    pub execution_identity_hash: String,
    pub signer_identity_hash: String,
    pub market_metadata_digest: String,
}

impl LiveProbeBinding {
    /// Derives the durable execution and signer identities from the connector's
    /// configured public addresses. No private signing material is exposed.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector is missing an execution account or
    /// API-wallet signer, or when the metadata digest is non-canonical.
    pub fn from_connector(
        connector: &HyperliquidConnector,
        market_metadata_digest: impl Into<String>,
    ) -> Result<Self, LiveProbeError> {
        let market_metadata_digest = market_metadata_digest.into();
        if market_metadata_digest.trim().is_empty()
            || market_metadata_digest != market_metadata_digest.trim()
        {
            return Err(LiveProbeError::BindingMismatch("market metadata"));
        }
        let (execution_identity_hash, signer_identity_hash) = connector_identity_hashes(connector)?;
        Ok(Self {
            symbol: HYPE_SPOT_MARKET.to_string(),
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
        })
    }

    fn validate_connector(&self, connector: &HyperliquidConnector) -> Result<(), LiveProbeError> {
        let (execution_identity_hash, signer_identity_hash) = connector_identity_hashes(connector)?;
        if self.execution_identity_hash != execution_identity_hash {
            return Err(LiveProbeError::BindingMismatch("execution identity"));
        }
        if self.signer_identity_hash != signer_identity_hash {
            return Err(LiveProbeError::BindingMismatch("signer identity"));
        }
        if self.symbol != HYPE_SPOT_MARKET {
            return Err(LiveProbeError::BindingMismatch("symbol"));
        }
        if self.market_metadata_digest.trim().is_empty()
            || self.market_metadata_digest != self.market_metadata_digest.trim()
        {
            return Err(LiveProbeError::BindingMismatch("market metadata"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedIocOrder {
    pub action_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub quantity: Decimal,
    pub limit_price: Decimal,
    pub nonce: u64,
    pub expires_after_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeSubmission {
    pub action_id: String,
    pub client_order_id: String,
    pub exchange_order_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeReconciliation {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: String,
    pub filled_hype: HypeAtoms,
    pub remaining_hype: HypeAtoms,
}

#[derive(Debug, Error)]
pub enum LiveProbeError {
    #[error("prepared action is not an order submission")]
    NotOrder,
    #[error("prepared order does not match the approved probe binding: {0}")]
    BindingMismatch(&'static str),
    #[error("prepared order is expired or has a non-canonical millisecond expiry")]
    InvalidExpiry,
    #[error("prepared order contains an invalid exact decimal: {0}")]
    InvalidDecimal(&'static str),
    #[error("prepared order limit notional exceeds its durable capital bounds")]
    CapitalBound,
    #[error("Hyperliquid action requires CLOID reconciliation: {0}")]
    Connector(#[from] DexError),
}

pub struct HyperliquidLiveProbe {
    connector: HyperliquidConnector,
    binding: LiveProbeBinding,
}

impl HyperliquidLiveProbe {
    /// Constructs a probe only when its durable identity binding matches the
    /// connector's actual execution account and API-wallet signer.
    ///
    /// # Errors
    ///
    /// Rejects missing or mismatched connector identities and invalid binding
    /// metadata before any nonce can be reserved or action submitted.
    pub fn new(
        connector: HyperliquidConnector,
        binding: LiveProbeBinding,
    ) -> Result<Self, LiveProbeError> {
        binding.validate_connector(&connector)?;
        Ok(Self { connector, binding })
    }

    /// Durably reserves one API-wallet nonce without signing or submitting.
    /// The caller must place the returned value in `OrderEnvelopeBinding` and
    /// fsync the resulting workflow action before calling [`Self::submit`].
    ///
    /// # Errors
    ///
    /// Propagates signer or persistent nonce-state failures.
    pub async fn reserve_nonce(&self) -> Result<u64, LiveProbeError> {
        Ok(self.connector.reserve_l1_action_nonce().await?)
    }

    /// Retires the nonce reservation for a durably prepared action that was
    /// never submitted and is now past its signed expiry. This performs no
    /// venue action and refuses unexpired or mismatched workflow actions.
    ///
    /// # Errors
    ///
    /// Rejects a mismatched/non-order action, a non-canonical or unexpired
    /// envelope, or a reservation that was already consumed.
    pub async fn abandon_expired(
        &self,
        action: &ExternalAction,
        now: DateTime<Utc>,
    ) -> Result<(), LiveProbeError> {
        let ExternalAction::SubmitOrder {
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
            l1_nonce,
            signed_expiry_at,
            ..
        } = action
        else {
            return Err(LiveProbeError::NotOrder);
        };
        validate_binding(
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
            &self.binding,
        )?;
        let expires_after_ms = u64::try_from(signed_expiry_at.timestamp_millis())
            .map_err(|_| LiveProbeError::InvalidExpiry)?;
        if signed_expiry_at.timestamp_subsec_nanos() % 1_000_000 != 0 || now < *signed_expiry_at {
            return Err(LiveProbeError::InvalidExpiry);
        }
        self.connector
            .abandon_expired_l1_action_envelope(HyperliquidL1ActionEnvelope {
                nonce: *l1_nonce,
                expires_after_ms,
            })
            .await?;
        Ok(())
    }

    /// Converts and submits one already-fsynced workflow action exactly once.
    ///
    /// # Errors
    ///
    /// Rejects identity, metadata, expiry, decimal, and capital mismatches
    /// before submission. Any connector error after this call begins is
    /// reconciliation-only; callers must never resubmit the action.
    pub async fn submit(
        &self,
        action: &ExternalAction,
        now: DateTime<Utc>,
    ) -> Result<ProbeSubmission, LiveProbeError> {
        let prepared = PreparedIocOrder::from_action(action, &self.binding, now)?;
        let response = self
            .connector
            .create_spot_ioc_order_with_envelope(
                &prepared.symbol,
                prepared.quantity,
                OrderSide::Long,
                prepared.limit_price,
                prepared.client_order_id.clone(),
                HyperliquidL1ActionEnvelope {
                    nonce: prepared.nonce,
                    expires_after_ms: prepared.expires_after_ms,
                },
            )
            .await?;
        Ok(ProbeSubmission {
            action_id: prepared.action_id,
            client_order_id: prepared.client_order_id,
            exchange_order_id: response.order_id,
        })
    }

    /// Performs an authenticated exact-CLOID lookup after any submission
    /// attempt or restart. This method never submits an economic action.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed quantities or connector failures.
    pub async fn reconcile(
        &self,
        action: &ExternalAction,
    ) -> Result<ProbeReconciliation, LiveProbeError> {
        let ExternalAction::SubmitOrder {
            client_order_id,
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
            hype_atoms_per_hype,
            ..
        } = action
        else {
            return Err(LiveProbeError::NotOrder);
        };
        validate_binding(
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
            &self.binding,
        )?;
        let evidence = self
            .connector
            .reconcile_order_by_client_id(client_order_id)
            .await?;
        reconciliation_from_connector(evidence, *hype_atoms_per_hype)
    }
}

impl PreparedIocOrder {
    fn from_action(
        action: &ExternalAction,
        binding: &LiveProbeBinding,
        now: DateTime<Utc>,
    ) -> Result<Self, LiveProbeError> {
        let ExternalAction::SubmitOrder {
            action_id,
            client_order_id,
            execution_identity_hash,
            signer_identity_hash,
            notional_usdc,
            max_debit_usdc,
            original_quantity_hype,
            hype_atoms_per_hype,
            market_metadata_digest,
            limit_price_usdc_per_hype,
            l1_nonce,
            signed_expiry_at,
        } = action
        else {
            return Err(LiveProbeError::NotOrder);
        };
        validate_binding(
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
            binding,
        )?;
        let expires_after_ms = u64::try_from(signed_expiry_at.timestamp_millis())
            .map_err(|_| LiveProbeError::InvalidExpiry)?;
        if signed_expiry_at.timestamp_subsec_nanos() % 1_000_000 != 0 || now >= *signed_expiry_at {
            return Err(LiveProbeError::InvalidExpiry);
        }
        let quantity = atoms_to_decimal(*original_quantity_hype, *hype_atoms_per_hype)?;
        let limit_price = micros_to_decimal(*limit_price_usdc_per_hype)?;
        let limit_notional = quantity
            .checked_mul(limit_price)
            .ok_or(LiveProbeError::CapitalBound)?;
        if limit_notional > micros_to_decimal(*notional_usdc)?
            || limit_notional > micros_to_decimal(*max_debit_usdc)?
        {
            return Err(LiveProbeError::CapitalBound);
        }
        Ok(Self {
            action_id: action_id.clone(),
            client_order_id: client_order_id.clone(),
            symbol: binding.symbol.clone(),
            quantity,
            limit_price,
            nonce: *l1_nonce,
            expires_after_ms,
        })
    }
}

fn validate_binding(
    execution_identity_hash: &str,
    signer_identity_hash: &str,
    market_metadata_digest: &str,
    binding: &LiveProbeBinding,
) -> Result<(), LiveProbeError> {
    if execution_identity_hash != binding.execution_identity_hash {
        return Err(LiveProbeError::BindingMismatch("execution identity"));
    }
    if signer_identity_hash != binding.signer_identity_hash {
        return Err(LiveProbeError::BindingMismatch("signer identity"));
    }
    if market_metadata_digest != binding.market_metadata_digest {
        return Err(LiveProbeError::BindingMismatch("market metadata"));
    }
    if binding.symbol != HYPE_SPOT_MARKET {
        return Err(LiveProbeError::BindingMismatch("symbol"));
    }
    Ok(())
}

fn connector_identity_hashes(
    connector: &HyperliquidConnector,
) -> Result<(String, String), LiveProbeError> {
    let execution_account = connector.execution_account_address()?;
    let api_wallet = connector.api_wallet_address()?;
    Ok((
        identity_hash(EXECUTION_IDENTITY_DOMAIN, execution_account),
        identity_hash(SIGNER_IDENTITY_DOMAIN, &api_wallet),
    ))
}

fn identity_hash(domain: &[u8], canonical_address: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(canonical_address.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn atoms_to_decimal(atoms: HypeAtoms, atoms_per_hype: u64) -> Result<Decimal, LiveProbeError> {
    if atoms.is_zero() || atoms_per_hype == 0 {
        return Err(LiveProbeError::InvalidDecimal("HYPE quantity"));
    }
    let original = Decimal::from(atoms.as_atoms());
    let value = original
        .checked_div(Decimal::from(atoms_per_hype))
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(LiveProbeError::InvalidDecimal("HYPE quantity"))?;
    if value.checked_mul(Decimal::from(atoms_per_hype)) != Some(original) {
        return Err(LiveProbeError::InvalidDecimal(
            "HYPE atom scale is not exactly representable",
        ));
    }
    Ok(value)
}

fn micros_to_decimal(value: UsdcMicros) -> Result<Decimal, LiveProbeError> {
    Decimal::from(value.as_micros())
        .checked_div(Decimal::from(USDC_MICROS_PER_USDC))
        .ok_or(LiveProbeError::InvalidDecimal("USDC amount"))
}

fn decimal_to_atoms(value: Decimal, atoms_per_hype: u64) -> Result<HypeAtoms, LiveProbeError> {
    if value < Decimal::ZERO || atoms_per_hype == 0 {
        return Err(LiveProbeError::InvalidDecimal("reconciled HYPE quantity"));
    }
    let scaled = value
        .checked_mul(Decimal::from(atoms_per_hype))
        .ok_or(LiveProbeError::InvalidDecimal("reconciled HYPE quantity"))?;
    if !scaled.fract().is_zero() {
        return Err(LiveProbeError::InvalidDecimal(
            "reconciled HYPE quantity precision",
        ));
    }
    scaled
        .to_u64()
        .map(HypeAtoms::from_atoms)
        .ok_or(LiveProbeError::InvalidDecimal("reconciled HYPE quantity"))
}

fn reconciliation_from_connector(
    evidence: HyperliquidOrderReconciliation,
    hype_atoms_per_hype: u64,
) -> Result<ProbeReconciliation, LiveProbeError> {
    Ok(ProbeReconciliation {
        client_order_id: evidence.client_order_id,
        exchange_order_id: evidence.order_id,
        status: evidence.status,
        filled_hype: decimal_to_atoms(evidence.filled_size, hype_atoms_per_hype)?,
        remaining_hype: decimal_to_atoms(evidence.remaining_size, hype_atoms_per_hype)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use dex_connector::{HyperliquidAccountConfig, HyperliquidConnectorConfig};
    use std::path::Path;

    const TEST_SIGNER_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    fn at(second: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(second, 0).single().unwrap()
    }

    fn binding() -> LiveProbeBinding {
        LiveProbeBinding {
            symbol: "HYPE/USDC".to_string(),
            execution_identity_hash: "execution-a".to_string(),
            signer_identity_hash: "signer-a".to_string(),
            market_metadata_digest: "market-a".to_string(),
        }
    }

    fn test_connector(account: &str, nonce_path: &Path) -> HyperliquidConnector {
        HyperliquidConnector::new(HyperliquidConnectorConfig {
            base_url: "http://127.0.0.1:1".to_string(),
            tracked_symbols: vec![HYPE_SPOT_MARKET.to_string()],
        })
        .unwrap()
        .with_account(HyperliquidAccountConfig {
            account_address: account.to_string(),
            signer_private_key: Some(TEST_SIGNER_KEY.to_string()),
            vault_address: None,
            is_mainnet: false,
            nonce_state_path: Some(nonce_path.to_path_buf()),
            max_taker_notional: Some(Decimal::from(25)),
            max_taker_slippage_bps: Some(50),
            max_taker_book_age_ms: 1_000,
        })
        .unwrap()
    }

    fn order() -> ExternalAction {
        ExternalAction::SubmitOrder {
            action_id: "action-a".to_string(),
            client_order_id: "0x00112233445566778899aabbccddeeff".to_string(),
            execution_identity_hash: "execution-a".to_string(),
            signer_identity_hash: "signer-a".to_string(),
            notional_usdc: UsdcMicros::from_micros(25_000_000),
            max_debit_usdc: UsdcMicros::from_micros(25_100_000),
            original_quantity_hype: HypeAtoms::from_atoms(100_000_000),
            hype_atoms_per_hype: 100_000_000,
            market_metadata_digest: "market-a".to_string(),
            limit_price_usdc_per_hype: UsdcMicros::from_micros(25_000_000),
            l1_nonce: 1_700_000_000_123,
            signed_expiry_at: at(30),
        }
    }

    #[test]
    fn maps_exact_workflow_envelope_without_regeneration() {
        let prepared = PreparedIocOrder::from_action(&order(), &binding(), at(1)).unwrap();
        assert_eq!(prepared.action_id, "action-a");
        assert_eq!(
            prepared.client_order_id,
            "0x00112233445566778899aabbccddeeff"
        );
        assert_eq!(prepared.quantity, Decimal::ONE);
        assert_eq!(prepared.limit_price, Decimal::from(25));
        assert_eq!(prepared.nonce, 1_700_000_000_123);
        assert_eq!(prepared.expires_after_ms, 30_000);
    }

    #[test]
    fn rejects_identity_metadata_expiry_and_capital_mismatches() {
        let mut mismatched = order();
        if let ExternalAction::SubmitOrder {
            execution_identity_hash,
            ..
        } = &mut mismatched
        {
            *execution_identity_hash = "other".to_string();
        }
        assert!(matches!(
            PreparedIocOrder::from_action(&mismatched, &binding(), at(1)),
            Err(LiveProbeError::BindingMismatch("execution identity"))
        ));

        let mut expired = order();
        if let ExternalAction::SubmitOrder {
            signed_expiry_at, ..
        } = &mut expired
        {
            *signed_expiry_at = at(1);
        }
        assert!(matches!(
            PreparedIocOrder::from_action(&expired, &binding(), at(1)),
            Err(LiveProbeError::InvalidExpiry)
        ));

        let mut oversized = order();
        if let ExternalAction::SubmitOrder { notional_usdc, .. } = &mut oversized {
            *notional_usdc = UsdcMicros::from_micros(24_999_999);
        }
        assert!(matches!(
            PreparedIocOrder::from_action(&oversized, &binding(), at(1)),
            Err(LiveProbeError::CapitalBound)
        ));

        let mut other_market = binding();
        other_market.symbol = "PURR/USDC".to_string();
        assert!(matches!(
            PreparedIocOrder::from_action(&order(), &other_market, at(1)),
            Err(LiveProbeError::BindingMismatch("symbol"))
        ));

        let mut inexact_scale = order();
        if let ExternalAction::SubmitOrder {
            original_quantity_hype,
            hype_atoms_per_hype,
            ..
        } = &mut inexact_scale
        {
            *original_quantity_hype = HypeAtoms::from_atoms(1);
            *hype_atoms_per_hype = 3;
        }
        assert!(matches!(
            PreparedIocOrder::from_action(&inexact_scale, &binding(), at(1)),
            Err(LiveProbeError::InvalidDecimal(
                "HYPE atom scale is not exactly representable"
            ))
        ));
    }

    #[test]
    fn reconciliation_requires_exact_atom_precision() {
        assert_eq!(
            decimal_to_atoms(Decimal::new(123, 2), 100).unwrap(),
            HypeAtoms::from_atoms(123)
        );
        assert!(decimal_to_atoms(Decimal::new(1231, 3), 100).is_err());
        assert!(decimal_to_atoms(Decimal::NEGATIVE_ONE, 100).is_err());
    }

    #[test]
    fn derives_and_enforces_actual_connector_identities() {
        let temp = tempfile::tempdir().unwrap();
        let connector = test_connector(
            "0x1111111111111111111111111111111111111111",
            &temp.path().join("nonce-a.json"),
        );
        let binding = LiveProbeBinding::from_connector(&connector, "market-a").unwrap();
        assert_eq!(
            binding.execution_identity_hash,
            identity_hash(
                EXECUTION_IDENTITY_DOMAIN,
                connector.execution_account_address().unwrap()
            )
        );
        assert_eq!(
            binding.signer_identity_hash,
            identity_hash(
                SIGNER_IDENTITY_DOMAIN,
                &connector.api_wallet_address().unwrap()
            )
        );
        assert!(HyperliquidLiveProbe::new(connector, binding).is_ok());

        let connector = test_connector(
            "0x2222222222222222222222222222222222222222",
            &temp.path().join("nonce-b.json"),
        );
        let mut stale = LiveProbeBinding::from_connector(&connector, "market-a").unwrap();
        stale.execution_identity_hash = "stale-account".to_string();
        assert!(matches!(
            HyperliquidLiveProbe::new(connector, stale),
            Err(LiveProbeError::BindingMismatch("execution identity"))
        ));

        let connector = test_connector(
            "0x3333333333333333333333333333333333333333",
            &temp.path().join("nonce-c.json"),
        );
        let mut stale = LiveProbeBinding::from_connector(&connector, "market-a").unwrap();
        stale.signer_identity_hash = "stale-api-wallet".to_string();
        assert!(matches!(
            HyperliquidLiveProbe::new(connector, stale),
            Err(LiveProbeError::BindingMismatch("signer identity"))
        ));
    }
}
