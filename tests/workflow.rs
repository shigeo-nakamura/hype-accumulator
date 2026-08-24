use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use fs2::FileExt as _;
use hype_accumulator::{
    pacing::{DailyDecision, DecisionAllocation, DecisionReason, PacingExplanation, UsdcMicros},
    workflow::{
        ActionKind, AppendOutcome, AuthenticatedOrderSubmission, AuthorizationInputFreshness,
        BoundFillEvidence, BoundMovementEvidence, ConclusiveAbsenceEvidence, DecisionBinding,
        DurableWorkflow, EligibilityPolicyBinding, ExchangeFillOwner, ExchangeOrderOwner,
        ExchangeOrderOwnerStore, ExternalAction, ExternalReceipt, GapFreeHistoryWatermark,
        HistoryDomain, HypeAtoms, InventoryBaseline, OrderBoundEligibilityEvidence,
        OrderEnvelopeBinding, OrderFinality, OwnershipCommitOutcome, PrepareOutcome,
        ProtectedWorkflowHead, ProtectedWorkflowHeadStore, WorkflowError, WorkflowStage,
    },
};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Default)]
struct MemoryProtectedHeadStore {
    head: Mutex<Option<ProtectedWorkflowHead>>,
    reject_next_compare_and_swap: Mutex<bool>,
    lose_next_compare_and_swap_response: Mutex<bool>,
}

#[derive(Default)]
struct MemoryExchangeOrderOwnerStore {
    order_owners: Mutex<BTreeMap<(String, String), ExchangeOrderOwner>>,
    fill_owners: Mutex<BTreeMap<(String, String), ExchangeFillOwner>>,
}

struct InitializationLockCheckingHeadStore {
    inner: MemoryProtectedHeadStore,
    append_lock_path: PathBuf,
    initial_load_checked: Mutex<bool>,
}

impl InitializationLockCheckingHeadStore {
    fn new(path: &Path) -> Self {
        let mut append_lock_path = path.as_os_str().to_os_string();
        append_lock_path.push(".append.lock");
        Self {
            inner: MemoryProtectedHeadStore::default(),
            append_lock_path: PathBuf::from(append_lock_path),
            initial_load_checked: Mutex::new(false),
        }
    }

    fn require_append_lock(&self) -> Result<(), String> {
        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.append_lock_path)
            .map_err(|error| error.to_string())?;
        match probe.try_lock_exclusive() {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Ok(()) => Err("initialization accessed protected state without the append lock".into()),
            Err(error) => Err(error.to_string()),
        }
    }

    fn initial_load_checked(&self) -> bool {
        *self
            .initial_load_checked
            .lock()
            .expect("initial load check locks")
    }
}

impl ProtectedWorkflowHeadStore for InitializationLockCheckingHeadStore {
    fn load(&self) -> Result<Option<ProtectedWorkflowHead>, String> {
        let mut checked = self
            .initial_load_checked
            .lock()
            .map_err(|error| error.to_string())?;
        if !*checked {
            self.require_append_lock()?;
            *checked = true;
        }
        drop(checked);
        self.inner.load()
    }

    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedWorkflowHead>,
        next: &ProtectedWorkflowHead,
    ) -> Result<bool, String> {
        self.require_append_lock()?;
        self.inner.compare_and_swap(expected, next)
    }
}

impl ExchangeOrderOwnerStore for MemoryExchangeOrderOwnerStore {
    fn claim(&self, owner: &ExchangeOrderOwner) -> Result<bool, String> {
        let key = (
            owner.execution_identity_hash.clone(),
            owner.exchange_order_id.clone(),
        );
        let mut owners = self
            .order_owners
            .lock()
            .map_err(|error| error.to_string())?;
        if let Some(existing) = owners.get(&key) {
            Ok(existing == owner)
        } else {
            owners.insert(key, owner.clone());
            Ok(true)
        }
    }

    fn claim_and_commit(
        &self,
        owner: &ExchangeOrderOwner,
        commit: &mut dyn FnMut() -> bool,
    ) -> Result<OwnershipCommitOutcome, String> {
        let key = (
            owner.execution_identity_hash.clone(),
            owner.exchange_order_id.clone(),
        );
        let mut owners = self
            .order_owners
            .lock()
            .map_err(|error| error.to_string())?;
        if owners.get(&key).is_some_and(|existing| existing != owner) {
            return Ok(OwnershipCommitOutcome::Conflict);
        }
        if !commit() {
            return Ok(OwnershipCommitOutcome::CommitRejected);
        }
        owners.entry(key).or_insert_with(|| owner.clone());
        Ok(OwnershipCommitOutcome::Committed)
    }

    fn claim_fills(&self, owners: &[ExchangeFillOwner]) -> Result<bool, String> {
        let Some(claims) = Self::fill_claims(owners) else {
            return Ok(false);
        };
        let mut owners = self.fill_owners.lock().map_err(|error| error.to_string())?;
        if claims
            .iter()
            .any(|(key, owner)| owners.get(key).is_some_and(|existing| existing != owner))
        {
            return Ok(false);
        }
        for (key, owner) in claims {
            owners.entry(key).or_insert(owner);
        }
        Ok(true)
    }

    fn claim_fills_and_commit(
        &self,
        owners: &[ExchangeFillOwner],
        commit: &mut dyn FnMut() -> bool,
    ) -> Result<OwnershipCommitOutcome, String> {
        let Some(claims) = Self::fill_claims(owners) else {
            return Ok(OwnershipCommitOutcome::Conflict);
        };
        let mut owners = self.fill_owners.lock().map_err(|error| error.to_string())?;
        if claims
            .iter()
            .any(|(key, owner)| owners.get(key).is_some_and(|existing| existing != owner))
        {
            return Ok(OwnershipCommitOutcome::Conflict);
        }
        if !commit() {
            return Ok(OwnershipCommitOutcome::CommitRejected);
        }
        for (key, owner) in claims {
            owners.entry(key).or_insert(owner);
        }
        Ok(OwnershipCommitOutcome::Committed)
    }
}

impl MemoryExchangeOrderOwnerStore {
    fn fill_claims(
        owners: &[ExchangeFillOwner],
    ) -> Option<BTreeMap<(String, String), ExchangeFillOwner>> {
        let mut claims = BTreeMap::new();
        for owner in owners {
            let key = (owner.execution_identity_hash.clone(), owner.fill_id.clone());
            if claims
                .insert(key, owner.clone())
                .is_some_and(|existing| existing != *owner)
            {
                return None;
            }
        }
        Some(claims)
    }

    fn has_fill_owner(&self, execution_identity_hash: &str, fill_id: &str) -> bool {
        self.fill_owners
            .lock()
            .expect("fill owners lock")
            .contains_key(&(execution_identity_hash.to_owned(), fill_id.to_owned()))
    }

    fn has_order_owner(&self, execution_identity_hash: &str, exchange_order_id: &str) -> bool {
        self.order_owners
            .lock()
            .expect("order owners lock")
            .contains_key(&(
                execution_identity_hash.to_owned(),
                exchange_order_id.to_owned(),
            ))
    }
}

impl ProtectedWorkflowHeadStore for MemoryProtectedHeadStore {
    fn load(&self) -> Result<Option<ProtectedWorkflowHead>, String> {
        self.head
            .lock()
            .map(|head| head.clone())
            .map_err(|error| error.to_string())
    }

    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedWorkflowHead>,
        next: &ProtectedWorkflowHead,
    ) -> Result<bool, String> {
        let mut reject = self
            .reject_next_compare_and_swap
            .lock()
            .map_err(|error| error.to_string())?;
        if *reject {
            *reject = false;
            return Err("injected protected-head rejection".into());
        }
        drop(reject);
        let mut lose_response = self
            .lose_next_compare_and_swap_response
            .lock()
            .map_err(|error| error.to_string())?;
        let mut head = self.head.lock().map_err(|error| error.to_string())?;
        if head.as_ref() != expected {
            return Ok(false);
        }
        *head = Some(next.clone());
        if *lose_response {
            *lose_response = false;
            return Err("injected protected-head response loss".into());
        }
        Ok(true)
    }
}

impl MemoryProtectedHeadStore {
    fn reject_next_compare_and_swap(&self) {
        *self
            .reject_next_compare_and_swap
            .lock()
            .expect("protected-head rejection flag locks") = true;
    }

    fn lose_next_compare_and_swap_response(&self) {
        *self
            .lose_next_compare_and_swap_response
            .lock()
            .expect("protected-head response-loss flag locks") = true;
    }
}

fn protected_head_store(path: &Path) -> Arc<MemoryProtectedHeadStore> {
    static STORES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<MemoryProtectedHeadStore>>>> =
        OnceLock::new();
    let mut stores = STORES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("protected head registry locks");
    stores
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(MemoryProtectedHeadStore::default()))
        .clone()
}

fn exchange_order_owner_store(path: &Path) -> Arc<MemoryExchangeOrderOwnerStore> {
    static STORES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<MemoryExchangeOrderOwnerStore>>>> =
        OnceLock::new();
    let mut stores = STORES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("exchange order owner registry locks");
    stores
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(MemoryExchangeOrderOwnerStore::default()))
        .clone()
}

fn at(minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 24, 12, minute, 0)
        .single()
        .expect("valid UTC fixture")
}

const fn usdc(micros: u64) -> UsdcMicros {
    UsdcMicros::from_micros(micros)
}

const fn hype(atoms: u64) -> HypeAtoms {
    HypeAtoms::from_atoms(atoms)
}

fn decision() -> DailyDecision {
    DailyDecision {
        decision_id: "decision-2026-08-24".to_owned(),
        decision_date: at(0).date_naive(),
        decided_at: at(0),
        capital_snapshot_hash: "capital-snapshot-a".to_owned(),
        input_snapshot_hash: "input-snapshot-a".to_owned(),
        planned_usdc: usdc(50_000_000),
        committed_usdc: usdc(51_000_000),
        filled_usdc: usdc(0),
        debited_usdc: usdc(0),
        settled: false,
        reason: DecisionReason::Planned,
        allocations: vec![
            DecisionAllocation {
                tranche_id: "deposit-before-a".to_owned(),
                planned_usdc: usdc(20_000_000),
                committed_usdc: usdc(20_400_000),
                filled_usdc: usdc(0),
                debited_usdc: usdc(0),
            },
            DecisionAllocation {
                tranche_id: "deposit-before-b".to_owned(),
                planned_usdc: usdc(30_000_000),
                committed_usdc: usdc(30_600_000),
                filled_usdc: usdc(0),
                debited_usdc: usdc(0),
            },
        ],
        alerts: Vec::new(),
        explanation: PacingExplanation {
            admitted_unspent_usdc: usdc(50_000_000),
            unadmitted_usdc: usdc(0),
            fixed_required_usdc: usdc(50_000_000),
            observed_budget_after_reserve_usdc: usdc(50_000_000),
            admitted_budget_after_reserve_usdc: usdc(50_000_000),
            exchange_minimum_usdc: usdc(10_000_000),
            daily_cap_usdc: usdc(50_000_000),
            fee_spread_reserve_bps: 0,
            final_catch_up_active: false,
            active_tranches: 2,
        },
    }
}

fn eligibility_policy() -> EligibilityPolicyBinding {
    EligibilityPolicyBinding {
        policy_version: "custody-policy-v1".to_owned(),
        fill_registration_deadline_seconds: 60,
        lot_eligibility_max_age_seconds: 3_600,
    }
}

fn binding() -> DecisionBinding {
    DecisionBinding::from_pacing_decision(
        &decision(),
        InventoryBaseline {
            execution_identity_hash: "signer-identity-hash-a".to_owned(),
            spot_hype_atoms: hype(10_000),
            staking_hype_atoms: hype(20_000),
            delegated_hype_atoms: hype(19_000),
            configured_residual_hype_atoms: hype(10),
            unconsumed_residual_spot_hype_atoms: hype(10),
        },
        OrderEnvelopeBinding {
            signer_identity_hash: "api-wallet-identity-hash-a".to_owned(),
            original_quantity_hype: hype(250),
            hype_atoms_per_hype: 1,
            market_metadata_digest: "hype-market-metadata-a".to_owned(),
            limit_price_usdc_per_hype: usdc(200_000),
            l1_nonce: 7,
            signed_expiry_at: at(29),
            effective_expiry_at: at(30),
            venue_clock_evidence_at: at(0),
            venue_clock_evidence_valid_through_at: at(31),
            venue_clock_evidence_digest: "venue-clock-evidence-a".to_owned(),
            max_venue_clock_lag_ms: 59_999,
            input_freshness: AuthorizationInputFreshness {
                decision_valid_through_at: at(30),
                signal_evidence_valid_through_at: at(30),
                book_evidence_valid_through_at: at(30),
                account_evidence_valid_through_at: at(30),
                fee_schedule_valid_through_at: at(30),
                policy_acknowledgement_valid_through_at: at(30),
            },
        },
        eligibility_policy(),
    )
    .expect("valid workflow binding")
}

fn submission_evidence(
    workflow: &DurableWorkflow,
    exchange_order_id: &str,
    accepted_at: DateTime<Utc>,
) -> AuthenticatedOrderSubmission {
    let binding = workflow.state().binding();
    AuthenticatedOrderSubmission {
        observation_id: format!("venue-order-observation-{exchange_order_id}"),
        account_scope_evidence_hash: format!("account-scope-evidence-{exchange_order_id}"),
        order_envelope_evidence_hash: format!("order-envelope-evidence-{exchange_order_id}"),
        execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
        signer_identity_hash: binding.order_envelope.signer_identity_hash.clone(),
        decision_id: binding.decision_id.clone(),
        client_order_id: workflow.state().client_order_id(),
        exchange_order_id: exchange_order_id.to_owned(),
        canonical_order_envelope_hash: workflow
            .state()
            .canonical_order_envelope_hash()
            .expect("canonical order envelope hashes"),
        planned_usdc: binding.planned_usdc,
        max_debit_usdc: binding.committed_usdc,
        original_quantity_hype: binding.order_envelope.original_quantity_hype,
        hype_atoms_per_hype: binding.order_envelope.hype_atoms_per_hype,
        market_metadata_digest: binding.order_envelope.market_metadata_digest.clone(),
        limit_price_usdc_per_hype: binding.order_envelope.limit_price_usdc_per_hype,
        l1_nonce: binding.order_envelope.l1_nonce,
        signed_expiry_at: binding.order_envelope.signed_expiry_at,
        effective_expiry_at: binding.order_envelope.effective_expiry_at,
        market: "HYPE/USDC".to_owned(),
        side: "buy".to_owned(),
        time_in_force: "IOC".to_owned(),
        accepted_at,
    }
}

fn observe_submission(
    workflow: &mut DurableWorkflow,
    exchange_order_id: &str,
    accepted_at: DateTime<Utc>,
) -> Result<AppendOutcome, WorkflowError> {
    let evidence = submission_evidence(workflow, exchange_order_id, accepted_at);
    workflow.observe_order_submission(&evidence, accepted_at)
}

fn observe_submission_recorded_at(
    workflow: &mut DurableWorkflow,
    exchange_order_id: &str,
    accepted_at: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
) -> Result<AppendOutcome, WorkflowError> {
    let evidence = submission_evidence(workflow, exchange_order_id, accepted_at);
    workflow.observe_order_submission(&evidence, recorded_at)
}

fn bound_evidence(
    workflow: &DurableWorkflow,
    fills: &[(&str, u64, u32)],
    recorded_at: DateTime<Utc>,
) -> OrderBoundEligibilityEvidence {
    let state = workflow.state();
    let binding = state.binding();
    let residual_reservation = binding
        .inventory_before
        .configured_residual_hype_atoms
        .as_atoms()
        .saturating_sub(
            binding
                .inventory_before
                .unconsumed_residual_spot_hype_atoms
                .as_atoms(),
        );
    OrderBoundEligibilityEvidence {
        authorization_id: "order-bound-authorization-a".to_owned(),
        authorization_record_hash: "authorization-record-hash-a".to_owned(),
        decision_id: binding.decision_id.clone(),
        execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
        signer_identity_hash: binding.order_envelope.signer_identity_hash.clone(),
        client_order_id: state.client_order_id(),
        canonical_order_envelope_hash: state
            .canonical_order_envelope_hash()
            .expect("canonical order envelope hashes"),
        authorized_planned_usdc: binding.planned_usdc,
        authorized_max_debit_usdc: binding.committed_usdc,
        original_quantity_hype: binding.order_envelope.original_quantity_hype,
        hype_atoms_per_hype: binding.order_envelope.hype_atoms_per_hype,
        market_metadata_digest: binding.order_envelope.market_metadata_digest.clone(),
        limit_price_usdc_per_hype: binding.order_envelope.limit_price_usdc_per_hype,
        l1_nonce: binding.order_envelope.l1_nonce,
        signed_expiry_at: binding.order_envelope.signed_expiry_at,
        authorized_at: binding.decided_at,
        order_id: state
            .exchange_order_id()
            .expect("accepted order identity")
            .to_owned(),
        accepted_at: state.order_accepted_at().expect("accepted order timestamp"),
        order_bound_at: state.order_accepted_at().expect("accepted order timestamp"),
        effective_expiry_at: binding.order_envelope.effective_expiry_at,
        residual_reservation_hype: hype(residual_reservation),
        policy_version: binding.eligibility_policy.policy_version.clone(),
        fill_history: GapFreeHistoryWatermark {
            domain: HistoryDomain::Fill,
            watermark_id: "eligibility-fill-watermark-a".to_owned(),
            cursor: 303,
            gap_free_from_at: binding.decided_at,
            through_at: recorded_at,
            evidence_hash: "eligibility-fill-history-a".to_owned(),
        },
        movement_history: GapFreeHistoryWatermark {
            domain: HistoryDomain::Movement,
            watermark_id: "eligibility-movement-watermark-a".to_owned(),
            cursor: 404,
            gap_free_from_at: binding.decided_at,
            through_at: recorded_at,
            evidence_hash: "eligibility-movement-history-a".to_owned(),
        },
        movements: Vec::new(),
        fills: fills
            .iter()
            .enumerate()
            .map(|(index, (fill_id, atoms, minute))| BoundFillEvidence {
                fill_id: (*fill_id).to_owned(),
                authorization_id: "order-bound-authorization-a".to_owned(),
                authorization_record_hash: "authorization-record-hash-a".to_owned(),
                execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
                client_order_id: state.client_order_id(),
                order_id: state
                    .exchange_order_id()
                    .expect("accepted order identity")
                    .to_owned(),
                purchased_hype: hype(*atoms),
                executed_at: at(*minute),
                first_observed_at: at(*minute),
                registration_record_id: format!("registration-{fill_id}"),
                registration_record_hash: format!("registration-hash-{fill_id}"),
                registration_cursor: 200 + u64::try_from(index).expect("fixture cursor"),
                registered_at: at(*minute),
                registration_deadline_at: at(*minute + 1),
            })
            .collect(),
    }
}

fn absence_evidence(workflow: &DurableWorkflow, observation_id: &str) -> ConclusiveAbsenceEvidence {
    let state = workflow.state();
    let gap_free_from_at = state.binding().decided_at;
    ConclusiveAbsenceEvidence {
        observation_id: observation_id.to_owned(),
        execution_identity_hash: state
            .binding()
            .inventory_before
            .execution_identity_hash
            .clone(),
        client_order_id: state.client_order_id(),
        effective_expiry_at: state.binding().order_envelope.effective_expiry_at,
        order_history: GapFreeHistoryWatermark {
            domain: HistoryDomain::Order,
            watermark_id: "order-history-watermark-a".to_owned(),
            cursor: 101,
            gap_free_from_at,
            through_at: at(31),
            evidence_hash: "order-history-evidence-hash-a".to_owned(),
        },
        fill_history: GapFreeHistoryWatermark {
            domain: HistoryDomain::Fill,
            watermark_id: "fill-history-watermark-a".to_owned(),
            cursor: 202,
            gap_free_from_at,
            through_at: at(31),
            evidence_hash: "fill-history-evidence-hash-a".to_owned(),
        },
    }
}

fn reopen(path: &Path, binding: &DecisionBinding) -> DurableWorkflow {
    DurableWorkflow::open_or_create(
        path,
        binding,
        protected_head_store(path),
        exchange_order_owner_store(path),
    )
    .expect("journal reopens")
}

fn checkpoint_path(path: &Path) -> PathBuf {
    let mut checkpoint = path.as_os_str().to_os_string();
    checkpoint.push(".head");
    PathBuf::from(checkpoint)
}

fn ready(outcome: PrepareOutcome) -> ExternalAction {
    match outcome {
        PrepareOutcome::Ready(action) => action,
        PrepareOutcome::ReconcileOnly { .. } => panic!("expected a newly prepared action"),
    }
}

fn assert_binding_mismatch(result: &Result<DurableWorkflow, WorkflowError>) {
    assert!(matches!(result, Err(WorkflowError::BindingMismatch)));
}

#[test]
fn deterministic_identity_is_independent_of_allocation_input_order() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("stable-identity.jsonl");
    let first_binding = binding();
    let mut reversed = decision();
    reversed.allocations.reverse();
    let second_binding = DecisionBinding::from_pacing_decision(
        &reversed,
        first_binding.inventory_before.clone(),
        first_binding.order_envelope.clone(),
        first_binding.eligibility_policy.clone(),
    )
    .expect("reversed decision binds");

    assert_eq!(first_binding, second_binding);
    let first = reopen(&path, &first_binding);
    let workflow_id = first.state().workflow_id().to_owned();
    drop(first);
    let mut reopened = reopen(&path, &second_binding);
    assert_eq!(reopened.state().workflow_id(), workflow_id);

    let first_action = ready(reopened.prepare_order(at(1)).expect("order prepared once"));
    assert!(matches!(
        first_action,
        ExternalAction::SubmitOrder {
            client_order_id,
            notional_usdc,
            max_debit_usdc,
            original_quantity_hype,
            hype_atoms_per_hype,
            market_metadata_digest,
            limit_price_usdc_per_hype,
            l1_nonce,
            signed_expiry_at,
            ..
        } if client_order_id.starts_with("0x")
            && client_order_id.len() == 34
            && notional_usdc == usdc(50_000_000)
            && max_debit_usdc == usdc(51_000_000)
            && original_quantity_hype == hype(250)
            && hype_atoms_per_hype == 1
            && market_metadata_digest == "hype-market-metadata-a"
            && limit_price_usdc_per_hype == usdc(200_000)
            && l1_nonce == 7
            && signed_expiry_at == at(29)
    ));
}

#[test]
fn execution_identity_hash_must_be_canonical() {
    for invalid_identity in [
        "",
        " ",
        " signer-identity-hash-a",
        "signer-identity-hash-a ",
    ] {
        let valid = binding();
        let mut inventory = valid.inventory_before;
        inventory.execution_identity_hash = invalid_identity.to_owned();
        assert!(matches!(
            DecisionBinding::from_pacing_decision(
                &decision(),
                inventory,
                valid.order_envelope,
                valid.eligibility_policy,
            ),
            Err(WorkflowError::InvalidBinding(_))
        ));
    }
}

#[test]
fn api_wallet_signer_identity_hash_must_be_canonical() {
    for invalid_identity in [
        "",
        " ",
        " api-wallet-identity-hash-a",
        "api-wallet-identity-hash-a ",
    ] {
        let valid = binding();
        let mut envelope = valid.order_envelope;
        envelope.signer_identity_hash = invalid_identity.to_owned();
        assert!(matches!(
            DecisionBinding::from_pacing_decision(
                &decision(),
                valid.inventory_before,
                envelope,
                valid.eligibility_policy,
            ),
            Err(WorkflowError::InvalidBinding(_))
        ));
    }
}

#[test]
fn api_wallet_signer_is_bound_separately_from_the_execution_account() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("separate-signer-account.jsonl");
    let binding = binding();
    assert_ne!(
        binding.inventory_before.execution_identity_hash,
        binding.order_envelope.signer_identity_hash,
    );
    let mut workflow = reopen(&path, &binding);
    let action = ready(workflow.prepare_order(at(1)).expect("order prepared"));
    assert!(matches!(
        action,
        ExternalAction::SubmitOrder {
            execution_identity_hash,
            signer_identity_hash,
            ..
        } if execution_identity_hash == binding.inventory_before.execution_identity_hash
            && signer_identity_hash == binding.order_envelope.signer_identity_hash
    ));
    observe_submission(&mut workflow, "separately-signed-order", at(2))
        .expect("separate signer submission binds");
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderSubmitted);
}

#[test]
fn expiry_binding_requires_exact_verified_clock_lag_gap() {
    for invalid in [
        "wrong-gap",
        "zero-lag",
        "overflow",
        "missing-evidence",
        "future-evidence",
        "stale-horizon",
    ] {
        let valid = binding();
        let mut envelope = valid.order_envelope;
        match invalid {
            "wrong-gap" => envelope.max_venue_clock_lag_ms -= 1,
            "zero-lag" => envelope.max_venue_clock_lag_ms = 0,
            "overflow" => envelope.max_venue_clock_lag_ms = u64::MAX,
            "missing-evidence" => envelope.venue_clock_evidence_digest.clear(),
            "future-evidence" => envelope.venue_clock_evidence_at = at(1),
            "stale-horizon" => envelope.venue_clock_evidence_valid_through_at = at(30),
            _ => unreachable!("complete fixture set"),
        }
        assert!(matches!(
            DecisionBinding::from_pacing_decision(
                &decision(),
                valid.inventory_before,
                envelope,
                valid.eligibility_policy,
            ),
            Err(WorkflowError::InvalidBinding(_))
        ));
    }
}

#[test]
fn one_stable_decision_cannot_own_alternative_workflow_bindings() {
    let temp = tempfile::tempdir().expect("temp directory");
    let first_path = temp.path().join("owned-decision-a.jsonl");
    let second_path = temp.path().join("owned-decision-b.jsonl");
    let original = binding();
    let mut altered = original.clone();
    altered.order_envelope.l1_nonce += 1;
    let ownership_store = Arc::new(MemoryProtectedHeadStore::default());
    let order_owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());

    let mut first = DurableWorkflow::open_or_create(
        &first_path,
        &original,
        ownership_store.clone(),
        order_owner_store.clone(),
    )
    .expect("stable decision acquires ownership");
    let expected_client_order_id = first.state().client_order_id();
    let first_action = ready(first.prepare_order(at(1)).expect("first order prepared"));
    drop(first);

    assert!(matches!(
        DurableWorkflow::open_or_create(&second_path, &altered, ownership_store, order_owner_store,),
        Err(WorkflowError::RollbackDetected(_))
    ));
    assert!(!second_path.exists());
    assert!(matches!(
        first_action,
        ExternalAction::SubmitOrder { client_order_id, .. }
            if client_order_id == expected_client_order_id
    ));
}

#[test]
fn one_exchange_order_cannot_be_owned_by_two_decision_workflows() {
    let temp = tempfile::tempdir().expect("temp directory");
    let first_path = temp.path().join("order-owner-first.jsonl");
    let second_path = temp.path().join("order-owner-second.jsonl");
    let first_binding = binding();
    let mut second_binding = first_binding.clone();
    second_binding.decision_id = "decision-2026-08-24-second".to_owned();
    let first_head_store = Arc::new(MemoryProtectedHeadStore::default());
    let second_head_store = Arc::new(MemoryProtectedHeadStore::default());
    let order_owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());

    let mut first = DurableWorkflow::open_or_create(
        &first_path,
        &first_binding,
        first_head_store.clone(),
        order_owner_store.clone(),
    )
    .expect("first decision workflow opens");
    let mut second = DurableWorkflow::open_or_create(
        &second_path,
        &second_binding,
        second_head_store.clone(),
        order_owner_store.clone(),
    )
    .expect("second decision workflow opens");
    ready(first.prepare_order(at(1)).expect("first order prepared"));
    ready(second.prepare_order(at(1)).expect("second order prepared"));

    observe_submission(&mut first, "shared-exchange-order", at(2))
        .expect("first decision claims exchange order");
    assert!(matches!(
        observe_submission(&mut second, "shared-exchange-order", at(2)),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(second.state().stage(), WorkflowStage::ManualReview);
    assert!(second.state().exchange_order_id().is_none());
    assert!(second
        .state()
        .manual_review_reason()
        .is_some_and(|reason| reason.contains("already owned by another workflow")));
    drop(first);
    drop(second);

    let first = DurableWorkflow::open_or_create(
        &first_path,
        &first_binding,
        first_head_store,
        order_owner_store.clone(),
    )
    .expect("first order ownership survives restart");
    assert_eq!(first.state().stage(), WorkflowStage::OrderSubmitted);
    let second = DurableWorkflow::open_or_create(
        &second_path,
        &second_binding,
        second_head_store,
        order_owner_store,
    )
    .expect("owner mismatch halt survives restart");
    assert_eq!(second.state().stage(), WorkflowStage::ManualReview);
}

#[test]
fn invalid_submission_does_not_claim_the_correct_workflows_order() {
    let temp = tempfile::tempdir().expect("temp directory");
    let invalid_path = temp.path().join("invalid-order-owner.jsonl");
    let correct_path = temp.path().join("correct-order-owner.jsonl");
    let invalid_binding = binding();
    let mut correct_binding = invalid_binding.clone();
    correct_binding.decision_id = "decision-2026-08-24-correct-owner".to_owned();
    let owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());
    let mut invalid = DurableWorkflow::open_or_create(
        &invalid_path,
        &invalid_binding,
        Arc::new(MemoryProtectedHeadStore::default()),
        owner_store.clone(),
    )
    .expect("invalid workflow opens");
    let mut correct = DurableWorkflow::open_or_create(
        &correct_path,
        &correct_binding,
        Arc::new(MemoryProtectedHeadStore::default()),
        owner_store.clone(),
    )
    .expect("correct workflow opens");

    assert!(matches!(
        observe_submission(&mut invalid, "correct-exchange-order", at(2)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    assert!(owner_store
        .order_owners
        .lock()
        .expect("order owners lock")
        .is_empty());

    ready(
        correct
            .prepare_order(at(1))
            .expect("correct order prepared"),
    );
    observe_submission(&mut correct, "correct-exchange-order", at(2))
        .expect("correct workflow claims order after validation");
    assert_eq!(correct.state().stage(), WorkflowStage::OrderSubmitted);
    assert_eq!(
        owner_store
            .order_owners
            .lock()
            .expect("order owners lock")
            .len(),
        1
    );
}

#[test]
fn mismatched_authenticated_order_envelope_halts_before_claim() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("mismatched-venue-envelope.jsonl");
    let binding = binding();
    let owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());
    let mut workflow = DurableWorkflow::open_or_create(
        &path,
        &binding,
        Arc::new(MemoryProtectedHeadStore::default()),
        owner_store.clone(),
    )
    .expect("workflow opens");
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    let mut evidence = submission_evidence(&workflow, "wrong-envelope-order", at(2));
    evidence.original_quantity_hype = hype(249);

    assert!(matches!(
        workflow.observe_order_submission(&evidence, at(2)),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    assert!(workflow.state().exchange_order_id().is_none());
    assert!(!owner_store.has_order_owner(
        &binding.inventory_before.execution_identity_hash,
        "wrong-envelope-order",
    ));
}

#[test]
fn losing_journal_commit_does_not_retain_an_order_claim() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("concurrent-order-claim.jsonl");
    let binding = binding();
    let head_store = Arc::new(MemoryProtectedHeadStore::default());
    let owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());
    let mut initializer =
        DurableWorkflow::open_or_create(&path, &binding, head_store.clone(), owner_store.clone())
            .expect("workflow initializes");
    ready(initializer.prepare_order(at(1)).expect("order prepared"));
    drop(initializer);
    let mut winner =
        DurableWorkflow::open_or_create(&path, &binding, head_store.clone(), owner_store.clone())
            .expect("winner opens");
    let mut loser =
        DurableWorkflow::open_or_create(&path, &binding, head_store, owner_store.clone())
            .expect("loser opens from the same head");

    observe_submission(&mut winner, "winning-order", at(2)).expect("winner commits");
    assert!(matches!(
        observe_submission(&mut loser, "losing-order", at(2)),
        Err(WorkflowError::ConcurrentModification | WorkflowError::RollbackDetected(_))
    ));
    assert!(owner_store.has_order_owner(
        &binding.inventory_before.execution_identity_hash,
        "winning-order",
    ));
    assert!(!owner_store.has_order_owner(
        &binding.inventory_before.execution_identity_hash,
        "losing-order",
    ));
}

#[test]
fn append_lock_prevents_replacing_a_pending_recovery_intent() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("serialized-pending-append.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".append.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(PathBuf::from(lock_path))
        .expect("append lock opens");
    lock.try_lock_exclusive().expect("append lock acquired");

    assert!(matches!(
        workflow.prepare_order(at(1)),
        Err(WorkflowError::ConcurrentModification)
    ));
    let mut pending_path = path.as_os_str().to_os_string();
    pending_path.push(".pending-append.json");
    assert!(!PathBuf::from(pending_path).exists());

    drop(lock);
    assert!(matches!(
        workflow.prepare_order(at(1)),
        Ok(PrepareOutcome::Ready(_))
    ));
}

#[test]
fn initialization_keeps_the_append_lock_through_the_initial_commit() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("locked-initialization.jsonl");
    let binding = binding();
    let head_store = Arc::new(InitializationLockCheckingHeadStore::new(&path));
    let owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());

    let mut workflow =
        DurableWorkflow::open_or_create(&path, &binding, head_store.clone(), owner_store.clone())
            .expect("initial workflow commit stays locked");
    assert!(head_store.initial_load_checked());
    assert_eq!(workflow.record_count(), 1);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    drop(workflow);

    let reopened = DurableWorkflow::open_or_create(&path, &binding, head_store, owner_store)
        .expect("initialized workflow reopens");
    assert_eq!(reopened.state().stage(), WorkflowStage::Decided);
    assert!(reopened.state().pending_action().is_some());
}

#[test]
fn losing_journal_commit_does_not_retain_fill_claims() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("concurrent-fill-claim.jsonl");
    let binding = binding();
    let head_store = Arc::new(MemoryProtectedHeadStore::default());
    let owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());
    let mut initializer =
        DurableWorkflow::open_or_create(&path, &binding, head_store.clone(), owner_store.clone())
            .expect("workflow initializes");
    ready(initializer.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut initializer, "exchange-order-1", at(2)).expect("submission observed");
    initializer
        .observe_order_fill(
            "fill-observation",
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            true,
            at(3),
        )
        .expect("fill observed");
    initializer
        .finalize_order(
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            OrderFinality::Filled,
            at(4),
        )
        .expect("order finalized");
    drop(initializer);
    let mut winner =
        DurableWorkflow::open_or_create(&path, &binding, head_store.clone(), owner_store.clone())
            .expect("winner opens");
    let mut loser =
        DurableWorkflow::open_or_create(&path, &binding, head_store, owner_store.clone())
            .expect("loser opens from the same head");
    let winner_evidence = bound_evidence(&winner, &[("winning-fill", 250, 3)], at(5));
    let loser_evidence = bound_evidence(&loser, &[("losing-fill", 250, 3)], at(5));

    winner
        .record_staking_eligibility(Some(winner_evidence), at(5))
        .expect("winner commits eligibility");
    assert!(matches!(
        loser.record_staking_eligibility(Some(loser_evidence), at(5)),
        Err(WorkflowError::ConcurrentModification | WorkflowError::RollbackDetected(_))
    ));
    assert!(owner_store.has_fill_owner(
        &binding.inventory_before.execution_identity_hash,
        "winning-fill",
    ));
    assert!(!owner_store.has_fill_owner(
        &binding.inventory_before.execution_identity_hash,
        "losing-fill",
    ));
}

#[test]
fn acceptance_at_signed_expiry_halts_before_claiming_the_order() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("acceptance-at-expiry.jsonl");
    let binding = binding();
    let head_store = Arc::new(MemoryProtectedHeadStore::default());
    let owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());
    let mut workflow =
        DurableWorkflow::open_or_create(&path, &binding, head_store.clone(), owner_store.clone())
            .expect("workflow opens");
    ready(workflow.prepare_order(at(1)).expect("order prepared"));

    assert!(matches!(
        observe_submission(
            &mut workflow,
            "late-exchange-order",
            binding.order_envelope.signed_expiry_at,
        ),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    assert!(workflow.state().exchange_order_id().is_none());
    assert!(!owner_store.has_order_owner(
        &binding.inventory_before.execution_identity_hash,
        "late-exchange-order",
    ));
    assert!(workflow
        .state()
        .manual_review_reason()
        .is_some_and(|reason| reason.contains("signed expiry horizon")));
    drop(workflow);
    let reopened = DurableWorkflow::open_or_create(&path, &binding, head_store, owner_store)
        .expect("manual review reopens");
    assert_eq!(reopened.state().stage(), WorkflowStage::ManualReview);
}

#[test]
fn duplicate_acceptance_at_signed_expiry_durably_halts() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("duplicate-acceptance-at-expiry.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2))
        .expect("timely acceptance recorded");

    assert!(matches!(
        observe_submission(
            &mut workflow,
            "exchange-order-1",
            binding.order_envelope.signed_expiry_at,
        ),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    assert!(workflow
        .state()
        .manual_review_reason()
        .is_some_and(|reason| reason.contains("signed expiry horizon")));
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn one_exchange_fill_cannot_be_owned_by_two_decision_workflows() {
    let temp = tempfile::tempdir().expect("temp directory");
    let first_path = temp.path().join("fill-owner-first.jsonl");
    let second_path = temp.path().join("fill-owner-second.jsonl");
    let first_binding = binding();
    let mut second_binding = first_binding.clone();
    second_binding.decision_id = "decision-2026-08-24-second-fill-owner".to_owned();
    second_binding.order_envelope.l1_nonce += 1;
    let first_head_store = Arc::new(MemoryProtectedHeadStore::default());
    let second_head_store = Arc::new(MemoryProtectedHeadStore::default());
    let owner_store = Arc::new(MemoryExchangeOrderOwnerStore::default());

    let mut first = DurableWorkflow::open_or_create(
        &first_path,
        &first_binding,
        first_head_store.clone(),
        owner_store.clone(),
    )
    .expect("first workflow opens");
    let mut second = DurableWorkflow::open_or_create(
        &second_path,
        &second_binding,
        second_head_store.clone(),
        owner_store.clone(),
    )
    .expect("second workflow opens");
    for (workflow, order_id) in [
        (&mut first, "exchange-order-first"),
        (&mut second, "exchange-order-second"),
    ] {
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        observe_submission(workflow, order_id, at(2)).expect("submission observed");
        workflow
            .observe_order_fill(
                "fill-observation",
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                true,
                at(3),
            )
            .expect("fill observed");
        workflow
            .finalize_order(
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                OrderFinality::Filled,
                at(4),
            )
            .expect("order finalized");
    }

    let first_evidence = bound_evidence(&first, &[("shared-venue-fill", 250, 3)], at(6));
    let second_evidence = bound_evidence(
        &second,
        &[
            ("fresh-before-conflict", 100, 3),
            ("shared-venue-fill", 150, 3),
        ],
        at(6),
    );
    first
        .record_staking_eligibility(Some(first_evidence), at(6))
        .expect("first workflow claims fill");
    assert!(matches!(
        second.record_staking_eligibility(Some(second_evidence), at(6)),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(second.state().stage(), WorkflowStage::ManualReview);
    assert!(second
        .state()
        .manual_review_reason()
        .is_some_and(|reason| reason.contains("fill ID is already owned")));
    assert!(!owner_store.has_fill_owner(
        &second_binding.inventory_before.execution_identity_hash,
        "fresh-before-conflict",
    ));
    drop(first);
    drop(second);

    let first = DurableWorkflow::open_or_create(
        &first_path,
        &first_binding,
        first_head_store,
        owner_store.clone(),
    )
    .expect("first fill ownership survives restart");
    assert_eq!(
        first.state().stage(),
        WorkflowStage::StakingEligibilityRecorded
    );
    let second = DurableWorkflow::open_or_create(
        &second_path,
        &second_binding,
        second_head_store,
        owner_store,
    )
    .expect("fill owner mismatch halt survives restart");
    assert_eq!(second.state().stage(), WorkflowStage::ManualReview);
}

#[test]
fn effective_expiry_is_capped_by_every_authorization_input_horizon() {
    for stale_input in [
        "decision",
        "signal",
        "book",
        "account",
        "fee-schedule",
        "policy-acknowledgement",
    ] {
        let valid = binding();
        let mut envelope = valid.order_envelope;
        let stale_at = at(29);
        match stale_input {
            "decision" => envelope.input_freshness.decision_valid_through_at = stale_at,
            "signal" => envelope.input_freshness.signal_evidence_valid_through_at = stale_at,
            "book" => envelope.input_freshness.book_evidence_valid_through_at = stale_at,
            "account" => envelope.input_freshness.account_evidence_valid_through_at = stale_at,
            "fee-schedule" => envelope.input_freshness.fee_schedule_valid_through_at = stale_at,
            "policy-acknowledgement" => {
                envelope
                    .input_freshness
                    .policy_acknowledgement_valid_through_at = stale_at;
            }
            _ => unreachable!("complete authorization input fixture"),
        }
        assert!(matches!(
            DecisionBinding::from_pacing_decision(
                &decision(),
                valid.inventory_before,
                envelope,
                valid.eligibility_policy,
            ),
            Err(WorkflowError::InvalidBinding(_))
        ));
    }
}

#[test]
fn order_binding_caps_quantity_times_limit_notional() {
    for invalid in [
        "above-planned",
        "above-committed",
        "overflow",
        "zero-scale",
        "missing-metadata",
    ] {
        let valid = binding();
        let mut envelope = valid.order_envelope;
        match invalid {
            "above-planned" => envelope.original_quantity_hype = hype(251),
            "above-committed" => envelope.original_quantity_hype = hype(256),
            "overflow" => {
                envelope.original_quantity_hype = hype(u64::MAX);
                envelope.limit_price_usdc_per_hype = usdc(u64::MAX);
                envelope.hype_atoms_per_hype = 1;
            }
            "zero-scale" => envelope.hype_atoms_per_hype = 0,
            "missing-metadata" => envelope.market_metadata_digest.clear(),
            _ => unreachable!("complete fixture set"),
        }
        assert!(matches!(
            DecisionBinding::from_pacing_decision(
                &decision(),
                valid.inventory_before,
                envelope,
                valid.eligibility_policy,
            ),
            Err(WorkflowError::InvalidBinding(_))
        ));
    }
}

#[test]
fn residual_lot_inventory_cannot_exceed_spot_or_its_configured_target() {
    for invalid in ["aggregate-spot", "configured-target"] {
        let valid = binding();
        let mut inventory = valid.inventory_before;
        match invalid {
            "aggregate-spot" => {
                inventory.spot_hype_atoms = hype(5);
                inventory.unconsumed_residual_spot_hype_atoms = hype(6);
            }
            "configured-target" => {
                inventory.configured_residual_hype_atoms = hype(10);
                inventory.unconsumed_residual_spot_hype_atoms = hype(11);
            }
            _ => unreachable!("complete residual baseline fixture"),
        }
        assert!(matches!(
            DecisionBinding::from_pacing_decision(
                &decision(),
                inventory,
                valid.order_envelope,
                valid.eligibility_policy,
            ),
            Err(WorkflowError::InvalidBinding(_))
        ));
    }
}

#[test]
fn eligibility_policy_durations_must_be_representable() {
    for invalid in ["version", "registration", "age"] {
        let valid = binding();
        let mut policy = valid.eligibility_policy;
        match invalid {
            "version" => policy.policy_version.clear(),
            "registration" => policy.fill_registration_deadline_seconds = u64::MAX,
            "age" => policy.lot_eligibility_max_age_seconds = u64::MAX,
            _ => unreachable!("complete fixture set"),
        }
        assert!(matches!(
            DecisionBinding::from_pacing_decision(
                &decision(),
                valid.inventory_before,
                valid.order_envelope,
                policy,
            ),
            Err(WorkflowError::InvalidBinding(_))
        ));
    }
}

#[test]
fn prepared_order_is_durable_and_restart_is_reconciliation_only() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("workflow.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    assert_eq!(workflow.record_count(), 1);

    let prepared = ready(workflow.prepare_order(at(1)).expect("order prepared"));
    let action_id = prepared.action_id().to_owned();
    assert_eq!(workflow.record_count(), 2);
    drop(workflow);

    let mut restarted = reopen(&path, &binding);
    assert_eq!(restarted.state().pending_action(), Some(&prepared));
    assert_eq!(
        restarted.prepare_order(at(2)).expect("order reconciled"),
        PrepareOutcome::ReconcileOnly {
            action_id,
            kind: ActionKind::SubmitOrder,
        }
    );
    assert_eq!(restarted.record_count(), 2);
}

#[test]
fn an_unprepared_order_is_rejected_at_both_bound_expiries_without_journaling() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("expired-before-prepare.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    let durable_before = fs::read(&path).expect("decision journal read");

    for expired_at in [
        binding.order_envelope.signed_expiry_at,
        binding.order_envelope.effective_expiry_at,
    ] {
        assert!(matches!(
            workflow.prepare_order(expired_at),
            Err(WorkflowError::InvalidTransition(_))
        ));
        assert_eq!(workflow.record_count(), 1);
        assert!(workflow.state().pending_action().is_none());
        assert_eq!(
            fs::read(&path).expect("unchanged journal read"),
            durable_before
        );
    }
}

#[test]
// Intentionally linear: each persisted step is followed by a simulated crash.
#[allow(clippy::too_many_lines)]
fn every_transition_survives_a_restart_without_double_counting() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("workflow.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::Decided);

    let order = ready(workflow.prepare_order(at(1)).expect("order prepared"));
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().pending_action(), Some(&order));

    observe_submission(&mut workflow, "exchange-order-1", at(2))
        .expect("order submission reconciled");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderSubmitted);

    workflow
        .observe_order_fill(
            "fill-1",
            hype(100),
            usdc(20_000_000),
            usdc(20_200_000),
            false,
            at(3),
        )
        .expect("partial fill reconciled");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::PartiallyFilled);
    assert_eq!(workflow.state().purchased_hype(), hype(100));

    workflow
        .observe_order_fill(
            "fill-2",
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            true,
            at(4),
        )
        .expect("full fill reconciled");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::Filled);
    assert_eq!(workflow.state().purchased_hype(), hype(250));

    workflow
        .finalize_order(
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            OrderFinality::Filled,
            at(5),
        )
        .expect("order finalized");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderFinalized);

    let evidence = bound_evidence(&workflow, &[("fill-1", 100, 3), ("fill-2", 150, 4)], at(6));
    let eligibility = workflow
        .record_staking_eligibility(Some(evidence), at(6))
        .expect("unsigned eligibility recorded");
    assert_eq!(eligibility.residual_hype, hype(0));
    assert_eq!(eligibility.eligible_hype, hype(250));
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(
        workflow.state().stage(),
        WorkflowStage::StakingEligibilityRecorded
    );
    assert_eq!(workflow.state().staking_eligibility(), eligibility);

    workflow.complete(at(7)).expect("workflow completed");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::Complete);
    assert_eq!(workflow.state().purchased_hype(), hype(250));
    assert_eq!(workflow.state().filled_usdc(), usdc(50_000_000));
    assert_eq!(workflow.state().debited_usdc(), usdc(50_500_000));
    assert_eq!(
        workflow.state().binding().inventory_before.spot_hype_atoms,
        hype(10_000)
    );
}

#[test]
fn duplicate_responses_are_idempotent_even_when_redelivered_later() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("workflow.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));

    assert_eq!(
        observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission appended"),
        AppendOutcome::Appended
    );
    let count = workflow.record_count();
    assert_eq!(
        observe_submission_recorded_at(&mut workflow, "exchange-order-1", at(2), at(3))
            .expect("duplicate submission ignored"),
        AppendOutcome::Duplicate
    );
    assert_eq!(workflow.record_count(), count);

    assert_eq!(
        workflow
            .observe_order_fill(
                "fill-1",
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                true,
                at(4),
            )
            .expect("fill appended"),
        AppendOutcome::Appended
    );
    let count = workflow.record_count();
    assert_eq!(
        workflow
            .observe_order_fill(
                "fill-1",
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                true,
                at(5),
            )
            .expect("duplicate fill ignored"),
        AppendOutcome::Duplicate
    );
    assert_eq!(workflow.record_count(), count);
    assert_eq!(workflow.state().purchased_hype(), hype(250));
    assert_eq!(workflow.state().filled_usdc(), usdc(50_000_000));
    assert_eq!(workflow.state().debited_usdc(), usdc(50_500_000));
}

#[test]
fn blank_fill_observation_ids_never_advance_the_journal() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("blank-fill-id.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    let count = workflow.record_count();

    for observation_id in ["", "   "] {
        assert!(matches!(
            workflow.observe_order_fill(
                observation_id,
                hype(100),
                usdc(20_000_000),
                usdc(20_200_000),
                false,
                at(3),
            ),
            Err(WorkflowError::InvalidTransition(_))
        ));
        assert_eq!(workflow.record_count(), count);
        assert_eq!(workflow.state().stage(), WorkflowStage::OrderSubmitted);
    }

    workflow
        .observe_order_fill(
            "  stable-fill-id  ",
            hype(100),
            usdc(20_000_000),
            usdc(20_200_000),
            false,
            at(3),
        )
        .expect("trimmed stable fill ID accepted");
    drop(workflow);
    let restarted = reopen(&path, &binding);
    assert_eq!(restarted.state().stage(), WorkflowStage::PartiallyFilled);
    assert_eq!(restarted.state().purchased_hype(), hype(100));
}

#[test]
fn conclusively_absent_submission_releases_pending_intent_and_completes() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("absent-order.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    let prepared = ready(workflow.prepare_order(at(1)).expect("order prepared"));
    drop(workflow);

    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().pending_action(), Some(&prepared));
    assert!(matches!(
        workflow.prepare_order(at(2)),
        Ok(PrepareOutcome::ReconcileOnly {
            kind: ActionKind::SubmitOrder,
            ..
        })
    ));
    let absence = absence_evidence(&workflow, "post-expiry-cloid-not-found");
    assert_eq!(
        workflow
            .record_order_submission_absent(absence.clone(), at(31))
            .expect("authoritative absence recorded"),
        AppendOutcome::Appended
    );
    let terminal_count = workflow.record_count();
    assert_eq!(
        workflow
            .record_order_submission_absent(absence, at(32))
            .expect("absence replay is idempotent"),
        AppendOutcome::Duplicate
    );
    assert_eq!(workflow.record_count(), terminal_count);
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderFinalized);
    assert!(workflow.state().pending_action().is_none());
    assert!(matches!(
        workflow.prepare_order(at(32)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    drop(workflow);

    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderFinalized);
    assert!(workflow.state().pending_action().is_none());
    assert_eq!(
        workflow
            .record_staking_eligibility(None, at(32))
            .expect("zero-fill eligibility recorded"),
        hype_accumulator::workflow::StakingEligibility {
            residual_hype: hype(0),
            eligible_hype: hype(0),
        }
    );
    workflow.complete(at(33)).expect("workflow completed");
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::Complete
    );
}

#[test]
fn absence_requires_effective_expiry_and_both_gap_free_watermarks() {
    let temp = tempfile::tempdir().expect("temp directory");
    let binding = binding();
    for invalid in [
        "before-expiry",
        "order-gap",
        "fill-gap",
        "order-domain",
        "fill-domain",
        "same-proof",
        "whitespace-variant-proof",
    ] {
        let path = temp.path().join(format!("invalid-absence-{invalid}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        let mut evidence = absence_evidence(&workflow, "cloid-not-found");
        let recorded_at = match invalid {
            "before-expiry" => at(29),
            "order-gap" => {
                evidence.order_history.through_at = at(30);
                at(31)
            }
            "fill-gap" => {
                evidence.fill_history.gap_free_from_at = at(1);
                at(31)
            }
            "order-domain" => {
                evidence.order_history.domain = HistoryDomain::Fill;
                at(31)
            }
            "fill-domain" => {
                evidence.fill_history.domain = HistoryDomain::Order;
                at(31)
            }
            "same-proof" => {
                evidence.fill_history.watermark_id = evidence.order_history.watermark_id.clone();
                evidence.fill_history.evidence_hash = evidence.order_history.evidence_hash.clone();
                at(31)
            }
            "whitespace-variant-proof" => {
                evidence.fill_history.watermark_id =
                    format!(" {}", evidence.order_history.watermark_id);
                evidence.fill_history.evidence_hash =
                    format!(" {}", evidence.order_history.evidence_hash);
                at(31)
            }
            _ => unreachable!("complete fixture set"),
        };

        assert!(matches!(
            workflow.record_order_submission_absent(evidence, recorded_at),
            Err(WorkflowError::ContradictoryObservation(_))
        ));
        assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
        drop(workflow);
        assert_eq!(
            reopen(&path, &binding).state().stage(),
            WorkflowStage::ManualReview
        );
    }
}

#[test]
fn fully_filled_observation_requires_the_full_signed_quantity() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("partial-fully-filled-observation.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");

    assert!(matches!(
        workflow.observe_order_fill(
            "partial-fill",
            hype(100),
            usdc(20_000_000),
            usdc(20_200_000),
            true,
            at(3),
        ),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn filled_finality_requires_the_full_signed_quantity() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("partial-filled-finality.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .observe_order_fill(
            "partial-fill",
            hype(100),
            usdc(20_000_000),
            usdc(20_200_000),
            false,
            at(3),
        )
        .expect("partial fill observed");

    assert!(matches!(
        workflow.finalize_order(
            hype(100),
            usdc(20_000_000),
            usdc(20_200_000),
            OrderFinality::Filled,
            at(4),
        ),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn partial_fill_cancel_race_uses_one_final_cumulative_fill_and_never_rebuys() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("workflow.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .observe_order_fill(
            "partial-before-cancel",
            hype(100),
            usdc(20_000_000),
            usdc(20_200_000),
            false,
            at(3),
        )
        .expect("partial fill observed");
    assert!(matches!(
        workflow.prepare_order(at(4)),
        Err(WorkflowError::InvalidTransition(_))
    ));

    workflow
        .finalize_order(
            hype(150),
            usdc(30_000_000),
            usdc(30_300_000),
            OrderFinality::Canceled,
            at(5),
        )
        .expect("cancel/fill race reconciled to final cumulative values");
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderFinalized);
    assert_eq!(workflow.state().purchased_hype(), hype(150));
    assert_eq!(workflow.state().filled_usdc(), usdc(30_000_000));
    assert_eq!(workflow.state().debited_usdc(), usdc(30_300_000));
    assert!(matches!(
        workflow.prepare_order(at(6)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    let evidence = bound_evidence(
        &workflow,
        &[("partial-before-cancel", 100, 3), ("terminal-fill", 50, 5)],
        at(7),
    );
    let eligibility = workflow
        .record_staking_eligibility(Some(evidence), at(7))
        .expect("unsigned eligibility recorded from final cumulative fill");
    assert_eq!(eligibility.residual_hype, hype(0));
    assert_eq!(eligibility.eligible_hype, hype(150));
}

#[test]
fn timely_fill_evidence_can_reconcile_after_effective_expiry() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("post-expiry-reconciliation.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .observe_order_fill(
            "timely-fill",
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            true,
            at(3),
        )
        .expect("fill observed before expiry");
    workflow
        .finalize_order(
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            OrderFinality::Filled,
            at(31),
        )
        .expect("terminal reconciliation may finish after expiry");
    let evidence = bound_evidence(&workflow, &[("timely-fill", 250, 3)], at(32));
    let eligibility = workflow
        .record_staking_eligibility(Some(evidence), at(32))
        .expect("timely fill remains eligible after delayed reconciliation");
    assert_eq!(eligibility.eligible_hype, hype(250));
}

#[test]
fn zero_fill_terminal_orders_complete_without_staking_intent() {
    let temp = tempfile::tempdir().expect("temp directory");
    let binding = binding();
    for (name, finality) in [
        ("canceled", OrderFinality::Canceled),
        ("expired", OrderFinality::Expired),
    ] {
        let path = temp.path().join(format!("{name}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
        workflow
            .finalize_order(hype(0), usdc(0), usdc(0), finality, at(3))
            .expect("zero-fill terminal order finalized");
        let evidence = bound_evidence(&workflow, &[], at(4));
        let eligibility = workflow
            .record_staking_eligibility(Some(evidence), at(4))
            .expect("zero eligibility recorded");
        assert_eq!(eligibility.residual_hype, hype(0));
        assert_eq!(eligibility.eligible_hype, hype(0));
        workflow
            .complete(at(5))
            .expect("zero-fill workflow completed");
        drop(workflow);
        assert_eq!(
            reopen(&path, &binding).state().stage(),
            WorkflowStage::Complete
        );
    }
}

#[test]
fn residual_only_fill_records_zero_eligibility_and_completes() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("residual-only.jsonl");
    let mut binding = binding();
    binding.inventory_before.spot_hype_atoms = hype(5);
    binding.inventory_before.unconsumed_residual_spot_hype_atoms = hype(5);
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .observe_order_fill(
            "residual-fill",
            hype(5),
            usdc(1_000_000),
            usdc(1_010_000),
            false,
            at(3),
        )
        .expect("residual fill observed");
    workflow
        .finalize_order(
            hype(5),
            usdc(1_000_000),
            usdc(1_010_000),
            OrderFinality::Canceled,
            at(4),
        )
        .expect("residual-only order finalized");
    let evidence = bound_evidence(&workflow, &[("residual-fill", 5, 3)], at(5));
    let eligibility = workflow
        .record_staking_eligibility(Some(evidence), at(5))
        .expect("residual-only eligibility recorded");
    assert_eq!(eligibility.residual_hype, hype(5));
    assert_eq!(eligibility.eligible_hype, hype(0));
    workflow
        .complete(at(6))
        .expect("residual-only workflow completed");
    assert_eq!(workflow.state().stage(), WorkflowStage::Complete);
}

#[test]
fn consumed_residual_inventory_is_replenished_before_eligible_spot() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("consumed-residual.jsonl");
    let mut binding = binding();
    binding.inventory_before.spot_hype_atoms = hype(90);
    binding.inventory_before.unconsumed_residual_spot_hype_atoms = hype(0);
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .observe_order_fill(
            "mixed-fill",
            hype(20),
            usdc(4_000_000),
            usdc(4_040_000),
            false,
            at(3),
        )
        .expect("mixed fill observed");
    workflow
        .finalize_order(
            hype(20),
            usdc(4_000_000),
            usdc(4_040_000),
            OrderFinality::Canceled,
            at(4),
        )
        .expect("mixed order finalized");
    let evidence = bound_evidence(&workflow, &[("mixed-fill", 20, 3)], at(5));
    let eligibility = workflow
        .record_staking_eligibility(Some(evidence), at(5))
        .expect("mixed eligibility recorded");

    assert_eq!(eligibility.residual_hype, hype(10));
    assert_eq!(eligibility.eligible_hype, hype(10));
}

#[test]
fn acceptance_predating_the_prepared_action_durably_halts() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("acceptance-before-preparation.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(2)).expect("order prepared"));

    assert!(matches!(
        observe_submission(&mut workflow, "exchange-order-1", at(1)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    assert!(workflow
        .state()
        .manual_review_reason()
        .is_some_and(|reason| reason.contains("predates the durable order preparation")));
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn late_event_collision_persists_manual_review_at_a_monotonic_time() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("late-collision.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .observe_order_fill(
            "fill-1",
            hype(100),
            usdc(20_000_000),
            usdc(20_200_000),
            false,
            at(3),
        )
        .expect("first fill observed");
    workflow
        .observe_order_fill(
            "fill-2",
            hype(150),
            usdc(30_000_000),
            usdc(30_300_000),
            false,
            at(5),
        )
        .expect("newer fill observed");

    assert!(matches!(
        workflow.observe_order_fill(
            "fill-1",
            hype(101),
            usdc(20_100_000),
            usdc(20_301_000),
            false,
            at(3),
        ),
        Err(WorkflowError::EventCollision(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn event_collision_after_completion_durably_invalidates_the_result() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("completed-collision.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .finalize_order(hype(0), usdc(0), usdc(0), OrderFinality::Expired, at(3))
        .expect("order finalized");
    workflow
        .record_staking_eligibility(Some(bound_evidence(&workflow, &[], at(4))), at(4))
        .expect("eligibility recorded");
    workflow.complete(at(5)).expect("workflow completed");

    assert!(matches!(
        observe_submission(&mut workflow, "conflicting-order", at(2)),
        Err(WorkflowError::EventCollision(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    assert!(workflow
        .state()
        .manual_review_reason()
        .is_some_and(|reason| reason.contains("conflicting replay")));
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn fresh_late_order_evidence_durably_invalidates_terminal_results() {
    let temp = tempfile::tempdir().expect("temp directory");
    let binding = binding();

    let absent_path = temp.path().join("accepted-after-absence.jsonl");
    let mut absent = reopen(&absent_path, &binding);
    ready(absent.prepare_order(at(1)).expect("order prepared"));
    let absence = absence_evidence(&absent, "post-expiry-cloid-not-found");
    absent
        .record_order_submission_absent(absence, at(31))
        .expect("authoritative absence recorded");
    assert!(matches!(
        observe_submission(&mut absent, "late-accepted-order", at(32)),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(absent.state().stage(), WorkflowStage::ManualReview);
    drop(absent);
    assert_eq!(
        reopen(&absent_path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );

    let fill_path = temp.path().join("fresh-fill-after-complete.jsonl");
    let mut completed = reopen(&fill_path, &binding);
    ready(completed.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut completed, "exchange-order-1", at(2)).expect("submission observed");
    completed
        .finalize_order(hype(0), usdc(0), usdc(0), OrderFinality::Expired, at(3))
        .expect("order finalized");
    completed
        .record_staking_eligibility(Some(bound_evidence(&completed, &[], at(4))), at(4))
        .expect("eligibility recorded");
    completed.complete(at(5)).expect("workflow completed");
    assert!(matches!(
        completed.observe_order_fill(
            "fresh-late-fill",
            hype(1),
            usdc(100_000),
            usdc(101_000),
            false,
            at(2),
        ),
        Err(WorkflowError::InvalidTransition(_))
    ));
    assert_eq!(completed.state().stage(), WorkflowStage::ManualReview);
    drop(completed);
    assert_eq!(
        reopen(&fill_path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn stale_contradictory_cumulative_evidence_durably_halts_before_completion() {
    let temp = tempfile::tempdir().expect("temp directory");
    let binding = binding();

    let fill_path = temp.path().join("stale-contradictory-fill.jsonl");
    let mut fill = reopen(&fill_path, &binding);
    ready(fill.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut fill, "exchange-order-1", at(2)).expect("submission observed");
    fill.observe_order_fill(
        "partial-fill",
        hype(100),
        usdc(20_000_000),
        usdc(20_200_000),
        false,
        at(4),
    )
    .expect("partial fill observed");
    assert!(matches!(
        fill.observe_order_fill(
            "fresh-but-stale-contradiction",
            hype(150),
            usdc(30_000_000),
            usdc(51_000_001),
            false,
            at(3),
        ),
        Err(WorkflowError::InvalidTransition(_))
    ));
    assert_eq!(fill.state().stage(), WorkflowStage::ManualReview);
    drop(fill);
    assert_eq!(
        reopen(&fill_path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );

    let final_path = temp.path().join("stale-contradictory-finalization.jsonl");
    let mut finalization = reopen(&final_path, &binding);
    ready(finalization.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut finalization, "exchange-order-1", at(2)).expect("submission observed");
    finalization
        .observe_order_fill(
            "partial-fill",
            hype(100),
            usdc(20_000_000),
            usdc(20_200_000),
            false,
            at(4),
        )
        .expect("partial fill observed");
    assert!(matches!(
        finalization.finalize_order(
            hype(150),
            usdc(30_000_000),
            usdc(51_000_001),
            OrderFinality::Canceled,
            at(3),
        ),
        Err(WorkflowError::InvalidTransition(_))
    ));
    assert_eq!(finalization.state().stage(), WorkflowStage::ManualReview);
    drop(finalization);
    assert_eq!(
        reopen(&final_path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn contradictory_authoritative_debits_fail_closed() {
    let temp = tempfile::tempdir().expect("temp directory");
    let binding = binding();
    for (name, filled, debited) in [
        ("debit-below-fill", 20_000_000, 19_999_999),
        ("debit-above-commitment", 50_000_000, 51_000_001),
    ] {
        let path = temp.path().join(format!("{name}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
        assert!(matches!(
            workflow.observe_order_fill(
                "contradictory-fill",
                hype(100),
                usdc(filled),
                usdc(debited),
                false,
                at(3),
            ),
            Err(WorkflowError::ContradictoryObservation(_))
        ));
        assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
        drop(workflow);
        assert_eq!(
            reopen(&path, &binding).state().stage(),
            WorkflowStage::ManualReview
        );
    }
}

#[test]
fn later_capital_never_resizes_a_decided_or_ambiguous_order() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("workflow.jsonl");
    let original = binding();
    let mut workflow = reopen(&path, &original);
    let order = ready(workflow.prepare_order(at(1)).expect("order prepared"));
    drop(workflow);

    let mut changed_decision = decision();
    changed_decision.capital_snapshot_hash = "capital-snapshot-after-deposit".to_owned();
    changed_decision.planned_usdc = usdc(60_000_000);
    changed_decision.committed_usdc = usdc(61_200_000);
    changed_decision.allocations.push(DecisionAllocation {
        tranche_id: "deposit-during-ambiguous-order".to_owned(),
        planned_usdc: usdc(10_000_000),
        committed_usdc: usdc(10_200_000),
        filled_usdc: usdc(0),
        debited_usdc: usdc(0),
    });
    let changed = DecisionBinding::from_pacing_decision(
        &changed_decision,
        original.inventory_before.clone(),
        original.order_envelope.clone(),
        original.eligibility_policy.clone(),
    )
    .expect("later decision shape is internally valid");
    assert_binding_mismatch(&DurableWorkflow::open_or_create(
        &path,
        &changed,
        protected_head_store(&path),
        exchange_order_owner_store(&path),
    ));

    let mut restarted = reopen(&path, &original);
    assert_eq!(restarted.state().binding().planned_usdc, usdc(50_000_000));
    assert_eq!(
        restarted
            .prepare_order(at(2))
            .expect("ambiguous order reconciled"),
        PrepareOutcome::ReconcileOnly {
            action_id: order.action_id().to_owned(),
            kind: ActionKind::SubmitOrder,
        }
    );
}

#[test]
fn staking_and_delegation_actions_remain_hard_disabled() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("workflow.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .observe_order_fill(
            "fill-1",
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            true,
            at(3),
        )
        .expect("fill observed");
    workflow
        .finalize_order(
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            OrderFinality::Filled,
            at(4),
        )
        .expect("order finalized");
    assert!(matches!(
        workflow.prepare_staking_deposit(at(5)),
        Err(WorkflowError::AutomaticStakingDisabled)
    ));
    assert!(matches!(
        workflow.prepare_delegation(at(5)),
        Err(WorkflowError::AutomaticStakingDisabled)
    ));
    assert!(matches!(
        workflow.observe_staking_deposit(ExternalReceipt::Ambiguous, at(6)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderFinalized);
    assert!(workflow.state().pending_action().is_none());
    let evidence = bound_evidence(&workflow, &[("fill-1", 250, 3)], at(7));
    let eligibility = workflow
        .record_staking_eligibility(Some(evidence), at(7))
        .expect("unsigned eligibility recorded");
    assert_eq!(eligibility.eligible_hype, hype(250));
}

#[test]
fn truncated_and_hash_corrupted_journals_fail_closed() {
    let temp = tempfile::tempdir().expect("temp directory");
    let truncated_path = temp.path().join("truncated.jsonl");
    let binding = binding();
    drop(reopen(&truncated_path, &binding));
    OpenOptions::new()
        .append(true)
        .open(&truncated_path)
        .expect("journal opens")
        .write_all(b"{")
        .expect("partial record injected");
    assert!(matches!(
        DurableWorkflow::open_or_create(
            &truncated_path,
            &binding,
            protected_head_store(&truncated_path),
            exchange_order_owner_store(&truncated_path),
        ),
        Err(WorkflowError::TruncatedJournal)
    ));

    let corrupt_path = temp.path().join("corrupt.jsonl");
    drop(reopen(&corrupt_path, &binding));
    let mut payload = fs::read(&corrupt_path).expect("journal read");
    let marker = b"\"record_hash\":\"";
    let hash_start = payload
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("record hash present")
        + marker.len();
    payload[hash_start] = if payload[hash_start] == b'a' {
        b'b'
    } else {
        b'a'
    };
    fs::write(&corrupt_path, payload).expect("corrupt journal written");
    assert!(matches!(
        DurableWorkflow::open_or_create(
            &corrupt_path,
            &binding,
            protected_head_store(&corrupt_path),
            exchange_order_owner_store(&corrupt_path),
        ),
        Err(WorkflowError::CorruptJournal(_))
    ));
}

#[test]
fn complete_record_prefix_rollback_is_detected_by_the_independent_head() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("rollback.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    let decision_only = fs::read(&path).expect("decision record read");
    let decision_head = fs::read(checkpoint_path(&path)).expect("decision head read");

    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    drop(workflow);
    fs::write(&path, decision_only).expect("journal rolled back to a complete valid prefix");
    fs::write(checkpoint_path(&path), decision_head)
        .expect("adjacent head rolled back to the same valid prefix");

    assert!(matches!(
        DurableWorkflow::open_or_create(
            &path,
            &binding,
            protected_head_store(&path),
            exchange_order_owner_store(&path),
        ),
        Err(WorkflowError::RollbackDetected(_))
    ));
}

#[test]
fn a_nonempty_journal_requires_its_independently_protected_head_scope() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("missing-protected-head.jsonl");
    let binding = binding();
    drop(reopen(&path, &binding));

    assert!(matches!(
        DurableWorkflow::open_or_create(
            &path,
            &binding,
            Arc::new(MemoryProtectedHeadStore::default()),
            exchange_order_owner_store(&path),
        ),
        Err(WorkflowError::RollbackDetected(_))
    ));
}

#[test]
fn durable_head_lag_after_journal_fsync_recovers_without_resubmission() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("checkpoint-lag.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    let decision_head = fs::read(checkpoint_path(&path)).expect("decision head read");
    let prepared = ready(workflow.prepare_order(at(1)).expect("order prepared"));
    drop(workflow);

    fs::write(checkpoint_path(&path), &decision_head).expect("stale head restored");
    let mut recovered = reopen(&path, &binding);
    assert_eq!(recovered.state().pending_action(), Some(&prepared));
    assert!(matches!(
        recovered.prepare_order(at(2)),
        Ok(PrepareOutcome::ReconcileOnly { .. })
    ));
    assert_ne!(
        fs::read(checkpoint_path(&path)).expect("advanced head read"),
        decision_head
    );
}

#[test]
fn late_terminal_finalization_after_absence_durably_halts() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("late-finalization.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    let absence = absence_evidence(&workflow, "post-expiry-cloid-not-found");
    workflow
        .record_order_submission_absent(absence, at(31))
        .expect("authoritative absence recorded");
    workflow
        .record_staking_eligibility(None, at(32))
        .expect("absence-only eligibility recorded");
    workflow.complete(at(33)).expect("workflow completed");

    assert!(matches!(
        workflow.finalize_order(
            hype(1),
            usdc(100_000),
            usdc(101_000),
            OrderFinality::Filled,
            at(34),
        ),
        Err(WorkflowError::InvalidTransition(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn eligibility_requires_durable_timely_fill_registration() {
    let temp = tempfile::tempdir().expect("temp directory");
    for invalid in [
        "missing-record-id",
        "missing-record-hash",
        "noncanonical-fill-id",
        "zero-cursor",
        "cursor-beyond-watermark",
        "registered-at-deadline",
        "wrong-deadline",
        "zero-window",
        "wrong-authorization",
        "wrong-authorization-hash",
        "wrong-execution-identity",
        "wrong-cloid",
        "wrong-order",
    ] {
        let mut binding = binding();
        if invalid == "zero-window" {
            binding
                .eligibility_policy
                .fill_registration_deadline_seconds = 0;
        }
        let path = temp
            .path()
            .join(format!("invalid-registration-{invalid}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
        workflow
            .observe_order_fill(
                "fill-1",
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                true,
                at(3),
            )
            .expect("fill observed");
        workflow
            .finalize_order(
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                OrderFinality::Filled,
                at(4),
            )
            .expect("order finalized");
        let mut evidence = bound_evidence(&workflow, &[("fill-1", 250, 3)], at(6));
        let fill = &mut evidence.fills[0];
        match invalid {
            "missing-record-id" => fill.registration_record_id.clear(),
            "missing-record-hash" => fill.registration_record_hash.clear(),
            "noncanonical-fill-id" => fill.fill_id = " fill-1 ".to_owned(),
            "zero-cursor" => fill.registration_cursor = 0,
            "cursor-beyond-watermark" => {
                fill.registration_cursor = evidence.fill_history.cursor + 1;
            }
            "registered-at-deadline" => fill.registered_at = fill.registration_deadline_at,
            "wrong-deadline" => fill.registration_deadline_at = at(5),
            "zero-window" => fill.registration_deadline_at = fill.first_observed_at,
            "wrong-authorization" => fill.authorization_id = "authorization-b".to_owned(),
            "wrong-authorization-hash" => {
                fill.authorization_record_hash = "authorization-record-hash-b".to_owned();
            }
            "wrong-execution-identity" => {
                fill.execution_identity_hash = "signer-identity-hash-b".to_owned();
            }
            "wrong-cloid" => fill.client_order_id = "0xother-cloid".to_owned(),
            "wrong-order" => fill.order_id = "exchange-order-2".to_owned(),
            _ => unreachable!("complete fixture set"),
        }

        assert!(matches!(
            workflow.record_staking_eligibility(Some(evidence), at(6)),
            Err(WorkflowError::ContradictoryObservation(_))
        ));
        assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
        drop(workflow);
        assert_eq!(
            reopen(&path, &binding).state().stage(),
            WorkflowStage::ManualReview
        );
    }
}

#[test]
fn eligibility_expires_at_configured_max_age_boundary() {
    let temp = tempfile::tempdir().expect("temp directory");
    for case in ["just-before", "boundary", "zero-age"] {
        let mut binding = binding();
        if case == "zero-age" {
            binding.eligibility_policy.lot_eligibility_max_age_seconds = 0;
        }
        let path = temp.path().join(format!("eligibility-age-{case}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
        workflow
            .observe_order_fill(
                "fill-1",
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                true,
                at(3),
            )
            .expect("fill observed");
        workflow
            .finalize_order(
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                OrderFinality::Filled,
                at(4),
            )
            .expect("order finalized");
        let expiry_boundary = at(3) + TimeDelta::seconds(3_600);
        let recorded_at = match case {
            "just-before" => expiry_boundary - TimeDelta::milliseconds(1),
            "boundary" => expiry_boundary,
            "zero-age" => at(5),
            _ => unreachable!("complete fixture set"),
        };
        let evidence = bound_evidence(&workflow, &[("fill-1", 250, 3)], recorded_at);
        let result = workflow.record_staking_eligibility(Some(evidence), recorded_at);
        if case == "just-before" {
            assert!(result.is_ok());
            assert_eq!(
                workflow.state().stage(),
                WorkflowStage::StakingEligibilityRecorded
            );
        } else {
            assert!(matches!(
                result,
                Err(WorkflowError::ContradictoryObservation(_))
            ));
            assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
        }
        drop(workflow);
        let reopened = reopen(&path, &binding);
        assert_eq!(
            reopened.state().stage(),
            if case == "just-before" {
                WorkflowStage::StakingEligibilityRecorded
            } else {
                WorkflowStage::ManualReview
            }
        );
    }
}

#[test]
fn eligibility_requires_fresh_gap_free_movement_coverage() {
    let temp = tempfile::tempdir().expect("temp directory");
    let binding = binding();
    for invalid in [
        "movement",
        "fill-watermark-stale",
        "movement-gap",
        "movement-zero-cursor",
        "fill-domain",
        "movement-domain",
        "same-proof",
        "whitespace-variant-proof",
    ] {
        let path = temp
            .path()
            .join(format!("invalid-movement-{invalid}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
        workflow
            .observe_order_fill(
                "fill-1",
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                true,
                at(3),
            )
            .expect("fill observed");
        workflow
            .finalize_order(
                hype(250),
                usdc(50_000_000),
                usdc(50_500_000),
                OrderFinality::Filled,
                at(4),
            )
            .expect("order finalized");
        let mut evidence = bound_evidence(&workflow, &[("fill-1", 250, 3)], at(6));
        match invalid {
            "movement" => evidence.movements.push(BoundMovementEvidence {
                movement_id: "external-sale-a".to_owned(),
                consumed_hype: hype(1),
                occurred_at: at(5),
            }),
            "fill-watermark-stale" => evidence.fill_history.through_at = at(5),
            "movement-gap" => evidence.movement_history.gap_free_from_at = at(1),
            "movement-zero-cursor" => evidence.movement_history.cursor = 0,
            "fill-domain" => evidence.fill_history.domain = HistoryDomain::Order,
            "movement-domain" => evidence.movement_history.domain = HistoryDomain::Fill,
            "same-proof" => {
                evidence.movement_history.watermark_id = evidence.fill_history.watermark_id.clone();
                evidence.movement_history.evidence_hash =
                    evidence.fill_history.evidence_hash.clone();
            }
            "whitespace-variant-proof" => {
                evidence.movement_history.watermark_id =
                    format!(" {}", evidence.fill_history.watermark_id);
                evidence.movement_history.evidence_hash =
                    format!(" {}", evidence.fill_history.evidence_hash);
            }
            _ => unreachable!("complete fixture set"),
        }

        assert!(matches!(
            workflow.record_staking_eligibility(Some(evidence), at(6)),
            Err(WorkflowError::ContradictoryObservation(_))
        ));
        assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
        drop(workflow);
        assert_eq!(
            reopen(&path, &binding).state().stage(),
            WorkflowStage::ManualReview
        );
    }
}

#[test]
fn accepted_order_without_bound_authorization_is_permanently_ineligible() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("missing-authorization.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .observe_order_fill(
            "fill-1",
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            true,
            at(3),
        )
        .expect("fill observed");
    workflow
        .finalize_order(
            hype(250),
            usdc(50_000_000),
            usdc(50_500_000),
            OrderFinality::Filled,
            at(4),
        )
        .expect("order finalized");

    assert!(matches!(
        workflow.record_staking_eligibility(None, at(5)),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    let valid_evidence = bound_evidence(&workflow, &[("fill-1", 250, 3)], at(6));
    assert!(matches!(
        workflow.record_staking_eligibility(Some(valid_evidence), at(6)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn every_signed_order_field_must_match_for_eligibility() {
    let temp = tempfile::tempdir().expect("temp directory");
    let binding = binding();
    for mismatch in [
        "quantity", "scale", "metadata", "limit", "nonce", "expiry", "policy",
    ] {
        let path = temp.path().join(format!("mismatched-{mismatch}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
        workflow
            .finalize_order(hype(0), usdc(0), usdc(0), OrderFinality::Expired, at(3))
            .expect("order finalized");
        let mut evidence = bound_evidence(&workflow, &[], at(4));
        match mismatch {
            "quantity" => evidence.original_quantity_hype = hype(251),
            "scale" => evidence.hype_atoms_per_hype = 2,
            "metadata" => evidence.market_metadata_digest = "hype-market-metadata-b".to_owned(),
            "limit" => evidence.limit_price_usdc_per_hype = usdc(200_001),
            "nonce" => evidence.l1_nonce = 8,
            "expiry" => evidence.signed_expiry_at = at(28),
            "policy" => evidence.policy_version = "custody-policy-v2".to_owned(),
            _ => unreachable!("complete fixture set"),
        }

        assert!(matches!(
            workflow.record_staking_eligibility(Some(evidence), at(4)),
            Err(WorkflowError::ContradictoryObservation(_))
        ));
        assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
        drop(workflow);
        assert_eq!(
            reopen(&path, &binding).state().stage(),
            WorkflowStage::ManualReview
        );
    }
}

#[test]
fn stale_invalid_eligibility_evidence_cannot_be_replaced() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("stale-eligibility.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-1", at(2)).expect("submission observed");
    workflow
        .finalize_order(hype(0), usdc(0), usdc(0), OrderFinality::Expired, at(4))
        .expect("order finalized");
    let valid_evidence = bound_evidence(&workflow, &[], at(5));

    assert!(matches!(
        workflow.record_staking_eligibility(None, at(3)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    assert!(matches!(
        workflow.record_staking_eligibility(Some(valid_evidence), at(5)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn cumulative_fill_notional_cannot_exceed_quantity_at_limit() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("proportional-fill-cap.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-limit", at(2))
        .expect("order submission observed");

    assert!(matches!(
        workflow.observe_order_fill(
            "fill-above-limit",
            hype(1),
            usdc(49_000_000),
            usdc(49_000_000),
            false,
            at(3),
        ),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
}

#[test]
fn expired_residual_fill_does_not_invalidate_fresh_eligible_allocation() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("residual-fill-age.jsonl");
    let mut binding = binding();
    binding.inventory_before.unconsumed_residual_spot_hype_atoms = hype(0);
    binding.eligibility_policy.lot_eligibility_max_age_seconds = 90;
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    observe_submission(&mut workflow, "exchange-order-residual", at(2))
        .expect("order submission observed");
    workflow
        .observe_order_fill(
            "residual-fill",
            hype(10),
            usdc(2_000_000),
            usdc(2_000_000),
            false,
            at(3),
        )
        .expect("residual fill observed");
    workflow
        .observe_order_fill(
            "eligible-fill",
            hype(20),
            usdc(4_000_000),
            usdc(4_000_000),
            false,
            at(4),
        )
        .expect("eligible fill observed");
    workflow
        .finalize_order(
            hype(20),
            usdc(4_000_000),
            usdc(4_000_000),
            OrderFinality::Canceled,
            at(5),
        )
        .expect("partial order finalized");
    let evidence = bound_evidence(
        &workflow,
        &[("residual-fill", 10, 3), ("eligible-fill", 10, 4)],
        at(5),
    );

    let eligibility = workflow
        .record_staking_eligibility(Some(evidence), at(5))
        .expect("fresh eligible allocation recorded");
    assert_eq!(eligibility.residual_hype, hype(10));
    assert_eq!(eligibility.eligible_hype, hype(10));
}

#[test]
fn protected_head_response_loss_recovers_committed_append_on_reopen() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("protected-head-recovery.jsonl");
    let binding = binding();
    let store = protected_head_store(&path);
    let mut workflow = DurableWorkflow::open_or_create(
        &path,
        &binding,
        store.clone(),
        exchange_order_owner_store(&path),
    )
    .expect("initial workflow opens");
    store.lose_next_compare_and_swap_response();

    assert!(matches!(
        workflow.prepare_order(at(1)),
        Err(WorkflowError::ProtectedHead(_))
    ));
    let mut pending_path = path.as_os_str().to_os_string();
    pending_path.push(".pending-append.json");
    let pending_path = PathBuf::from(pending_path);
    assert!(pending_path.exists());
    drop(workflow);

    let mut restarted =
        DurableWorkflow::open_or_create(&path, &binding, store, exchange_order_owner_store(&path))
            .expect("protected pending append restores its local journal on reopen");
    assert_eq!(restarted.record_count(), 2);
    assert!(!pending_path.exists());
    assert!(matches!(
        restarted.prepare_order(at(2)),
        Ok(PrepareOutcome::ReconcileOnly {
            kind: ActionKind::SubmitOrder,
            ..
        })
    ));
}

#[test]
fn complete_recovered_journal_tail_is_synced_before_pending_is_cleared() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("complete-pending-tail.jsonl");
    let binding = binding();
    let store = protected_head_store(&path);
    let mut workflow = DurableWorkflow::open_or_create(
        &path,
        &binding,
        store.clone(),
        exchange_order_owner_store(&path),
    )
    .expect("initial workflow opens");
    store.lose_next_compare_and_swap_response();
    assert!(matches!(
        workflow.prepare_order(at(1)),
        Err(WorkflowError::ProtectedHead(_))
    ));
    let mut pending_path = path.as_os_str().to_os_string();
    pending_path.push(".pending-append.json");
    let pending_path = PathBuf::from(pending_path);
    let pending = String::from_utf8(fs::read(&pending_path).expect("pending append reads"))
        .expect("pending append is UTF-8");
    let record_start = pending
        .find("\"record\":")
        .map(|offset| offset + "\"record\":".len())
        .expect("pending append contains a record");
    let record_end = pending[record_start..]
        .find(",\"next_head\":")
        .map(|offset| record_start + offset)
        .expect("pending append contains the next head");
    let mut line = pending.as_bytes()[record_start..record_end].to_vec();
    line.push(b'\n');
    OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("journal opens")
        .write_all(&line)
        .expect("complete pending line reaches the journal");
    drop(workflow);

    let recovered =
        DurableWorkflow::open_or_create(&path, &binding, store, exchange_order_owner_store(&path))
            .expect("complete tail recovers");
    assert_eq!(recovered.record_count(), 2);
    assert!(!pending_path.exists());
}

#[test]
fn rejected_protected_head_does_not_commit_a_local_pending_append() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("protected-head-rejection.jsonl");
    let binding = binding();
    let store = protected_head_store(&path);
    let mut workflow = DurableWorkflow::open_or_create(
        &path,
        &binding,
        store.clone(),
        exchange_order_owner_store(&path),
    )
    .expect("initial workflow opens");
    store.reject_next_compare_and_swap();

    assert!(matches!(
        workflow.prepare_order(at(1)),
        Err(WorkflowError::ProtectedHead(_))
    ));
    drop(workflow);

    let mut restarted =
        DurableWorkflow::open_or_create(&path, &binding, store, exchange_order_owner_store(&path))
            .expect("uncommitted local pending append is discarded on reopen");
    assert_eq!(restarted.record_count(), 1);
    assert!(matches!(
        restarted.prepare_order(at(2)),
        Ok(PrepareOutcome::Ready(_))
    ));
}

#[test]
fn bootstrap_head_recovers_initial_journal_fsync_crash_window() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("initial-checkpoint-lag.jsonl");
    let binding = binding();
    let workflow = reopen(&path, &binding);
    let workflow_id = workflow.state().workflow_id().to_owned();
    drop(workflow);

    let checkpoint = checkpoint_path(&path);
    let payload = fs::read(&checkpoint).expect("durable head read");
    let mut bootstrap: serde_json::Value = serde_json::from_slice(
        payload
            .strip_suffix(b"\n")
            .expect("durable head is newline terminated"),
    )
    .expect("durable head parses");
    bootstrap["sequence"] = serde_json::Value::Null;
    bootstrap["record_hash"] = serde_json::Value::String(String::new());
    bootstrap["journal_len"] = serde_json::Value::from(0_u64);
    let mut encoded = serde_json::to_vec(&bootstrap).expect("bootstrap head serializes");
    encoded.push(b'\n');
    fs::write(&checkpoint, &encoded).expect("bootstrap head restored");

    let mut recovered = reopen(&path, &binding);
    assert_eq!(recovered.record_count(), 1);
    assert_eq!(recovered.state().workflow_id(), workflow_id);
    let advanced = fs::read(&checkpoint).expect("advanced durable head read");
    assert!(!String::from_utf8(advanced)
        .expect("head is UTF-8 JSON")
        .contains("\"sequence\":null"));

    ready(recovered.prepare_order(at(1)).expect("order prepared"));
    drop(recovered);
    fs::write(&checkpoint, encoded).expect("bootstrap head restored after later record");
    assert!(matches!(
        DurableWorkflow::open_or_create(
            &path,
            &binding,
            protected_head_store(&path),
            exchange_order_owner_store(&path),
        ),
        Err(WorkflowError::RollbackDetected(_))
    ));
}
