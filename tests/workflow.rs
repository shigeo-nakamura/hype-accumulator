use chrono::{DateTime, TimeZone, Utc};
use hype_accumulator::{
    pacing::{DailyDecision, DecisionAllocation, DecisionReason, PacingExplanation, UsdcMicros},
    workflow::{
        ActionKind, AppendOutcome, BoundFillEvidence, BoundMovementEvidence,
        ConclusiveAbsenceEvidence, DecisionBinding, DurableWorkflow, ExternalAction,
        ExternalReceipt, GapFreeHistoryWatermark, HypeAtoms, InventoryBaseline,
        OrderBoundEligibilityEvidence, OrderEnvelopeBinding, OrderFinality, PrepareOutcome,
        WorkflowError, WorkflowStage,
    },
};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

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

fn binding() -> DecisionBinding {
    DecisionBinding::from_pacing_decision(
        &decision(),
        InventoryBaseline {
            execution_identity_hash: "signer-identity-hash-a".to_owned(),
            spot_hype_atoms: hype(10_000),
            staking_hype_atoms: hype(20_000),
            delegated_hype_atoms: hype(19_000),
            configured_residual_hype_atoms: hype(10),
        },
        OrderEnvelopeBinding {
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
        },
    )
    .expect("valid workflow binding")
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
        .saturating_sub(binding.inventory_before.spot_hype_atoms.as_atoms());
    OrderBoundEligibilityEvidence {
        authorization_id: "order-bound-authorization-a".to_owned(),
        authorization_record_hash: "authorization-record-hash-a".to_owned(),
        decision_id: binding.decision_id.clone(),
        execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
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
        policy_version: "custody-policy-v1".to_owned(),
        fill_history: GapFreeHistoryWatermark {
            watermark_id: "eligibility-fill-watermark-a".to_owned(),
            cursor: 303,
            gap_free_from_at: binding.decided_at,
            through_at: recorded_at,
            evidence_hash: "eligibility-fill-history-a".to_owned(),
        },
        movement_history: GapFreeHistoryWatermark {
            watermark_id: "eligibility-movement-watermark-a".to_owned(),
            cursor: 404,
            gap_free_from_at: binding.decided_at,
            through_at: recorded_at,
            evidence_hash: "eligibility-movement-history-a".to_owned(),
        },
        movements: Vec::new(),
        fills: fills
            .iter()
            .map(|(fill_id, atoms, minute)| BoundFillEvidence {
                fill_id: (*fill_id).to_owned(),
                purchased_hype: hype(*atoms),
                executed_at: at(*minute),
                first_observed_at: at(*minute),
                registration_deadline_at: at(30),
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
            watermark_id: "order-history-watermark-a".to_owned(),
            cursor: 101,
            gap_free_from_at,
            through_at: at(31),
            evidence_hash: "order-history-evidence-hash-a".to_owned(),
        },
        fill_history: GapFreeHistoryWatermark {
            watermark_id: "fill-history-watermark-a".to_owned(),
            cursor: 202,
            gap_free_from_at,
            through_at: at(31),
            evidence_hash: "fill-history-evidence-hash-a".to_owned(),
        },
    }
}

fn reopen(path: &Path, binding: &DecisionBinding) -> DurableWorkflow {
    DurableWorkflow::open_or_create(path, binding).expect("journal reopens")
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
    let first_binding = binding();
    let mut reversed = decision();
    reversed.allocations.reverse();
    let second_binding = DecisionBinding::from_pacing_decision(
        &reversed,
        first_binding.inventory_before.clone(),
        first_binding.order_envelope.clone(),
    )
    .expect("reversed decision binds");

    assert_eq!(first_binding, second_binding);
    let mut first = reopen(&temp.path().join("first.jsonl"), &first_binding);
    let mut second = reopen(&temp.path().join("second.jsonl"), &second_binding);
    assert_eq!(first.state().workflow_id(), second.state().workflow_id());

    let first_action = ready(first.prepare_order(at(1)).expect("first order prepared"));
    let second_action = ready(second.prepare_order(at(1)).expect("second order prepared"));
    assert_eq!(first_action, second_action);
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
            DecisionBinding::from_pacing_decision(&decision(), valid.inventory_before, envelope,),
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
            DecisionBinding::from_pacing_decision(&decision(), valid.inventory_before, envelope),
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

    workflow
        .observe_order_submission("exchange-order-1", at(2))
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
        workflow
            .observe_order_submission("exchange-order-1", at(2))
            .expect("submission appended"),
        AppendOutcome::Appended
    );
    let count = workflow.record_count();
    assert_eq!(
        workflow
            .observe_order_submission("exchange-order-1", at(3))
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
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
    for invalid in ["before-expiry", "order-gap", "fill-gap"] {
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
fn partial_fill_cancel_race_uses_one_final_cumulative_fill_and_never_rebuys() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("workflow.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
        workflow
            .observe_order_submission("exchange-order-1", at(2))
            .expect("submission observed");
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
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
fn late_event_collision_persists_manual_review_at_a_monotonic_time() {
    let temp = tempfile::tempdir().expect("temp directory");
    let path = temp.path().join("late-collision.jsonl");
    let binding = binding();
    let mut workflow = reopen(&path, &binding);
    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
    workflow
        .finalize_order(hype(0), usdc(0), usdc(0), OrderFinality::Expired, at(3))
        .expect("order finalized");
    workflow
        .record_staking_eligibility(Some(bound_evidence(&workflow, &[], at(4))), at(4))
        .expect("eligibility recorded");
    workflow.complete(at(5)).expect("workflow completed");

    assert!(matches!(
        workflow.observe_order_submission("conflicting-order", at(2)),
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
        absent.observe_order_submission("late-accepted-order", at(32)),
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
    completed
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
    fill.observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
    finalization
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
        workflow
            .observe_order_submission("exchange-order-1", at(2))
            .expect("submission observed");
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
    )
    .expect("later decision shape is internally valid");
    assert_binding_mismatch(&DurableWorkflow::open_or_create(&path, &changed));

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
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
        DurableWorkflow::open_or_create(&truncated_path, &binding),
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
        DurableWorkflow::open_or_create(&corrupt_path, &binding),
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

    ready(workflow.prepare_order(at(1)).expect("order prepared"));
    drop(workflow);
    fs::write(&path, decision_only).expect("journal rolled back to a complete valid prefix");

    assert!(matches!(
        DurableWorkflow::open_or_create(&path, &binding),
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
fn eligibility_requires_fresh_gap_free_movement_coverage() {
    let temp = tempfile::tempdir().expect("temp directory");
    let binding = binding();
    for invalid in [
        "movement",
        "fill-watermark-stale",
        "movement-gap",
        "movement-zero-cursor",
    ] {
        let path = temp
            .path()
            .join(format!("invalid-movement-{invalid}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        workflow
            .observe_order_submission("exchange-order-1", at(2))
            .expect("submission observed");
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
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
    workflow
        .observe_order_fill("fill-1", hype(1), usdc(100_000), usdc(101_000), true, at(3))
        .expect("fill observed");
    workflow
        .finalize_order(
            hype(1),
            usdc(100_000),
            usdc(101_000),
            OrderFinality::Filled,
            at(4),
        )
        .expect("order finalized");

    assert!(matches!(
        workflow.record_staking_eligibility(None, at(5)),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    let valid_evidence = bound_evidence(&workflow, &[("fill-1", 1, 3)], at(6));
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
    for mismatch in ["quantity", "scale", "metadata", "limit", "nonce", "expiry"] {
        let path = temp.path().join(format!("mismatched-{mismatch}.jsonl"));
        let mut workflow = reopen(&path, &binding);
        ready(workflow.prepare_order(at(1)).expect("order prepared"));
        workflow
            .observe_order_submission("exchange-order-1", at(2))
            .expect("submission observed");
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
    workflow
        .observe_order_submission("exchange-order-1", at(2))
        .expect("submission observed");
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
        DurableWorkflow::open_or_create(&path, &binding),
        Err(WorkflowError::RollbackDetected(_))
    ));
}
