use chrono::{DateTime, TimeZone, Utc};
use hype_accumulator::{
    pacing::{DailyDecision, DecisionAllocation, DecisionReason, PacingExplanation, UsdcMicros},
    workflow::{
        ActionKind, AppendOutcome, DecisionBinding, DurableWorkflow, ExternalAction,
        ExternalReceipt, HypeAtoms, InventoryBaseline, OrderFinality, PrepareOutcome,
        WorkflowError, WorkflowStage,
    },
};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
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
            spot_hype_atoms: hype(10_000),
            staking_hype_atoms: hype(20_000),
            delegated_hype_atoms: hype(19_000),
            configured_residual_hype_atoms: hype(10),
        },
    )
    .expect("valid workflow binding")
}

fn reopen(path: &Path, binding: &DecisionBinding) -> DurableWorkflow {
    DurableWorkflow::open_or_create(path, binding).expect("journal reopens")
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
    let second_binding =
        DecisionBinding::from_pacing_decision(&reversed, first_binding.inventory_before.clone())
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
            ..
        } if client_order_id.starts_with("0x")
            && client_order_id.len() == 34
            && notional_usdc == usdc(50_000_000)
            && max_debit_usdc == usdc(51_000_000)
    ));
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

    let eligibility = workflow
        .record_staking_eligibility(at(6))
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
    assert_eq!(
        workflow
            .record_order_submission_absent("post-expiry-cloid-not-found", at(3))
            .expect("authoritative absence recorded"),
        AppendOutcome::Appended
    );
    let terminal_count = workflow.record_count();
    assert_eq!(
        workflow
            .record_order_submission_absent("post-expiry-cloid-not-found", at(4))
            .expect("absence replay is idempotent"),
        AppendOutcome::Duplicate
    );
    assert_eq!(workflow.record_count(), terminal_count);
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderFinalized);
    assert!(workflow.state().pending_action().is_none());
    assert!(matches!(
        workflow.prepare_order(at(4)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    drop(workflow);

    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::OrderFinalized);
    assert!(workflow.state().pending_action().is_none());
    assert_eq!(
        workflow
            .record_staking_eligibility(at(5))
            .expect("zero-fill eligibility recorded"),
        hype_accumulator::workflow::StakingEligibility {
            residual_hype: hype(0),
            eligible_hype: hype(0),
        }
    );
    workflow.complete(at(6)).expect("workflow completed");
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::Complete
    );
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
    let eligibility = workflow
        .record_staking_eligibility(at(7))
        .expect("unsigned eligibility recorded from final cumulative fill");
    assert_eq!(eligibility.residual_hype, hype(0));
    assert_eq!(eligibility.eligible_hype, hype(150));
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
        let eligibility = workflow
            .record_staking_eligibility(at(4))
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
    let eligibility = workflow
        .record_staking_eligibility(at(5))
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
        .record_staking_eligibility(at(4))
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
    absent
        .record_order_submission_absent("post-expiry-cloid-not-found", at(2))
        .expect("authoritative absence recorded");
    assert!(matches!(
        absent.observe_order_submission("late-accepted-order", at(3)),
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
        .record_staking_eligibility(at(4))
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
    let changed =
        DecisionBinding::from_pacing_decision(&changed_decision, original.inventory_before.clone())
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
    let eligibility = workflow
        .record_staking_eligibility(at(7))
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
