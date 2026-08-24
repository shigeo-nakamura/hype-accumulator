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

    let staking = ready(
        workflow
            .prepare_staking_deposit(at(6))
            .expect("staking deposit prepared"),
    );
    assert!(matches!(
        staking,
        ExternalAction::DepositToStaking {
            amount_hype,
            ..
        } if amount_hype == hype(240)
    ));
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().pending_action(), Some(&staking));

    workflow
        .observe_staking_deposit(ExternalReceipt::Ambiguous, at(7))
        .expect("ambiguous staking response persisted");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(
        workflow.state().stage(),
        WorkflowStage::StakingDepositSubmitted
    );

    assert!(matches!(
        workflow.confirm_staking_balance("stale", hype(240), at(6)),
        Err(WorkflowError::StaleObservation)
    ));
    workflow
        .confirm_staking_balance("staking-balance-1", hype(240), at(8))
        .expect("staking balance reconciled");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(
        workflow.state().stage(),
        WorkflowStage::StakingBalanceConfirmed
    );

    let delegation = ready(
        workflow
            .prepare_delegation(at(9))
            .expect("delegation prepared"),
    );
    assert!(matches!(
        delegation,
        ExternalAction::Delegate { amount_hype, .. } if amount_hype == hype(240)
    ));
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().pending_action(), Some(&delegation));

    workflow
        .observe_delegation(
            ExternalReceipt::Confirmed("delegation-receipt-1".to_owned()),
            at(10),
        )
        .expect("delegation response reconciled");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::DelegationSubmitted);

    workflow
        .confirm_delegated_balance("delegation-balance-1", hype(240), at(11))
        .expect("delegated balance reconciled");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::DelegatedConfirmed);

    workflow.complete(at(12)).expect("workflow completed");
    drop(workflow);
    workflow = reopen(&path, &binding);
    assert_eq!(workflow.state().stage(), WorkflowStage::Complete);
    assert_eq!(workflow.state().purchased_hype(), hype(250));
    assert_eq!(workflow.state().filled_usdc(), usdc(50_000_000));
    assert_eq!(workflow.state().debited_usdc(), usdc(50_500_000));
    assert_eq!(workflow.state().staking_target_hype(), hype(240));
    assert_eq!(workflow.state().delegated_hype(), hype(240));
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
    assert!(matches!(
        ready(
            workflow
                .prepare_staking_deposit(at(7))
                .expect("staking prepared from final cumulative fill")
        ),
        ExternalAction::DepositToStaking {
            amount_hype,
            ..
        } if amount_hype == hype(140)
    ));
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
fn contradictory_staking_balance_enters_manual_review_without_guessing() {
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
    ready(
        workflow
            .prepare_staking_deposit(at(5))
            .expect("staking prepared"),
    );
    workflow
        .observe_staking_deposit(
            ExternalReceipt::Confirmed("staking-receipt-1".to_owned()),
            at(6),
        )
        .expect("staking observed");

    assert!(matches!(
        workflow.confirm_staking_balance("wrong-balance", hype(1_240), at(7)),
        Err(WorkflowError::ContradictoryObservation(_))
    ));
    assert_eq!(workflow.state().stage(), WorkflowStage::ManualReview);
    assert!(workflow.state().manual_review_reason().is_some());
    assert!(matches!(
        workflow.prepare_delegation(at(8)),
        Err(WorkflowError::InvalidTransition(_))
    ));
    drop(workflow);
    assert_eq!(
        reopen(&path, &binding).state().stage(),
        WorkflowStage::ManualReview
    );
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
