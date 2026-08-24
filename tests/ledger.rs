use chrono::{DateTime, TimeZone, Utc};
use hype_accumulator::{
    ledger::{
        AppendOutcome, DurableLedger, LedgerError, LedgerEvent, LedgerEventKind,
        ProtectedAnchorStore, ProtectedHeadAnchor, LEDGER_FILE_NAME, SNAPSHOT_FILE_NAME,
    },
    pacing::UsdcMicros,
};
use serde_json::Value;
use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct MemoryProtectedAnchorStore(Mutex<Option<ProtectedHeadAnchor>>);

impl ProtectedAnchorStore for MemoryProtectedAnchorStore {
    fn load(&self) -> Result<Option<ProtectedHeadAnchor>, String> {
        self.0
            .lock()
            .map(|anchor| anchor.clone())
            .map_err(|_| "protected anchor lock poisoned".into())
    }

    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedHeadAnchor>,
        next: &ProtectedHeadAnchor,
    ) -> Result<bool, String> {
        let mut anchor = self
            .0
            .lock()
            .map_err(|_| "protected anchor lock poisoned".to_owned())?;
        if anchor.as_ref() != expected {
            return Ok(false);
        }
        *anchor = Some(next.clone());
        Ok(true)
    }
}

impl MemoryProtectedAnchorStore {
    fn replace_for_test(&self, anchor: Option<ProtectedHeadAnchor>) {
        *self.0.lock().expect("protected anchor lock") = anchor;
    }
}

type TestAnchor = Arc<MemoryProtectedAnchorStore>;

fn anchor_store() -> TestAnchor {
    Arc::new(MemoryProtectedAnchorStore::default())
}

fn open(directory: &Path, anchor: &TestAnchor) -> Result<DurableLedger, LedgerError> {
    DurableLedger::open(directory, anchor.clone())
}

fn restore(
    source: &Path,
    destination: &Path,
    source_anchor: &TestAnchor,
    destination_anchor: &TestAnchor,
) -> Result<DurableLedger, LedgerError> {
    DurableLedger::restore_clean(
        source,
        destination,
        source_anchor.clone(),
        destination_anchor.clone(),
    )
}

fn at(hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 24, hour, 0, 0)
        .single()
        .expect("valid UTC fixture")
}

fn usd(value: u64) -> UsdcMicros {
    UsdcMicros::checked_from_whole_usdc(value).expect("small test amount")
}

fn event(id: &str, hour: u32, kind: LedgerEventKind) -> LedgerEvent {
    LedgerEvent {
        event_id: id.into(),
        occurred_at: at(hour),
        kind,
    }
}

fn deposit(id: &str, hour: u32, amount: u64) -> LedgerEvent {
    event(
        id,
        hour,
        LedgerEventKind::AuthoritativeDeposit {
            amount_usdc: usd(amount),
        },
    )
}

fn observed(id: &str, hour: u32, usdc: u64, hype_atoms: u64) -> LedgerEvent {
    event(
        id,
        hour,
        LedgerEventKind::BalanceObserved {
            observed_usdc: usd(usdc),
            observed_hype_atoms: hype_atoms,
        },
    )
}

fn daily_outcome(
    event_id: &str,
    decision_id: &str,
    decision_date: chrono::NaiveDate,
    outcome: &str,
) -> LedgerEvent {
    let kind = match outcome {
        "decision" => LedgerEventKind::DailyDecision {
            decision_id: decision_id.to_owned(),
            decision_date,
            commitment_id: format!("commitment-{decision_id}"),
            planned_usdc: usd(10),
            committed_usdc: usd(10),
        },
        "skip" => LedgerEventKind::DailySkip {
            decision_id: decision_id.to_owned(),
            decision_date,
            reason: "no eligible daily capital".to_owned(),
        },
        _ => unreachable!("complete daily outcome fixture"),
    };
    LedgerEvent {
        event_id: event_id.to_owned(),
        occurred_at: decision_date
            .and_hms_opt(1, 0, 0)
            .expect("valid daily outcome time")
            .and_utc(),
        kind,
    }
}

fn dated_event(
    event_id: impl Into<String>,
    date: chrono::NaiveDate,
    hour: u32,
    kind: LedgerEventKind,
) -> LedgerEvent {
    LedgerEvent {
        event_id: event_id.into(),
        occurred_at: date
            .and_hms_opt(hour, 0, 0)
            .expect("valid dated fixture time")
            .and_utc(),
        kind,
    }
}

fn append_decision_backing(
    ledger: &mut DurableLedger,
    decision_id: &str,
    date: chrono::NaiveDate,
    amount: u64,
) {
    let deposit_id = format!("deposit-{decision_id}");
    ledger
        .append(dated_event(
            deposit_id.clone(),
            date,
            2,
            LedgerEventKind::AuthoritativeDeposit {
                amount_usdc: usd(amount),
            },
        ))
        .expect("append decision deposit");
    ledger
        .append(dated_event(
            format!("admission-{decision_id}"),
            date,
            3,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: deposit_id,
                amount_usdc: usd(amount),
            },
        ))
        .expect("append decision admission");
    ledger
        .append(dated_event(
            format!("capital-{decision_id}"),
            date,
            4,
            LedgerEventKind::CapitalCommitted {
                commitment_id: format!("commitment-{decision_id}"),
                amount_usdc: usd(amount),
            },
        ))
        .expect("append decision commitment");
}

fn ledger_path(directory: &Path) -> std::path::PathBuf {
    directory.join(LEDGER_FILE_NAME)
}

fn snapshot_path(directory: &Path) -> std::path::PathBuf {
    directory.join(SNAPSHOT_FILE_NAME)
}

#[test]
fn duplicate_event_is_idempotent_and_id_collision_fails_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let first = deposit("deposit-source", 1, 100);

    assert_eq!(
        ledger.append(first.clone()).expect("append deposit"),
        AppendOutcome::Appended
    );
    let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");
    assert_eq!(
        ledger.append(first.clone()).expect("replay duplicate"),
        AppendOutcome::Duplicate
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read ledger"),
        durable_before
    );

    let collision = LedgerEvent {
        kind: LedgerEventKind::AuthoritativeDeposit {
            amount_usdc: usd(101),
        },
        ..first
    };
    assert_eq!(
        ledger.append(collision),
        Err(LedgerError::EventCollision("deposit-source".into()))
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read ledger"),
        durable_before
    );
}

#[test]
fn staking_action_and_reward_ids_cannot_be_reused() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(event(
            "staking-deposit-first",
            1,
            LedgerEventKind::StakingDepositRecorded {
                action_id: "shared-action".into(),
                hype_atoms: 10,
            },
        ))
        .expect("append staking action");
    let durable_before_action = fs::read(ledger_path(directory.path())).expect("read ledger");

    assert_eq!(
        ledger.append(event(
            "delegation-second",
            2,
            LedgerEventKind::DelegationRecorded {
                action_id: "shared-action".into(),
                validator_id: "validator-1".into(),
                hype_atoms: 10,
            },
        )),
        Err(LedgerError::ActionIdCollision("shared-action".into()))
    );
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged action ledger"),
        durable_before_action
    );

    ledger
        .append(event(
            "reward-first",
            3,
            LedgerEventKind::RewardRecorded {
                reward_id: "shared-reward".into(),
                hype_atoms: 2,
            },
        ))
        .expect("append reward");
    let durable_before_reward = fs::read(ledger_path(directory.path())).expect("read ledger");
    assert_eq!(
        ledger.append(event(
            "reward-second",
            4,
            LedgerEventKind::RewardRecorded {
                reward_id: "shared-reward".into(),
                hype_atoms: 2,
            },
        )),
        Err(LedgerError::RewardIdCollision("shared-reward".into()))
    );
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged reward ledger"),
        durable_before_reward
    );
}

#[test]
fn reconciliation_correction_ids_cannot_be_reused() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(event(
            "correction-first",
            1,
            LedgerEventKind::ReconciliationCorrection {
                correction_id: "shared-correction".into(),
                observed_usdc: usd(10),
                observed_hype_atoms: 20,
                reason: "authoritative correction".into(),
            },
        ))
        .expect("append correction");
    let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");

    assert_eq!(
        ledger.append(event(
            "correction-second",
            2,
            LedgerEventKind::ReconciliationCorrection {
                correction_id: "shared-correction".into(),
                observed_usdc: usd(11),
                observed_hype_atoms: 21,
                reason: "conflicting retry".into(),
            },
        )),
        Err(LedgerError::CorrectionIdCollision(
            "shared-correction".into()
        ))
    );
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged ledger"),
        durable_before
    );
    assert_eq!(ledger.state().observed_usdc(), usd(10));
    assert_eq!(ledger.state().observed_hype_atoms(), 20);
}

#[test]
fn late_observations_remain_auditable_without_replacing_fresher_state() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(observed("newer-observation", 3, 50, 500))
        .expect("append newer observation");
    ledger
        .append(event(
            "late-correction",
            2,
            LedgerEventKind::ReconciliationCorrection {
                correction_id: "late-correction-id".into(),
                observed_usdc: usd(10),
                observed_hype_atoms: 100,
                reason: "delayed authoritative correction".into(),
            },
        ))
        .expect("append late correction for audit");

    assert_eq!(ledger.state().observed_usdc(), usd(50));
    assert_eq!(ledger.state().observed_hype_atoms(), 500);
    assert_eq!(ledger.state().last_event_at(), Some(&at(3)));
    let replayed_state = ledger.state().clone();
    drop(ledger);

    let reopened = open(directory.path(), &anchor).expect("reopen ledger");
    assert_eq!(reopened.state(), &replayed_state);
}

#[test]
fn stable_order_ids_are_idempotent_and_cannot_change_ownership() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let date = at(0).date_naive();
    ledger
        .append(daily_outcome(
            "daily-order-identity",
            "decision-order-identity",
            date,
            "decision",
        ))
        .expect("append decision");
    append_decision_backing(&mut ledger, "decision-order-identity", date, 10);
    let order = dated_event(
        "order-envelope-first",
        date,
        5,
        LedgerEventKind::OrderRecorded {
            order_id: "venue-order-stable".into(),
            decision_id: "decision-order-identity".into(),
        },
    );
    ledger.append(order.clone()).expect("append venue order");
    let durable_before_retry = fs::read(ledger_path(directory.path())).expect("read ledger");
    let record_count = ledger.record_count();

    assert_eq!(
        ledger
            .append(LedgerEvent {
                event_id: "order-envelope-retry".into(),
                ..order
            })
            .expect("identical order retry"),
        AppendOutcome::Duplicate
    );
    assert_eq!(ledger.record_count(), record_count);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged retry ledger"),
        durable_before_retry
    );
    assert_eq!(
        ledger.append(dated_event(
            "order-envelope-conflict",
            date,
            6,
            LedgerEventKind::OrderRecorded {
                order_id: "venue-order-stable".into(),
                decision_id: "decision-order-identity".into(),
            },
        )),
        Err(LedgerError::OrderIdCollision("venue-order-stable".into()))
    );
    assert_eq!(ledger.record_count(), record_count);
}

#[test]
fn stable_fill_ids_are_idempotent_and_cannot_change_ownership() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let date = at(0).date_naive();
    ledger
        .append(daily_outcome(
            "daily-fill-identity",
            "decision-fill-identity",
            date,
            "decision",
        ))
        .expect("append decision");
    append_decision_backing(&mut ledger, "decision-fill-identity", date, 10);
    ledger
        .append(dated_event(
            "order-fill-identity",
            date,
            5,
            LedgerEventKind::OrderRecorded {
                order_id: "order-fill-identity".into(),
                decision_id: "decision-fill-identity".into(),
            },
        ))
        .expect("append order");
    let fill = dated_event(
        "fill-envelope-first",
        date,
        6,
        LedgerEventKind::FillRecorded {
            fill_id: "venue-fill-stable".into(),
            order_id: "order-fill-identity".into(),
            filled_usdc: usd(5),
            received_hype_atoms: 1,
        },
    );
    ledger.append(fill.clone()).expect("append venue fill");
    let durable_before_retry = fs::read(ledger_path(directory.path())).expect("read ledger");
    let record_count = ledger.record_count();

    assert_eq!(
        ledger
            .append(LedgerEvent {
                event_id: "fill-envelope-retry".into(),
                ..fill
            })
            .expect("identical fill retry"),
        AppendOutcome::Duplicate
    );
    assert_eq!(ledger.record_count(), record_count);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged retry ledger"),
        durable_before_retry
    );
    assert_eq!(
        ledger.append(dated_event(
            "fill-envelope-conflict",
            date,
            7,
            LedgerEventKind::FillRecorded {
                fill_id: "venue-fill-stable".into(),
                order_id: "order-fill-identity".into(),
                filled_usdc: usd(4),
                received_hype_atoms: 1,
            },
        )),
        Err(LedgerError::FillIdCollision("venue-fill-stable".into()))
    );
    assert_eq!(ledger.record_count(), record_count);
}

#[test]
fn order_linked_costs_respect_event_time_causality() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let date = at(0).date_naive();
    ledger
        .append(daily_outcome(
            "daily-causal-order",
            "decision-causal-order",
            date,
            "decision",
        ))
        .expect("append decision");
    append_decision_backing(&mut ledger, "decision-causal-order", date, 10);

    assert_eq!(
        ledger.append(dated_event(
            "order-before-decision",
            date,
            0,
            LedgerEventKind::OrderRecorded {
                order_id: "order-before-decision".into(),
                decision_id: "decision-causal-order".into(),
            },
        )),
        Err(LedgerError::InvalidEvent(
            "order predates its owning decision".into()
        ))
    );

    assert_eq!(
        ledger.append(dated_event(
            "order-before-commitment",
            date,
            3,
            LedgerEventKind::OrderRecorded {
                order_id: "order-before-commitment".into(),
                decision_id: "decision-causal-order".into(),
            },
        )),
        Err(LedgerError::InvalidEvent(
            "order predates its backing commitment".into()
        ))
    );

    ledger
        .append(dated_event(
            "causal-order",
            date,
            5,
            LedgerEventKind::OrderRecorded {
                order_id: "causal-order".into(),
                decision_id: "decision-causal-order".into(),
            },
        ))
        .expect("append causal order");
    let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");

    assert_eq!(
        ledger.append(dated_event(
            "fill-before-order",
            date,
            4,
            LedgerEventKind::FillRecorded {
                fill_id: "fill-before-order".into(),
                order_id: "causal-order".into(),
                filled_usdc: usd(5),
                received_hype_atoms: 1,
            },
        )),
        Err(LedgerError::InvalidEvent(
            "fill predates its owning order or decision".into()
        ))
    );
    assert_eq!(
        ledger.append(dated_event(
            "fee-before-order",
            date,
            4,
            LedgerEventKind::FeeRecorded {
                fee_id: "fee-before-order".into(),
                order_id: "causal-order".into(),
                fee_usdc: usd(1),
            },
        )),
        Err(LedgerError::InvalidEvent(
            "fee predates its owning order or decision".into()
        ))
    );
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged ledger"),
        durable_before
    );
}

#[test]
fn settlement_cannot_predate_recorded_execution_costs() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let date = at(0).date_naive();
    ledger
        .append(daily_outcome(
            "daily-causal-settlement",
            "decision-causal-settlement",
            date,
            "decision",
        ))
        .expect("append decision");
    append_decision_backing(&mut ledger, "decision-causal-settlement", date, 10);
    ledger
        .append(dated_event(
            "causal-settlement-order",
            date,
            5,
            LedgerEventKind::OrderRecorded {
                order_id: "causal-settlement-order".into(),
                decision_id: "decision-causal-settlement".into(),
            },
        ))
        .expect("append causal order");
    let durable_before_order_settlement =
        fs::read(ledger_path(directory.path())).expect("read ledger before settlement");
    assert_eq!(
        ledger.append(dated_event(
            "settlement-before-order",
            date,
            4,
            LedgerEventKind::CapitalSettled {
                commitment_id: "commitment-decision-causal-settlement".into(),
                debited_usdc: UsdcMicros::default(),
            },
        )),
        Err(LedgerError::InvalidEvent(
            "settlement predates its commitment or linked activity".into()
        ))
    );
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged ledger"),
        durable_before_order_settlement
    );

    ledger
        .append(dated_event(
            "causal-settlement-fill",
            date,
            6,
            LedgerEventKind::FillRecorded {
                fill_id: "causal-settlement-fill".into(),
                order_id: "causal-settlement-order".into(),
                filled_usdc: usd(5),
                received_hype_atoms: 1,
            },
        ))
        .expect("append causal fill");
    let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");

    assert_eq!(
        ledger.append(dated_event(
            "settlement-before-fill",
            date,
            5,
            LedgerEventKind::CapitalSettled {
                commitment_id: "commitment-decision-causal-settlement".into(),
                debited_usdc: usd(5),
            },
        )),
        Err(LedgerError::InvalidEvent(
            "settlement predates its commitment or linked activity".into()
        ))
    );
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged ledger"),
        durable_before
    );
}

#[test]
fn stable_fee_ids_are_idempotent_and_cannot_change_ownership() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let date = at(0).date_naive();
    ledger
        .append(daily_outcome(
            "daily-fee-identity",
            "decision-fee-identity",
            date,
            "decision",
        ))
        .expect("append decision");
    append_decision_backing(&mut ledger, "decision-fee-identity", date, 10);
    ledger
        .append(dated_event(
            "order-fee-identity",
            date,
            5,
            LedgerEventKind::OrderRecorded {
                order_id: "order-fee-identity".into(),
                decision_id: "decision-fee-identity".into(),
            },
        ))
        .expect("append order");
    let fee = dated_event(
        "fee-envelope-first",
        date,
        6,
        LedgerEventKind::FeeRecorded {
            fee_id: "venue-fee-stable".into(),
            order_id: "order-fee-identity".into(),
            fee_usdc: usd(1),
        },
    );
    ledger.append(fee.clone()).expect("append venue fee");
    let durable_before_retry = fs::read(ledger_path(directory.path())).expect("read ledger");
    let record_count = ledger.record_count();

    assert_eq!(
        ledger
            .append(LedgerEvent {
                event_id: "fee-envelope-retry".into(),
                ..fee
            })
            .expect("identical fee retry"),
        AppendOutcome::Duplicate
    );
    assert_eq!(ledger.record_count(), record_count);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged retry ledger"),
        durable_before_retry
    );
    assert_eq!(
        ledger.append(dated_event(
            "fee-envelope-conflict",
            date,
            7,
            LedgerEventKind::FeeRecorded {
                fee_id: "venue-fee-stable".into(),
                order_id: "order-fee-identity".into(),
                fee_usdc: usd(2),
            },
        )),
        Err(LedgerError::FeeIdCollision("venue-fee-stable".into()))
    );
    assert_eq!(ledger.record_count(), record_count);
}

#[test]
fn one_daily_decision_or_skip_outcome_is_allowed_per_date() {
    for (first, second) in [
        ("decision", "decision"),
        ("decision", "skip"),
        ("skip", "decision"),
        ("skip", "skip"),
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
        let decision_date = at(0).date_naive();
        ledger
            .append(daily_outcome(
                "daily-first",
                "decision-first",
                decision_date,
                first,
            ))
            .expect("first daily outcome appends");
        let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");

        assert_eq!(
            ledger.append(daily_outcome(
                "daily-second",
                "decision-second",
                decision_date,
                second,
            )),
            Err(LedgerError::DecisionDateCollision(at(0).date_naive())),
            "{first} followed by {second} must fail closed"
        );
        assert_eq!(ledger.record_count(), 1);
        assert_eq!(
            fs::read(ledger_path(directory.path())).expect("read unchanged ledger"),
            durable_before
        );
    }
}

#[test]
fn daily_decision_ids_cannot_be_reused_across_dates() {
    for (first, second) in [
        ("decision", "decision"),
        ("decision", "skip"),
        ("skip", "decision"),
        ("skip", "skip"),
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
        let first_date = at(0).date_naive();
        let second_date = first_date.succ_opt().expect("fixture date has a successor");
        ledger
            .append(daily_outcome(
                "daily-first",
                "decision-reused",
                first_date,
                first,
            ))
            .expect("first daily outcome appends");
        let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");

        assert_eq!(
            ledger.append(daily_outcome(
                "daily-second",
                "decision-reused",
                second_date,
                second,
            )),
            Err(LedgerError::DecisionIdCollision("decision-reused".into())),
            "{first} followed by {second} must reject a reused decision ID"
        );
        assert_eq!(ledger.record_count(), 1);
        assert_eq!(
            fs::read(ledger_path(directory.path())).expect("read unchanged ledger"),
            durable_before
        );
    }
}

#[test]
fn daily_outcome_date_must_match_its_occurrence_date() {
    for outcome in ["decision", "skip"] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
        let declared = at(0).date_naive();
        let occurred = declared.succ_opt().expect("fixture date has a successor");
        let mut event = daily_outcome("mismatched-date", "decision-date", declared, outcome);
        event.occurred_at = occurred
            .and_hms_opt(1, 0, 0)
            .expect("valid mismatched occurrence")
            .and_utc();

        assert_eq!(
            ledger.append(event),
            Err(LedgerError::DecisionDateMismatch { declared, occurred }),
            "{outcome} must use the occurrence UTC date"
        );
        assert_eq!(ledger.record_count(), 0);
    }
}

#[test]
// Intentionally linear: ownership is established once before every linked-event guard.
#[allow(clippy::too_many_lines)]
fn order_linked_events_require_a_unique_owned_purchase_order() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let first_date = at(0).date_naive();
    let purchase_date = first_date.succ_opt().expect("fixture date has a successor");
    let second_purchase_date = purchase_date
        .succ_opt()
        .expect("fixture date has a second successor");

    assert_eq!(
        ledger.append(LedgerEvent {
            event_id: "unknown-decision-order".into(),
            occurred_at: at(1),
            kind: LedgerEventKind::OrderRecorded {
                order_id: "order-unknown-decision".into(),
                decision_id: "decision-missing".into(),
            },
        }),
        Err(LedgerError::UnknownDecision("decision-missing".into()))
    );

    ledger
        .append(daily_outcome(
            "daily-skip",
            "decision-skip",
            first_date,
            "skip",
        ))
        .expect("daily skip appends");
    assert_eq!(
        ledger.append(LedgerEvent {
            event_id: "skip-order".into(),
            occurred_at: first_date
                .and_hms_opt(2, 0, 0)
                .expect("valid order time")
                .and_utc(),
            kind: LedgerEventKind::OrderRecorded {
                order_id: "order-for-skip".into(),
                decision_id: "decision-skip".into(),
            },
        }),
        Err(LedgerError::UnknownDecision("decision-skip".into()))
    );

    ledger
        .append(daily_outcome(
            "daily-purchase",
            "decision-purchase",
            purchase_date,
            "decision",
        ))
        .expect("purchase decision appends");
    assert_eq!(
        ledger.append(dated_event(
            "order-before-backing",
            purchase_date,
            2,
            LedgerEventKind::OrderRecorded {
                order_id: "order-before-backing".into(),
                decision_id: "decision-purchase".into(),
            },
        )),
        Err(LedgerError::InsufficientDecisionBacking(
            "decision-purchase".into()
        ))
    );
    append_decision_backing(&mut ledger, "decision-purchase", purchase_date, 10);
    ledger
        .append(LedgerEvent {
            event_id: "owned-order".into(),
            occurred_at: purchase_date
                .and_hms_opt(5, 0, 0)
                .expect("valid order time")
                .and_utc(),
            kind: LedgerEventKind::OrderRecorded {
                order_id: "order-owned".into(),
                decision_id: "decision-purchase".into(),
            },
        })
        .expect("owned order appends");
    ledger
        .append(daily_outcome(
            "daily-purchase-second",
            "decision-purchase-second",
            second_purchase_date,
            "decision",
        ))
        .expect("second purchase decision appends");
    append_decision_backing(
        &mut ledger,
        "decision-purchase-second",
        second_purchase_date,
        10,
    );
    assert_eq!(
        ledger.append(LedgerEvent {
            event_id: "conflicting-order-owner".into(),
            occurred_at: second_purchase_date
                .and_hms_opt(5, 0, 0)
                .expect("valid order time")
                .and_utc(),
            kind: LedgerEventKind::OrderRecorded {
                order_id: "order-owned".into(),
                decision_id: "decision-purchase-second".into(),
            },
        }),
        Err(LedgerError::OrderIdCollision("order-owned".into()))
    );

    for (event_id, kind) in [
        (
            "unknown-order-fill",
            LedgerEventKind::FillRecorded {
                fill_id: "fill-unknown-order".into(),
                order_id: "order-missing".into(),
                filled_usdc: usd(1),
                received_hype_atoms: 1,
            },
        ),
        (
            "unknown-order-fee",
            LedgerEventKind::FeeRecorded {
                fee_id: "fee-unknown-order".into(),
                order_id: "order-missing".into(),
                fee_usdc: usd(1),
            },
        ),
    ] {
        assert_eq!(
            ledger.append(LedgerEvent {
                event_id: event_id.into(),
                occurred_at: second_purchase_date
                    .and_hms_opt(6, 0, 0)
                    .expect("valid linked-event time")
                    .and_utc(),
                kind,
            }),
            Err(LedgerError::UnknownOrder("order-missing".into()))
        );
    }

    ledger
        .append(LedgerEvent {
            event_id: "owned-order-fill".into(),
            occurred_at: second_purchase_date
                .and_hms_opt(7, 0, 0)
                .expect("valid fill time")
                .and_utc(),
            kind: LedgerEventKind::FillRecorded {
                fill_id: "fill-owned-order".into(),
                order_id: "order-owned".into(),
                filled_usdc: usd(5),
                received_hype_atoms: 2,
            },
        })
        .expect("owned fill appends");
    ledger
        .append(LedgerEvent {
            event_id: "owned-order-fee".into(),
            occurred_at: second_purchase_date
                .and_hms_opt(8, 0, 0)
                .expect("valid fee time")
                .and_utc(),
            kind: LedgerEventKind::FeeRecorded {
                fee_id: "fee-owned-order".into(),
                order_id: "order-owned".into(),
                fee_usdc: usd(1),
            },
        })
        .expect("owned fee appends");
}

#[test]
fn fills_are_capped_across_all_orders_for_one_decision() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let date = at(0).date_naive();
    ledger
        .append(daily_outcome(
            "daily-capped",
            "decision-capped",
            date,
            "decision",
        ))
        .expect("append capped decision");
    append_decision_backing(&mut ledger, "decision-capped", date, 10);
    for (hour, order_id) in [(5, "order-cap-a"), (6, "order-cap-b")] {
        ledger
            .append(dated_event(
                format!("record-{order_id}"),
                date,
                hour,
                LedgerEventKind::OrderRecorded {
                    order_id: order_id.into(),
                    decision_id: "decision-capped".into(),
                },
            ))
            .expect("append decision order");
    }
    for (hour, event_id, order_id, amount) in [
        (7, "fill-cap-a", "order-cap-a", 6),
        (8, "fill-cap-b", "order-cap-b", 4),
    ] {
        ledger
            .append(dated_event(
                event_id,
                date,
                hour,
                LedgerEventKind::FillRecorded {
                    fill_id: event_id.into(),
                    order_id: order_id.into(),
                    filled_usdc: usd(amount),
                    received_hype_atoms: 1,
                },
            ))
            .expect("append fill within decision plan");
    }
    let durable_before = fs::read(ledger_path(directory.path())).expect("read capped ledger");
    assert_eq!(
        ledger.append(dated_event(
            "fill-over-plan",
            date,
            9,
            LedgerEventKind::FillRecorded {
                fill_id: "fill-over-plan".into(),
                order_id: "order-cap-b".into(),
                filled_usdc: usd(1),
                received_hype_atoms: 1,
            },
        )),
        Err(LedgerError::FillExceedsDecisionPlan(
            "decision-capped".into()
        ))
    );
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged capped ledger"),
        durable_before
    );
    assert_eq!(
        open(directory.path(), &anchor)
            .expect("replay capped ledger")
            .state(),
        ledger.state()
    );
}

#[test]
fn fills_and_fees_share_the_decision_commitment_cap() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let date = at(0).date_naive();
    ledger
        .append(dated_event(
            "daily-cost-cap",
            date,
            1,
            LedgerEventKind::DailyDecision {
                decision_id: "decision-cost-cap".into(),
                decision_date: date,
                commitment_id: "commitment-decision-cost-cap".into(),
                planned_usdc: usd(10),
                committed_usdc: usd(12),
            },
        ))
        .expect("append cost-capped decision");
    append_decision_backing(&mut ledger, "decision-cost-cap", date, 12);
    ledger
        .append(dated_event(
            "order-cost-cap",
            date,
            5,
            LedgerEventKind::OrderRecorded {
                order_id: "order-cost-cap".into(),
                decision_id: "decision-cost-cap".into(),
            },
        ))
        .expect("append backed order");
    ledger
        .append(dated_event(
            "fill-cost-cap",
            date,
            6,
            LedgerEventKind::FillRecorded {
                fill_id: "fill-cost-cap".into(),
                order_id: "order-cost-cap".into(),
                filled_usdc: usd(10),
                received_hype_atoms: 1,
            },
        ))
        .expect("append planned fill");
    ledger
        .append(dated_event(
            "fee-cost-cap",
            date,
            7,
            LedgerEventKind::FeeRecorded {
                fee_id: "fee-cost-cap".into(),
                order_id: "order-cost-cap".into(),
                fee_usdc: usd(2),
            },
        ))
        .expect("append fee within commitment");
    assert_eq!(
        ledger.append(dated_event(
            "fee-over-cost-cap",
            date,
            8,
            LedgerEventKind::FeeRecorded {
                fee_id: "fee-over-cost-cap".into(),
                order_id: "order-cost-cap".into(),
                fee_usdc: usd(1),
            },
        )),
        Err(LedgerError::DecisionCostsExceedCommitment(
            "decision-cost-cap".into()
        ))
    );
}

#[test]
fn fills_and_fees_are_rejected_after_backing_settles() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let date = at(0).date_naive();
    ledger
        .append(daily_outcome(
            "daily-settled-costs",
            "decision-settled-costs",
            date,
            "decision",
        ))
        .expect("append decision");
    append_decision_backing(&mut ledger, "decision-settled-costs", date, 10);
    ledger
        .append(dated_event(
            "order-settled-costs",
            date,
            5,
            LedgerEventKind::OrderRecorded {
                order_id: "order-settled-costs".into(),
                decision_id: "decision-settled-costs".into(),
            },
        ))
        .expect("append backed order");
    ledger
        .append(dated_event(
            "settle-before-costs",
            date,
            6,
            LedgerEventKind::CapitalSettled {
                commitment_id: "commitment-decision-settled-costs".into(),
                debited_usdc: usd(0),
            },
        ))
        .expect("settle backing");
    for (event_id, kind) in [
        (
            "fill-after-settle",
            LedgerEventKind::FillRecorded {
                fill_id: "fill-after-settle".into(),
                order_id: "order-settled-costs".into(),
                filled_usdc: usd(1),
                received_hype_atoms: 1,
            },
        ),
        (
            "fee-after-settle",
            LedgerEventKind::FeeRecorded {
                fee_id: "fee-after-settle".into(),
                order_id: "order-settled-costs".into(),
                fee_usdc: usd(1),
            },
        ),
    ] {
        assert_eq!(
            ledger.append(dated_event(event_id, date, 10, kind)),
            Err(LedgerError::InsufficientDecisionBacking(
                "decision-settled-costs".into()
            ))
        );
    }
}

#[test]
fn purchase_order_requires_sufficient_unsettled_backing() {
    for (case, backing, settle) in [("insufficient", 9, false), ("settled", 10, true)] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
        let date = at(0).date_naive();
        let decision_id = format!("decision-{case}");
        ledger
            .append(daily_outcome(
                &format!("daily-{case}"),
                &decision_id,
                date,
                "decision",
            ))
            .expect("append decision");
        append_decision_backing(&mut ledger, &decision_id, date, backing);
        if settle {
            ledger
                .append(dated_event(
                    "settle-before-order",
                    date,
                    5,
                    LedgerEventKind::CapitalSettled {
                        commitment_id: format!("commitment-{decision_id}"),
                        debited_usdc: usd(10),
                    },
                ))
                .expect("settle backing before order");
        }

        assert_eq!(
            ledger.append(dated_event(
                format!("order-{case}"),
                date,
                6,
                LedgerEventKind::OrderRecorded {
                    order_id: format!("order-{case}"),
                    decision_id: decision_id.clone(),
                },
            )),
            Err(LedgerError::InsufficientDecisionBacking(decision_id))
        );
    }
}

#[test]
fn open_durably_creates_a_missing_nested_ledger_directory() {
    let container = tempfile::tempdir().expect("ledger container");
    let directory = container.path().join("first/second/ledger");
    let anchor = anchor_store();

    let mut ledger = open(&directory, &anchor).expect("open missing nested ledger directory");
    ledger
        .append(deposit("deposit-after-directory-create", 1, 100))
        .expect("append after directory creation");
    drop(ledger);

    assert!(directory.is_dir());
    assert_eq!(
        open(&directory, &anchor)
            .expect("reopen durably created ledger")
            .record_count(),
        1
    );
}

#[test]
fn duplicate_retry_verifies_that_the_event_is_still_durable() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    let durable_event = deposit("deposit-durable-retry", 1, 100);
    ledger
        .append(durable_event.clone())
        .expect("append deposit");
    fs::remove_file(ledger_path(directory.path())).expect("delete journal fixture");

    assert!(matches!(
        ledger.append(durable_event),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn concurrent_writers_serialize_and_rebase_from_the_durable_head() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let first = open(directory.path(), &anchor).expect("open first writer");
    let second = open(directory.path(), &anchor).expect("open second writer");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let first_barrier = std::sync::Arc::clone(&barrier);
    let second_barrier = std::sync::Arc::clone(&barrier);

    let first_thread = std::thread::spawn(move || {
        let mut first = first;
        first_barrier.wait();
        first.append(deposit("deposit-concurrent-a", 1, 100))
    });
    let second_thread = std::thread::spawn(move || {
        let mut second = second;
        second_barrier.wait();
        second.append(deposit("deposit-concurrent-b", 1, 100))
    });
    let outcomes = [
        first_thread.join().expect("first writer joined"),
        second_thread.join().expect("second writer joined"),
    ];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(AppendOutcome::Appended)))
            .count(),
        2
    );
    assert_eq!(
        open(directory.path(), &anchor)
            .expect("journal remains replayable")
            .record_count(),
        2
    );
}

#[test]
fn only_admitted_authoritative_deposits_increase_deployable_capital() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");

    ledger
        .append(observed("balance-before", 0, 9_999, 70_000_000))
        .expect("append balance");
    assert_eq!(ledger.state().deployable_usdc(), usd(0));

    ledger
        .append(deposit("deposit-authoritative", 1, 100))
        .expect("append authoritative deposit");
    assert_eq!(ledger.state().deployable_usdc(), usd(0));

    ledger
        .append(event(
            "deposit-admission",
            2,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: "deposit-authoritative".into(),
                amount_usdc: usd(60),
            },
        ))
        .expect("append admission");
    assert_eq!(ledger.state().admitted_usdc(), usd(60));
    assert_eq!(ledger.state().deployable_usdc(), usd(60));

    ledger
        .append(observed("balance-after", 3, 50_000, 80_000_000))
        .expect("append later balance");
    assert_eq!(ledger.state().observed_usdc(), usd(50_000));
    assert_eq!(ledger.state().observed_hype_atoms(), 80_000_000);
    assert_eq!(ledger.state().deployable_usdc(), usd(60));

    ledger
        .append(event(
            "capital-commit",
            4,
            LedgerEventKind::CapitalCommitted {
                commitment_id: "commitment-1".into(),
                amount_usdc: usd(20),
            },
        ))
        .expect("append commitment");
    assert_eq!(ledger.state().deployable_usdc(), usd(40));

    ledger
        .append(event(
            "capital-settlement",
            5,
            LedgerEventKind::CapitalSettled {
                commitment_id: "commitment-1".into(),
                debited_usdc: usd(15),
            },
        ))
        .expect("append settlement");
    assert_eq!(ledger.state().committed_usdc(), usd(0));
    assert_eq!(ledger.state().spent_usdc(), usd(15));
    assert_eq!(ledger.state().deployable_usdc(), usd(45));

    ledger
        .append(event(
            "withdrawal-1",
            6,
            LedgerEventKind::AuthoritativeWithdrawal {
                amount_usdc: usd(5),
            },
        ))
        .expect("append withdrawal");
    assert_eq!(ledger.state().withdrawn_usdc(), usd(5));
    assert_eq!(ledger.state().deployable_usdc(), usd(40));

    let replayed = open(directory.path(), &anchor).expect("replay ledger");
    assert_eq!(replayed.state(), ledger.state());
}

#[test]
fn rejected_capital_transition_does_not_mutate_memory_or_disk() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-authoritative", 1, 10))
        .expect("append deposit");
    ledger
        .append(event(
            "deposit-admission",
            2,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: "deposit-authoritative".into(),
                amount_usdc: usd(10),
            },
        ))
        .expect("append admission");
    let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");
    let state_before = ledger.state().clone();

    assert_eq!(
        ledger.append(event(
            "oversized-commitment",
            3,
            LedgerEventKind::CapitalCommitted {
                commitment_id: "commitment-too-large".into(),
                amount_usdc: usd(11),
            },
        )),
        Err(LedgerError::InsufficientDeployableCapital)
    );
    assert_eq!(ledger.state(), &state_before);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read ledger"),
        durable_before
    );
}

#[test]
fn capital_commitment_cannot_use_a_future_admission() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("future-deposit", 10, 10))
        .expect("append authoritative deposit");
    assert_eq!(
        ledger.append(event(
            "admission-before-deposit",
            9,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: "future-deposit".into(),
                amount_usdc: usd(10),
            },
        )),
        Err(LedgerError::InvalidEvent(
            "deposit admission predates its authoritative deposit".into()
        ))
    );
    ledger
        .append(event(
            "future-admission",
            11,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: "future-deposit".into(),
                amount_usdc: usd(10),
            },
        ))
        .expect("append future admission");
    let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");
    let state_before = ledger.state().clone();

    assert_eq!(
        ledger.append(event(
            "commitment-before-admission",
            9,
            LedgerEventKind::CapitalCommitted {
                commitment_id: "commitment-before-admission".into(),
                amount_usdc: usd(10),
            },
        )),
        Err(LedgerError::InsufficientDeployableCapital)
    );
    assert_eq!(ledger.state(), &state_before);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read unchanged ledger"),
        durable_before
    );
}

#[test]
fn journal_hash_tampering_is_detected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-source", 1, 100))
        .expect("append deposit");
    ledger
        .append(observed("balance-source", 2, 100, 1))
        .expect("append balance");
    drop(ledger);

    let path = ledger_path(directory.path());
    let original = fs::read_to_string(&path).expect("read ledger");
    let tampered = original.replacen("deposit-source", "deposit-tamper", 1);
    assert_eq!(tampered.len(), original.len());
    assert_ne!(tampered, original);
    fs::write(path, tampered).expect("tamper fixture");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::CorruptLedger(_))
    ));
}

#[test]
fn unknown_unhashed_journal_field_is_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-unknown-field", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let path = ledger_path(directory.path());
    let original = fs::read_to_string(&path).expect("read ledger");
    let tampered = original.replacen(
        ",\"record_hash\"",
        ",\"unhashed_field\":\"injected\",\"record_hash\"",
        1,
    );
    assert_ne!(tampered, original);
    fs::write(path, tampered).expect("tamper fixture");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::Json(_))
    ));
}

#[test]
fn blank_journal_records_are_rejected() {
    for placement in ["between", "tail"] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
        ledger
            .append(deposit("deposit-blank-line", 1, 100))
            .expect("append deposit");
        ledger
            .append(observed("balance-blank-line", 2, 100, 1))
            .expect("append balance");
        drop(ledger);

        let path = ledger_path(directory.path());
        let mut payload = fs::read(&path).expect("read ledger");
        match placement {
            "between" => {
                let first_terminator = payload
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .expect("first record terminator");
                payload.insert(first_terminator + 1, b'\n');
            }
            "tail" => payload.push(b'\n'),
            _ => unreachable!("complete placement fixture"),
        }
        fs::write(path, payload).expect("insert blank record");

        assert!(matches!(
            open(directory.path(), &anchor),
            Err(LedgerError::CorruptLedger(_))
        ));
    }
}

#[test]
fn partial_final_record_is_reported_as_truncation() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-partial", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let path = ledger_path(directory.path());
    let mut payload = fs::read(&path).expect("read ledger");
    assert_eq!(payload.pop(), Some(b'\n'));
    fs::write(path, payload).expect("truncate fixture");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn checkpoint_anchor_detects_complete_tail_loss() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-tail", 1, 100))
        .expect("append deposit");
    ledger.checkpoint().expect("write earlier checkpoint");
    ledger
        .append(observed("balance-tail", 2, 100, 1))
        .expect("append and advance latest-head snapshot");
    drop(ledger);

    let path = ledger_path(directory.path());
    let payload = fs::read(&path).expect("read ledger");
    let first_line = payload
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .expect("first complete record");
    fs::write(path, &payload[..first_line]).expect("remove complete tail record");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn protected_anchor_rejects_a_matching_local_journal_and_snapshot_rollback() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-rollback", 1, 100))
        .expect("append deposit");
    let old_journal = fs::read(ledger_path(directory.path())).expect("read old journal");
    let old_snapshot = fs::read(snapshot_path(directory.path())).expect("read old snapshot");
    ledger
        .append(observed("balance-after-rollback", 2, 100, 1))
        .expect("append newer state");
    drop(ledger);

    fs::write(ledger_path(directory.path()), old_journal).expect("roll back local journal");
    fs::write(snapshot_path(directory.path()), old_snapshot).expect("roll back local snapshot");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn protected_anchor_rejects_an_old_anchor_reappearing() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-anchor-rollback", 1, 100))
        .expect("append deposit");
    let old_anchor = anchor.load().expect("load old anchor");
    ledger
        .append(observed("balance-after-anchor-rollback", 2, 100, 1))
        .expect("append newer state");
    drop(ledger);

    anchor.replace_for_test(old_anchor);
    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::ProtectedAnchorMismatch)
    ));
}

#[test]
fn nonempty_ledger_requires_its_protected_anchor_scope() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-missing-anchor", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let unrelated_empty_scope = anchor_store();
    assert!(matches!(
        open(directory.path(), &unrelated_empty_scope),
        Err(LedgerError::MissingProtectedAnchor)
    ));
}

#[test]
fn snapshot_checksum_tampering_and_empty_snapshot_are_detected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-snapshot", 1, 100))
        .expect("append deposit");
    ledger.checkpoint().expect("write checkpoint");
    drop(ledger);

    let path = snapshot_path(directory.path());
    let mut document: Value =
        serde_json::from_slice(&fs::read(&path).expect("read snapshot")).expect("valid JSON");
    let checksum = document["checksum"]
        .as_str()
        .expect("checksum string")
        .to_owned();
    let first = if checksum.starts_with('0') { '1' } else { '0' };
    document["checksum"] = Value::String(format!("{first}{}", &checksum[1..]));
    fs::write(
        &path,
        serde_json::to_vec(&document).expect("encode fixture"),
    )
    .expect("tamper fixture");
    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::CorruptSnapshot)
    ));

    fs::write(path, []).expect("empty snapshot fixture");
    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::CorruptSnapshot)
    ));
}

#[test]
fn clean_directory_restore_round_trips_exact_checkpoint() {
    let source = tempfile::tempdir().expect("source directory");
    let container = tempfile::tempdir().expect("destination container");
    let destination = container.path().join("restored");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-restore", 1, 100))
        .expect("append deposit");
    ledger
        .append(event(
            "admission-restore",
            2,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: "deposit-restore".into(),
                amount_usdc: usd(40),
            },
        ))
        .expect("append admission");
    let expected_state = ledger.state().clone();
    let expected_head = ledger.head_hash().to_owned();
    ledger.checkpoint().expect("write checkpoint");
    drop(ledger);

    let restored = restore(
        source.path(),
        &destination,
        &source_anchor,
        &destination_anchor,
    )
    .expect("restore");
    assert_eq!(restored.state(), &expected_state);
    assert_eq!(restored.head_hash(), expected_head);
    assert_eq!(restored.record_count(), 2);
    let file_names = fs::read_dir(&destination)
        .expect("read restored directory")
        .map(|entry| {
            entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(file_names.len(), 3);
    assert!(file_names.contains(LEDGER_FILE_NAME));
    assert!(file_names.contains(SNAPSHOT_FILE_NAME));
    assert!(file_names.contains(".ledger.lock"));
    assert_eq!(
        open(&destination, &destination_anchor)
            .expect("reopen restored ledger")
            .state(),
        &expected_state
    );
}

#[test]
fn restore_reconstructs_missing_files_when_protected_anchor_matches() {
    let source = tempfile::tempdir().expect("source directory");
    let destination = tempfile::tempdir().expect("destination directory");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
    ledger
        .append(deposit("deposit-before-local-loss", 1, 100))
        .expect("append deposit");
    let expected_state = ledger.state().clone();
    let expected_head = ledger.head_hash().to_owned();
    drop(ledger);

    destination_anchor.replace_for_test(
        source_anchor
            .load()
            .expect("load source anchor accepted by destination"),
    );

    let restored = restore(
        source.path(),
        destination.path(),
        &source_anchor,
        &destination_anchor,
    )
    .expect("matching protected anchor permits reconstruction");
    assert_eq!(restored.state(), &expected_state);
    assert_eq!(restored.head_hash(), expected_head);
}

#[test]
fn restore_accepts_initialized_empty_journal_when_protected_anchor_matches() {
    let source = tempfile::tempdir().expect("source directory");
    let destination = tempfile::tempdir().expect("destination directory");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
    ledger
        .append(deposit("deposit-before-initialized-loss", 1, 100))
        .expect("append deposit");
    let expected_state = ledger.state().clone();
    drop(ledger);

    destination_anchor.replace_for_test(
        source_anchor
            .load()
            .expect("load source anchor accepted by destination"),
    );
    assert!(matches!(
        open(destination.path(), &destination_anchor),
        Err(LedgerError::TruncatedLedger)
    ));
    assert_eq!(
        fs::metadata(ledger_path(destination.path()))
            .expect("initialized empty journal")
            .len(),
        0
    );
    assert!(!snapshot_path(destination.path()).exists());

    let restored = restore(
        source.path(),
        destination.path(),
        &source_anchor,
        &destination_anchor,
    )
    .expect("matching protected anchor permits initialized recovery");
    assert_eq!(restored.state(), &expected_state);
}

#[test]
fn restore_rejects_missing_files_when_protected_anchor_differs() {
    let source = tempfile::tempdir().expect("source directory");
    let destination = tempfile::tempdir().expect("destination directory");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
    ledger
        .append(deposit("deposit-before-anchor-mismatch", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let mut mismatched_anchor = source_anchor
        .load()
        .expect("load source anchor")
        .expect("nonempty source anchor");
    mismatched_anchor.head_hash = "0".repeat(64);
    destination_anchor.replace_for_test(Some(mismatched_anchor));

    assert!(matches!(
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
}

#[test]
fn restore_preserves_unverified_atomic_temporary_and_fails_closed() {
    let source = tempfile::tempdir().expect("source directory");
    let container = tempfile::tempdir().expect("destination container");
    let destination = container.path().join("restored");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
    ledger
        .append(deposit("deposit-before-orphan", 1, 100))
        .expect("append deposit");
    drop(ledger);

    fs::create_dir_all(&destination).expect("create destination");
    let orphan = destination.join(".pending-restore.json.123.456.tmp");
    fs::write(&orphan, b"partial restore intent").expect("write orphaned intent temporary");

    assert!(matches!(
        restore(
            source.path(),
            &destination,
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
    assert_eq!(
        fs::read(&orphan).expect("unverified temporary remains"),
        b"partial restore intent"
    );
}

#[test]
fn restore_preserves_lookalike_temporary_and_fails_closed() {
    let source = tempfile::tempdir().expect("source directory");
    let destination = tempfile::tempdir().expect("destination directory");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
    ledger
        .append(deposit("deposit-before-lookalike", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let lookalike = destination.path().join("snapshot.json.pid.456.tmp");
    fs::write(&lookalike, b"foreign data").expect("write lookalike temporary");

    assert!(matches!(
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
    assert_eq!(
        fs::read(&lookalike).expect("lookalike remains"),
        b"foreign data"
    );
}

#[cfg(unix)]
#[test]
fn restore_preserves_symlink_with_atomic_temporary_name_and_fails_closed() {
    use std::os::unix::fs::symlink;

    let source = tempfile::tempdir().expect("source directory");
    let container = tempfile::tempdir().expect("destination container");
    let destination = container.path().join("restored");
    let target = container.path().join("foreign-target");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
    ledger
        .append(deposit("deposit-before-symlink", 1, 100))
        .expect("append deposit");
    drop(ledger);

    fs::create_dir_all(&destination).expect("create destination");
    fs::write(&target, b"foreign data").expect("write symlink target");
    let link = destination.join("snapshot.json.123.456.tmp");
    symlink(&target, &link).expect("create temporary-name symlink");

    assert!(matches!(
        restore(
            source.path(),
            &destination,
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
    assert!(fs::symlink_metadata(&link)
        .expect("symlink remains")
        .file_type()
        .is_symlink());
    assert_eq!(
        fs::read(&target).expect("symlink target remains"),
        b"foreign data"
    );
}

#[cfg(unix)]
#[test]
fn restore_rejects_a_source_directory_alias() {
    use std::os::unix::fs::symlink;

    let source = tempfile::tempdir().expect("source directory");
    let container = tempfile::tempdir().expect("alias container");
    let alias = container.path().join("source-alias");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-alias", 1, 100))
        .expect("append deposit");
    drop(ledger);
    symlink(source.path(), &alias).expect("create source alias");

    assert!(matches!(
        restore(source.path(), &alias, &source_anchor, &destination_anchor,),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
}

#[test]
fn opposing_restores_acquire_directory_locks_in_the_same_order() {
    use std::{sync::Barrier, time::Duration};

    let left = tempfile::tempdir().expect("left directory");
    let right = tempfile::tempdir().expect("right directory");
    let left_path = left.path().to_path_buf();
    let right_path = right.path().to_path_buf();
    let left_anchor = anchor_store();
    let right_anchor = anchor_store();
    let mut left_ledger = open(&left_path, &left_anchor).expect("open left ledger");
    left_ledger
        .append(deposit("deposit-left", 1, 100))
        .expect("append left deposit");
    let mut right_ledger = open(&right_path, &right_anchor).expect("open right ledger");
    right_ledger
        .append(deposit("deposit-right", 1, 100))
        .expect("append right deposit");
    drop((left_ledger, right_ledger));

    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = std::sync::mpsc::channel();
    let left_to_right = {
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        let left_path = left_path.clone();
        let right_path = right_path.clone();
        let left_anchor = Arc::clone(&left_anchor);
        let right_anchor = Arc::clone(&right_anchor);
        std::thread::spawn(move || {
            barrier.wait();
            sender
                .send(restore(
                    &left_path,
                    &right_path,
                    &left_anchor,
                    &right_anchor,
                ))
                .expect("send left-to-right result");
        })
    };
    let right_to_left = {
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        let left_anchor = Arc::clone(&left_anchor);
        let right_anchor = Arc::clone(&right_anchor);
        std::thread::spawn(move || {
            barrier.wait();
            sender
                .send(restore(
                    &right_path,
                    &left_path,
                    &right_anchor,
                    &left_anchor,
                ))
                .expect("send right-to-left result");
        })
    };
    drop(sender);

    for _ in 0..2 {
        assert!(matches!(
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("opposing restore must not deadlock"),
            Err(LedgerError::RestoreDestinationNotEmpty)
        ));
    }
    left_to_right.join().expect("left-to-right thread");
    right_to_left.join().expect("right-to-left thread");
}

#[cfg(unix)]
#[test]
fn opening_a_symlinked_journal_fails_without_modifying_its_target() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("ledger directory");
    let foreign_directory = tempfile::tempdir().expect("foreign directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-symlink", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let journal = ledger_path(directory.path());
    let foreign_journal = foreign_directory.path().join("foreign-ledger.jsonl");
    let expected = fs::read(&journal).expect("read valid journal");
    fs::write(&foreign_journal, &expected).expect("write matching foreign journal");
    fs::remove_file(&journal).expect("remove local journal");
    symlink(&foreign_journal, &journal).expect("replace journal with symlink");

    assert!(open(directory.path(), &anchor).is_err());
    assert_eq!(
        fs::read(&foreign_journal).expect("foreign journal remains"),
        expected
    );
}

#[cfg(any(unix, windows))]
#[test]
fn appending_rejects_a_multiply_linked_journal() {
    let directory = tempfile::tempdir().expect("ledger directory");
    let foreign_directory = tempfile::tempdir().expect("foreign directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-hard-link", 1, 100))
        .expect("append deposit");

    let journal = ledger_path(directory.path());
    let foreign_journal = foreign_directory.path().join("foreign-ledger.jsonl");
    fs::hard_link(&journal, &foreign_journal).expect("hard link journal");
    let expected = fs::read(&foreign_journal).expect("read linked journal");

    assert!(matches!(
        ledger.append(observed("observation-after-hard-link", 2, 100, 1)),
        Err(LedgerError::UnsafeJournalFile)
    ));
    assert_eq!(
        fs::read(&foreign_journal).expect("linked journal remains"),
        expected
    );
}

#[cfg(unix)]
#[test]
fn opening_a_symlinked_lock_file_fails_without_modifying_its_target() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("ledger directory");
    let foreign_directory = tempfile::tempdir().expect("foreign directory");
    let foreign_lock = foreign_directory.path().join("foreign.lock");
    fs::write(&foreign_lock, b"foreign lock contents").expect("write foreign lock");
    symlink(&foreign_lock, directory.path().join(".ledger.lock")).expect("symlink lock");

    assert!(open(directory.path(), &anchor_store()).is_err());
    assert_eq!(
        fs::read(&foreign_lock).expect("foreign lock remains"),
        b"foreign lock contents"
    );
}

#[cfg(any(unix, windows))]
#[test]
fn opening_rejects_a_multiply_linked_lock() {
    let directory = tempfile::tempdir().expect("ledger directory");
    let foreign_directory = tempfile::tempdir().expect("foreign directory");
    let foreign_lock = foreign_directory.path().join("foreign.lock");
    fs::write(&foreign_lock, b"foreign lock contents").expect("write foreign lock");
    fs::hard_link(&foreign_lock, directory.path().join(".ledger.lock")).expect("hard-link lock");

    assert!(matches!(
        open(directory.path(), &anchor_store()),
        Err(LedgerError::UnsafeLockFile)
    ));
    assert_eq!(
        fs::read(&foreign_lock).expect("foreign lock remains"),
        b"foreign lock contents"
    );
}

#[cfg(any(unix, windows))]
#[test]
fn restore_rejects_shared_hard_linked_locks_without_blocking() {
    let source = tempfile::tempdir().expect("source directory");
    let destination = tempfile::tempdir().expect("destination directory");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
    ledger
        .append(deposit("deposit-before-shared-lock", 1, 100))
        .expect("append deposit");
    ledger.checkpoint().expect("checkpoint source");
    drop(ledger);

    fs::hard_link(
        source.path().join(".ledger.lock"),
        destination.path().join(".ledger.lock"),
    )
    .expect("share lock inode");

    assert!(matches!(
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::UnsafeLockFile)
    ));
}

#[test]
fn protected_anchor_rejects_complete_local_ledger_and_snapshot_loss() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-paired-loss", 1, 100))
        .expect("append deposit");
    drop(ledger);

    fs::remove_file(directory.path().join(LEDGER_FILE_NAME)).expect("remove ledger fixture");
    fs::remove_file(directory.path().join(SNAPSHOT_FILE_NAME)).expect("remove snapshot fixture");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn restore_rejects_stale_or_missing_snapshot_and_nonempty_destination() {
    let source = tempfile::tempdir().expect("source directory");
    let destination = tempfile::tempdir().expect("destination directory");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-restore-guard", 1, 100))
        .expect("append deposit");

    let first_snapshot = fs::read(snapshot_path(source.path())).expect("read exact snapshot");
    fs::remove_file(snapshot_path(source.path())).expect("remove snapshot fixture");

    assert!(matches!(
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::MissingSnapshot)
    ));

    fs::write(snapshot_path(source.path()), &first_snapshot)
        .expect("restore exact snapshot fixture");
    ledger
        .append(observed("balance-after-checkpoint", 2, 100, 1))
        .expect("append event after checkpoint");
    let current_snapshot = fs::read(snapshot_path(source.path())).expect("read current snapshot");
    fs::write(snapshot_path(source.path()), first_snapshot)
        .expect("restore stale snapshot fixture");
    assert!(matches!(
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::StaleSnapshot)
    ));
    assert!(matches!(
        open(source.path(), &source_anchor),
        Err(LedgerError::StaleSnapshot)
    ));

    fs::write(snapshot_path(source.path()), current_snapshot).expect("restore current snapshot");
    fs::write(destination.path().join("occupied"), b"data").expect("occupy destination");
    assert!(matches!(
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
}
