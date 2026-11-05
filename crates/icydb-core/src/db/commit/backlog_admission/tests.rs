use super::*;
use crate::{
    db::{
        commit::CommitMarker,
        data::{DecodedDataStoreKey, RawDataStoreKey},
        journal::{
            DatabaseCommitSequence, JournalBatch, JournalRecord, JournalSequence, JournalTailStore,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
    },
    testing::test_memory,
    types::EntityTag,
};

const MARKER_ID: [u8; 16] = [0xA1; 16];

#[cfg(feature = "metrics")]
#[test]
fn public_debt_and_movements_follow_publication_replay_rejection_and_retirement() {
    use super::super::store::{
        apply_prepared_commit_control_replacement, prepare_commit_control_replacement,
    };
    use crate::db::{
        DatabaseIncarnationId,
        commit::{commit_memory_handle, select_commit_memory_allocation},
        journal::FoldWatermark,
        registry::StoreAllocationIdentities,
    };

    thread_local! {
        static TAIL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(test_memory(225)).expect("fixture storage should initialize"));
    }
    let identities = StoreAllocationIdentities::new_journaled(
        StoreAllocationIdentity::new(222, "icydb.test.metrics.data.v1"),
        StoreAllocationIdentity::new(223, "icydb.test.metrics.index.v1"),
        StoreAllocationIdentity::new(224, "icydb.test.metrics.schema.v1"),
        StoreAllocationIdentity::new(225, "icydb.test.metrics.journal.v1"),
    );
    select_commit_memory_allocation(231, "icydb.core_tests.slot_231.v1");
    let owner = current_commit_memory_allocation().unwrap();
    let memory = commit_memory_handle(owner).unwrap();
    crate::db::database_format::initialize_current_database_control_for_tests(&memory);
    let replacement = prepare_commit_control_replacement(
        memory,
        DatabaseIncarnationId::for_tests(0x71),
        [0x72; 32],
        0,
        &[super::super::PersistedStoreAllocation::active(identities).unwrap()],
    )
    .unwrap();
    apply_prepared_commit_control_replacement(replacement);
    RUNTIME_JOURNAL_TAILS
        .with_borrow_mut(|tails| tails.push((owner, identities.journal().unwrap(), &TAIL)));
    TAIL.with_borrow_mut(|tail| tail.initialize_current_tail_control().unwrap());
    crate::metrics::metrics_reset_all();
    let first = batch(1, vec![row_record(1), row_record(2)]);
    TAIL.with_borrow_mut(|tail| tail.append_batch(&first).unwrap());
    let report = crate::metrics::metrics_report().unwrap();
    assert_eq!(report.journal_debt(), report.convergence().appended());
    assert_eq!(report.journal_debt().batch_count(), 1);
    assert_eq!(report.journal_debt().record_count(), 2);
    assert_eq!(
        report.convergence().retired(),
        ExactBacklogMeasurement::EMPTY
    );
    // An identical replay and a conflicting retry publish no additional debt.
    TAIL.with_borrow_mut(|tail| tail.append_batch(&first).unwrap());
    TAIL.with_borrow_mut(|tail| {
        assert!(tail.append_batch(&batch(2, vec![row_record(3)])).is_err());
    });
    let retried = crate::metrics::metrics_report().unwrap();
    assert_eq!(retried.journal_debt(), report.journal_debt());
    assert_eq!(retried.convergence(), report.convergence());
    TAIL.with_borrow_mut(|tail| {
        let retirement = tail
            .prepare_batch_retirement(&first, FoldWatermark::new(JournalSequence::new(1), 1))
            .unwrap();
        tail.apply_prepared_batch_retirement(retirement).unwrap();
    });
    let retired = crate::metrics::metrics_report().unwrap();
    assert_eq!(retired.journal_debt(), ExactBacklogMeasurement::EMPTY);
    assert_eq!(retired.convergence().retired(), report.journal_debt());
    assert_eq!(
        retired.convergence().appended(),
        report.convergence().appended()
    );
    RUNTIME_JOURNAL_TAILS.with_borrow_mut(Vec::clear);
    assert!(
        crate::metrics::metrics_report().is_err(),
        "missing live owner must not report zero"
    );
}

fn row_key(value: u64) -> RawDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(1),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
    )
    .to_raw()
    .expect("test row key should encode")
}

fn row_record(value: u64) -> JournalRecord {
    JournalRecord::row_put("tests::Backlog", row_key(value), Vec::new(), [0xB2; 16])
        .expect("test row record should admit")
}

fn batch(ordinal: u8, records: Vec<JournalRecord>) -> JournalBatch {
    JournalBatch::new_with_database_commit_sequence(
        [ordinal; 16],
        MARKER_ID,
        JournalSequence::new(1),
        DatabaseCommitSequence::new(1),
        records,
    )
    .expect("test backlog batch should admit")
}

fn proposal(batches: Vec<JournalBatch>) -> PreparedBacklogProposal {
    PreparedBacklogProposal::from_marker(
        CommitMarker::from_parts(MARKER_ID, batches).expect("test marker should admit"),
    )
    .expect("test proposal should prepare")
}

#[test]
fn exact_prepared_bytes_and_tail_controls_share_one_measurement() {
    let first = batch(1, vec![row_record(1)]);
    let second = batch(2, vec![row_record(2), row_record(3)]);
    let prepared = proposal(vec![first.clone(), second.clone()]);
    let exact_bytes = prepared
        .exact_batches()
        .map(|(batch, bytes)| {
            assert_eq!(decode_journal_batch(bytes).unwrap(), *batch);
            bytes.len() as u64
        })
        .sum::<u64>();
    assert_eq!(prepared.contribution().batch_count(), 2);
    assert_eq!(prepared.contribution().record_count(), 3);
    assert_eq!(prepared.contribution().encoded_batch_bytes(), exact_bytes);

    let mut first_tail =
        JournalTailStore::init(test_memory(248)).expect("fixture storage should initialize");
    first_tail.initialize_current_tail_control().unwrap();
    first_tail.append_batch(&first).unwrap();
    let mut second_tail =
        JournalTailStore::init(test_memory(249)).expect("fixture storage should initialize");
    second_tail.initialize_current_tail_control().unwrap();
    second_tail.append_batch(&second).unwrap();
    let current = ExactBacklogMeasurement::from_tail_controls(&[
        first_tail.current_tail_control().unwrap(),
        second_tail.current_tail_control().unwrap(),
    ])
    .unwrap();
    assert_eq!(current, prepared.contribution());
}

#[test]
fn every_pressure_dimension_is_typed_and_drain_restores_admission() {
    let proposed = ExactBacklogMeasurement::new(1, 2, 3);
    let cases = [
        (
            ExactBacklogMeasurement::new(1, 0, 0),
            BacklogLimits::new(1, u64::MAX, u64::MAX),
            BacklogPressureDimension::BatchCount,
        ),
        (
            ExactBacklogMeasurement::new(0, 1, 0),
            BacklogLimits::new(u64::MAX, 2, u64::MAX),
            BacklogPressureDimension::RecordCount,
        ),
        (
            ExactBacklogMeasurement::new(0, 0, 1),
            BacklogLimits::new(u64::MAX, u64::MAX, 3),
            BacklogPressureDimension::EncodedBatchBytes,
        ),
    ];
    for (current, limits, expected_dimension) in cases {
        let BacklogAdmission::Pressure(pressure) =
            admit_backlog(current, proposed, limits).unwrap()
        else {
            panic!("retained debt should exceed the isolated candidate dimension");
        };
        assert_eq!(pressure.dimension(), expected_dimension);
        assert!(pressure.current() > 0);
        assert_eq!(
            pressure.proposed(),
            match expected_dimension {
                BacklogPressureDimension::BatchCount => 1,
                BacklogPressureDimension::RecordCount => 2,
                BacklogPressureDimension::EncodedBatchBytes => 3,
            }
        );
        assert!(pressure.limit() > 0);
    }

    let limits = BacklogLimits::from_measurement(proposed);
    assert_eq!(
        admit_backlog(ExactBacklogMeasurement::EMPTY, proposed, limits,).unwrap(),
        BacklogAdmission::Admitted {
            projected: proposed,
        },
    );
}

#[test]
fn maximum_zero_index_multi_store_gate_one_shape_fits_at_zero_debt() {
    let mut next_row = 0_u64;
    let batches = (0..MAX_PERSISTED_STORE_ALLOCATIONS)
        .map(|store_ordinal| {
            let rows = 4_096 / MAX_PERSISTED_STORE_ALLOCATIONS;
            let records = (0..rows)
                .map(|_| {
                    next_row += 1;
                    row_record(next_row)
                })
                .collect();
            batch(u8::try_from(store_ordinal + 1).unwrap(), records)
        })
        .collect();
    let prepared = proposal(batches);
    assert_eq!(prepared.contribution().batch_count(), 16);
    assert_eq!(prepared.contribution().record_count(), 4_096);
    assert_eq!(
        admit_backlog(
            ExactBacklogMeasurement::EMPTY,
            prepared.contribution(),
            BacklogLimits::from_measurement(prepared.contribution()),
        )
        .unwrap(),
        BacklogAdmission::Admitted {
            projected: prepared.contribution(),
        },
    );
}

#[test]
fn current_sum_and_projection_overflow_fail_closed() {
    let malformed = ExactBacklogMeasurement::new(u64::MAX, 0, 0);
    assert!(
        admit_backlog(
            malformed,
            ExactBacklogMeasurement::new(1, 0, 0),
            BacklogLimits::new(u64::MAX, u64::MAX, u64::MAX),
        )
        .is_err(),
    );
    let too_many_controls = vec![JournalTailControl::empty(); 17];
    assert!(ExactBacklogMeasurement::from_tail_controls(&too_many_controls).is_err());
}
