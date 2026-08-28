//! Module: db::commit::convergence_candidate
//! Responsibility: integrate and measure the dormant bounded-convergence candidate.
//! Does not own: production admission, online publication, scheduling, or lifecycle state.
//! Boundary: exact marker envelopes + tail controls + positioned metadata -> test evidence.

use super::{
    BacklogLimits, CommitMarker, ExactBacklogMeasurement, MAX_COMMIT_BYTES,
    MAX_PERSISTED_STORE_ALLOCATIONS, admit_backlog,
    backlog_admission::{BacklogAdmission, MAX_RETAINED_JOURNAL_BATCHES, PreparedBacklogProposal},
};
use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey},
        journal::{
            DatabaseCommitSequence, FoldWatermark, JOURNAL_TAIL_CHUNK_BYTES, JournalBatch,
            JournalRecord, JournalSequence, JournalTailStore,
            MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD, MAX_JOURNAL_BATCH_RECORDS,
            decode_journal_batch,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        positioned_overlay::{JournalOverlayPosition, PositionedOverlayMetadata},
        registry::StoreAllocationIdentity,
    },
    error::InternalError,
    types::EntityTag,
};
use ic_stable_structures::{
    DefaultMemoryImpl, Memory, VectorMemory,
    memory_manager::{MemoryId, MemoryManager, VirtualMemory},
};
use std::collections::{BTreeMap, BTreeSet};

/// Frozen cumulative limits proved by the convergence candidate.
const BACKLOG_BATCH_LIMIT: u64 = MAX_RETAINED_JOURNAL_BATCHES;
const BACKLOG_RECORD_LIMIT: u64 = MAX_JOURNAL_BATCH_RECORDS as u64;
const BACKLOG_ENCODED_BYTE_LIMIT: u64 = MAX_COMMIT_BYTES as u64;
const FROZEN_BACKLOG_LIMITS: BacklogLimits = BacklogLimits::new(
    BACKLOG_BATCH_LIMIT,
    BACKLOG_RECORD_LIMIT,
    BACKLOG_ENCODED_BYTE_LIMIT,
);

/// Existing exact Gate-1 work ceiling retained by the Patch-6 fit proof.
const GATE_ONE_WORK_LIMIT: u64 = 16_384;
/// Maximum effect multiplicity owned by one accepted-index journal record.
const MAX_EFFECTS_PER_JOURNAL_RECORD: u64 = MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD as u64;
/// Conservative total positioned targets for one admitted debt ceiling.
const MAX_POSITIONED_EFFECTS: u64 =
    BACKLOG_RECORD_LIMIT * MAX_EFFECTS_PER_JOURNAL_RECORD + GATE_ONE_WORK_LIMIT;
/// Largest maintained positioned-effect family in one complete callback.
const MAX_CALLBACK_POSITIONED_EFFECTS: u64 = 65_536;
/// Maximum abstract candidate work after control, selector, and retirement.
const CANDIDATE_WORK_LIMIT: u64 = GATE_ONE_WORK_LIMIT
    + 1
    + MAX_PERSISTED_STORE_ALLOCATIONS as u64
    + MAX_CALLBACK_POSITIONED_EFFECTS;
/// Complete callback ceiling, retaining ten billion instructions below the IC limit.
const FOLD_CALLBACK_INSTRUCTION_LIMIT: u64 = 30_000_000_000;
/// Reviewed fixed driver/lifecycle work outside one callback body.
const DRIVER_OVERHEAD_INSTRUCTION_LIMIT: u64 = 5_000_000;
/// Initial request, terminal handoff, one coalesced wake-up, and watchdog transition.
const DRIVER_FIXED_MESSAGE_COUNT: u64 = 4;

/// Conservative heap coefficients for decoded payload and B-tree overlay ownership.
const FIXED_OVERLAY_HEAP_BYTES: u64 = 65_536;
const OVERLAY_ENTRY_BYTES: u64 = 256;
const ENCODED_BYTE_HEAP_MULTIPLIER: u64 = 4;
const BATCH_HEAP_BYTES: u64 = 256;
const STORE_HEAP_BYTES: u64 = 4_096;

type CandidateMemory = VirtualMemory<DefaultMemoryImpl>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CandidateHeadOrder {
    database_commit_sequence: DatabaseCommitSequence,
    store_allocation: u8,
    journal_sequence: JournalSequence,
}

struct DormantCandidateTail {
    allocation: StoreAllocationIdentity,
    tail: JournalTailStore,
    positions: PositionedOverlayMetadata<u64>,
    batch_targets: BTreeMap<JournalSequence, Vec<u64>>,
    last_database_commit_sequence: Option<DatabaseCommitSequence>,
}

impl DormantCandidateTail {
    fn new(allocation: StoreAllocationIdentity, memory: CandidateMemory) -> Self {
        let mut tail = JournalTailStore::init(memory);
        tail.initialize_current_tail_control()
            .expect("candidate tail should initialize");
        Self {
            allocation,
            tail,
            positions: PositionedOverlayMetadata::new(),
            batch_targets: BTreeMap::new(),
            last_database_commit_sequence: None,
        }
    }

    fn preflight_append(&self, batch: &JournalBatch, bytes: &[u8]) -> Result<(), InternalError> {
        if decode_journal_batch(bytes)? != *batch
            || self.tail.next_append_sequence()? != batch.journal_sequence()
            || self
                .last_database_commit_sequence
                .is_some_and(|current| current >= batch.database_commit_sequence())
        {
            return Err(InternalError::store_invariant());
        }
        Ok(())
    }
}

struct DormantConvergenceCandidate {
    tails: Vec<DormantCandidateTail>,
    canonical_batches: u64,
}

impl DormantConvergenceCandidate {
    fn new(memories: impl IntoIterator<Item = CandidateMemory>) -> Self {
        let tails = memories
            .into_iter()
            .enumerate()
            .map(|(ordinal, memory)| {
                let memory_id =
                    u8::try_from(100 + ordinal).expect("candidate allocation should fit");
                DormantCandidateTail::new(
                    StoreAllocationIdentity::new(memory_id, "icydb.test.convergence.journal.v1"),
                    memory,
                )
            })
            .collect::<Vec<_>>();
        assert!(tails.len() <= MAX_PERSISTED_STORE_ALLOCATIONS);
        Self {
            tails,
            canonical_batches: 0,
        }
    }

    fn current_measurement(&self) -> Result<ExactBacklogMeasurement, InternalError> {
        let controls = self
            .tails
            .iter()
            .map(|candidate| candidate.tail.current_tail_control())
            .collect::<Result<Vec<_>, _>>()?;
        ExactBacklogMeasurement::from_tail_controls(&controls)
    }

    fn admit_and_publish(
        &mut self,
        proposal: PreparedBacklogProposal,
        routes: &[(usize, Vec<u64>)],
    ) -> Result<BacklogAdmission, InternalError> {
        let proposal_batch_count = usize::try_from(proposal.contribution().batch_count())
            .map_err(|_| InternalError::store_invariant())?;
        if routes.len() != proposal_batch_count {
            return Err(InternalError::store_invariant());
        }
        let decision = admit_backlog(
            self.current_measurement()?,
            proposal.contribution(),
            FROZEN_BACKLOG_LIMITS,
        )?;
        if matches!(decision, BacklogAdmission::Pressure(_)) {
            return Ok(decision);
        }

        let mut routed_tails = BTreeSet::new();
        for ((batch, bytes), (tail_index, targets)) in proposal.exact_batches().zip(routes) {
            let candidate = self
                .tails
                .get(*tail_index)
                .ok_or_else(InternalError::store_invariant)?;
            if !routed_tails.insert(*tail_index)
                || candidate
                    .batch_targets
                    .contains_key(&batch.journal_sequence())
            {
                return Err(InternalError::store_invariant());
            }
            candidate.preflight_append(batch, bytes)?;
            let position =
                JournalOverlayPosition::new(candidate.allocation, batch.journal_sequence());
            for target in targets {
                candidate.positions.preflight_publish(target, position)?;
            }
        }

        // Every normally returning candidate check precedes publication. The
        // append owner consumes the exact marker-owned envelope; a contradiction
        // after preflight traps so an IC message cannot return partial state.
        for ((batch, bytes), (tail_index, targets)) in proposal.exact_batches().zip(routes) {
            let candidate = &mut self.tails[*tail_index];
            if candidate
                .tail
                .append_marker_encoded_batch(batch, bytes)
                .is_err()
            {
                panic!("preflighted dormant candidate append contradicted its proof");
            }
            candidate.last_database_commit_sequence = Some(batch.database_commit_sequence());
            let position =
                JournalOverlayPosition::new(candidate.allocation, batch.journal_sequence());
            for target in targets {
                candidate.positions.publish_preflighted(*target, position);
            }
            candidate
                .batch_targets
                .insert(batch.journal_sequence(), targets.clone());
        }

        Ok(decision)
    }

    fn select_oldest_head(&self) -> Result<Option<(usize, CandidateHeadOrder)>, InternalError> {
        if self.tails.len() > MAX_PERSISTED_STORE_ALLOCATIONS {
            return Err(InternalError::store_corruption());
        }
        let mut selected = None;
        for (tail_index, candidate) in self.tails.iter().enumerate() {
            let control = candidate.tail.current_tail_control()?;
            if control.is_empty() {
                continue;
            }
            let database_commit_sequence = control
                .head_database_commit_sequence()
                .ok_or_else(InternalError::store_corruption)?;
            let journal_sequence = candidate
                .tail
                .fold_watermark()?
                .highest_folded_journal_sequence()
                .next()
                .ok_or_else(InternalError::store_corruption)?;
            let order = CandidateHeadOrder {
                database_commit_sequence,
                store_allocation: candidate.allocation.memory_id(),
                journal_sequence,
            };
            if selected.is_none_or(|(_, current)| order < current) {
                selected = Some((tail_index, order));
            }
        }
        Ok(selected)
    }

    fn fold_one(&mut self) -> Result<Option<CandidateHeadOrder>, InternalError> {
        let Some((tail_index, order)) = self.select_oldest_head()? else {
            return Ok(None);
        };
        let candidate = &mut self.tails[tail_index];
        let watermark = candidate.tail.fold_watermark()?;
        let batch = candidate
            .tail
            .next_batch_after(watermark.highest_folded_journal_sequence())?
            .ok_or_else(InternalError::store_corruption)?;
        if batch.database_commit_sequence() != order.database_commit_sequence
            || batch.journal_sequence() != order.journal_sequence
        {
            return Err(InternalError::store_corruption());
        }
        let next_watermark = FoldWatermark::new(
            batch.journal_sequence(),
            watermark
                .fold_epoch()
                .checked_add(1)
                .ok_or_else(InternalError::store_corruption)?,
        );
        let tail_retirement = candidate
            .tail
            .prepare_batch_retirement(&batch, next_watermark)?;
        let targets = candidate
            .batch_targets
            .get(&batch.journal_sequence())
            .ok_or_else(InternalError::store_invariant)?;
        let position = JournalOverlayPosition::new(candidate.allocation, batch.journal_sequence());
        let retirements = targets
            .iter()
            .map(|target| {
                candidate
                    .positions
                    .preflight_retirement(target, position)
                    .map(|retirement| (*target, retirement))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let canonical_batches = self
            .canonical_batches
            .checked_add(1)
            .ok_or_else(InternalError::store_invariant)?;

        // This is the candidate's canonical-mutation boundary. Everything
        // below is infallible so a normal error cannot expose partial state.
        self.canonical_batches = canonical_batches;
        for (target, retirement) in retirements {
            candidate.positions.retire_preflighted(&target, retirement);
        }
        candidate
            .tail
            .apply_prepared_batch_retirement(tail_retirement);
        candidate.batch_targets.remove(&batch.journal_sequence());

        Ok(Some(order))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CandidateHeapBound {
    overlay_bytes: u64,
    callback_peak_bytes: u64,
}

fn candidate_heap_bound(
    batches: u64,
    records: u64,
    encoded_bytes: u64,
    stores: u64,
) -> Option<CandidateHeapBound> {
    let effects = records
        .checked_mul(MAX_EFFECTS_PER_JOURNAL_RECORD)?
        .checked_add(GATE_ONE_WORK_LIMIT)?;
    let overlay_bytes = FIXED_OVERLAY_HEAP_BYTES
        .checked_add(effects.checked_mul(OVERLAY_ENTRY_BYTES)?)?
        .checked_add(encoded_bytes.checked_mul(ENCODED_BYTE_HEAP_MULTIPLIER)?)?
        .checked_add(batches.checked_mul(BATCH_HEAP_BYTES)?)?
        .checked_add(stores.checked_mul(STORE_HEAP_BYTES)?)?;
    let head_scan = stores.checked_mul(u64::try_from(size_of::<CandidateHeadOrder>()).ok()?)?;
    let callback_scratch = head_scan
        .checked_add(encoded_bytes.checked_mul(3)?)?
        .checked_add(effects.checked_mul(128)?)?;
    Some(CandidateHeapBound {
        overlay_bytes,
        callback_peak_bytes: overlay_bytes.checked_add(callback_scratch)?,
    })
}

const fn retained_chunk_bound(batches: u64, encoded_bytes: u64) -> u64 {
    batches + encoded_bytes / JOURNAL_TAIL_CHUNK_BYTES as u64
}

const fn normal_residual_message_bound(interval_start_batches: u64) -> u64 {
    interval_start_batches + DRIVER_FIXED_MESSAGE_COUNT
}

const fn normal_residual_instruction_bound(interval_start_batches: u64) -> Option<u64> {
    match interval_start_batches.checked_mul(FOLD_CALLBACK_INSTRUCTION_LIMIT) {
        Some(callbacks) => {
            match DRIVER_FIXED_MESSAGE_COUNT.checked_mul(DRIVER_OVERHEAD_INSTRUCTION_LIMIT) {
                Some(driver) => callbacks.checked_add(driver),
                None => None,
            }
        }
        None => None,
    }
}

fn row_key(value: u64) -> RawDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(1),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
    )
    .to_raw()
    .expect("candidate row key should encode")
}

fn row_record(value: u64, row_bytes: Vec<u8>) -> JournalRecord {
    JournalRecord::row_put(
        "tests::ConvergenceCandidate",
        row_key(value),
        row_bytes,
        [0xC6; 16],
    )
    .expect("candidate row record should admit")
}

fn batch(
    identity: u64,
    journal_sequence: u64,
    database_commit_sequence: u64,
    records: Vec<JournalRecord>,
) -> JournalBatch {
    let mut batch_id = [0_u8; 16];
    batch_id[8..].copy_from_slice(&identity.to_be_bytes());
    let mut marker_id = [0_u8; 16];
    marker_id[8..].copy_from_slice(&database_commit_sequence.to_be_bytes());
    JournalBatch::new_with_database_commit_sequence(
        batch_id,
        marker_id,
        JournalSequence::new(journal_sequence),
        DatabaseCommitSequence::new(database_commit_sequence),
        records,
    )
    .expect("candidate batch should admit")
}

fn proposal(batches: Vec<JournalBatch>) -> PreparedBacklogProposal {
    let marker_id = batches
        .first()
        .map_or([0_u8; 16], JournalBatch::commit_marker_id);
    PreparedBacklogProposal::from_marker(
        CommitMarker::from_parts(marker_id, batches).expect("candidate marker should admit"),
    )
    .expect("candidate proposal should encode once")
}

fn isolated_memories(count: usize) -> Vec<CandidateMemory> {
    (0..count)
        .map(|ordinal| {
            let manager = MemoryManager::init(DefaultMemoryImpl::default());
            manager.get(MemoryId::new(
                u8::try_from(ordinal + 1).expect("test memory id should fit"),
            ))
        })
        .collect()
}

#[test]
fn frozen_tuple_dominates_every_gate_one_axis_and_relieves_pressure_at_zero() {
    let maximum = ExactBacklogMeasurement::new(
        BACKLOG_BATCH_LIMIT,
        BACKLOG_RECORD_LIMIT,
        BACKLOG_ENCODED_BYTE_LIMIT,
    );
    assert_eq!(
        admit_backlog(
            ExactBacklogMeasurement::EMPTY,
            maximum,
            FROZEN_BACKLOG_LIMITS,
        )
        .unwrap(),
        BacklogAdmission::Admitted { projected: maximum },
    );
    for proposed in [
        ExactBacklogMeasurement::new(BACKLOG_BATCH_LIMIT + 1, 0, 0),
        ExactBacklogMeasurement::new(0, BACKLOG_RECORD_LIMIT + 1, 0),
        ExactBacklogMeasurement::new(0, 0, BACKLOG_ENCODED_BYTE_LIMIT + 1),
    ] {
        assert!(matches!(
            admit_backlog(
                ExactBacklogMeasurement::EMPTY,
                proposed,
                FROZEN_BACKLOG_LIMITS,
            )
            .unwrap(),
            BacklogAdmission::Pressure(_),
        ));
    }

    assert_eq!(BACKLOG_BATCH_LIMIT, 64);
    assert_eq!(BACKLOG_RECORD_LIMIT, 16_384);
    assert_eq!(BACKLOG_ENCODED_BYTE_LIMIT, 16 * 1_024 * 1_024);
    assert_eq!(MAX_EFFECTS_PER_JOURNAL_RECORD, 64);
}

#[test]
fn integrated_candidate_is_oldest_first_and_preserves_newer_overlay_authority() {
    let mut candidate = DormantConvergenceCandidate::new(isolated_memories(2));
    let cold = batch(1, 1, 1, vec![row_record(11, Vec::new())]);
    let older_hot = batch(2, 1, 2, vec![row_record(7, vec![2])]);
    let newer_hot = batch(3, 2, 3, vec![row_record(7, vec![3])]);
    assert!(matches!(
        candidate
            .admit_and_publish(proposal(vec![cold]), &[(1, vec![11])])
            .unwrap(),
        BacklogAdmission::Admitted { .. },
    ));
    assert!(matches!(
        candidate
            .admit_and_publish(proposal(vec![older_hot]), &[(0, vec![7])])
            .unwrap(),
        BacklogAdmission::Admitted { .. },
    ));
    assert!(matches!(
        candidate
            .admit_and_publish(proposal(vec![newer_hot]), &[(0, vec![7])])
            .unwrap(),
        BacklogAdmission::Admitted { .. },
    ));

    let first = candidate.fold_one().unwrap().unwrap();
    assert_eq!(first.database_commit_sequence.get(), 1);
    assert_eq!(candidate.tails[1].positions.len(), 0);
    assert_eq!(candidate.tails[0].positions.len(), 1);
    let second = candidate.fold_one().unwrap().unwrap();
    assert_eq!(second.database_commit_sequence.get(), 2);
    assert_eq!(candidate.tails[0].positions.len(), 1);
    let third = candidate.fold_one().unwrap().unwrap();
    assert_eq!(third.database_commit_sequence.get(), 3);
    assert_eq!(candidate.tails[0].positions.len(), 0);
    assert!(candidate.fold_one().unwrap().is_none());
    assert_eq!(candidate.canonical_batches, 3);
    assert_eq!(
        candidate.current_measurement().unwrap(),
        ExactBacklogMeasurement::EMPTY
    );
}

#[test]
fn continuously_hot_early_allocation_cannot_pass_an_older_cold_head() {
    let mut candidate = DormantConvergenceCandidate::new(isolated_memories(2));
    candidate
        .admit_and_publish(
            proposal(vec![batch(11, 1, 1, vec![row_record(11, Vec::new())])]),
            &[(1, vec![11])],
        )
        .unwrap();
    candidate
        .admit_and_publish(
            proposal(vec![batch(12, 1, 2, vec![row_record(12, Vec::new())])]),
            &[(0, vec![12])],
        )
        .unwrap();
    let selected = candidate.select_oldest_head().unwrap().unwrap();
    assert_eq!(selected.0, 1);
    assert_eq!(selected.1.database_commit_sequence.get(), 1);

    candidate.fold_one().unwrap();
    for sequence in 2..=8 {
        let database_sequence = sequence + 1;
        candidate
            .admit_and_publish(
                proposal(vec![batch(
                    20 + sequence,
                    sequence,
                    database_sequence,
                    vec![row_record(20 + sequence, Vec::new())],
                )]),
                &[(0, vec![20 + sequence])],
            )
            .unwrap();
    }
    let orders = std::iter::from_fn(|| candidate.fold_one().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(orders.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        candidate.current_measurement().unwrap(),
        ExactBacklogMeasurement::EMPTY
    );
}

#[test]
fn maximum_overlay_and_residual_formulas_are_checked_and_history_independent() {
    assert_eq!(MAX_POSITIONED_EFFECTS, 1_064_960);
    assert_eq!(MAX_CALLBACK_POSITIONED_EFFECTS, 65_536);
    assert_eq!(CANDIDATE_WORK_LIMIT, 81_937);
    let maximum = candidate_heap_bound(
        BACKLOG_BATCH_LIMIT,
        BACKLOG_RECORD_LIMIT,
        BACKLOG_ENCODED_BYTE_LIMIT,
        MAX_PERSISTED_STORE_ALLOCATIONS as u64,
    )
    .expect("maximum candidate heap arithmetic should fit");
    assert_eq!(
        maximum,
        CandidateHeapBound {
            overlay_bytes: 339_886_080,
            callback_peak_bytes: 526_532_992,
        }
    );
    assert!(maximum.overlay_bytes < 512 * 1_024 * 1_024);
    assert!(maximum.callback_peak_bytes < 768 * 1_024 * 1_024);
    assert!(candidate_heap_bound(u64::MAX, u64::MAX, u64::MAX, u64::MAX).is_none());
    assert_eq!(normal_residual_message_bound(64), 68);
    assert_eq!(
        normal_residual_instruction_bound(64),
        Some(1_920_020_000_000),
    );
    assert_eq!(retained_chunk_bound(64, BACKLOG_ENCODED_BYTE_LIMIT), 320);
}

#[test]
fn maximum_store_scan_and_accepted_index_retirement_use_the_canonical_owners() {
    let mut candidate =
        DormantConvergenceCandidate::new(isolated_memories(MAX_PERSISTED_STORE_ALLOCATIONS));
    let batches = (0..MAX_PERSISTED_STORE_ALLOCATIONS)
        .map(|ordinal| {
            let identity = u64::try_from(ordinal + 1).unwrap();
            batch(identity, 1, 1, vec![row_record(identity, Vec::new())])
        })
        .collect::<Vec<_>>();
    let routes = (0..MAX_PERSISTED_STORE_ALLOCATIONS)
        .map(|ordinal| (ordinal, vec![ordinal as u64]))
        .collect::<Vec<_>>();
    candidate
        .admit_and_publish(proposal(batches), &routes)
        .unwrap();
    let selected = candidate.select_oldest_head().unwrap().unwrap();
    assert_eq!(selected.0, 0);
    assert_eq!(selected.1.store_allocation, 100);

    let mut accepted_index_positions = PositionedOverlayMetadata::new();
    let position = JournalOverlayPosition::new(
        StoreAllocationIdentity::new(100, "icydb.test.maximum-index.journal.v1"),
        JournalSequence::new(1),
    );
    for target in 0..65_536_u64 {
        accepted_index_positions
            .preflight_publish(&target, position)
            .unwrap();
        accepted_index_positions.publish_preflighted(target, position);
    }
    assert_eq!(accepted_index_positions.len(), 65_536);
    for target in 0..65_536_u64 {
        let retirement = accepted_index_positions
            .preflight_retirement(&target, position)
            .unwrap();
        accepted_index_positions.retire_preflighted(&target, retirement);
    }
    assert_eq!(accepted_index_positions.len(), 0);
}

#[test]
fn maximum_batch_and_byte_fill_drain_refill_reuses_stable_high_water() {
    const ROW_BYTES: usize = 255 * 1_024;
    let physical = VectorMemory::default();
    let manager = MemoryManager::init(physical.clone());
    let memory = manager.get(MemoryId::new(1));
    let mut candidate = DormantConvergenceCandidate::new([memory]);
    let mut high_water = None;
    let mut measured_encoded_bytes = 0;
    let mut measured_chunks = 0;

    for cycle in 0..3_u64 {
        let first_sequence = cycle * BACKLOG_BATCH_LIMIT + 1;
        let batches = (0..BACKLOG_BATCH_LIMIT)
            .map(|offset| {
                let sequence = first_sequence + offset;
                batch(
                    sequence,
                    sequence,
                    sequence,
                    vec![row_record(sequence, vec![0xA5; ROW_BYTES])],
                )
            })
            .collect::<Vec<_>>();
        let encoded_batches = batches
            .iter()
            .map(|batch| crate::db::journal::encode_journal_batch(batch).unwrap())
            .collect::<Vec<_>>();
        let encoded_bytes = encoded_batches.iter().map(Vec::len).sum::<usize>();
        let chunks = encoded_batches
            .iter()
            .map(|bytes| bytes.len().div_ceil(JOURNAL_TAIL_CHUNK_BYTES as usize))
            .sum::<usize>();
        assert!(
            encoded_bytes
                <= usize::try_from(BACKLOG_ENCODED_BYTE_LIMIT)
                    .expect("the candidate byte limit should fit usize")
        );
        assert!(chunks as u64 <= retained_chunk_bound(BACKLOG_BATCH_LIMIT, encoded_bytes as u64));
        measured_encoded_bytes = encoded_bytes;
        measured_chunks = chunks;
        let routes = (0..BACKLOG_BATCH_LIMIT)
            .map(|offset| (0_usize, vec![first_sequence + offset]))
            .collect::<Vec<_>>();

        // The production proposal shape admits at most one batch per store.
        // This physical allocator probe intentionally fills one isolated tail
        // directly so reuse can be observed without reserving 16 memories.
        for ((batch, bytes), (_, targets)) in batches.iter().zip(&encoded_batches).zip(&routes) {
            let tail = &mut candidate.tails[0];
            let position = JournalOverlayPosition::new(tail.allocation, batch.journal_sequence());
            tail.positions
                .preflight_publish(&targets[0], position)
                .unwrap();
            tail.tail.append_marker_encoded_batch(batch, bytes).unwrap();
            tail.positions.publish_preflighted(targets[0], position);
            tail.batch_targets
                .insert(batch.journal_sequence(), targets.clone());
        }
        assert!(
            candidate
                .current_measurement()
                .unwrap()
                .encoded_batch_bytes()
                <= BACKLOG_ENCODED_BYTE_LIMIT
        );
        while candidate.fold_one().unwrap().is_some() {}
        assert_eq!(
            candidate.current_measurement().unwrap(),
            ExactBacklogMeasurement::EMPTY
        );

        let pages = physical.size();
        if let Some(first) = high_water {
            assert_eq!(pages, first, "retired stable capacity should be reused");
        } else {
            high_water = Some(pages);
        }
    }
    println!(
        "0.229 stable reuse candidate: encoded_bytes={} live_chunks={} stable_high_water_pages={}",
        measured_encoded_bytes,
        measured_chunks,
        high_water.unwrap_or_default(),
    );
}

#[test]
fn pressure_and_corruption_fail_before_candidate_publication() {
    let mut candidate = DormantConvergenceCandidate::new(isolated_memories(1));
    let before = candidate.current_measurement().unwrap();
    let oversized = ExactBacklogMeasurement::new(BACKLOG_BATCH_LIMIT + 1, 0, 0);
    assert!(matches!(
        admit_backlog(before, oversized, FROZEN_BACKLOG_LIMITS).unwrap(),
        BacklogAdmission::Pressure(_),
    ));
    assert_eq!(candidate.current_measurement().unwrap(), before);
    assert_eq!(candidate.tails[0].positions.len(), 0);

    assert!(decode_journal_batch(&[0xFF]).is_err());
    candidate
        .admit_and_publish(
            proposal(vec![batch(1, 1, 1, vec![row_record(1, Vec::new())])]),
            &[(0, vec![1])],
        )
        .unwrap();
    candidate.tails[0].batch_targets.clear();
    assert!(candidate.fold_one().is_err());
    assert_eq!(candidate.canonical_batches, 0);
    assert_eq!(candidate.tails[0].positions.len(), 1);
    assert_eq!(candidate.current_measurement().unwrap().batch_count(), 1);
}
