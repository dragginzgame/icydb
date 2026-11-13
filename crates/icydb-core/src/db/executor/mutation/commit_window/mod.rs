//! Module: executor::mutation::commit_window
//! Responsibility: commit-window open/apply orchestration for prepared row ops.
//! Does not own: save/delete logical planning or relation policy decisions.
//! Boundary: shared commit marker and prepared-op apply pipeline for mutations.

// Test interruption hooks remain at individual commit publication edges; rollback state lives in helper_tests.

use crate::{
    db::journal::{DatabaseCommitSequence, JournalBatch, JournalRecord},
    db::{
        Db,
        commit::{
            CanonicalRowCommitOp, CommitGuard, CommitMarker, CommitPrepareContextCache,
            CommitPrepareMode, PreparedRowCommitOp, begin_commit, begin_mutation_progress_commit,
            database_incarnation_id, finish_commit, generate_commit_id, generate_marker_batch_id,
            next_database_commit_sequence, prepare_canonical_row_commit_with_context,
        },
        data::{DecodedDataStoreKey, RawDataStoreKey, RawRow},
        direction::Direction,
        index::{
            IndexEntryValue, IndexStore, RawIndexStoreKey, StructuralIndexEntryReader,
            StructuralPrimaryRowReader, key_within_envelope,
            push_structural_index_entry_primary_key_values_limited,
        },
        integrity::{MutationProgressRecordOp, apply_preflighted_mutation_progress_record_op},
        key_taxonomy::PrimaryKeyValue,
        positioned_overlay::JournalOverlayPosition,
        query::preparation::PreparationWork,
        registry::{StoreHandle, StoreRecoveryCapability, StoreSchemaMetadataCapability},
        relation::RelationCommitBudget,
        resource::{
            budget::finish_current_execution_instruction_watermark,
            construction::ConstructionBudget,
        },
        schema::{
            IdentityAdvanceId, IdentityRangeAdvance, PreparedLiveIdentityCheckpoint,
            PreparedSchemaPositionPublication, SchemaStore,
            apply_prepared_live_identity_checkpoints, enum_catalog::AcceptedSchemaRootSelection,
            prepare_live_identity_ranges_checkpoint,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    types::Timestamp,
};
use ic_storage::adapter::Storable;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::Bound,
    ptr,
    thread::LocalKey,
};

use super::constraint_scheduler::AcceptedMutationConstraintBatch;

#[cfg(test)]
use crate::db::commit::{CommitApplyGuard, rollback_prepared_row_ops_reverse};

const MUTATION_COMMIT_INITIAL_RESERVE_ROWS: usize = 64;
const MUTATION_COMMIT_WORK_UNITS_PER_ROW: usize = 4;
const MUTATION_COMMIT_WORK_UNITS_PER_IDENTITY_RANGE: usize = 4;
const MUTATION_COMMIT_WORK_UNITS_PER_PROGRESS_SUCCESSOR: usize = 4;
const MAX_MUTATION_COMMIT_WORK_UNITS: usize = 16_384;
pub(in crate::db) const MAX_MUTATION_PROGRESS_BATCH_ROWS_AT_MAX_INDEX_FANOUT: usize =
    (MAX_MUTATION_COMMIT_WORK_UNITS - MUTATION_COMMIT_WORK_UNITS_PER_PROGRESS_SUCCESSOR)
        / (MUTATION_COMMIT_WORK_UNITS_PER_ROW + icydb_schema::MAX_FRAGMENT_INDEXES);

///
/// OpenCommitWindow
///
/// Commit-window staging bundle shared across save/delete executors.
/// Contains the persisted commit guard, preflight-prepared row ops, and
/// precomputed apply metadata.
///

pub(in crate::db::executor) struct OpenCommitWindow {
    pub(in crate::db::executor) commit: CommitGuard,
    pub(in crate::db::executor) prepared_row_ops: Vec<PreparedRowCommitOp>,
    positioned_rows: Vec<Option<JournalOverlayPosition>>,
    effects: PreparedCommitEffects,
}

pub(in crate::db::executor) struct PreparedJournalAppend {
    journal_store: &'static LocalKey<RefCell<crate::db::journal::JournalTailStore>>,
    marker_batch_ordinal: usize,
    data_store: &'static LocalKey<RefCell<crate::db::data::DataStore>>,
    position: JournalOverlayPosition,
    schema_store: &'static LocalKey<RefCell<SchemaStore>>,
    schema_positions: PreparedSchemaPositionPublication,
}

struct PreparedCommitEffects {
    // The marker owns recovery payloads; publication and live application borrow them.
    marker: CommitMarker,
    journal_appends: Vec<PreparedJournalAppend>,
    identity_range_applies: Vec<PreparedIdentityRangeApply>,
    live_identity_checkpoints: Vec<PreparedLiveIdentityCheckpoint>,
}

#[derive(Clone, Copy)]
struct PreparedIdentityRangeApply {
    store_path: &'static str,
    handle: StoreHandle,
    range: IdentityRangeAdvance,
    advance_id: IdentityAdvanceId,
}

///
/// IndexStoreGenerationGuard
///
/// Snapshot of one index store generation captured during preflight.
/// Publication requires the same generation before creating recovery authority.
///

pub(in crate::db::executor) struct IndexStoreGenerationGuard {
    index_store: &'static LocalKey<RefCell<IndexStore>>,
    expected_generation: u64,
}

impl IndexStoreGenerationGuard {
    // Capture one index store generation at preflight time.
    fn capture(index_store: &'static LocalKey<RefCell<IndexStore>>) -> Self {
        Self {
            index_store,
            expected_generation: index_store.with_borrow(IndexStore::generation),
        }
    }

    // Verify one touched index store still matches its preflight generation.
    fn verify(&self) -> Result<(), InternalError> {
        let observed_generation = self.index_store.with_borrow(IndexStore::generation);
        if observed_generation != self.expected_generation {
            return Err(InternalError::executor_invariant());
        }

        Ok(())
    }
}

///
/// PreparedRowOpBatch
///
/// Streaming preflight output for one commit window.
/// The batch keeps prepared row operations and generation guards together so
/// preflight can produce all apply metadata in one pass.
///

struct PreparedRowOpBatch {
    prepared_row_ops: Vec<PreparedRowCommitOp>,
    index_store_guards: Vec<IndexStoreGenerationGuard>,
    commit_work_units: usize,
}

impl PreparedRowOpBatch {
    // Allocate one preflight batch with enough room for the expected row count.
    fn with_row_capacity(
        row_count: usize,
        fixed_commit_work_units: usize,
    ) -> Result<Self, InternalError> {
        ensure_mutation_commit_work_admitted(fixed_commit_work_units)?;
        let reserve_rows = row_count.min(MUTATION_COMMIT_INITIAL_RESERVE_ROWS);

        Ok(Self {
            prepared_row_ops: Vec::with_capacity(reserve_rows),
            index_store_guards: Vec::new(),
            commit_work_units: fixed_commit_work_units,
        })
    }

    // Add one prepared row op and update all derived apply metadata immediately.
    fn push(&mut self, row_op: PreparedRowCommitOp) -> Result<(), InternalError> {
        let next_commit_work_units =
            next_mutation_commit_work_units(self.commit_work_units, row_op.index_ops.len())?;

        for index_op in &row_op.index_ops {
            record_index_store_generation_guard(&mut self.index_store_guards, index_op.index_store);
        }

        self.prepared_row_ops.push(row_op);
        self.commit_work_units = next_commit_work_units;
        Ok(())
    }
}

fn next_mutation_commit_work_units(
    current_units: usize,
    prepared_index_mutations: usize,
) -> Result<usize, InternalError> {
    let row_work_units = MUTATION_COMMIT_WORK_UNITS_PER_ROW
        .checked_add(prepared_index_mutations)
        .ok_or_else(|| {
            InternalError::mutation_batch_commit_work_exceeded(None, MAX_MUTATION_COMMIT_WORK_UNITS)
        })?;
    let next_units = current_units.checked_add(row_work_units).ok_or_else(|| {
        InternalError::mutation_batch_commit_work_exceeded(None, MAX_MUTATION_COMMIT_WORK_UNITS)
    })?;
    ensure_mutation_commit_work_admitted(next_units)?;

    Ok(next_units)
}

fn ensure_mutation_commit_work_admitted(actual_units: usize) -> Result<(), InternalError> {
    if actual_units > MAX_MUTATION_COMMIT_WORK_UNITS {
        return Err(InternalError::mutation_batch_commit_work_exceeded(
            Some(actual_units),
            MAX_MUTATION_COMMIT_WORK_UNITS,
        ));
    }

    Ok(())
}

fn fixed_mutation_commit_work_units(
    identity_range_count: usize,
    has_progress_successor: bool,
) -> Result<usize, InternalError> {
    let identity_units = identity_range_count
        .checked_mul(MUTATION_COMMIT_WORK_UNITS_PER_IDENTITY_RANGE)
        .ok_or_else(|| {
            InternalError::mutation_batch_commit_work_exceeded(None, MAX_MUTATION_COMMIT_WORK_UNITS)
        })?;
    let progress_units = usize::from(has_progress_successor)
        .checked_mul(MUTATION_COMMIT_WORK_UNITS_PER_PROGRESS_SUCCESSOR)
        .ok_or_else(|| {
            InternalError::mutation_batch_commit_work_exceeded(None, MAX_MUTATION_COMMIT_WORK_UNITS)
        })?;

    identity_units.checked_add(progress_units).ok_or_else(|| {
        InternalError::mutation_batch_commit_work_exceeded(None, MAX_MUTATION_COMMIT_WORK_UNITS)
    })
}

///
/// PreflightStoreOverlay
///
/// In-memory simulation overlay for commit-window preflight.
/// Data reads first consult the complete final-row batch overlay. Index reads
/// consult staged earlier row operations before falling back to committed
/// stores.
///

struct PreflightStoreOverlay<'a, C: CanisterKind> {
    db: &'a Db<C>,
    data_overrides: HashMap<RawDataStoreKey, Option<RawRow>>,
    index_overrides: HashMap<usize, HashMap<RawIndexStoreKey, Option<IndexEntryValue>>>,
}

impl<'a, C: CanisterKind> PreflightStoreOverlay<'a, C> {
    /// Construct one preflight overlay with every submitted final data image
    /// visible before storage-backed constraint scheduling begins.
    fn from_row_ops(
        db: &'a Db<C>,
        row_ops: &[CanonicalRowCommitOp],
    ) -> Result<Self, InternalError> {
        let reserve_rows = row_ops.len().min(MUTATION_COMMIT_INITIAL_RESERVE_ROWS);
        let mut overlay = Self {
            db,
            data_overrides: HashMap::with_capacity(reserve_rows),
            index_overrides: HashMap::with_capacity(reserve_rows),
        };
        for row_op in row_ops {
            let after = row_op.after.as_ref().map(|row| row.as_raw_row().clone());
            if overlay
                .data_overrides
                .insert(row_op.key.clone(), after)
                .is_some()
            {
                return Err(InternalError::query_executor_invariant());
            }
        }
        Ok(overlay)
    }

    // Stage one prepared row-op into overlay data/index maps.
    fn stage_prepared_row_op(&mut self, row_op: &PreparedRowCommitOp) {
        for index_op in &row_op.index_ops {
            let store_id = index_store_id(index_op.index_store);
            self.index_overrides
                .entry(store_id)
                .or_default()
                .insert(index_op.key.clone(), index_op.value.clone());
        }
        self.data_overrides.insert(
            row_op.data_key.clone(),
            row_op
                .data_value
                .as_ref()
                .map(|row| row.as_raw_row().clone()),
        );
    }
}

impl<C: CanisterKind> StructuralPrimaryRowReader for PreflightStoreOverlay<'_, C> {
    fn read_primary_row(&self, key: &DecodedDataStoreKey) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.raw_key()?;
        if let Some(override_row) = self.data_overrides.get(raw_key) {
            return Ok(override_row.clone());
        }

        let runtime_entity = self.db.accepted_runtime_entity_for_tag(key.entity_tag())?;
        let store = self.db.recovered_store(runtime_entity.store_path())?;

        Ok(store.with_data(|data_store| data_store.get(raw_key)))
    }

    fn contains_primary_row(&self, key: &DecodedDataStoreKey) -> Result<bool, InternalError> {
        let raw_key = key.raw_key()?;
        if let Some(override_row) = self.data_overrides.get(raw_key) {
            return Ok(override_row.is_some());
        }

        let runtime_entity = self.db.accepted_runtime_entity_for_tag(key.entity_tag())?;
        let store = self.db.recovered_store(runtime_entity.store_path())?;

        Ok(store.with_data(|data_store| data_store.contains(raw_key)))
    }

    fn has_primary_row_override(&self, key: &DecodedDataStoreKey) -> Result<bool, InternalError> {
        Ok(self.data_overrides.contains_key(key.raw_key()?))
    }
}

impl<C: CanisterKind> StructuralIndexEntryReader for PreflightStoreOverlay<'_, C> {
    fn read_index_entry(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexStoreKey,
    ) -> Result<Option<IndexEntryValue>, InternalError> {
        let store_id = index_store_id(index_store);
        if let Some(store_overrides) = self.index_overrides.get(&store_id)
            && let Some(override_entry) = store_overrides.get(key)
        {
            return Ok(override_entry.clone());
        }

        Ok(index_store.with_borrow(|store| store.get(key)))
    }

    fn read_index_keys_in_raw_range(
        &self,
        index_store: &'static LocalKey<RefCell<IndexStore>>,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        limit: usize,
    ) -> Result<Vec<PrimaryKeyValue>, InternalError> {
        // Phase 1: untouched stores can use the canonical index-store range
        // reader directly instead of materializing one merged entry map first.
        let store_id = index_store_id(index_store);
        let Some(store_overrides) = self.index_overrides.get(&store_id) else {
            let mut out = Vec::with_capacity(limit.min(32));
            index_store.with_borrow(|store| {
                store.visit_raw_entries_in_range(
                    (bounds.0.as_ref(), bounds.1.as_ref()),
                    Direction::Asc,
                    |raw_key, raw_entry| {
                        push_structural_index_entry_primary_key_values_limited(
                            raw_key, raw_entry, &mut out, limit,
                        )
                    },
                )
            })?;

            return Ok(out);
        };

        // Phase 2: staged stores stream a sorted merge of committed entries and
        // staged overrides. Only the small override set is ordered here; the
        // committed store still streams through its canonical range visitor.
        let mut out = Vec::new();
        let mut bounded_overrides = BTreeMap::new();
        for (raw_key, entry) in store_overrides {
            if key_within_bounds(raw_key, bounds) {
                bounded_overrides.insert(raw_key, entry);
            }
        }
        let mut overrides = bounded_overrides.into_iter().peekable();
        let mut limit_reached = false;

        index_store.with_borrow(|index_store| {
            index_store.visit_raw_entries_in_range(
                (bounds.0.as_ref(), bounds.1.as_ref()),
                Direction::Asc,
                |raw_key, raw_entry| {
                    while let Some((override_key, _)) = overrides.peek() {
                        match (*override_key).cmp(raw_key) {
                            std::cmp::Ordering::Less => {
                                let Some((override_key, override_entry)) = overrides.next() else {
                                    return Err(InternalError::query_executor_invariant());
                                };
                                if push_optional_index_entry_primary_key_values(
                                    override_key,
                                    override_entry.as_ref(),
                                    &mut out,
                                    limit,
                                )? {
                                    limit_reached = true;
                                    return Ok(true);
                                }
                            }
                            std::cmp::Ordering::Equal => {
                                let Some((override_key, override_entry)) = overrides.next() else {
                                    return Err(InternalError::query_executor_invariant());
                                };
                                if push_optional_index_entry_primary_key_values(
                                    override_key,
                                    override_entry.as_ref(),
                                    &mut out,
                                    limit,
                                )? {
                                    limit_reached = true;
                                    return Ok(true);
                                }

                                return Ok(false);
                            }
                            std::cmp::Ordering::Greater => break,
                        }
                    }

                    if push_structural_index_entry_primary_key_values_limited(
                        raw_key, raw_entry, &mut out, limit,
                    )? {
                        limit_reached = true;
                        return Ok(true);
                    }

                    Ok(false)
                },
            )
        })?;

        if !limit_reached {
            for (override_key, override_entry) in overrides {
                if push_optional_index_entry_primary_key_values(
                    override_key,
                    override_entry.as_ref(),
                    &mut out,
                    limit,
                )? {
                    break;
                }
            }
        }

        Ok(out)
    }
}

fn push_optional_index_entry_primary_key_values(
    raw_key: &RawIndexStoreKey,
    raw_entry: Option<&IndexEntryValue>,
    out: &mut Vec<PrimaryKeyValue>,
    limit: usize,
) -> Result<bool, InternalError> {
    let Some(raw_entry) = raw_entry else {
        return Ok(false);
    };

    push_structural_index_entry_primary_key_values_limited(raw_key, raw_entry, out, limit)
}

// Capture one unique index-store guard while preflight streams prepared row
// operations. This replaces the old post-preflight guard collection pass.
fn record_index_store_generation_guard(
    guards: &mut Vec<IndexStoreGenerationGuard>,
    index_store: &'static LocalKey<RefCell<IndexStore>>,
) {
    if guards
        .iter()
        .any(|existing| ptr::eq(existing.index_store, index_store))
    {
        return;
    }

    guards.push(IndexStoreGenerationGuard::capture(index_store));
}

// Structural preflight variant used by nongeneric delete paths. It shares the
// same streaming batch accumulator as the typed save/delete path.
fn preflight_prepare_row_op_batch_structural<C: CanisterKind>(
    db: &Db<C>,
    row_ops: &[CanonicalRowCommitOp],
    overlay: &mut PreflightStoreOverlay<'_, C>,
    fixed_commit_work_units: usize,
    relation_budget: &mut RelationCommitBudget,
) -> Result<PreparedRowOpBatch, InternalError> {
    if row_ops.is_empty() {
        return PreparedRowOpBatch::with_row_capacity(0, fixed_commit_work_units);
    }

    let mut batch = PreparedRowOpBatch::with_row_capacity(row_ops.len(), fixed_commit_work_units)?;
    let mut contexts = CommitPrepareContextCache::new(CommitPrepareMode::NormalWrite);

    for (ordinal, row_op) in row_ops.iter().enumerate() {
        let context = contexts.get_or_prepare(
            row_op.entity_path.as_ref(),
            row_op.schema_fingerprint,
            |mode| {
                db.accepted_runtime_entity_for_path(row_op.entity_path.as_ref())?
                    .prepare_commit_context(db, row_op.schema_fingerprint, mode)
            },
        )?;
        let row = prepare_canonical_row_commit_with_context(
            db,
            row_op,
            context,
            overlay,
            overlay,
            relation_budget,
        )?;
        // Only later rows consume staged canonical images and index changes.
        if ordinal + 1 < row_ops.len() {
            overlay.stage_prepared_row_op(&row);
        }
        batch.push(row)?;
    }

    Ok(batch)
}

// Preflight the complete batch before publishing its marker and commit window.
fn open_commit_window_structural<C: CanisterKind>(
    db: &Db<C>,
    row_ops: Vec<CanonicalRowCommitOp>,
    deleted_key_groups: &[(String, BTreeSet<RawDataStoreKey>)],
    identity_ranges: &[IdentityRangeAdvance],
    mutation_progress: Option<MutationProgressRecordOp>,
    operation_timestamp: Timestamp,
) -> Result<OpenCommitWindow, InternalError> {
    let fixed_commit_work_units =
        fixed_mutation_commit_work_units(identity_ranges.len(), mutation_progress.is_some())?;
    let mut overlay = PreflightStoreOverlay::<C>::from_row_ops(db, &row_ops)?;
    let mut relation_budget = RelationCommitBudget::default();
    for (entity_path, deleted_keys) in deleted_key_groups {
        db.validate_delete_relations_with_reader(
            entity_path,
            deleted_keys,
            &overlay,
            &mut relation_budget,
        )?;
    }
    let PreparedRowOpBatch {
        prepared_row_ops,
        index_store_guards,
        ..
    } = preflight_prepare_row_op_batch_structural(
        db,
        &row_ops,
        &mut overlay,
        fixed_commit_work_units,
        &mut relation_budget,
    )?;
    // Journal preparation and publication use owned prepared operations only.
    drop(overlay);
    let mut effects = prepare_commit_effects_for_row_ops(
        db,
        row_ops,
        &prepared_row_ops,
        identity_ranges,
        mutation_progress,
        operation_timestamp,
    )?;
    effects.live_identity_checkpoints = PreparationWork::run_internal(
        db.request_execution_scope(),
        icydb_diagnostic_code::DiagnosticExecutionLane::Mutation,
        |work| preflight_identity_range_applies(effects.identity_range_applies.as_slice(), work),
    )?;
    let positioned_rows = preflight_positioned_rows(&prepared_row_ops, &effects.journal_appends)?;
    if positioned_rows.len() != prepared_row_ops.len() {
        return Err(InternalError::query_executor_invariant());
    }
    finish_current_execution_instruction_watermark()?;
    let commit = begin_commit_window_payload::<C>(&effects.marker, &index_store_guards)?;

    Ok(OpenCommitWindow {
        commit,
        prepared_row_ops,
        positioned_rows,
        effects,
    })
}

/// Apply one prepared window and finish its marker; readiness belongs to recovery/rebuild.
fn apply_prepared_row_ops<C: CanisterKind>(window: OpenCommitWindow) -> Result<(), InternalError> {
    let OpenCommitWindow {
        commit,
        prepared_row_ops,
        positioned_rows,
        effects,
    } = window;
    finish_commit(commit, |guard| {
        #[cfg(test)]
        let mut apply_guard = CommitApplyGuard::new();
        #[cfg(test)]
        if take_mutation_commit_interruption(MutationCommitInterruption::MarkerPersisted) {
            std::mem::forget(apply_guard);
            return Err(InternalError::executor_invariant());
        }
        append_prepared_journal_batches(guard, &effects)?;
        #[cfg(test)]
        if take_mutation_commit_interruption(MutationCommitInterruption::JournalPublished) {
            std::mem::forget(apply_guard);
            return Err(InternalError::executor_invariant());
        }

        // Rollback snapshots are native-test support, not a production apply route.
        #[cfg(test)]
        let multiple_rows = prepared_row_ops.len() > 1;
        #[cfg(test)]
        if let [row_op] = prepared_row_ops.as_slice() {
            apply_guard.record_single_row_rollback(row_op.snapshot_rollback());
        } else {
            let mut rollback = Vec::with_capacity(prepared_row_ops.len());
            for row_op in &prepared_row_ops {
                rollback.push(row_op.snapshot_rollback());
            }
            apply_guard.record_rollback(move || rollback_prepared_row_ops_reverse(rollback));
        }

        #[cfg(test)]
        let mut row_index = 0_usize;
        for (row_op, position) in prepared_row_ops.into_iter().zip(positioned_rows) {
            match position {
                Some(position) => row_op.apply_positioned(position)?,
                None => row_op.apply()?,
            }
            #[cfg(test)]
            if multiple_rows
                && row_index == 0
                && take_mutation_commit_interruption(MutationCommitInterruption::RowPrefixPublished)
            {
                std::mem::forget(apply_guard);
                return Err(InternalError::executor_invariant());
            }
            #[cfg(test)]
            {
                row_index = row_index.saturating_add(1);
            }
        }
        #[cfg(test)]
        if take_mutation_commit_interruption(MutationCommitInterruption::RowsPublished) {
            std::mem::forget(apply_guard);
            return Err(InternalError::executor_invariant());
        }
        apply_prepared_state_effects::<C>(effects)?;
        #[cfg(test)]
        if take_mutation_commit_interruption(MutationCommitInterruption::ProgressReplaced)
            || take_mutation_commit_interruption(MutationCommitInterruption::StateMaterialized)
        {
            std::mem::forget(apply_guard);
            return Err(InternalError::executor_invariant());
        }
        #[cfg(test)]
        apply_guard.finish()?;

        Ok(())
    })
}

fn apply_prepared_state_effects<C: CanisterKind>(
    effects: PreparedCommitEffects,
) -> Result<(), InternalError> {
    apply_prepared_live_identity_checkpoints(effects.live_identity_checkpoints)?;
    apply_identity_range_applies(effects.identity_range_applies.as_slice())?;
    // This is the final use of prepared positions; transfer their keys directly.
    for append in effects.journal_appends {
        append.schema_store.with_borrow_mut(|store| {
            store.publish_prepared_journal_batch_positions(append.schema_positions);
        });
    }
    if let Some(operation) = effects.marker.mutation_progress() {
        apply_preflighted_mutation_progress_record_op::<C>(operation)?;
    }
    Ok(())
}

/// Commit one accepted mixed structural row-operation batch through one
/// nongeneric commit window.
pub(in crate::db) fn commit_structural_row_ops_with_window<C: CanisterKind>(
    db: &Db<C>,
    batch: AcceptedMutationConstraintBatch,
    identity_ranges: &[IdentityRangeAdvance],
    operation_timestamp: Timestamp,
) -> Result<(), InternalError> {
    commit_structural_row_ops(db, batch, identity_ranges, None, operation_timestamp)
}

/// Commit one accepted row batch and exact mutation-progress successor together.
pub(in crate::db) fn commit_structural_row_ops_with_mutation_progress<C: CanisterKind>(
    db: &Db<C>,
    batch: AcceptedMutationConstraintBatch,
    identity_ranges: &[IdentityRangeAdvance],
    mutation_progress: MutationProgressRecordOp,
    operation_timestamp: Timestamp,
) -> Result<(), InternalError> {
    commit_structural_row_ops(
        db,
        batch,
        identity_ranges,
        Some(mutation_progress),
        operation_timestamp,
    )
}

// Publish the row batch and its optional durable progress through the same window.
fn commit_structural_row_ops<C: CanisterKind>(
    db: &Db<C>,
    batch: AcceptedMutationConstraintBatch,
    identity_ranges: &[IdentityRangeAdvance],
    mutation_progress: Option<MutationProgressRecordOp>,
    operation_timestamp: Timestamp,
) -> Result<(), InternalError> {
    let (row_ops, deleted_key_groups) = batch.into_parts();
    let window = open_commit_window_structural(
        db,
        row_ops,
        &deleted_key_groups,
        identity_ranges,
        mutation_progress,
        operation_timestamp,
    )?;

    apply_prepared_row_ops::<C>(window)
}

// Project durable recovery payloads into one marker-bound commit payload.
// Journaled stores embed logical journal records in the marker so journal
// publication can happen before live projections are updated. Heap stores
// remain live-only and absent from durable recovery payloads.
fn prepare_commit_effects_for_row_ops<C: CanisterKind>(
    db: &Db<C>,
    row_ops: Vec<CanonicalRowCommitOp>,
    prepared_row_ops: &[PreparedRowCommitOp],
    identity_ranges: &[IdentityRangeAdvance],
    mutation_progress: Option<MutationProgressRecordOp>,
    operation_timestamp: Timestamp,
) -> Result<PreparedCommitEffects, InternalError> {
    if row_ops.len() != prepared_row_ops.len() {
        return Err(InternalError::executor_invariant());
    }

    // Preserve the frozen timestamp's exact bits, including signed test inputs.
    let marker_id = generate_commit_id(operation_timestamp.as_millis().cast_unsigned())?;
    let registered_stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    let incarnation = database_incarnation_id()?;
    let mut range_routes = Vec::with_capacity(identity_ranges.len());
    for (ordinal, range) in identity_ranges.iter().copied().enumerate() {
        if range.owner().database_incarnation_id() != incarnation
            || identity_ranges[..ordinal]
                .iter()
                .any(|existing| existing.owner() == range.owner())
        {
            return Err(InternalError::identity_state_corruption());
        }
        let runtime_entity = db.accepted_runtime_entity_for_tag(range.owner().entity_tag())?;
        let (store_path, handle) = registered_stores
            .iter()
            .copied()
            .find(|(store_path, _)| *store_path == runtime_entity.store_path())
            .ok_or_else(InternalError::executor_invariant)?;
        range_routes.push((range, store_path, handle));
    }
    let mut journal_records = Vec::<(StoreHandle, Vec<JournalRecord>)>::new();

    // Preflight is complete. Move original recovery bytes into the journal;
    // the prepared operations independently retain their canonical apply rows.
    for (row_op, prepared_row_op) in row_ops.into_iter().zip(prepared_row_ops) {
        let handle = registered_stores
            .iter()
            .map(|(_, handle)| handle)
            .find(|handle| ptr::eq(handle.data_store(), prepared_row_op.data_store))
            .ok_or_else(InternalError::executor_invariant)?;

        let range_binds_store = range_routes
            .iter()
            .any(|(_, _, route_handle)| ptr::eq(route_handle.data_store(), handle.data_store()));
        if handle.storage_capabilities().recovery()
            == StoreRecoveryCapability::StableBasePlusJournalReplay
            || range_binds_store
        {
            let record = journal_record_for_row_op(row_op)?;
            push_journal_record(&mut journal_records, *handle, record);
        }
    }
    for (range, _, handle) in &range_routes {
        push_journal_record(
            &mut journal_records,
            *handle,
            JournalRecord::identity_range_advance(*range)?,
        );
    }

    let mut journal_appends = Vec::with_capacity(journal_records.len());
    let mut marker_batches = Vec::with_capacity(journal_records.len());
    let mut identity_range_applies = Vec::with_capacity(identity_ranges.len());
    let database_commit_sequence = DatabaseCommitSequence::new(next_database_commit_sequence()?);
    for (ordinal, (handle, records)) in journal_records.into_iter().enumerate() {
        let journal_store = handle.journal_tail_store();
        let sequence =
            journal_store.map_or(Ok(crate::db::journal::JournalSequence::new(0)), |store| {
                store.with_borrow(
                    crate::db::journal::JournalTailStore::next_mutation_append_sequence,
                )
            })?;
        // Preserve the established single-store bytes while giving every
        // additional tail its own marker-local batch identity.
        let batch_id = generate_marker_batch_id(marker_id, ordinal)?;
        let batch = JournalBatch::new_with_database_commit_sequence(
            batch_id,
            marker_id,
            sequence,
            database_commit_sequence,
            records,
        )?;
        let store_path = registered_stores
            .iter()
            .find_map(|(store_path, registered_handle)| {
                ptr::eq(registered_handle.data_store(), handle.data_store()).then_some(*store_path)
            })
            .ok_or_else(InternalError::executor_invariant)?;
        for (record_ordinal, record) in batch.records().iter().enumerate() {
            let JournalRecord::IdentityRangeAdvance { range } = record else {
                continue;
            };
            let record_ordinal =
                u32::try_from(record_ordinal).map_err(|_| InternalError::store_invariant())?;
            let advance_id = IdentityAdvanceId::try_new(
                batch.commit_marker_id(),
                batch.batch_id(),
                batch.journal_sequence().get(),
                record_ordinal,
            )?;
            identity_range_applies.push(PreparedIdentityRangeApply {
                store_path,
                handle,
                range: *range,
                advance_id,
            });
        }
        let marker_batch_ordinal = marker_batches.len();
        if let Some(journal_store) = journal_store {
            let position = JournalOverlayPosition::new(
                handle
                    .journal_allocation()
                    .ok_or_else(InternalError::store_invariant)?,
                sequence,
            );
            let schema_positions = handle.with_schema(|store| {
                store.prepare_positioned_journal_batch_publication(incarnation, &batch, position)
            })?;
            journal_appends.push(PreparedJournalAppend {
                journal_store,
                marker_batch_ordinal,
                data_store: handle.data_store(),
                position,
                schema_store: handle.schema_store(),
                schema_positions,
            });
        }
        marker_batches.push(batch);
    }
    if identity_range_applies.len() != identity_ranges.len() {
        return Err(InternalError::identity_state_corruption());
    }

    let marker = match mutation_progress {
        Some(operation) => {
            CommitMarker::from_parts_with_mutation_progress(marker_id, marker_batches, operation)?
        }
        None => CommitMarker::from_parts(marker_id, marker_batches)?,
    };

    Ok(PreparedCommitEffects {
        marker,
        journal_appends,
        identity_range_applies,
        live_identity_checkpoints: Vec::new(),
    })
}

fn preflight_positioned_rows(
    rows: &[PreparedRowCommitOp],
    appends: &[PreparedJournalAppend],
) -> Result<Vec<Option<JournalOverlayPosition>>, InternalError> {
    rows.iter()
        .map(|row| {
            let position = appends
                .iter()
                .find(|append| ptr::eq(append.data_store, row.data_store))
                .map(|append| append.position);
            if let Some(position) = position {
                row.preflight_positioned(position)?;
            }
            Ok(position)
        })
        .collect()
}

fn preflight_identity_range_applies(
    identity_ranges: &[PreparedIdentityRangeApply],
    budget: &dyn ConstructionBudget,
) -> Result<Vec<PreparedLiveIdentityCheckpoint>, InternalError> {
    let mut live_ranges = BTreeMap::<_, (_, Vec<_>)>::new();
    for prepared in identity_ranges {
        prepared
            .handle
            .with_schema(|store| store.preflight_identity_range_advance(prepared.range))?;
        if prepared.handle.storage_capabilities().schema_metadata()
            == StoreSchemaMetadataCapability::LiveRebuiltMetadata
        {
            let root = prepared.handle.with_schema(|store| {
                store
                    .current_accepted_schema_root()?
                    .map(AcceptedSchemaRootSelection::root)
                    .ok_or_else(InternalError::identity_state_corruption)
            })?;
            let (accepted_root, ranges) = live_ranges
                .entry(prepared.store_path)
                .or_insert_with(|| (root, Vec::new()));
            if *accepted_root != root {
                return Err(InternalError::identity_state_corruption());
            }
            ranges.push((prepared.range, prepared.advance_id));
        }
    }
    live_ranges
        .into_iter()
        .map(|(path, (root, ranges))| {
            prepare_live_identity_ranges_checkpoint(path, root, &ranges, budget)
        })
        .collect()
}

fn apply_identity_range_applies(
    identity_ranges: &[PreparedIdentityRangeApply],
) -> Result<(), InternalError> {
    for prepared in identity_ranges {
        prepared.handle.with_schema_mut(|store| {
            store.apply_identity_range_advance(prepared.range, prepared.advance_id)
        })?;
    }
    Ok(())
}

fn begin_commit_window_payload<C: CanisterKind>(
    marker: &CommitMarker,
    index_store_guards: &[IndexStoreGenerationGuard],
) -> Result<CommitGuard, InternalError> {
    // Reject stale preflight while no marker can turn a failed write into
    // recovery work. Opening and applying this window are synchronous.
    for index_store_guard in index_store_guards {
        index_store_guard.verify()?;
    }
    if marker.mutation_progress().is_some() {
        begin_mutation_progress_commit::<C>(marker)
    } else {
        begin_commit(marker)
    }
}

fn journal_record_for_row_op(row_op: CanonicalRowCommitOp) -> Result<JournalRecord, InternalError> {
    match row_op.after {
        Some(after) => JournalRecord::row_put(
            row_op.entity_path.as_ref(),
            row_op.key,
            after.into_raw_row().into_bytes()?,
            row_op.schema_fingerprint,
        ),
        None => JournalRecord::row_delete(
            row_op.entity_path.as_ref(),
            row_op.key,
            row_op.schema_fingerprint,
        ),
    }
}

fn push_journal_record(
    journal_records: &mut Vec<(StoreHandle, Vec<JournalRecord>)>,
    handle: StoreHandle,
    record: JournalRecord,
) {
    if let Some((_, records)) = journal_records
        .iter_mut()
        .find(|(existing, _)| ptr::eq(existing.data_store(), handle.data_store()))
    {
        records.push(record);
        return;
    }

    journal_records.push((handle, vec![record]));
}

fn append_prepared_journal_batches(
    guard: &CommitGuard,
    effects: &PreparedCommitEffects,
) -> Result<(), InternalError> {
    for append in &effects.journal_appends {
        let batch = effects
            .marker
            .journal_batches()
            .get(append.marker_batch_ordinal)
            .ok_or_else(InternalError::executor_invariant)?;
        let marker_bytes = guard.journal_batch_bytes(append.marker_batch_ordinal)?;
        append
            .journal_store
            .with_borrow_mut(|store| store.append_marker_encoded_batch(batch, marker_bytes))?;
    }

    Ok(())
}

fn index_store_id(index_store: &'static LocalKey<RefCell<IndexStore>>) -> usize {
    std::ptr::from_ref::<LocalKey<RefCell<IndexStore>>>(index_store) as usize
}

fn key_within_bounds(
    key: &RawIndexStoreKey,
    bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
) -> bool {
    key_within_envelope(key, bounds.0, bounds.1)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::interrupt_next_mutation_commit_for_tests;
#[cfg(test)]
use helper_tests::take_mutation_commit_interruption;

#[cfg(test)]
pub(in crate::db) use helper_tests::MutationCommitInterruption;
