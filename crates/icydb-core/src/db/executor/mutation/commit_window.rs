//! Module: executor::mutation::commit_window
//! Responsibility: commit-window open/apply orchestration for prepared row ops.
//! Does not own: save/delete logical planning or relation policy decisions.
//! Boundary: shared commit marker and prepared-op apply pipeline for mutations.

use crate::{
    db::journal::{DatabaseCommitSequence, JournalBatch, JournalRecord},
    db::{
        Db,
        commit::{
            CommitGuard, CommitMarker, CommitPrepareContext, CommitRowOp, PreparedRowCommitOp,
            begin_commit, begin_mutation_progress_commit, database_incarnation_id, finish_commit,
            generate_commit_id, generate_marker_batch_id, next_database_commit_sequence,
            prepare_row_commit_with_context,
        },
        data::{DecodedDataStoreKey, RawDataStoreKey, RawRow},
        direction::Direction,
        executor::budget::finish_current_execution_instruction_watermark,
        index::{
            IndexEntryValue, IndexStore, RawIndexStoreKey, StructuralIndexEntryReader,
            StructuralPrimaryRowReader, key_within_envelope,
            push_structural_index_entry_primary_key_values_limited,
        },
        integrity::{MutationProgressRecordOp, apply_preflighted_mutation_progress_record_op},
        key_taxonomy::PrimaryKeyValue,
        positioned_overlay::JournalOverlayPosition,
        registry::{StoreHandle, StoreRecoveryCapability, StoreSchemaMetadataCapability},
        schema::{
            IdentityAdvanceId, IdentityRangeAdvance, PreparedSchemaPositionPublication,
            SchemaStore, apply_live_identity_range_checkpoint,
            preflight_live_identity_range_checkpoint,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::Bound,
    ptr,
    rc::Rc,
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

/// Test-only durable interruption boundaries around mutation publication.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum MutationCommitInterruption {
    MarkerPersisted,
    JournalPublished,
    RowPrefixPublished,
    RowsPublished,
    StateMaterialized,
    ProgressReplaced,
}

#[cfg(test)]
thread_local! {
    static NEXT_MUTATION_COMMIT_INTERRUPTION: std::cell::Cell<Option<MutationCommitInterruption>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(in crate::db) fn interrupt_next_mutation_commit_for_tests(
    interruption: MutationCommitInterruption,
) {
    NEXT_MUTATION_COMMIT_INTERRUPTION.with(|next| next.set(Some(interruption)));
}

#[cfg(test)]
fn take_mutation_commit_interruption(interruption: MutationCommitInterruption) -> bool {
    NEXT_MUTATION_COMMIT_INTERRUPTION.with(|next| {
        if next.get() == Some(interruption) {
            next.set(None);
            true
        } else {
            false
        }
    })
}

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
    pub(in crate::db::executor) index_store_guards: Vec<IndexStoreGenerationGuard>,
}

pub(in crate::db::executor) struct PreparedJournalAppend {
    journal_store: &'static LocalKey<RefCell<crate::db::journal::JournalTailStore>>,
    batch: JournalBatch,
    marker_batch_ordinal: usize,
    data_store: &'static LocalKey<RefCell<crate::db::data::DataStore>>,
    position: JournalOverlayPosition,
    schema_store: &'static LocalKey<RefCell<SchemaStore>>,
    schema_positions: PreparedSchemaPositionPublication,
}

struct CommitWindowPayload {
    marker: CommitMarker,
    effects: PreparedCommitEffects,
}

struct PreparedCommitEffects {
    journal_appends: Vec<PreparedJournalAppend>,
    identity_range_applies: Vec<PreparedIdentityRangeApply>,
    mutation_progress: Option<MutationProgressRecordOp>,
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
/// Snapshot of one index store generation captured after preflight.
/// Apply must observe the same generation before it starts mutating state.
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
            return Err(InternalError::mutation_index_store_generation_changed(
                self.expected_generation,
                observed_generation,
            ));
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
    fn from_row_ops(db: &'a Db<C>, row_ops: &[CommitRowOp]) -> Result<Self, InternalError> {
        let reserve_rows = row_ops.len().min(MUTATION_COMMIT_INITIAL_RESERVE_ROWS);
        let mut overlay = Self {
            db,
            data_overrides: HashMap::with_capacity(reserve_rows),
            index_overrides: HashMap::with_capacity(reserve_rows),
        };
        for row_op in row_ops {
            let after = row_op
                .after
                .as_ref()
                .map(|bytes| RawRow::from_untrusted_bytes(bytes.clone()))
                .transpose()?;
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
        let raw_key = key.to_raw()?;
        if let Some(override_row) = self.data_overrides.get(&raw_key) {
            return Ok(override_row.clone());
        }

        let runtime_entity = self.db.accepted_runtime_entity_for_tag(key.entity_tag())?;
        let store = self.db.recovered_store(runtime_entity.store_path())?;

        Ok(store.with_data(|data_store| data_store.get(&raw_key)))
    }

    fn has_primary_row_override(&self, key: &DecodedDataStoreKey) -> Result<bool, InternalError> {
        Ok(self.data_overrides.contains_key(&key.to_raw()?))
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
                store.visit_raw_entries_in_range(bounds, Direction::Asc, |raw_key, raw_entry| {
                    push_structural_index_entry_primary_key_values_limited(
                        raw_key, raw_entry, &mut out, limit,
                    )
                })
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
            index_store.visit_raw_entries_in_range(bounds, Direction::Asc, |raw_key, raw_entry| {
                while let Some((override_key, _)) = overrides.peek() {
                    match (*override_key).cmp(raw_key) {
                        std::cmp::Ordering::Less => {
                            let override_key = (*override_key).clone();
                            let Some((_, override_entry)) = overrides.next() else {
                                return Err(InternalError::query_executor_invariant());
                            };
                            if push_optional_index_entry_primary_key_values(
                                &override_key,
                                override_entry.as_ref(),
                                &mut out,
                                limit,
                            )? {
                                limit_reached = true;
                                return Ok(true);
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            let override_key = (*override_key).clone();
                            let Some((_, override_entry)) = overrides.next() else {
                                return Err(InternalError::query_executor_invariant());
                            };
                            if push_optional_index_entry_primary_key_values(
                                &override_key,
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
            })
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
    row_ops: &[CommitRowOp],
    overlay: &mut PreflightStoreOverlay<'_, C>,
    fixed_commit_work_units: usize,
) -> Result<PreparedRowOpBatch, InternalError> {
    if row_ops.is_empty() {
        return PreparedRowOpBatch::with_row_capacity(0, fixed_commit_work_units);
    }

    let mut batch = PreparedRowOpBatch::with_row_capacity(row_ops.len(), fixed_commit_work_units)?;
    let mut contexts: Vec<(
        Rc<str>,
        [u8; 16],
        crate::types::EntityTag,
        CommitPrepareContext,
    )> = Vec::new();

    for row_op in row_ops {
        let context_index = contexts.iter().position(|(path, fingerprint, _, _)| {
            path.as_ref() == row_op.entity_path.as_ref()
                && *fingerprint == row_op.schema_fingerprint
        });
        let context_index = if let Some(index) = context_index {
            index
        } else {
            let runtime_entity =
                db.accepted_runtime_entity_for_path(row_op.entity_path.as_ref())?;
            let context = runtime_entity.prepare_commit_context(
                db,
                row_op.schema_fingerprint,
                crate::db::commit::CommitPrepareMode::NormalWrite,
            )?;
            contexts.push((
                runtime_entity.entity_path_handle(),
                row_op.schema_fingerprint,
                runtime_entity.entity_tag(),
                context,
            ));
            contexts.len().saturating_sub(1)
        };
        let decoded_key = DecodedDataStoreKey::try_from_raw(&row_op.key)
            .map_err(|_| InternalError::query_executor_invariant())?;
        if decoded_key.entity_tag() != contexts[context_index].2 {
            return Err(InternalError::query_executor_invariant());
        }
        let row = prepare_row_commit_with_context(
            db,
            row_op,
            &contexts[context_index].3,
            overlay,
            overlay,
        )?;
        overlay.stage_prepared_row_op(&row);
        batch.push(row)?;
    }

    Ok(batch)
}

/// Preflight row ops, build marker, and persist the nongeneric delete commit window.
pub(in crate::db::executor) fn open_commit_window_structural<C: CanisterKind>(
    db: &Db<C>,
    row_ops: Vec<CommitRowOp>,
    deleted_key_groups: &[(String, BTreeSet<RawDataStoreKey>)],
    identity_ranges: Vec<IdentityRangeAdvance>,
) -> Result<OpenCommitWindow, InternalError> {
    open_commit_window_structural_inner(db, row_ops, deleted_key_groups, identity_ranges, None)
}

fn open_commit_window_structural_inner<C: CanisterKind>(
    db: &Db<C>,
    row_ops: Vec<CommitRowOp>,
    deleted_key_groups: &[(String, BTreeSet<RawDataStoreKey>)],
    identity_ranges: Vec<IdentityRangeAdvance>,
    mutation_progress: Option<MutationProgressRecordOp>,
) -> Result<OpenCommitWindow, InternalError> {
    let fixed_commit_work_units =
        fixed_mutation_commit_work_units(identity_ranges.len(), mutation_progress.is_some())?;
    let mut overlay = PreflightStoreOverlay::<C>::from_row_ops(db, &row_ops)?;
    for (entity_path, deleted_keys) in deleted_key_groups {
        db.validate_delete_relations_with_reader(entity_path, deleted_keys, &overlay)?;
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
    )?;
    let CommitWindowPayload { marker, effects } = commit_window_payload_for_prepared_row_ops(
        db,
        &row_ops,
        &prepared_row_ops,
        identity_ranges.as_slice(),
        mutation_progress,
    )?;
    preflight_identity_range_applies(effects.identity_range_applies.as_slice())?;
    let positioned_rows = preflight_positioned_rows(&prepared_row_ops, &effects.journal_appends)?;
    finish_current_execution_instruction_watermark()?;
    let commit = begin_commit_window_payload::<C>(marker, effects.mutation_progress.is_some())?;

    Ok(OpenCommitWindow {
        commit,
        prepared_row_ops,
        positioned_rows,
        effects,
        index_store_guards,
    })
}

/// Apply prepared row ops under the shared commit-window guard.
fn apply_prepared_row_ops<C: CanisterKind>(
    _db: &Db<C>,
    commit: CommitGuard,
    apply_phase: &'static str,
    prepared_row_ops: Vec<PreparedRowCommitOp>,
    positioned_rows: Vec<Option<JournalOverlayPosition>>,
    effects: PreparedCommitEffects,
    index_store_guards: Vec<IndexStoreGenerationGuard>,
) -> Result<(), InternalError> {
    if positioned_rows.len() != prepared_row_ops.len() {
        return Err(InternalError::query_executor_invariant());
    }
    finish_commit(commit, |guard| {
        #[cfg(not(test))]
        let _ = apply_phase;
        #[cfg(test)]
        let mut apply_guard = CommitApplyGuard::new(apply_phase);
        // Enforce that index stores are unchanged between preflight and apply.
        for index_store_guard in &index_store_guards {
            index_store_guard.verify()?;
        }
        #[cfg(test)]
        if take_mutation_commit_interruption(MutationCommitInterruption::MarkerPersisted) {
            std::mem::forget(apply_guard);
            return Err(InternalError::executor_invariant());
        }
        append_prepared_journal_batches(guard, &effects.journal_appends)?;
        #[cfg(test)]
        if take_mutation_commit_interruption(MutationCommitInterruption::JournalPublished) {
            std::mem::forget(apply_guard);
            return Err(InternalError::executor_invariant());
        }

        // Single-row writes dominate the hot write lanes, so avoid the extra
        // rollback vector and reverse-apply scaffolding when only one prepared
        // row op remains.
        if prepared_row_ops.len() == 1 {
            let mut prepared_iter = prepared_row_ops.into_iter();
            let mut position_iter = positioned_rows.into_iter();
            let Some(row_op) = prepared_iter.next() else {
                return Err(InternalError::query_executor_invariant());
            };
            let Some(position) = position_iter.next() else {
                return Err(InternalError::query_executor_invariant());
            };
            #[cfg(test)]
            apply_guard.record_single_row_rollback(row_op.snapshot_rollback());

            match position {
                Some(position) => enforce_preflighted_apply(row_op.apply_positioned(position))?,
                None => row_op.apply(),
            }
            #[cfg(test)]
            if take_mutation_commit_interruption(MutationCommitInterruption::RowsPublished) {
                std::mem::forget(apply_guard);
                return Err(InternalError::executor_invariant());
            }
            enforce_preflighted_apply(apply_prepared_state_effects::<C>(&effects))?;
            #[cfg(test)]
            if take_mutation_commit_interruption(MutationCommitInterruption::ProgressReplaced)
                || take_mutation_commit_interruption(MutationCommitInterruption::StateMaterialized)
            {
                std::mem::forget(apply_guard);
                return Err(InternalError::executor_invariant());
            }
            #[cfg(test)]
            apply_guard.finish()?;

            return Ok(());
        }

        #[cfg(test)]
        {
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
                Some(position) => enforce_preflighted_apply(row_op.apply_positioned(position))?,
                None => row_op.apply(),
            }
            #[cfg(test)]
            if row_index == 0
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
        enforce_preflighted_apply(apply_prepared_state_effects::<C>(&effects))?;
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

#[cfg(test)]
const fn enforce_preflighted_apply(result: Result<(), InternalError>) -> Result<(), InternalError> {
    result
}

#[cfg(not(test))]
fn enforce_preflighted_apply(result: Result<(), InternalError>) -> Result<(), InternalError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) => trap_preflighted_commit_apply_contradiction(error),
    }
}

#[cfg(all(not(test), target_arch = "wasm32"))]
fn trap_preflighted_commit_apply_contradiction(_error: InternalError) -> ! {
    ic_cdk::trap("preflighted commit application contradicted its complete preflight")
}

#[cfg(all(not(test), not(target_arch = "wasm32")))]
fn trap_preflighted_commit_apply_contradiction(_error: InternalError) -> ! {
    std::process::abort()
}

fn apply_prepared_state_effects<C: CanisterKind>(
    effects: &PreparedCommitEffects,
) -> Result<(), InternalError> {
    apply_identity_range_applies(effects.identity_range_applies.as_slice())?;
    for append in &effects.journal_appends {
        append.schema_store.with_borrow_mut(|store| {
            store.publish_prepared_journal_batch_positions(append.schema_positions.clone());
        });
    }
    if let Some(operation) = effects.mutation_progress.as_ref() {
        apply_preflighted_mutation_progress_record_op::<C>(operation)?;
    }
    Ok(())
}

/// Commit one accepted mixed structural row-operation batch through one
/// nongeneric commit window.
pub(in crate::db) fn commit_structural_row_ops_with_window<C: CanisterKind>(
    db: &Db<C>,
    batch: AcceptedMutationConstraintBatch,
    identity_ranges: Vec<IdentityRangeAdvance>,
    apply_phase: &'static str,
) -> Result<(), InternalError> {
    let (row_ops, deleted_key_groups) = batch.into_parts();
    let OpenCommitWindow {
        commit,
        prepared_row_ops,
        positioned_rows,
        effects,
        index_store_guards,
    } = open_commit_window_structural(db, row_ops, &deleted_key_groups, identity_ranges)?;
    let synchronized_store_handles =
        synchronized_store_handles_for_prepared_row_ops(db, prepared_row_ops.as_slice());

    apply_prepared_row_ops(
        db,
        commit,
        apply_phase,
        prepared_row_ops,
        positioned_rows,
        effects,
        index_store_guards,
    )?;
    mark_store_handles_index_ready(synchronized_store_handles.as_slice())?;
    Ok(())
}

/// Commit one accepted row batch and exact mutation-progress successor together.
pub(in crate::db) fn commit_structural_row_ops_with_mutation_progress<C: CanisterKind>(
    db: &Db<C>,
    batch: AcceptedMutationConstraintBatch,
    identity_ranges: Vec<IdentityRangeAdvance>,
    mutation_progress: MutationProgressRecordOp,
    apply_phase: &'static str,
) -> Result<(), InternalError> {
    let (row_ops, deleted_key_groups) = batch.into_parts();
    let OpenCommitWindow {
        commit,
        prepared_row_ops,
        positioned_rows,
        effects,
        index_store_guards,
    } = open_commit_window_structural_inner(
        db,
        row_ops,
        &deleted_key_groups,
        identity_ranges,
        Some(mutation_progress),
    )?;
    let synchronized_store_handles =
        synchronized_store_handles_for_prepared_row_ops(db, prepared_row_ops.as_slice());

    apply_prepared_row_ops(
        db,
        commit,
        apply_phase,
        prepared_row_ops,
        positioned_rows,
        effects,
        index_store_guards,
    )?;
    mark_store_handles_index_ready(synchronized_store_handles.as_slice())?;
    Ok(())
}
/// Resolve the exact registered store pairs that one prepared-op batch
/// synchronized through authoritative row + paired index mutation.
#[must_use]
pub(in crate::db::executor) fn synchronized_store_handles_for_prepared_row_ops<C: CanisterKind>(
    db: &Db<C>,
    prepared_row_ops: &[PreparedRowCommitOp],
) -> Vec<StoreHandle> {
    let registered_handles = db.with_store_registry(|registry| {
        registry
            .iter()
            .map(|(_, handle)| handle)
            .collect::<Vec<StoreHandle>>()
    });

    registered_handles
        .into_iter()
        .filter(|handle| {
            prepared_row_ops.iter().any(|row_op| {
                ptr::eq(handle.data_store(), row_op.data_store)
                    && row_op
                        .index_ops
                        .iter()
                        .any(|index_op| ptr::eq(handle.index_store(), index_op.index_store))
            })
        })
        .collect()
}

// Project durable recovery payloads into one marker-bound commit payload.
// Journaled stores embed logical journal records in the marker so journal
// publication can happen before live projections are updated. Heap stores
// remain live-only and absent from durable recovery payloads.
#[expect(
    clippy::too_many_lines,
    reason = "one builder must bind row records, range ordinals, journal sequence, and marker identity before publication"
)]
fn commit_window_payload_for_prepared_row_ops<C: CanisterKind>(
    db: &Db<C>,
    row_ops: &[CommitRowOp],
    prepared_row_ops: &[PreparedRowCommitOp],
    identity_ranges: &[IdentityRangeAdvance],
    mutation_progress: Option<MutationProgressRecordOp>,
) -> Result<CommitWindowPayload, InternalError> {
    if row_ops.len() != prepared_row_ops.len() {
        return Err(InternalError::executor_invariant());
    }

    let marker_id = generate_commit_id()?;
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

    for (row_op, prepared_row_op) in row_ops.iter().zip(prepared_row_ops) {
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
        marker_batches.push(batch.clone());
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
                batch,
                marker_batch_ordinal,
                data_store: handle.data_store(),
                position,
                schema_store: handle.schema_store(),
                schema_positions,
            });
        }
    }
    if identity_range_applies.len() != identity_ranges.len() {
        return Err(InternalError::identity_state_corruption());
    }

    let marker = match mutation_progress.as_ref() {
        Some(operation) => CommitMarker::from_parts_with_mutation_progress(
            marker_id,
            marker_batches,
            operation.clone(),
        )?,
        None => CommitMarker::from_parts(marker_id, marker_batches)?,
    };

    Ok(CommitWindowPayload {
        marker,
        effects: PreparedCommitEffects {
            journal_appends,
            identity_range_applies,
            mutation_progress,
        },
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
) -> Result<(), InternalError> {
    for prepared in identity_ranges {
        prepared
            .handle
            .with_schema(|store| store.preflight_identity_range_advance(prepared.range))?;
        if prepared.handle.storage_capabilities().schema_metadata()
            == StoreSchemaMetadataCapability::LiveRebuiltMetadata
        {
            preflight_live_identity_range_checkpoint(prepared.store_path, prepared.range)?;
        }
    }
    Ok(())
}

fn apply_identity_range_applies(
    identity_ranges: &[PreparedIdentityRangeApply],
) -> Result<(), InternalError> {
    for prepared in identity_ranges {
        if prepared.handle.storage_capabilities().schema_metadata()
            == StoreSchemaMetadataCapability::LiveRebuiltMetadata
        {
            apply_live_identity_range_checkpoint(
                prepared.store_path,
                prepared.range,
                prepared.advance_id,
            )?;
        }
        prepared.handle.with_schema_mut(|store| {
            store.apply_identity_range_advance(prepared.range, prepared.advance_id)
        })?;
    }
    Ok(())
}

fn begin_commit_window_payload<C: CanisterKind>(
    marker: CommitMarker,
    has_mutation_progress: bool,
) -> Result<CommitGuard, InternalError> {
    if has_mutation_progress {
        begin_mutation_progress_commit::<C>(marker)
    } else {
        begin_commit(marker)
    }
}

fn journal_record_for_row_op(row_op: &CommitRowOp) -> Result<JournalRecord, InternalError> {
    match row_op.after.as_ref() {
        Some(after) => JournalRecord::row_put(
            row_op.entity_path.as_ref(),
            row_op.key.clone(),
            after.clone(),
            row_op.schema_fingerprint,
        ),
        None => JournalRecord::row_delete(
            row_op.entity_path.as_ref(),
            row_op.key.clone(),
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
    appends: &[PreparedJournalAppend],
) -> Result<(), InternalError> {
    for append in appends {
        let marker_bytes = guard.journal_batch_bytes(append.marker_batch_ordinal)?;
        append.journal_store.with_borrow_mut(|store| {
            store.append_marker_encoded_batch(&append.batch, marker_bytes)
        })?;
    }

    Ok(())
}

// Mark one batch of synchronized index stores as `Ready` after commit apply
// succeeds and the commit marker is already closed.
fn mark_store_handles_index_ready(handles: &[StoreHandle]) -> Result<(), InternalError> {
    for handle in handles {
        handle.mark_index_ready()?;
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
mod tests {
    use super::*;
    use crate::{
        db::{
            data::{DataStore, DecodedDataStoreKey},
            index::IndexStore,
            integrity::DatabaseIncarnationId,
            journal::JournalTailStore,
            key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
            registry::{
                StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
                StoreRuntimeStorageCapabilities,
            },
            schema::{
                AcceptedFieldKind, AcceptedSchemaRevision, FieldId, FieldInsertGeneration,
                FieldStorageDecode, PersistedFieldSnapshot, PersistedSchemaSnapshot,
                SchemaFieldSlot, SchemaFieldWritePolicy, SchemaInsertDefault, SchemaRowLayout,
                SchemaStore, SchemaVersion, accepted_schema_candidate_for_tests,
            },
        },
        error::{ErrorClass, ErrorOrigin},
        testing::test_memory,
        traits::Path,
        types::EntityTag,
    };
    use std::collections::BTreeMap;

    struct SchedulerOverlayTestCanister;

    impl Path for SchedulerOverlayTestCanister {
        const PATH: &'static str = "executor::mutation::tests::SchedulerOverlayTestCanister";
    }

    impl CanisterKind for SchedulerOverlayTestCanister {
        const COMMIT_MEMORY_ID: u8 = 1;
        const COMMIT_STABLE_KEY: &'static str = "icydb.scheduler_overlay.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 3;
        const STARTUP_STABLE_KEY: &'static str = "icydb.scheduler_overlay.startup.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 2;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.scheduler_overlay.integrity.progress.v1";
    }

    thread_local! {
        static TEST_REGISTRY: StoreRegistry = StoreRegistry::new();
        static FIRST_IDENTITY_DATA: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(240)));
        static FIRST_IDENTITY_INDEX: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(241)));
        static FIRST_IDENTITY_SCHEMA: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(242)));
        static FIRST_IDENTITY_JOURNAL: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(243)));
        static SECOND_IDENTITY_DATA: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(244)));
        static SECOND_IDENTITY_INDEX: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(245)));
        static SECOND_IDENTITY_SCHEMA: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(246)));
        static SECOND_IDENTITY_JOURNAL: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(247)));
    }

    const IDENTITY_INCARNATION: DatabaseIncarnationId = DatabaseIncarnationId::for_tests(0x71);

    #[test]
    fn canonical_commit_work_bound_tracks_exact_prepared_fanout() {
        assert_eq!(MAX_MUTATION_PROGRESS_BATCH_ROWS_AT_MAX_INDEX_FANOUT, 240,);
        let final_zero_index_units =
            next_mutation_commit_work_units((4_096 - 1) * MUTATION_COMMIT_WORK_UNITS_PER_ROW, 0)
                .expect("the final zero-index row should be admitted");
        assert_eq!(final_zero_index_units, MAX_MUTATION_COMMIT_WORK_UNITS);
        let zero_index_error = next_mutation_commit_work_units(final_zero_index_units, 0)
            .expect_err("the next zero-index row must exceed the fixed work bound");
        assert_eq!(
            zero_index_error.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ActualCount,
                    16_388,
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::Limit, 16_384),
            ],
        );

        let mut max_fanout_units = 0;
        for _ in 0..MAX_MUTATION_PROGRESS_BATCH_ROWS_AT_MAX_INDEX_FANOUT {
            max_fanout_units = next_mutation_commit_work_units(max_fanout_units, 64)
                .expect("240 maximum-fanout inserts should be admitted");
        }
        assert_eq!(max_fanout_units, 16_320);
        let max_fanout_error = next_mutation_commit_work_units(max_fanout_units, 64)
            .expect_err("the 241st maximum-fanout insert must be rejected");
        assert!(matches!(
            max_fanout_error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary:
                    icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchCommitWorkExceeded,
            })
        ));
        assert_eq!(
            max_fanout_error.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ActualCount,
                    16_388,
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::Limit, 16_384),
            ],
        );

        let mut identity_boundary_units =
            fixed_mutation_commit_work_units(1, false).expect("one Identity range should fit");
        for _ in 0..4_095 {
            identity_boundary_units = next_mutation_commit_work_units(identity_boundary_units, 0)
                .expect("4,095 zero-index rows plus one Identity range should fit");
        }
        assert_eq!(identity_boundary_units, MAX_MUTATION_COMMIT_WORK_UNITS);
        assert!(next_mutation_commit_work_units(identity_boundary_units, 0).is_err());
    }

    #[test]
    fn individual_gate_keeps_priority_over_cumulative_pressure() {
        let gate_two_called = std::cell::Cell::new(false);
        let result = ensure_mutation_commit_work_admitted(MAX_MUTATION_COMMIT_WORK_UNITS + 1)
            .and_then(|()| {
                gate_two_called.set(true);
                crate::db::commit::admit_backlog(
                    crate::db::commit::ExactBacklogMeasurement::new(1, 1, 1),
                    crate::db::commit::ExactBacklogMeasurement::new(1, 1, 1),
                    crate::db::commit::BacklogLimits::new(1, 1, 1),
                )
            });
        let error = result.expect_err("individual admission must reject before dormant Gate 2");
        assert!(!gate_two_called.get());
        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary:
                    icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchCommitWorkExceeded,
            })
        ));
    }

    fn identity_candidate(
        store_path: &str,
        entity_tag: EntityTag,
    ) -> crate::db::schema::CandidateSchemaRevision {
        let field_id = FieldId::new(1);
        let kind = AcceptedFieldKind::Nat64;
        let leaf_codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            format!("tests::Identity{}", entity_tag.value()),
            format!("Identity{}", entity_tag.value()),
            field_id,
            SchemaRowLayout::initial(vec![(field_id, SchemaFieldSlot::new(0))]),
            vec![PersistedFieldSnapshot::new_initial_with_write_policy(
                field_id,
                "id".to_string(),
                SchemaFieldSlot::new(0),
                kind,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::from_model_policies(
                    Some(FieldInsertGeneration::Identity),
                    None,
                ),
                FieldStorageDecode::ByKind,
                leaf_codec,
            )],
        );
        accepted_schema_candidate_for_tests(
            store_path,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([(entity_tag, snapshot)]),
        )
    }

    fn identity_store_handles() -> (StoreHandle, StoreHandle) {
        (
            StoreHandle::new_journaled(
                &FIRST_IDENTITY_DATA,
                &FIRST_IDENTITY_INDEX,
                &FIRST_IDENTITY_SCHEMA,
                &FIRST_IDENTITY_JOURNAL,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(240, "icydb.test.identity_set.first.data.v1"),
                    StoreAllocationIdentity::new(241, "icydb.test.identity_set.first.index.v1"),
                    StoreAllocationIdentity::new(242, "icydb.test.identity_set.first.schema.v1"),
                    StoreAllocationIdentity::new(243, "icydb.test.identity_set.first.journal.v1"),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ),
            StoreHandle::new_journaled(
                &SECOND_IDENTITY_DATA,
                &SECOND_IDENTITY_INDEX,
                &SECOND_IDENTITY_SCHEMA,
                &SECOND_IDENTITY_JOURNAL,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(244, "icydb.test.identity_set.second.data.v1"),
                    StoreAllocationIdentity::new(245, "icydb.test.identity_set.second.index.v1"),
                    StoreAllocationIdentity::new(246, "icydb.test.identity_set.second.schema.v1"),
                    StoreAllocationIdentity::new(247, "icydb.test.identity_set.second.journal.v1"),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ),
        )
    }

    fn test_key(value: u64) -> DecodedDataStoreKey {
        DecodedDataStoreKey::new(
            EntityTag::new(41),
            &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(value)),
        )
    }

    fn test_row_op(key: &DecodedDataStoreKey, after: Option<Vec<u8>>) -> CommitRowOp {
        CommitRowOp::new(
            "tests::SelfRelation",
            key.to_raw().expect("test key should encode"),
            None,
            after,
            [7; 16],
        )
    }

    #[test]
    fn scheduler_overlay_seeds_later_final_after_images_before_preflight() {
        let db: Db<SchedulerOverlayTestCanister> = Db::new(
            &TEST_REGISTRY,
            crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
        );
        let first = test_key(1);
        let later = test_key(2);
        let row_ops = vec![
            test_row_op(&first, Some(vec![1])),
            test_row_op(&later, Some(vec![2])),
        ];

        let overlay = PreflightStoreOverlay::from_row_ops(&db, row_ops.as_slice())
            .expect("complete batch overlay should build");
        let visible = overlay
            .read_primary_row(&later)
            .expect("later batch target lookup should succeed")
            .expect("later batch target must be visible before row-order preflight");

        assert_eq!(visible.as_bytes(), &[2]);
    }

    #[test]
    fn scheduler_overlay_seeds_delete_absence_before_preflight() {
        let db: Db<SchedulerOverlayTestCanister> = Db::new(
            &TEST_REGISTRY,
            crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
        );
        let deleted = test_key(3);
        let row_ops = vec![test_row_op(&deleted, None)];

        let overlay = PreflightStoreOverlay::from_row_ops(&db, row_ops.as_slice())
            .expect("delete overlay should build");

        assert!(
            overlay
                .read_primary_row(&deleted)
                .expect("delete target lookup should succeed")
                .is_none(),
            "the complete batch must mask deleted rows before storage-backed proofs",
        );
    }

    #[test]
    fn stale_multi_owner_preflight_is_read_only_for_the_complete_owner_set() {
        let (first, second) = identity_store_handles();
        let first_entity = EntityTag::new(51);
        let second_entity = EntityTag::new(52);
        for (store_path, handle, entity_tag) in [
            ("tests::FirstIdentityStore", first, first_entity),
            ("tests::SecondIdentityStore", second, second_entity),
        ] {
            handle
                .with_schema_mut(|store| {
                    *store =
                        SchemaStore::init_journaled(test_memory(if entity_tag == first_entity {
                            242
                        } else {
                            246
                        }));
                    store.publish_accepted_schema_candidate(
                        IDENTITY_INCARNATION,
                        AcceptedSchemaRevision::NONE,
                        &identity_candidate(store_path, entity_tag),
                    )
                })
                .expect("the Identity owner should publish with explicit zero state");
        }

        let first_owner = crate::db::schema::IdentityStateOwner::try_new(
            IDENTITY_INCARNATION,
            first_entity,
            FieldId::new(1),
        )
        .expect("the first owner should admit");
        let second_owner = crate::db::schema::IdentityStateOwner::try_new(
            IDENTITY_INCARNATION,
            second_entity,
            FieldId::new(1),
        )
        .expect("the second owner should admit");
        let second_committed = IdentityRangeAdvance::try_new(second_owner, 0, 1, 1)
            .expect("the committed second-owner range should admit");
        second
            .with_schema_mut(|store| {
                store.apply_identity_range_advance(
                    second_committed,
                    IdentityAdvanceId::try_new([0x11; 16], [0x21; 16], 1, 0)
                        .expect("the committed advance identity should admit"),
                )
            })
            .expect("the competing second-owner range should materialize");

        let prepared = [
            PreparedIdentityRangeApply {
                store_path: "tests::FirstIdentityStore",
                handle: first,
                range: IdentityRangeAdvance::try_new(first_owner, 0, 1, 1)
                    .expect("the first pending range should admit"),
                advance_id: IdentityAdvanceId::try_new([0x12; 16], [0x22; 16], 2, 0)
                    .expect("the first pending advance identity should admit"),
            },
            PreparedIdentityRangeApply {
                store_path: "tests::SecondIdentityStore",
                handle: second,
                range: IdentityRangeAdvance::try_new(second_owner, 0, 1, 1)
                    .expect("the stale second-owner range should remain structurally valid"),
                advance_id: IdentityAdvanceId::try_new([0x12; 16], [0x23; 16], 3, 0)
                    .expect("the second pending advance identity should admit"),
            },
        ];

        let error = preflight_identity_range_applies(prepared.as_slice())
            .expect_err("one stale owner must reject the complete set before publication");
        assert_eq!(error.class(), ErrorClass::Conflict);
        assert_eq!(error.origin(), ErrorOrigin::Identity);
        for (handle, entity_tag, expected_high_water) in
            [(first, first_entity, 0), (second, second_entity, 1)]
        {
            let cursor = handle
                .with_schema(|store| {
                    store.identity_statement_cursor(
                        IDENTITY_INCARNATION,
                        entity_tag,
                        FieldId::new(1),
                        &AcceptedFieldKind::Nat64,
                    )
                })
                .expect("set preflight must leave every owner unchanged");
            assert_eq!(cursor.expected_high_water(), expected_high_water);
        }
    }
}
