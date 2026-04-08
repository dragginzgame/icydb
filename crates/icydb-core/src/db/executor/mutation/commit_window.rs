//! Module: executor::mutation::commit_window
//! Responsibility: commit-window open/apply orchestration for prepared row ops.
//! Does not own: save/delete logical planning or relation policy decisions.
//! Boundary: shared commit marker and prepared-op apply pipeline for mutations.

use crate::{
    db::{
        Db,
        commit::{
            CommitApplyGuard, CommitGuard, CommitMarker, CommitRowOp, CommitSchemaFingerprint,
            PreparedIndexDeltaKind, PreparedRowCommitOp, begin_commit, begin_single_row_commit,
            finish_commit, prepare_row_commit_for_entity_with_readers_and_schema_fingerprint,
            prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
            rollback_prepared_row_ops_reverse, snapshot_row_rollback,
        },
        cursor::IndexScanContinuationInput,
        data::{DataKey, RawDataKey, RawRow, StorageKey},
        direction::Direction,
        index::{
            IndexEntryReader, IndexStore, PrimaryRowReader, RawIndexEntry, RawIndexKey,
            SealedIndexEntryReader, SealedPrimaryRowReader, SealedStructuralIndexEntryReader,
            SealedStructuralPrimaryRowReader, StructuralIndexEntryReader,
            StructuralPrimaryRowReader, key_within_envelope,
        },
        registry::StoreHandle,
        schema::commit_schema_fingerprint_for_entity,
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, record},
    model::index::IndexModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    ops::Bound,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    thread::LocalKey,
};

///
/// PreparedRowOpDelta
///
/// Aggregated mutation deltas from preflight-prepared row operations.
/// Used by save/delete executors to emit consistent metrics without duplicating
/// per-field folding logic.
///

pub(in crate::db::executor) struct PreparedRowOpDelta {
    pub(in crate::db::executor) index_inserts: usize,
    pub(in crate::db::executor) index_removes: usize,
    pub(in crate::db::executor) reverse_index_inserts: usize,
    pub(in crate::db::executor) reverse_index_removes: usize,
}

///
/// OpenCommitWindow
///
/// Commit-window staging bundle shared across save/delete executors.
/// Contains the persisted commit guard, preflight-prepared row ops, and
/// precomputed delta counters.
///

pub(in crate::db::executor) struct OpenCommitWindow {
    pub(in crate::db::executor) commit: CommitGuard,
    pub(in crate::db::executor) prepared_row_ops: Vec<PreparedRowCommitOp>,
    pub(in crate::db::executor) index_store_guards: Vec<IndexStoreGenerationGuard>,
    pub(in crate::db::executor) delta: PreparedRowOpDelta,
}

///
/// IndexStoreGenerationGuard
///
/// Snapshot of one index store generation captured after preflight.
/// Apply must observe the same generation before it starts mutating state.
///

pub(in crate::db::executor) struct IndexStoreGenerationGuard {
    store: &'static LocalKey<RefCell<IndexStore>>,
    expected_generation: u64,
}

///
/// SingleRowIndexStoreGuards
///
/// One-row index-store generation snapshot optimized for the hot 0/1-store
/// case. This preserves the same preflight/apply invariant as the batch guard
/// path without forcing heap allocation when a row touches no indexes or only
/// one unique index store.
///

enum SingleRowIndexStoreGuards {
    Empty,
    One(IndexStoreGenerationGuard),
    Many(Vec<IndexStoreGenerationGuard>),
}

impl SingleRowIndexStoreGuards {
    // Record one unique touched index store under the single-row guard shape.
    fn record(&mut self, store: &'static LocalKey<RefCell<IndexStore>>) {
        match self {
            Self::Empty => {
                let expected_generation = store.with_borrow(IndexStore::generation);
                *self = Self::One(IndexStoreGenerationGuard {
                    store,
                    expected_generation,
                });
            }
            Self::One(existing) => {
                if ptr::eq(existing.store, store) {
                    return;
                }

                let first = IndexStoreGenerationGuard {
                    store: existing.store,
                    expected_generation: existing.expected_generation,
                };
                let second = IndexStoreGenerationGuard {
                    store,
                    expected_generation: store.with_borrow(IndexStore::generation),
                };
                *self = Self::Many(vec![first, second]);
            }
            Self::Many(guards) => {
                if guards.iter().any(|existing| ptr::eq(existing.store, store)) {
                    return;
                }

                let expected_generation = store.with_borrow(IndexStore::generation);
                guards.push(IndexStoreGenerationGuard {
                    store,
                    expected_generation,
                });
            }
        }
    }

    // Verify every captured index store still matches its preflight generation.
    fn verify(&self) -> Result<(), InternalError> {
        match self {
            Self::Empty => Ok(()),
            Self::One(guard) => verify_index_store_generation(guard),
            Self::Many(guards) => verify_index_store_generations(guards.as_slice()),
        }
    }
}

///
/// SingleRowApplyPrep
///
/// Single-row preapply bundle derived from one prepared row operation.
/// This keeps delta aggregation and index-generation capture in one scan so the
/// hot one-row save/delete lanes do not rewalk `index_ops` separately.
///

struct SingleRowApplyPrep {
    guards: SingleRowIndexStoreGuards,
    delta: PreparedRowOpDelta,
}

///
/// SingleRowRollbackGuard
///
/// Best-effort unwind cleanup specialized for one prepared row op.
/// This keeps the hot one-row apply path allocation-free while preserving the
/// same "rollback on unwind before finish" behavior the boxed apply guard owns
/// for multi-row commit windows.
///

struct SingleRowRollbackGuard {
    rollback: Option<PreparedRowCommitOp>,
    finished: bool,
}

impl SingleRowRollbackGuard {
    // Start one best-effort rollback guard for a single prepared row op.
    const fn new(rollback: PreparedRowCommitOp) -> Self {
        Self {
            rollback: Some(rollback),
            finished: false,
        }
    }

    // Mark the guarded apply phase complete and drop the rollback snapshot.
    fn finish(&mut self) {
        self.rollback = None;
        self.finished = true;
    }
}

impl Drop for SingleRowRollbackGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        if let Some(rollback) = self.rollback.take() {
            let _ = catch_unwind(AssertUnwindSafe(|| rollback.apply()));
        }
    }
}

///
/// PreflightStoreOverlay
///
/// In-memory simulation overlay for commit-window preflight.
/// Reads first consult staged row/index overrides from earlier row ops and
/// fall back to committed stores when no staged value exists.
///

struct PreflightStoreOverlay<'a, C: CanisterKind> {
    db: &'a Db<C>,
    data_overrides: HashMap<RawDataKey, Option<RawRow>>,
    index_overrides: HashMap<usize, HashMap<RawIndexKey, Option<RawIndexEntry>>>,
}

impl<'a, C: CanisterKind> PreflightStoreOverlay<'a, C> {
    /// Construct one empty preflight overlay for staged mutation simulation.
    fn with_row_capacity(db: &'a Db<C>, row_count: usize) -> Self {
        Self {
            db,
            data_overrides: HashMap::with_capacity(row_count),
            index_overrides: HashMap::with_capacity(row_count),
        }
    }

    // Stage one prepared row-op into overlay data/index maps.
    fn stage_prepared_row_op(&mut self, row_op: &PreparedRowCommitOp) {
        for index_op in &row_op.index_ops {
            let store_id = index_store_id(index_op.store);
            self.index_overrides
                .entry(store_id)
                .or_default()
                .insert(index_op.key.clone(), index_op.value.clone());
        }
        self.data_overrides.insert(
            row_op.data_key,
            row_op
                .data_value
                .as_ref()
                .map(|row| row.as_raw_row().clone()),
        );
    }
}

impl<C: CanisterKind> StructuralPrimaryRowReader for PreflightStoreOverlay<'_, C> {
    fn read_primary_row_structural(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.to_raw()?;
        if let Some(override_row) = self.data_overrides.get(&raw_key) {
            return Ok(override_row.clone());
        }

        let hooks = self.db.runtime_hook_for_entity_tag(key.entity_tag())?;
        let store = self.db.recovered_store(hooks.store_path)?;

        Ok(store.with_data(|data_store| data_store.get(&raw_key)))
    }
}

impl<C: CanisterKind> SealedStructuralPrimaryRowReader for PreflightStoreOverlay<'_, C> {}

impl<E> PrimaryRowReader<E> for PreflightStoreOverlay<'_, E::Canister>
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.to_raw()?;
        if let Some(override_row) = self.data_overrides.get(&raw_key) {
            return Ok(override_row.clone());
        }

        let store = self.db.recovered_store(E::Store::PATH)?;

        Ok(store.with_data(|data_store| data_store.get(&raw_key)))
    }
}

impl<E> SealedPrimaryRowReader<E> for PreflightStoreOverlay<'_, E::Canister> where
    E: EntityKind + EntityValue
{
}

impl<C: CanisterKind> StructuralIndexEntryReader for PreflightStoreOverlay<'_, C> {
    fn read_index_entry_structural(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        let store_id = index_store_id(store);
        if let Some(store_overrides) = self.index_overrides.get(&store_id)
            && let Some(override_entry) = store_overrides.get(key)
        {
            return Ok(override_entry.clone());
        }

        Ok(store.with_borrow(|index_store| index_store.get(key)))
    }

    fn read_index_keys_in_raw_range_structural(
        &self,
        entity_path: &'static str,
        entity_tag: crate::types::EntityTag,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        // Phase 1: untouched stores can use the canonical index-store range
        // reader directly instead of materializing one merged entry map first.
        let store_id = index_store_id(store);
        let Some(store_overrides) = self.index_overrides.get(&store_id) else {
            let data_keys = store.with_borrow(|index_store| {
                index_store.resolve_data_values_in_raw_range_limited(
                    entity_tag,
                    index,
                    bounds,
                    IndexScanContinuationInput::new(None, Direction::Asc),
                    limit,
                    None,
                )
            })?;

            let mut out = Vec::with_capacity(data_keys.len());
            for data_key in data_keys {
                out.push(data_key.storage_key());
            }

            return Ok(out);
        };

        // Phase 2: staged stores still need one merged view so later row ops
        // observe earlier preflight effects before marker persistence.
        let mut effective_entries = store
            .with_borrow(IndexStore::entries)
            .into_iter()
            .filter(|(raw_key, _)| key_within_bounds(raw_key, bounds))
            .collect::<BTreeMap<RawIndexKey, RawIndexEntry>>();

        for (raw_key, raw_entry) in store_overrides {
            if !key_within_bounds(raw_key, bounds) {
                continue;
            }

            if let Some(raw_entry) = raw_entry {
                effective_entries.insert(raw_key.clone(), raw_entry.clone());
            } else {
                effective_entries.remove(raw_key);
            }
        }

        let mut out = Vec::new();
        for (_, raw_entry) in effective_entries {
            let entry = raw_entry.try_decode().map_err(|err| {
                InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    entity_path,
                    index.fields().join(", "),
                    err
                ))
            })?;

            for key in entry.iter_ids() {
                out.push(key);
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }

        Ok(out)
    }
}

impl<C: CanisterKind> SealedStructuralIndexEntryReader for PreflightStoreOverlay<'_, C> {}

impl<E> IndexEntryReader<E> for PreflightStoreOverlay<'_, E::Canister>
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        self.read_index_entry_structural(store, key)
    }

    fn read_index_keys_in_raw_range(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<StorageKey>, InternalError> {
        self.read_index_keys_in_raw_range_structural(
            E::PATH,
            E::ENTITY_TAG,
            store,
            index,
            bounds,
            limit,
        )
    }
}

impl<E> SealedIndexEntryReader<E> for PreflightStoreOverlay<'_, E::Canister> where
    E: EntityKind + EntityValue
{
}

// Fold one prepared index delta kind into saturated commit-window counters.
const fn record_prepared_index_delta(
    summary: &mut PreparedRowOpDelta,
    delta_kind: PreparedIndexDeltaKind,
) {
    let (index_inserts, index_removes, reverse_index_inserts, reverse_index_removes) =
        delta_kind.counter_increments();

    summary.index_inserts = summary.index_inserts.saturating_add(index_inserts);
    summary.index_removes = summary.index_removes.saturating_add(index_removes);
    summary.reverse_index_inserts = summary
        .reverse_index_inserts
        .saturating_add(reverse_index_inserts);
    summary.reverse_index_removes = summary
        .reverse_index_removes
        .saturating_add(reverse_index_removes);
}

/// Emit index and reverse-index delta metrics with saturated diagnostics counts.
pub(in crate::db::executor) fn emit_index_delta_metrics<E: EntityKind>(delta: &PreparedRowOpDelta) {
    emit_index_delta_metrics_for_path(E::PATH, delta);
}

/// Prepare row ops for commit-time apply by simulating sequential execution.
///
/// This preflight ensures later row ops are prepared against the state produced
/// by earlier row ops without mutating real stores before marker persistence.
pub(in crate::db::executor) fn preflight_prepare_row_ops<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: &[CommitRowOp],
) -> Result<Vec<PreparedRowCommitOp>, InternalError> {
    let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();

    // Single-row writes do not need staged overlay simulation because no later
    // row op can observe earlier preflight effects.
    if let [row_op] = row_ops {
        let context = db.context::<E>();

        return prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<E>(
            db,
            row_op,
            &context,
            &context,
            schema_fingerprint,
        )
        .map(|prepared| vec![prepared]);
    }

    let mut prepared = Vec::with_capacity(row_ops.len());
    let mut overlay = PreflightStoreOverlay::<E::Canister>::with_row_capacity(db, row_ops.len());

    for row_op in row_ops {
        let row = prepare_row_commit_for_entity_with_readers_and_schema_fingerprint::<E, _, _>(
            db,
            row_op,
            &overlay,
            &overlay,
            schema_fingerprint,
        )?;
        overlay.stage_prepared_row_op(&row);
        prepared.push(row);
    }

    Ok(prepared)
}

/// Prepare delete row ops for commit-time apply through nongeneric runtime hooks.
pub(in crate::db::executor) fn preflight_prepare_row_ops_structural<C: CanisterKind>(
    db: &Db<C>,
    row_ops: &[CommitRowOp],
) -> Result<Vec<PreparedRowCommitOp>, InternalError> {
    // The structural runtime-hook path can also bypass overlay simulation for
    // one-row commits because there is no staged cross-row state to read.
    if let [row_op] = row_ops {
        return db
            .prepare_row_commit_op(row_op)
            .map(|prepared| vec![prepared]);
    }

    let mut prepared = Vec::with_capacity(row_ops.len());
    let mut overlay = PreflightStoreOverlay::<C>::with_row_capacity(db, row_ops.len());

    for row_op in row_ops {
        let row = db.prepare_row_commit_op_with_readers(row_op, &overlay, &overlay)?;
        overlay.stage_prepared_row_op(&row);
        prepared.push(row);
    }

    Ok(prepared)
}

/// Preflight row ops, build marker, and persist the commit window.
///
/// This is the single orchestration entry point for executor commit-window
/// setup so save/delete paths stay behaviorally aligned.
pub(in crate::db::executor) fn open_commit_window<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: Vec<CommitRowOp>,
) -> Result<OpenCommitWindow, InternalError> {
    let prepared_row_ops = preflight_prepare_row_ops::<E>(db, &row_ops)?;
    let index_store_guards = snapshot_index_store_generations(&prepared_row_ops);
    let delta = summarize_prepared_row_ops(&prepared_row_ops);
    let marker = CommitMarker::new(row_ops)?;
    let commit = begin_commit(marker)?;

    Ok(OpenCommitWindow {
        commit,
        prepared_row_ops,
        index_store_guards,
        delta,
    })
}

/// Preflight row ops, build marker, and persist the nongeneric delete commit window.
pub(in crate::db::executor) fn open_commit_window_structural<C: CanisterKind>(
    db: &Db<C>,
    row_ops: Vec<CommitRowOp>,
) -> Result<OpenCommitWindow, InternalError> {
    let prepared_row_ops = preflight_prepare_row_ops_structural(db, &row_ops)?;
    let index_store_guards = snapshot_index_store_generations(&prepared_row_ops);
    let delta = summarize_prepared_row_ops(&prepared_row_ops);
    let marker = CommitMarker::new(row_ops)?;
    let commit = begin_commit(marker)?;

    Ok(OpenCommitWindow {
        commit,
        prepared_row_ops,
        index_store_guards,
        delta,
    })
}

/// Apply prepared row ops under the shared commit-window guard.
pub(in crate::db::executor) fn apply_prepared_row_ops(
    commit: CommitGuard,
    apply_phase: &'static str,
    prepared_row_ops: Vec<PreparedRowCommitOp>,
    index_store_guards: Vec<IndexStoreGenerationGuard>,
    on_index_applied: impl FnOnce(),
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    finish_commit(commit, |guard| {
        let mut apply_guard = CommitApplyGuard::new(apply_phase);
        let _ = guard;

        // Enforce that index stores are unchanged between preflight and apply.
        verify_index_store_generations(index_store_guards.as_slice())?;

        // Single-row writes dominate the hot write lanes, so avoid the extra
        // rollback vector and reverse-apply scaffolding when only one prepared
        // row op remains.
        if prepared_row_ops.len() == 1 {
            let mut prepared_iter = prepared_row_ops.into_iter();
            let row_op = prepared_iter
                .next()
                .expect("single-row prepared path requires exactly one row op");
            let rollback = snapshot_row_rollback(&row_op);
            let mut rollback_guard = SingleRowRollbackGuard::new(rollback);

            row_op.apply();
            on_index_applied();
            on_data_applied();
            rollback_guard.finish();
            apply_guard.finish()?;

            return Ok(());
        }

        let mut rollback = Vec::with_capacity(prepared_row_ops.len());
        for row_op in &prepared_row_ops {
            rollback.push(snapshot_row_rollback(row_op));
        }
        apply_guard.record_rollback(move || rollback_prepared_row_ops_reverse(rollback));

        for row_op in prepared_row_ops {
            row_op.apply();
        }
        on_index_applied();
        on_data_applied();
        apply_guard.finish()?;

        Ok(())
    })
}

// Apply one prepared row op under the shared commit-window guard without
// routing through the multi-row vector machinery.
fn apply_prepared_single_row_op(
    commit: CommitGuard,
    apply_phase: &'static str,
    prepared_row_op: PreparedRowCommitOp,
    index_store_guards: SingleRowIndexStoreGuards,
    on_index_applied: impl FnOnce(),
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    finish_commit(commit, |guard| {
        let _ = guard;
        let _ = apply_phase;

        // Enforce that index stores are unchanged between preflight and apply.
        index_store_guards.verify()?;

        let rollback = snapshot_row_rollback(&prepared_row_op);
        let mut rollback_guard = SingleRowRollbackGuard::new(rollback);

        prepared_row_op.apply();
        on_index_applied();
        on_data_applied();
        rollback_guard.finish();

        Ok(())
    })
}

/// Open one commit window and apply row ops through the shared apply boundary.
///
/// Save/delete executors should use this helper so commit-window sequencing
/// (preflight marker open + mechanical apply) stays behaviorally aligned.
pub(in crate::db::executor) fn commit_row_ops_with_window<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: Vec<CommitRowOp>,
    apply_phase: &'static str,
    on_index_applied: impl FnOnce(&PreparedRowOpDelta),
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    let OpenCommitWindow {
        commit,
        prepared_row_ops,
        index_store_guards,
        delta,
    } = open_commit_window::<E>(db, row_ops)?;
    let synchronized_store_handles =
        synchronized_store_handles_for_prepared_row_ops(db, prepared_row_ops.as_slice());

    apply_prepared_row_ops(
        commit,
        apply_phase,
        prepared_row_ops,
        index_store_guards,
        || on_index_applied(&delta),
        on_data_applied,
    )?;
    mark_store_handles_index_ready(synchronized_store_handles.as_slice());
    Ok(())
}

/// Commit save-mode row operations through one shared commit window.
///
/// This helper keeps save metrics wiring (`PreparedRowOpDelta`) and commit-window
/// sequencing aligned across single-row and batch save call sites.
pub(in crate::db::executor) fn commit_save_row_ops_with_window<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: Vec<CommitRowOp>,
    apply_phase: &'static str,
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    if let [row_op] = row_ops.as_slice() {
        return commit_single_save_row_op_with_window::<E>(
            db,
            row_op.clone(),
            apply_phase,
            |delta| emit_index_delta_metrics::<E>(delta),
            on_data_applied,
        );
    }

    commit_row_ops_with_window::<E>(
        db,
        row_ops,
        apply_phase,
        |delta| emit_index_delta_metrics::<E>(delta),
        on_data_applied,
    )
}

/// Commit delete-mode row operations through one typed commit window.
pub(in crate::db::executor) fn commit_delete_row_ops_with_window<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: Vec<CommitRowOp>,
    apply_phase: &'static str,
) -> Result<(), InternalError> {
    if row_ops.len() == 1 {
        let row_op = row_ops
            .into_iter()
            .next()
            .expect("single-row delete fast path requires exactly one row op");

        return commit_single_delete_row_op_with_window::<E>(db, row_op, apply_phase);
    }

    commit_row_ops_with_window::<E>(
        db,
        row_ops,
        apply_phase,
        |delta| emit_index_delta_metrics::<E>(delta),
        || {},
    )
}

/// Commit delete-mode row operations through one nongeneric commit window.
pub(in crate::db::executor) fn commit_delete_row_ops_with_window_for_path<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &'static str,
    row_ops: Vec<CommitRowOp>,
    apply_phase: &'static str,
) -> Result<(), InternalError> {
    if row_ops.len() == 1 {
        let row_op = row_ops
            .into_iter()
            .next()
            .expect("single-row structural delete fast path requires exactly one row op");

        return commit_single_delete_row_op_with_window_for_path(
            db,
            entity_path,
            row_op,
            apply_phase,
        );
    }

    let OpenCommitWindow {
        commit,
        prepared_row_ops,
        index_store_guards,
        delta,
    } = open_commit_window_structural(db, row_ops)?;
    let synchronized_store_handles =
        synchronized_store_handles_for_prepared_row_ops(db, prepared_row_ops.as_slice());

    apply_prepared_row_ops(
        commit,
        apply_phase,
        prepared_row_ops,
        index_store_guards,
        || {
            emit_index_delta_metrics_for_path(
                entity_path,
                &PreparedRowOpDelta {
                    index_inserts: 0,
                    index_removes: delta.index_removes,
                    reverse_index_inserts: 0,
                    reverse_index_removes: delta.reverse_index_removes,
                },
            );
        },
        || {},
    )?;
    mark_store_handles_index_ready(synchronized_store_handles.as_slice());
    Ok(())
}
// Commit one save-mode row operation through the single-row commit-window fast
// path used by insert/update/replace.
pub(in crate::db::executor) fn commit_single_save_row_op_with_window<
    E: EntityKind + EntityValue,
>(
    db: &Db<E::Canister>,
    row_op: CommitRowOp,
    apply_phase: &'static str,
    on_index_applied: impl FnOnce(&PreparedRowOpDelta),
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    commit_single_save_row_op_with_window_and_schema_fingerprint::<E>(
        db,
        row_op,
        apply_phase,
        commit_schema_fingerprint_for_entity::<E>(),
        on_index_applied,
        on_data_applied,
    )
}

// Commit one save-mode row operation through the single-row fast path with a
// caller-resolved schema fingerprint so batch save lanes do not rehash it.
pub(in crate::db::executor) fn commit_single_save_row_op_with_window_and_schema_fingerprint<
    E: EntityKind + EntityValue,
>(
    db: &Db<E::Canister>,
    row_op: CommitRowOp,
    apply_phase: &'static str,
    schema_fingerprint: CommitSchemaFingerprint,
    on_index_applied: impl FnOnce(&PreparedRowOpDelta),
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    let context = db.context::<E>();
    let prepared_row_op =
        prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<E>(
            db,
            &row_op,
            &context,
            &context,
            schema_fingerprint,
        )?;
    let synchronized_store_handles =
        synchronized_store_handles_for_prepared_row_ops(db, std::slice::from_ref(&prepared_row_op));

    commit_prepared_single_save_row_op_with_window(
        row_op,
        prepared_row_op,
        synchronized_store_handles,
        apply_phase,
        on_index_applied,
        on_data_applied,
    )
}

// Commit one already-prepared save row op through the single-row fast path.
pub(in crate::db::executor) fn commit_prepared_single_save_row_op_with_window(
    row_op: CommitRowOp,
    prepared_row_op: PreparedRowCommitOp,
    synchronized_store_handles: Vec<StoreHandle>,
    apply_phase: &'static str,
    on_index_applied: impl FnOnce(&PreparedRowOpDelta),
    on_data_applied: impl FnOnce(),
) -> Result<(), InternalError> {
    let SingleRowApplyPrep {
        guards: index_store_guards,
        delta,
    } = prepare_single_row_apply(&prepared_row_op);
    let commit = begin_single_row_commit(row_op)?;

    apply_prepared_single_row_op(
        commit,
        apply_phase,
        prepared_row_op,
        index_store_guards,
        || on_index_applied(&delta),
        on_data_applied,
    )?;
    mark_store_handles_index_ready(synchronized_store_handles.as_slice());
    Ok(())
}

// Commit one delete-mode row operation through the typed single-row
// commit-window fast path.
fn commit_single_delete_row_op_with_window<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_op: CommitRowOp,
    apply_phase: &'static str,
) -> Result<(), InternalError> {
    let context = db.context::<E>();
    let prepared_row_op =
        prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<E>(
            db,
            &row_op,
            &context,
            &context,
            commit_schema_fingerprint_for_entity::<E>(),
        )?;
    let synchronized_store_handles =
        synchronized_store_handles_for_prepared_row_ops(db, std::slice::from_ref(&prepared_row_op));
    let SingleRowApplyPrep {
        guards: index_store_guards,
        delta,
    } = prepare_single_row_apply(&prepared_row_op);
    let commit = begin_single_row_commit(row_op)?;

    apply_prepared_single_row_op(
        commit,
        apply_phase,
        prepared_row_op,
        index_store_guards,
        || emit_index_delta_metrics::<E>(&delta),
        || {},
    )?;
    mark_store_handles_index_ready(synchronized_store_handles.as_slice());
    Ok(())
}

// Commit one delete-mode row operation through the runtime-hook single-row
// structural commit-window fast path.
fn commit_single_delete_row_op_with_window_for_path<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &'static str,
    row_op: CommitRowOp,
    apply_phase: &'static str,
) -> Result<(), InternalError> {
    let prepared_row_op = db.prepare_row_commit_op(&row_op)?;
    let synchronized_store_handles =
        synchronized_store_handles_for_prepared_row_ops(db, std::slice::from_ref(&prepared_row_op));
    let SingleRowApplyPrep {
        guards: index_store_guards,
        delta,
    } = prepare_single_row_apply(&prepared_row_op);
    let commit = begin_single_row_commit(row_op)?;

    apply_prepared_single_row_op(
        commit,
        apply_phase,
        prepared_row_op,
        index_store_guards,
        || {
            emit_index_delta_metrics_for_path(
                entity_path,
                &PreparedRowOpDelta {
                    index_inserts: 0,
                    index_removes: delta.index_removes,
                    reverse_index_inserts: 0,
                    reverse_index_removes: delta.reverse_index_removes,
                },
            );
        },
        || {},
    )?;
    mark_store_handles_index_ready(synchronized_store_handles.as_slice());
    Ok(())
}

// Derive single-row delta metrics and index-store generation guards in one
// scan so the hot write lane does not rewalk the same `index_ops` slice.
fn prepare_single_row_apply(prepared_row_op: &PreparedRowCommitOp) -> SingleRowApplyPrep {
    let mut delta = PreparedRowOpDelta {
        index_inserts: 0,
        index_removes: 0,
        reverse_index_inserts: 0,
        reverse_index_removes: 0,
    };
    let mut guards = SingleRowIndexStoreGuards::Empty;

    for index_op in &prepared_row_op.index_ops {
        record_prepared_index_delta(&mut delta, index_op.delta_kind);
        guards.record(index_op.store);
    }

    SingleRowApplyPrep { guards, delta }
}

/// Aggregate index and reverse-index deltas across prepared row operations.
#[must_use]
pub(in crate::db::executor) fn summarize_prepared_row_ops(
    prepared_row_ops: &[PreparedRowCommitOp],
) -> PreparedRowOpDelta {
    let mut summary = PreparedRowOpDelta {
        index_inserts: 0,
        index_removes: 0,
        reverse_index_inserts: 0,
        reverse_index_removes: 0,
    };

    for row_op in prepared_row_ops {
        for index_op in &row_op.index_ops {
            record_prepared_index_delta(&mut summary, index_op.delta_kind);
        }
    }

    summary
}

// Capture unique touched index stores and their generation after preflight.
fn snapshot_index_store_generations(
    prepared_row_ops: &[PreparedRowCommitOp],
) -> Vec<IndexStoreGenerationGuard> {
    let mut guards = Vec::<IndexStoreGenerationGuard>::new();

    for row_op in prepared_row_ops {
        for index_op in &row_op.index_ops {
            if guards
                .iter()
                .any(|existing| ptr::eq(existing.store, index_op.store))
            {
                continue;
            }
            let expected_generation = index_op.store.with_borrow(IndexStore::generation);
            guards.push(IndexStoreGenerationGuard {
                store: index_op.store,
                expected_generation,
            });
        }
    }

    guards
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
                        .any(|index_op| ptr::eq(handle.index_store(), index_op.store))
            })
        })
        .collect()
}

// Mark one batch of synchronized index stores as `Ready` after commit apply
// succeeds and the commit marker is already closed.
fn mark_store_handles_index_ready(handles: &[StoreHandle]) {
    for handle in handles {
        handle.mark_index_ready();
    }
}

// Verify index stores have not changed since preflight snapshot capture.
fn verify_index_store_generations(
    guards: &[IndexStoreGenerationGuard],
) -> Result<(), InternalError> {
    for guard in guards {
        verify_index_store_generation(guard)?;
    }

    Ok(())
}

// Verify one touched index store still matches its preflight generation.
fn verify_index_store_generation(guard: &IndexStoreGenerationGuard) -> Result<(), InternalError> {
    let observed_generation = guard.store.with_borrow(IndexStore::generation);
    if observed_generation != guard.expected_generation {
        return Err(InternalError::mutation_index_store_generation_changed(
            guard.expected_generation,
            observed_generation,
        ));
    }

    Ok(())
}

fn index_store_id(store: &'static LocalKey<RefCell<IndexStore>>) -> usize {
    std::ptr::from_ref::<LocalKey<RefCell<IndexStore>>>(store) as usize
}

fn emit_index_delta_metrics_for_path(entity_path: &'static str, delta: &PreparedRowOpDelta) {
    record(MetricsEvent::IndexDelta {
        entity_path,
        inserts: u64::try_from(delta.index_inserts).unwrap_or(u64::MAX),
        removes: u64::try_from(delta.index_removes).unwrap_or(u64::MAX),
    });

    record(MetricsEvent::ReverseIndexDelta {
        entity_path,
        inserts: u64::try_from(delta.reverse_index_inserts).unwrap_or(u64::MAX),
        removes: u64::try_from(delta.reverse_index_removes).unwrap_or(u64::MAX),
    });
}

fn key_within_bounds(
    key: &RawIndexKey,
    bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
) -> bool {
    key_within_envelope(key, bounds.0, bounds.1)
}
