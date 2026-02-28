use crate::{
    db::{
        Db,
        commit::{
            CommitApplyGuard, CommitGuard, CommitMarker, CommitRowOp, PreparedIndexDeltaKind,
            PreparedRowCommitOp, begin_commit, commit_schema_fingerprint_for_entity, finish_commit,
            prepare_row_commit_for_entity_with_readers, rollback_prepared_row_ops_reverse,
            snapshot_row_rollback,
        },
        data::{RawDataKey, RawRow},
        index::{IndexEntryReader, IndexStore, PrimaryRowReader, RawIndexEntry, RawIndexKey},
    },
    error::InternalError,
    model::index::IndexModel,
    obs::sink::{MetricsEvent, record},
    traits::{EntityKind, EntityValue},
};
use std::{cell::RefCell, collections::BTreeMap, ops::Bound, ptr, thread::LocalKey};

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
/// PreflightStoreOverlay
///
/// In-memory simulation overlay for commit-window preflight.
/// Reads first consult staged row/index overrides from earlier row ops and
/// fall back to committed stores when no staged value exists.
///

struct PreflightStoreOverlay<'a, E: EntityKind + EntityValue> {
    db: &'a Db<E::Canister>,
    data_overrides: BTreeMap<RawDataKey, Option<RawRow>>,
    index_overrides: BTreeMap<usize, BTreeMap<RawIndexKey, Option<RawIndexEntry>>>,
}

impl<'a, E> PreflightStoreOverlay<'a, E>
where
    E: EntityKind + EntityValue,
{
    const fn new(db: &'a Db<E::Canister>) -> Self {
        Self {
            db,
            data_overrides: BTreeMap::new(),
            index_overrides: BTreeMap::new(),
        }
    }

    fn stage_prepared_row_op(&mut self, row_op: &PreparedRowCommitOp) {
        for index_op in &row_op.index_ops {
            let store_id = index_store_id(index_op.store);
            self.index_overrides
                .entry(store_id)
                .or_default()
                .insert(index_op.key.clone(), index_op.value.clone());
        }
        self.data_overrides
            .insert(row_op.data_key, row_op.data_value.clone());
    }
}

impl<E> PrimaryRowReader<E> for PreflightStoreOverlay<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row(
        &self,
        key: &crate::db::data::DataKey,
    ) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.to_raw()?;
        if let Some(override_row) = self.data_overrides.get(&raw_key) {
            return Ok(override_row.clone());
        }

        self.db.context::<E>().read_primary_row(key)
    }
}

impl<E> IndexEntryReader<E> for PreflightStoreOverlay<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry(
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

    fn read_index_keys_in_raw_range(
        &self,
        store: &'static LocalKey<RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<E::Key>, InternalError> {
        let mut effective_entries = store
            .with_borrow(IndexStore::entries)
            .into_iter()
            .filter(|(raw_key, _)| key_within_bounds(raw_key, bounds))
            .collect::<BTreeMap<RawIndexKey, RawIndexEntry>>();

        let store_id = index_store_id(store);
        if let Some(store_overrides) = self.index_overrides.get(&store_id) {
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
        }

        let mut out = Vec::new();
        for (_, raw_entry) in effective_entries {
            let entry = raw_entry.try_decode::<E>().map_err(|err| {
                InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields.join(", "),
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
            match index_op.delta_kind {
                PreparedIndexDeltaKind::None => {}
                PreparedIndexDeltaKind::IndexInsert => {
                    summary.index_inserts = summary.index_inserts.saturating_add(1);
                }
                PreparedIndexDeltaKind::IndexRemove => {
                    summary.index_removes = summary.index_removes.saturating_add(1);
                }
                PreparedIndexDeltaKind::ReverseIndexInsert => {
                    summary.reverse_index_inserts = summary.reverse_index_inserts.saturating_add(1);
                }
                PreparedIndexDeltaKind::ReverseIndexRemove => {
                    summary.reverse_index_removes = summary.reverse_index_removes.saturating_add(1);
                }
            }
        }
    }

    summary
}

/// Emit index and reverse-index metrics from one prepared-row delta aggregate.
pub(in crate::db::executor) fn emit_prepared_row_op_delta_metrics<E: EntityKind>(
    delta: &PreparedRowOpDelta,
) {
    emit_index_delta_metrics::<E>(
        delta.index_inserts,
        delta.index_removes,
        delta.reverse_index_inserts,
        delta.reverse_index_removes,
    );
}

/// Emit index and reverse-index delta metrics with saturated diagnostics counts.
pub(in crate::db::executor) fn emit_index_delta_metrics<E: EntityKind>(
    index_inserts: usize,
    index_removes: usize,
    reverse_index_inserts: usize,
    reverse_index_removes: usize,
) {
    record(MetricsEvent::IndexDelta {
        entity_path: E::PATH,
        inserts: u64::try_from(index_inserts).unwrap_or(u64::MAX),
        removes: u64::try_from(index_removes).unwrap_or(u64::MAX),
    });

    record(MetricsEvent::ReverseIndexDelta {
        entity_path: E::PATH,
        inserts: u64::try_from(reverse_index_inserts).unwrap_or(u64::MAX),
        removes: u64::try_from(reverse_index_removes).unwrap_or(u64::MAX),
    });
}

/// Prepare row ops for commit-time apply by simulating sequential execution.
///
/// This preflight ensures later row ops are prepared against the state produced
/// by earlier row ops without mutating real stores before marker persistence.
pub(in crate::db::executor) fn preflight_prepare_row_ops<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: &[CommitRowOp],
) -> Result<Vec<PreparedRowCommitOp>, InternalError> {
    let mut prepared = Vec::with_capacity(row_ops.len());
    let mut overlay = PreflightStoreOverlay::<E>::new(db);

    for row_op in row_ops {
        let row = prepare_row_commit_for_entity_with_readers::<E>(db, row_op, &overlay, &overlay)?;
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
    let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
    let row_ops = row_ops
        .into_iter()
        .map(|row_op| row_op.with_schema_fingerprint(schema_fingerprint))
        .collect::<Vec<_>>();

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

    apply_prepared_row_ops(
        commit,
        apply_phase,
        prepared_row_ops,
        index_store_guards,
        || on_index_applied(&delta),
        on_data_applied,
    )?;

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
    commit_row_ops_with_window::<E>(
        db,
        row_ops,
        apply_phase,
        |delta| emit_prepared_row_op_delta_metrics::<E>(delta),
        on_data_applied,
    )
}

/// Commit delete-mode row operations through one shared commit window.
///
/// Delete execution emits remove-only index deltas while preserving the same
/// commit-window sequencing and apply guarantees as other mutation paths.
pub(in crate::db::executor) fn commit_delete_row_ops_with_window<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: Vec<CommitRowOp>,
    apply_phase: &'static str,
) -> Result<(), InternalError> {
    commit_row_ops_with_window::<E>(
        db,
        row_ops,
        apply_phase,
        |delta| {
            emit_index_delta_metrics::<E>(0, delta.index_removes, 0, delta.reverse_index_removes);
        },
        || {},
    )
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

// Verify index stores have not changed since preflight snapshot capture.
fn verify_index_store_generations(
    guards: &[IndexStoreGenerationGuard],
) -> Result<(), InternalError> {
    for guard in guards {
        let observed_generation = guard.store.with_borrow(IndexStore::generation);
        if observed_generation != guard.expected_generation {
            return Err(InternalError::executor_invariant(format!(
                "index store generation changed between preflight and apply: expected {}, found {}",
                guard.expected_generation, observed_generation
            )));
        }
    }

    Ok(())
}

fn index_store_id(store: &'static LocalKey<RefCell<IndexStore>>) -> usize {
    std::ptr::from_ref::<LocalKey<RefCell<IndexStore>>>(store) as usize
}

fn key_within_bounds(
    key: &RawIndexKey,
    bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
) -> bool {
    lower_bound_matches(key, bounds.0) && upper_bound_matches(key, bounds.1)
}

fn lower_bound_matches(key: &RawIndexKey, bound: &Bound<RawIndexKey>) -> bool {
    match bound {
        Bound::Included(start) => key >= start,
        Bound::Excluded(start) => key > start,
        Bound::Unbounded => true,
    }
}

fn upper_bound_matches(key: &RawIndexKey, bound: &Bound<RawIndexKey>) -> bool {
    match bound {
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
        Bound::Unbounded => true,
    }
}
