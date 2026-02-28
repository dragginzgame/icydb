use crate::{
    db::{
        Db,
        commit::{
            CommitApplyGuard, CommitGuard, CommitMarker, CommitRowOp, PreparedIndexDeltaKind,
            PreparedRowCommitOp, begin_commit, commit_schema_fingerprint_for_entity, finish_commit,
            prepare_row_commit_for_entity, rollback_prepared_row_ops_reverse,
            snapshot_row_rollback,
        },
        index::IndexStore,
    },
    error::InternalError,
    obs::sink::{MetricsEvent, record},
    traits::{EntityKind, EntityValue},
};
use std::{cell::RefCell, ptr, thread::LocalKey};

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
/// by earlier row ops, then restores the original state before returning.
pub(in crate::db::executor) fn preflight_prepare_row_ops<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    row_ops: &[CommitRowOp],
) -> Result<Vec<PreparedRowCommitOp>, InternalError> {
    let mut prepared = Vec::with_capacity(row_ops.len());
    let mut rollback = Vec::with_capacity(row_ops.len());

    for row_op in row_ops {
        let row = match prepare_row_commit_for_entity::<E>(db, row_op) {
            Ok(op) => op,
            Err(err) => {
                rollback_prepared_row_ops_reverse(rollback);
                return Err(err);
            }
        };
        rollback.push(snapshot_row_rollback(&row));
        row.clone().apply();
        prepared.push(row);
    }

    rollback_prepared_row_ops_reverse(rollback);
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
