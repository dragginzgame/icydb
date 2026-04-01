//! Module: commit::guard
//! Responsibility: enforce commit-window marker lifecycle and rollback guards.
//! Does not own: mutation planning, marker payload semantics, or recovery orchestration.
//! Boundary: executor::mutation -> commit::guard -> commit::store (one-way).

use crate::{
    db::commit::{
        marker::{COMMIT_ID_BYTES, CommitMarker, CommitRowOp, generate_commit_id},
        store::{CommitStore, with_commit_store, with_commit_store_infallible},
    },
    error::InternalError,
};
use std::panic::{AssertUnwindSafe, catch_unwind};

///
/// CommitApplyGuard
///
/// Executor-internal guard for the commit-marker apply phase.
///
/// This guard is strictly best-effort infrastructure:
/// - Durable atomicity is owned by commit markers + recovery replay.
/// - Rollback closures here are best-effort, in-process cleanup only.
/// - This type does not provide transactional semantics or durable undo.
/// - New code must not rely on closure-based rollback for correctness.
///
/// Long-term direction:
/// marker application should become fully mechanical/idempotent so this guard
/// can be removed without changing user-visible correctness.
///

pub(crate) struct CommitApplyGuard {
    phase: &'static str,
    finished: bool,
    rollback: Option<Box<dyn FnOnce()>>,
}

impl CommitApplyGuard {
    /// Create one apply-phase rollback guard for diagnostic context `phase`.
    pub(crate) const fn new(phase: &'static str) -> Self {
        Self {
            phase,
            finished: false,
            rollback: None,
        }
    }

    pub(crate) fn record_rollback(&mut self, rollback: impl FnOnce() + 'static) {
        let rollback = Box::new(rollback);

        // Keep the hot one-rollback case allocation-light while still
        // preserving reverse-order semantics if a future caller records more
        // than one cleanup closure.
        self.rollback = Some(match self.rollback.take() {
            None => rollback,
            Some(previous) => Box::new(move || {
                rollback();
                previous();
            }),
        });
    }

    /// Mark the guarded apply phase complete and drop rollback closures.
    pub(crate) fn finish(mut self) -> Result<(), InternalError> {
        if self.finished {
            return Err(InternalError::executor_invariant(format!(
                "commit apply guard invariant violated: finish called twice ({})",
                self.phase
            )));
        }

        self.finished = true;
        self.rollback = None;
        Ok(())
    }

    fn rollback_best_effort(&mut self) {
        if self.finished {
            // Defensive: rollback after finish is a logic error, but must not panic.
            return;
        }

        // Best-effort cleanup only:
        // - reverse order to mirror write application
        // - never unwind past this boundary
        while let Some(rollback) = self.rollback.take() {
            let _ = catch_unwind(AssertUnwindSafe(rollback));
        }
    }
}

impl Drop for CommitApplyGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.rollback_best_effort();
        }
    }
}

///
/// CommitGuard
///
/// In-flight commit handle that clears the marker on completion.
/// Must not be leaked across mutation boundaries.
///

#[derive(Clone, Debug)]
pub(crate) struct CommitGuard {
    commit_id: [u8; COMMIT_ID_BYTES],
    marker: Option<CommitMarker>,
}

impl CommitGuard {
    // Create one guard that only needs persisted marker identity.
    const fn for_persisted_id(commit_id: [u8; COMMIT_ID_BYTES]) -> Self {
        Self {
            commit_id,
            marker: None,
        }
    }

    /// Retain one full marker for finish-time consumers that need row ops.
    pub(in crate::db) const fn from_marker(marker: CommitMarker) -> Self {
        let commit_id = marker.id;

        Self {
            commit_id,
            marker: Some(marker),
        }
    }

    /// Borrow the retained marker row ops when the caller explicitly needs them.
    pub(in crate::db) fn row_ops(&self) -> Option<&[CommitRowOp]> {
        self.marker.as_ref().map(|marker| marker.row_ops.as_slice())
    }

    /// Clear the commit marker without surfacing errors.
    fn clear(self) {
        let _ = self;
        with_commit_store_infallible(CommitStore::clear_infallible);
    }
}

/// Persist a commit marker and open the commit window.
pub(crate) fn begin_commit(marker: CommitMarker) -> Result<CommitGuard, InternalError> {
    with_commit_store(|store| {
        // Phase 1: enforce one in-flight marker at a time while preserving any
        // existing migration-state bytes through the same decoded control slot.
        let commit_id = marker.id;
        store.set_if_empty(&marker)?;

        Ok(CommitGuard::for_persisted_id(commit_id))
    })
}

/// Persist one single-row commit marker and open the commit window.
pub(crate) fn begin_single_row_commit(row_op: CommitRowOp) -> Result<CommitGuard, InternalError> {
    with_commit_store(|store| {
        // Phase 1: generate durable marker identity before any stable write.
        let commit_id = generate_commit_id()?;

        // Phase 2: persist the single-row marker directly through the hot path.
        store.set_single_row_op_if_empty(commit_id, &row_op)?;

        Ok(CommitGuard::for_persisted_id(commit_id))
    })
}

/// Persist a commit marker plus migration progress and open the commit window.
///
/// This variant atomically binds migration-step progress to the same durable
/// write as marker persistence, so replay/recovery can never observe a marker
/// without corresponding migration-step ownership.
pub(crate) fn begin_commit_with_migration_state(
    marker: CommitMarker,
    migration_state_bytes: Vec<u8>,
) -> Result<CommitGuard, InternalError> {
    with_commit_store(|store| {
        // Phase 1: enforce one in-flight marker at a time.
        if !store.marker_is_empty()? {
            return Err(InternalError::store_invariant(
                "commit marker already present before begin",
            ));
        }

        // Phase 2: persist marker + migration step progress atomically.
        store.set_with_migration_state(&marker, migration_state_bytes)?;

        Ok(CommitGuard::from_marker(marker))
    })
}

/// Apply commit ops and clear the marker only on successful completion.
///
/// The apply closure performs mechanical marker application only.
/// Any in-process rollback guard used by the closure is non-authoritative
/// cleanup; durable authority remains the commit marker protocol.
///
/// Durability rule:
/// - `Ok(())` => marker is cleared.
/// - `Err(_)` => marker remains persisted for recovery replay.
pub(crate) fn finish_commit(
    mut guard: CommitGuard,
    apply: impl FnOnce(&mut CommitGuard) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    // COMMIT WINDOW:
    // Apply mutates stores from a prevalidated marker payload.
    // Marker durability + recovery replay remain the atomicity authority.
    // We only clear on success; failures keep the marker durable so recovery can
    // re-run the marker payload instead of losing commit authority.
    let result = apply(&mut guard);
    let commit_id = guard.commit_id;
    if result.is_ok() {
        // Phase 1: successful apply must clear marker authority immediately.
        guard.clear();
        // Internal invariant: successful commit windows must clear the marker.
        assert!(
            with_commit_store_infallible(|store| store.is_empty()),
            "commit marker must be cleared after successful finish_commit (commit_id={commit_id:?})"
        );
    } else {
        // Phase 1 (error path): failed apply must preserve marker authority.
        // Internal invariant: failed commit windows must preserve marker authority.
        assert!(
            with_commit_store_infallible(|store| !store.is_empty()),
            "commit marker must remain persisted after failed finish_commit (commit_id={commit_id:?})"
        );
    }

    result
}
