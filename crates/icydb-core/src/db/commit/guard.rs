use crate::{
    db::commit::{
        marker::CommitMarker,
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
/// This guard is strictly transitional infrastructure:
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
    rollbacks: Vec<Box<dyn FnOnce()>>,
}

impl CommitApplyGuard {
    pub(crate) const fn new(phase: &'static str) -> Self {
        Self {
            phase,
            finished: false,
            rollbacks: Vec::new(),
        }
    }

    pub(crate) fn record_rollback(&mut self, rollback: impl FnOnce() + 'static) {
        self.rollbacks.push(Box::new(rollback));
    }

    pub(crate) fn finish(mut self) -> Result<(), InternalError> {
        if self.finished {
            return Err(InternalError::executor_invariant(format!(
                "commit apply guard invariant violated: finish called twice ({})",
                self.phase
            )));
        }

        self.finished = true;
        self.rollbacks.clear();
        Ok(())
    }

    fn rollback_best_effort(&mut self) {
        if self.finished {
            // Defensive: rollback after finish is a logic error, but must not panic.
            return;
        }

        // Transitional cleanup only:
        // - reverse order to mirror write application
        // - never unwind past this boundary
        while let Some(rollback) = self.rollbacks.pop() {
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
    pub(crate) marker: CommitMarker,
}

impl CommitGuard {
    // Clear the commit marker without surfacing errors.
    fn clear(self) {
        let _ = self;
        with_commit_store_infallible(CommitStore::clear_infallible);
    }
}

/// Persist a commit marker and open the commit window.
pub(crate) fn begin_commit(marker: CommitMarker) -> Result<CommitGuard, InternalError> {
    with_commit_store(|store| {
        if store.load()?.is_some() {
            return Err(InternalError::store_invariant(
                "commit marker already present before begin",
            ));
        }
        store.set(&marker)?;

        Ok(CommitGuard { marker })
    })
}

/// Apply commit ops and clear the marker only on successful completion.
///
/// The apply closure performs mechanical marker application only.
/// Any in-process rollback guard used by the closure is non-authoritative
/// transitional cleanup; durable authority remains the commit marker protocol.
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
    let commit_id = guard.marker.id;
    if result.is_ok() {
        guard.clear();
        // Internal invariant: successful commit windows must clear the marker.
        assert!(
            with_commit_store_infallible(|store| store.is_empty()),
            "commit marker must be cleared after successful finish_commit (commit_id={commit_id:?})"
        );
    } else {
        // Internal invariant: failed commit windows must preserve marker authority.
        assert!(
            with_commit_store_infallible(|store| !store.is_empty()),
            "commit marker must remain persisted after failed finish_commit (commit_id={commit_id:?})"
        );
    }

    result
}
