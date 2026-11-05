//! Test-only helpers for the parent module.

use super::*;

// Core unit fixtures exercise commit mechanics without generated lifecycle
// wiring; generated-canister tests still install and exercise the real hook.
pub(super) const fn noop_startup_recovery_wakeup() {}

pub(super) const DEFAULT_STARTUP_RECOVERY_WAKEUP: Option<fn()> = Some(noop_startup_recovery_wakeup);

thread_local! {
    // Existing native fault fixtures return errors to inspect retained state.
    // Child-process qualification disables that exception to exercise the same
    // terminating path used unconditionally by production.
    pub(super) static RETURN_COMMIT_FAILURES_FOR_TESTS: Cell<bool> = const { Cell::new(true) };
}

///
/// ApplyRollback
///
/// Best-effort rollback payload owned by one in-flight apply guard.
/// This remains intentionally narrow:
/// - one closure for the batch rollback path
/// - one prepared row op for the single-row hot path
/// - no transactional semantics beyond "try to unwind local process state"
///

pub(super) enum ApplyRollback {
    None,
    Closure(Box<dyn FnOnce()>),
    SinglePreparedRow(PreparedRowCommitOp),
}

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
    pub(super) finished: bool,
    pub(super) rollback: ApplyRollback,
}

impl CommitApplyGuard {
    /// Create one apply-phase rollback guard.
    pub(crate) const fn new() -> Self {
        Self {
            finished: false,
            rollback: ApplyRollback::None,
        }
    }

    pub(crate) fn record_rollback(&mut self, rollback: impl FnOnce() + 'static) {
        debug_assert!(
            matches!(self.rollback, ApplyRollback::None),
            "commit apply guard currently owns exactly one rollback closure",
        );

        if matches!(self.rollback, ApplyRollback::None) {
            self.rollback = ApplyRollback::Closure(Box::new(rollback));
        }
    }

    /// Record one prepared row-op rollback snapshot for the single-row hot path.
    pub(crate) fn record_single_row_rollback(&mut self, rollback: PreparedRowCommitOp) {
        debug_assert!(
            matches!(self.rollback, ApplyRollback::None),
            "commit apply guard currently owns exactly one rollback payload",
        );

        if matches!(self.rollback, ApplyRollback::None) {
            self.rollback = ApplyRollback::SinglePreparedRow(rollback);
        }
    }

    /// Mark the guarded apply phase complete and drop rollback closures.
    pub(crate) fn finish(mut self) -> Result<(), InternalError> {
        if self.finished {
            return Err(InternalError::executor_invariant());
        }

        self.finished = true;
        self.rollback = ApplyRollback::None;
        Ok(())
    }

    pub(super) fn rollback_best_effort(&mut self) {
        if self.finished {
            // Defensive: rollback after finish is a logic error, but must not panic.
            return;
        }

        // Best-effort cleanup only:
        // - execute the one caller-owned rollback payload
        // - never unwind past this boundary
        match std::mem::replace(&mut self.rollback, ApplyRollback::None) {
            ApplyRollback::None => {}
            ApplyRollback::Closure(rollback) => {
                let _ = catch_unwind(AssertUnwindSafe(rollback));
            }
            ApplyRollback::SinglePreparedRow(rollback) => {
                let _ = catch_unwind(AssertUnwindSafe(|| rollback.apply()));
            }
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
