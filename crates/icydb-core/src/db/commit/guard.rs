use crate::error::{ErrorClass, ErrorOrigin, InternalError};
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

pub struct CommitApplyGuard {
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
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "commit apply guard invariant violated: finish called twice ({})",
                    self.phase
                ),
            ));
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
