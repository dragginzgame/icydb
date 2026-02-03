use crate::error::{ErrorClass, ErrorOrigin, InternalError};
use std::panic::{AssertUnwindSafe, catch_unwind};

///
/// WriteUnit
///
/// Atomic write scope with rollback-on-failure semantics.
/// Rollbacks are best-effort and must never panic the executor.
///

#[allow(dead_code)]
pub struct WriteUnit {
    label: &'static str,
    applied: bool,
    rollbacks: Vec<Box<dyn FnOnce()>>,
}

impl WriteUnit {
    pub(crate) const fn new(label: &'static str) -> Self {
        Self {
            label,
            applied: false,
            rollbacks: Vec::new(),
        }
    }

    pub(crate) fn record_rollback(&mut self, rollback: impl FnOnce() + 'static) {
        self.rollbacks.push(Box::new(rollback));
    }

    #[cfg(test)]
    pub(crate) fn checkpoint(&mut self, label: &'static str) -> Result<(), InternalError> {
        if should_fail_checkpoint(label) {
            self.rollback();
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!("forced write-unit failure: {} ({label})", self.label),
            ));
        }
        Ok(())
    }

    pub(crate) fn commit(mut self) -> Result<(), InternalError> {
        if self.applied {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                "write unit invariant violated: commit called twice",
            ));
        }

        self.applied = true;
        self.rollbacks.clear();
        Ok(())
    }

    pub(crate) fn rollback(&mut self) {
        if self.applied {
            // Defensive: rollback after commit is a logic error,
            // but must never panic the executor.
            return;
        }

        // Rollbacks are best-effort:
        // - must run in reverse order
        // - must never unwind past this boundary
        while let Some(rollback) = self.rollbacks.pop() {
            let _ = catch_unwind(AssertUnwindSafe(rollback));
        }
    }
}

impl Drop for WriteUnit {
    fn drop(&mut self) {
        if !self.applied {
            self.rollback();
        }
    }
}

//
// TEST FAIL INJECTION
//

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_CHECKPOINT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_CHECKPOINT_LABEL: std::cell::Cell<Option<&'static str>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub fn fail_next_checkpoint() {
    FAIL_NEXT_CHECKPOINT.with(|flag| flag.set(true));
}

#[cfg(test)]
pub fn fail_checkpoint_label(label: &'static str) {
    FAIL_CHECKPOINT_LABEL.with(|slot| slot.set(Some(label)));
}

#[cfg(test)]
#[expect(clippy::missing_const_for_fn)]
fn should_fail_checkpoint(label: &'static str) -> bool {
    #[cfg(test)]
    {
        let fail_next = FAIL_NEXT_CHECKPOINT.with(|flag| {
            let fail = flag.get();
            flag.set(false);
            fail
        });
        if fail_next {
            return true;
        }

        FAIL_CHECKPOINT_LABEL.with(|slot| {
            let fail = slot.get() == Some(label);
            if fail {
                slot.set(None);
            }
            fail
        })
    }

    #[cfg(not(test))]
    {
        false
    }
}
