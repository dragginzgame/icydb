use crate::error::{ErrorClass, ErrorOrigin, InternalError};
use std::panic::{AssertUnwindSafe, catch_unwind};

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

    pub(crate) fn run<T>(
        &mut self,
        step: impl FnOnce() -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        match step() {
            Ok(value) => Ok(value),
            Err(err) => {
                self.rollback();
                Err(err)
            }
        }
    }

    pub(crate) fn record_rollback(&mut self, rollback: impl FnOnce() + 'static) {
        self.rollbacks.push(Box::new(rollback));
    }

    pub(crate) fn checkpoint(&mut self, label: &'static str) -> Result<(), InternalError> {
        if should_fail_checkpoint(label) {
            self.rollback();
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Executor,
                format!("forced failure: {} ({label})", self.label),
            ));
        }
        Ok(())
    }

    pub(crate) fn commit(mut self) {
        // Internal invariant: a write unit can only be committed once.
        assert!(
            !self.applied,
            "write unit invariant violated: commit called twice"
        );
        self.applied = true;
        self.rollbacks.clear();
    }

    pub(crate) fn rollback(&mut self) {
        // Internal invariant: rollbacks must not run after commit.
        assert!(
            !self.applied,
            "write unit invariant violated: rollback called after commit"
        );
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

#[allow(clippy::missing_const_for_fn)]
#[allow(unused_variables)]
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
