//! Test-only constructors and adapters for the parent module.

use super::*;

impl<C: CanisterKind> Db<C> {
    /// Advance startup recovery by one durable bounded page in focused tests.
    pub(crate) fn drive_startup_recovery_page(&self) -> Result<bool, InternalError> {
        commit::continue_recovery(self)
            .map(|progress| progress == commit::RecoveryProgress::Complete)
    }
}
