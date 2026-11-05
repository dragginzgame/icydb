//! Test-only helpers for the parent module.

use super::*;

pub(crate) fn continue_recovery<C: CanisterKind>(
    db: &Db<C>,
) -> Result<RecoveryProgress, InternalError> {
    continue_recovery_with_failure_authority(db).map_err(StartupRecoveryFailure::into_error)
}

pub(in crate::db) fn mark_startup_recovery_complete_for_tests(
    stores: &'static LocalKey<StoreRegistry>,
) -> Result<(), InternalError> {
    let key = RecoveryDomainKey {
        commit_allocation: current_commit_memory_allocation()?,
        runtime_stores: runtime_store_domain_key(stores),
    };
    mark_recovery_domain_recovered(key)
}

pub(in crate::db) fn forget_recovered_domain_for_tests<C: CanisterKind>(
    db: &Db<C>,
) -> Result<(), InternalError> {
    let key = recovery_domain_key(db)?;
    clear_recovery_domain_in_progress(key);
    RECOVERED_KEYS.with(|keys| {
        keys.try_borrow_mut()
            .map_err(|_| InternalError::store_invariant())?
            .retain(|existing| *existing != key);
        Ok(())
    })
}
