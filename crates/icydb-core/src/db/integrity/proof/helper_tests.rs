//! Test-only helpers for the parent module.

use super::*;

#[expect(
    clippy::unnecessary_wraps,
    reason = "the test backend supplies generation zero while preserving the fallible production signature"
)]
pub(super) fn allocation_registry_generation() -> Result<u64, InternalError> {
    Ok(TEST_ALLOCATION_REGISTRY_GENERATION.with(Cell::get))
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "the test backend has no default ic-storage runtime"
)]
pub(in crate::db::integrity) const fn validate_integrity_allocation_registry()
-> Result<(), InternalError> {
    Ok(())
}

thread_local! {
    pub(super) static TEST_ALLOCATION_REGISTRY_GENERATION: Cell<u64> = const { Cell::new(0) };
}
