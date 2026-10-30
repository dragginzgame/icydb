//! Module: traits
//!
//! Responsibility: foundational kind, field metadata, and projection
//! contracts awaiting narrower domain ownership.
//! Does not own: entity composition, key taxonomy, runtime value conversion,
//! visitor traversal, executor policy, or public facade DTO behavior.
//! Boundary: remaining reusable contracts consumed throughout `icydb-core`.

use crate::db::memory_runtime;

// ============================================================================
// FOUNDATIONAL KINDS
// ============================================================================
//
// These traits define *where* something lives in the system,
// not what data it contains.
//

///
/// Path
/// Fully-qualified schema path.
///

pub trait Path {
    const PATH: &'static str;
}

///
/// CanisterKind
/// Marker for canister namespaces
///

pub trait CanisterKind: Path + 'static {
    /// Resolve the commit slot from committed allocation authority.
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        memory_runtime()
            .ok_or(ic_memory::RuntimeOpenError::NotBootstrapped)?
            .memory_id(Self::COMMIT_STABLE_KEY)
    }

    /// Durable stable-memory allocation key for commit marker storage.
    const COMMIT_STABLE_KEY: &'static str;

    /// Resolve the startup slot from committed allocation authority.
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        memory_runtime()
            .ok_or(ic_memory::RuntimeOpenError::NotBootstrapped)?
            .memory_id(Self::STARTUP_STABLE_KEY)
    }

    /// Durable stable-memory allocation key for startup coordination state.
    const STARTUP_STABLE_KEY: &'static str;

    /// Resolve the integrity slot from committed allocation authority.
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        memory_runtime()
            .ok_or(ic_memory::RuntimeOpenError::NotBootstrapped)?
            .memory_id(Self::INTEGRITY_PROGRESS_STABLE_KEY)
    }

    /// Durable stable-memory allocation key for integrity-inspection progress.
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str;

    /// Resolve the current generated receipt that must survive terminal retention.
    ///
    /// Generated canisters bind this to their immutable submission and registry.
    /// Canisters without generated readiness have no receipt to protect. The
    /// control allocation is selected before this read-only hook is invoked.
    #[doc(hidden)]
    fn generated_schema_application_record_key()
    -> Result<Option<[u8; 32]>, crate::error::InternalError> {
        Ok(None)
    }
}

///
/// Repr
///
/// Internal representation boundary for scalar wrapper types.
///

pub trait Repr {
    type Inner;

    fn repr(&self) -> Self::Inner;
    fn from_repr(inner: Self::Inner) -> Self;
}
