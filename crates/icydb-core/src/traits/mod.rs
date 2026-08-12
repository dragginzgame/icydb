//! Module: traits
//!
//! Responsibility: foundational kind, field metadata, and projection
//! contracts awaiting narrower domain ownership.
//! Does not own: entity composition, key taxonomy, runtime value conversion,
//! visitor traversal, executor policy, or public facade DTO behavior.
//! Boundary: remaining reusable contracts consumed throughout `icydb-core`.

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
    /// Stable memory slot used for commit marker storage.
    const COMMIT_MEMORY_ID: u8;

    /// Durable stable-memory allocation key for commit marker storage.
    const COMMIT_STABLE_KEY: &'static str;

    /// Stable memory slot used only for startup coordination state.
    const STARTUP_MEMORY_ID: u8;

    /// Durable stable-memory allocation key for startup coordination state.
    const STARTUP_STABLE_KEY: &'static str;

    /// Stable memory slot used only for integrity-inspection progress.
    const INTEGRITY_PROGRESS_MEMORY_ID: u8;

    /// Durable stable-memory allocation key for integrity-inspection progress.
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str;
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
