//! Module: db::executor::planning::continuation::capabilities
//! Defines the continuation capability checks used to decide whether one
//! execution shape can safely support cursors.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{executor::ContinuationMode, query::plan::ContinuationPolicy};

///
/// ContinuationCapabilities
///
/// Immutable continuation capability projection derived once from scalar
/// continuation runtime shape plus planner continuation policy.
/// Route/load consumers read this contract instead of re-deriving policy gates.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
pub(in crate::db::executor) struct ContinuationCapabilities {
    mode: ContinuationMode,
    applied: bool,
    strict_advance_required_when_applied: bool,
    grouped_safe_when_applied: bool,
    index_range_limit_pushdown_allowed: bool,
}

impl ContinuationCapabilities {
    /// Construct one immutable continuation capability projection.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        mode: ContinuationMode,
        continuation_policy: ContinuationPolicy,
    ) -> Self {
        let applied = !matches!(mode, ContinuationMode::Initial);

        Self {
            mode,
            applied,
            strict_advance_required_when_applied: !applied
                || continuation_policy.requires_strict_advance(),
            grouped_safe_when_applied: !applied || continuation_policy.is_grouped_safe(),
            index_range_limit_pushdown_allowed: !continuation_policy.requires_anchor()
                || !matches!(mode, ContinuationMode::CursorBoundary),
        }
    }

    /// Return route continuation mode projected by this capability snapshot.
    #[must_use]
    pub(in crate::db::executor) const fn mode(self) -> ContinuationMode {
        self.mode
    }

    /// Return whether continuation is applied for this execution.
    #[must_use]
    pub(in crate::db::executor) const fn applied(self) -> bool {
        self.applied
    }

    /// Return whether strict advancement is required under continuation.
    #[must_use]
    pub(in crate::db::executor) const fn strict_advance_required_when_applied(self) -> bool {
        self.strict_advance_required_when_applied
    }

    /// Return whether grouped continuation remains safe under this policy.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_safe_when_applied(self) -> bool {
        self.grouped_safe_when_applied
    }

    /// Return whether index-range limit pushdown can remain enabled.
    #[must_use]
    pub(in crate::db::executor) const fn index_range_limit_pushdown_allowed(self) -> bool {
        self.index_range_limit_pushdown_allowed
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        executor::{ContinuationCapabilities, ContinuationMode},
        query::plan::ContinuationPolicy,
    };

    #[test]
    fn continuation_capabilities_apply_policy_for_initial_mode() {
        let capabilities = ContinuationCapabilities::new(
            ContinuationMode::Initial,
            ContinuationPolicy::new(true, false, false),
        );

        assert!(!capabilities.applied());
        assert!(
            capabilities.strict_advance_required_when_applied(),
            "initial mode must satisfy strict-advance invariants unconditionally",
        );
        assert!(
            capabilities.grouped_safe_when_applied(),
            "initial mode must satisfy grouped safety invariants unconditionally",
        );
        assert!(
            capabilities.index_range_limit_pushdown_allowed(),
            "initial mode must not disable index-range limit pushdown",
        );
    }

    #[test]
    fn continuation_capabilities_disable_index_range_pushdown_for_cursor_boundary_anchor_policy() {
        let capabilities = ContinuationCapabilities::new(
            ContinuationMode::CursorBoundary,
            ContinuationPolicy::new(true, true, true),
        );

        assert!(capabilities.applied());
        assert!(capabilities.strict_advance_required_when_applied());
        assert!(capabilities.grouped_safe_when_applied());
        assert!(
            !capabilities.index_range_limit_pushdown_allowed(),
            "cursor-boundary mode with anchor-required policy must disable index-range pushdown",
        );
    }

    #[test]
    fn continuation_capabilities_keep_index_range_pushdown_for_anchor_mode() {
        let capabilities = ContinuationCapabilities::new(
            ContinuationMode::IndexRangeAnchor,
            ContinuationPolicy::new(true, true, true),
        );

        assert_eq!(capabilities.mode(), ContinuationMode::IndexRangeAnchor);
        assert!(capabilities.applied());
        assert!(capabilities.index_range_limit_pushdown_allowed());
    }
}
