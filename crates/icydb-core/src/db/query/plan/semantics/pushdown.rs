//! Module: db::query::plan::semantics::pushdown
//! Responsibility: module-local ownership and contracts for db::query::plan::semantics::pushdown.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::query::plan::{AccessPlannedQuery, DeterministicSecondaryOrderContract};

///
/// LogicalPushdownEligibility
///
/// Planner-owned logical pushdown contract projected once from validated
/// query semantics. Route/executor layers combine this contract with runtime
/// access capabilities and must not re-derive logical shape rules.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct LogicalPushdownEligibility {
    secondary_order_allowed: bool,
    grouped_aggregate_allowed: bool,
    requires_full_materialization: bool,
}

impl LogicalPushdownEligibility {
    /// Construct one planner-owned logical pushdown contract.
    #[must_use]
    pub(in crate::db) const fn new(
        secondary_order_allowed: bool,
        grouped_aggregate_allowed: bool,
        requires_full_materialization: bool,
    ) -> Self {
        Self {
            secondary_order_allowed,
            grouped_aggregate_allowed,
            requires_full_materialization,
        }
    }

    /// Return true when logical secondary ORDER BY pushdown is admissible.
    #[must_use]
    pub(in crate::db) const fn secondary_order_allowed(self) -> bool {
        self.secondary_order_allowed
    }

    /// Return true when grouped aggregate pushdown semantics are admissible.
    #[must_use]
    pub(in crate::db) const fn grouped_aggregate_allowed(self) -> bool {
        self.grouped_aggregate_allowed
    }

    /// Return true when logical semantics force full materialization.
    #[must_use]
    pub(in crate::db) const fn requires_full_materialization(self) -> bool {
        self.requires_full_materialization
    }
}

/// Derive planner-owned logical pushdown eligibility from validated semantics.
#[must_use]
pub(in crate::db) fn derive_logical_pushdown_eligibility(
    plan: &AccessPlannedQuery,
    secondary_order_contract: Option<&DeterministicSecondaryOrderContract>,
) -> LogicalPushdownEligibility {
    LogicalPushdownEligibility::new(
        secondary_order_contract.is_some(),
        plan.grouped_plan().is_some(),
        false,
    )
}
