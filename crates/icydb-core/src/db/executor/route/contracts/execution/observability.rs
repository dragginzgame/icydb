//! Module: db::executor::route::contracts::execution::observability
//! Responsibility: module-local ownership and contracts for db::executor::route::contracts::execution::observability.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::route::contracts::execution::{
    GroupedExecutionStrategy, RouteExecutionMode,
};

///
/// GroupedRouteDecisionOutcome
///
/// Grouped route decision outcome surface.
/// Keeps grouped route diagnostics aligned with route selection semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedRouteDecisionOutcome {
    Selected,
    Rejected,
    MaterializedFallback,
}

///
/// GroupedRouteRejectionReason
///
/// Grouped route rejection taxonomy.
/// These reasons are route-owned and represent route-gate failures only.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedRouteRejectionReason {
    CapabilityMismatch,
}

///
/// GroupedRouteObservability
///
/// Grouped route observability payload.
/// Carries route outcome, optional rejection reason, eligibility, and
/// selected execution mode for grouped intents.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedRouteObservability {
    pub(in crate::db::executor::route) outcome: GroupedRouteDecisionOutcome,
    pub(in crate::db::executor::route) rejection_reason: Option<GroupedRouteRejectionReason>,
    pub(in crate::db::executor::route) eligible: bool,
    pub(in crate::db::executor::route) execution_mode: RouteExecutionMode,
    pub(in crate::db::executor::route) grouped_execution_strategy: GroupedExecutionStrategy,
}

impl GroupedRouteObservability {
    #[must_use]
    pub(in crate::db::executor) const fn outcome(self) -> GroupedRouteDecisionOutcome {
        self.outcome
    }

    #[must_use]
    pub(in crate::db::executor) const fn rejection_reason(
        self,
    ) -> Option<GroupedRouteRejectionReason> {
        self.rejection_reason
    }

    #[must_use]
    pub(in crate::db::executor) const fn eligible(self) -> bool {
        self.eligible
    }

    #[must_use]
    pub(in crate::db::executor) const fn execution_mode(self) -> RouteExecutionMode {
        self.execution_mode
    }

    #[must_use]
    pub(in crate::db::executor) const fn grouped_execution_strategy(
        self,
    ) -> GroupedExecutionStrategy {
        self.grouped_execution_strategy
    }
}
