//! Module: executor::planning::route::contracts::execution::observability
//! Responsibility: grouped route observability DTOs.
//! Does not own: route decision derivation or planner fallback classification.
//! Boundary: carries route outcome metadata into explain and diagnostics surfaces.

use crate::db::executor::planning::route::contracts::execution::{
    GroupedExecutionMode, RouteExecutionMode,
};
use crate::db::query::plan::GroupedPlanFallbackReason;

///
/// GroupedRouteDecisionOutcome
///
/// Grouped route decision outcome surface.
/// Keeps grouped route diagnostics aligned with route selection semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedRouteDecisionOutcome {
    Rejected,
    MaterializedFallback,
}

impl GroupedRouteDecisionOutcome {
    /// Return the stable observability code for this grouped route outcome.
    #[must_use]
    pub(in crate::db::executor) const fn code(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::MaterializedFallback => "materialized_fallback",
        }
    }
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

impl GroupedRouteRejectionReason {
    /// Return the stable observability code for this grouped route rejection reason.
    #[must_use]
    pub(in crate::db::executor) const fn code(self) -> &'static str {
        match self {
            Self::CapabilityMismatch => "capability_mismatch",
        }
    }
}

///
/// GroupedRouteObservability
///
/// Grouped route observability payload.
/// Carries route outcome, optional rejection reason, eligibility, and
/// selected execution mode for grouped intents.
/// Planner-authored grouped fallback reasons stay separate from route-gate
/// rejection reasons so runtime/explain surfaces do not collapse planning and
/// capability failure into one taxonomy.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedRouteObservability {
    pub(in crate::db::executor::planning::route) outcome: GroupedRouteDecisionOutcome,
    pub(in crate::db::executor::planning::route) rejection_reason:
        Option<GroupedRouteRejectionReason>,
    pub(in crate::db::executor::planning::route) planner_fallback_reason:
        Option<GroupedPlanFallbackReason>,
    pub(in crate::db::executor::planning::route) eligible: bool,
    pub(in crate::db::executor::planning::route) execution_mode: RouteExecutionMode,
    pub(in crate::db::executor::planning::route) grouped_execution_mode: GroupedExecutionMode,
}

impl GroupedRouteObservability {
    /// Return the grouped route decision outcome.
    #[must_use]
    pub(in crate::db::executor) const fn outcome(self) -> GroupedRouteDecisionOutcome {
        self.outcome
    }

    /// Return the route-gate rejection reason, when grouped routing was rejected.
    #[must_use]
    pub(in crate::db::executor) const fn rejection_reason(
        self,
    ) -> Option<GroupedRouteRejectionReason> {
        self.rejection_reason
    }

    /// Return the planner-owned grouped fallback reason, when present.
    #[must_use]
    pub(in crate::db::executor) const fn planner_fallback_reason(
        self,
    ) -> Option<GroupedPlanFallbackReason> {
        self.planner_fallback_reason
    }

    /// Return whether grouped route planning considered this route eligible.
    #[must_use]
    pub(in crate::db::executor) const fn eligible(self) -> bool {
        self.eligible
    }

    /// Return the selected route execution mode.
    #[must_use]
    pub(in crate::db::executor) const fn execution_mode(self) -> RouteExecutionMode {
        self.execution_mode
    }

    /// Return the selected grouped execution mode.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_execution_mode(self) -> GroupedExecutionMode {
        self.grouped_execution_mode
    }
}
