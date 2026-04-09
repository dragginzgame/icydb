//! Module: db::executor::route::grouped_runtime
//! Responsibility: grouped route runtime projection helpers owned by route authority.
//! Does not own: grouped stream folding or grouped output materialization.
//! Boundary: route-to-runtime grouped observability + metrics execution-mode mapping.

use crate::{
    db::executor::{
        ExecutionPlan,
        route::{GroupedExecutionMode, GroupedRouteDecisionOutcome, GroupedRouteObservability},
    },
    error::InternalError,
    metrics::sink::GroupedExecutionMode as MetricsGroupedExecutionMode,
};

pub(in crate::db::executor) fn grouped_route_observability_for_runtime(
    grouped_route_plan: &ExecutionPlan,
) -> Result<GroupedRouteObservability, InternalError> {
    let grouped_route_observability =
        grouped_route_plan.grouped_observability().ok_or_else(|| {
            InternalError::query_executor_invariant(
                "grouped route planning must emit grouped observability payload",
            )
        })?;
    let grouped_route_outcome = grouped_route_observability.outcome();
    let grouped_route_rejection_reason = grouped_route_observability.rejection_reason();
    let grouped_route_eligible = grouped_route_observability.eligible();

    debug_assert!(
        grouped_route_eligible == grouped_route_rejection_reason.is_none(),
        "grouped route eligibility and rejection reason must stay aligned",
    );
    debug_assert!(
        grouped_route_outcome != GroupedRouteDecisionOutcome::Rejected
            || grouped_route_rejection_reason.is_some(),
        "grouped rejected outcomes must carry a rejection reason",
    );

    Ok(grouped_route_observability)
}

impl From<GroupedExecutionMode> for MetricsGroupedExecutionMode {
    fn from(grouped_execution_mode: GroupedExecutionMode) -> Self {
        match grouped_execution_mode {
            GroupedExecutionMode::HashMaterialized => Self::HashMaterialized,
            GroupedExecutionMode::OrderedMaterialized => Self::OrderedMaterialized,
        }
    }
}
