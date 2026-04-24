//! Module: db::executor::planning::route::contracts::capabilities
//! Defines the execution capabilities attached to planned executor routes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{
    aggregate::capability::AggregateFieldExtremaIneligibilityReason,
    route::{LoadOrderRouteContract, LoadOrderRouteDecision, LoadOrderRouteReason},
};

///
/// FieldExtremaIneligibilityReason
///
/// Route-surfaced alias of aggregate-policy field-extrema ineligibility reasons.
/// This preserves route diagnostics while aggregate capability policy owns derivation.
///

pub(in crate::db::executor) type FieldExtremaIneligibilityReason =
    AggregateFieldExtremaIneligibilityReason;

///
/// RouteCapabilities
///
/// Canonical derived capability snapshot for one logical plan and direction.
/// Route planning derives this once, then consumes it for eligibility and hint
/// decisions to reduce drift across helpers.
///

#[expect(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct RouteCapabilities {
    pub(in crate::db::executor) load_order_route_decision: LoadOrderRouteDecision,
    pub(in crate::db::executor) pk_order_fast_path_eligible: bool,
    pub(in crate::db::executor) count_pushdown_shape_supported: bool,
    pub(in crate::db::executor) composite_aggregate_fast_path_eligible: bool,
    pub(in crate::db::executor) residual_filter_present: bool,
    pub(in crate::db::executor) bounded_probe_hint_safe: bool,
    pub(in crate::db::executor) field_min_fast_path_eligible: bool,
    pub(in crate::db::executor) field_max_fast_path_eligible: bool,
    pub(in crate::db::executor) field_min_fast_path_ineligibility_reason:
        Option<FieldExtremaIneligibilityReason>,
    pub(in crate::db::executor) field_max_fast_path_ineligibility_reason:
        Option<FieldExtremaIneligibilityReason>,
}

impl RouteCapabilities {
    #[must_use]
    pub(in crate::db::executor) const fn load_order_route_contract(self) -> LoadOrderRouteContract {
        self.load_order_route_decision.contract()
    }

    #[must_use]
    pub(in crate::db::executor) const fn load_order_route_reason(self) -> LoadOrderRouteReason {
        self.load_order_route_decision.reason()
    }

    #[must_use]
    pub(in crate::db::executor) const fn residual_filter_present(self) -> bool {
        self.residual_filter_present
    }
}
