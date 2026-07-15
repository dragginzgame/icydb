//! Module: executor::planning::route::contracts::capability_facts
//! Responsibility: route capability fact snapshots.
//! Does not own: capability derivation algorithms or execution dispatch.
//! Boundary: exposes immutable capability facts consumed by route gates and hints.

use crate::db::executor::{
    aggregate::capability::AggregateFieldExtremaIneligibilityReason,
    route::{LoadOrderRouteDecision, LoadOrderRouteMode, LoadOrderRouteReason},
};

///
/// RouteCapabilityFacts
///
/// Canonical derived capability-fact snapshot for one logical plan and direction.
/// Route planning derives this once, then consumes it for eligibility and hint
/// decisions to reduce drift across helpers.
///

#[expect(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct RouteCapabilityFacts {
    pub(in crate::db::executor) load_order_route_decision: LoadOrderRouteDecision,
    pub(in crate::db::executor) ordered_index_leaf_stream_eligible: bool,
    pub(in crate::db::executor) pk_order_fast_path_eligible: bool,
    pub(in crate::db::executor) count_pushdown_shape_supported: bool,
    pub(in crate::db::executor) composite_aggregate_fast_path_eligible: bool,
    pub(in crate::db::executor) residual_filter_present: bool,
    pub(in crate::db::executor) bounded_probe_hint_safe: bool,
    pub(in crate::db::executor) field_min_fast_path_eligible: bool,
    pub(in crate::db::executor) field_max_fast_path_eligible: bool,
    pub(in crate::db::executor) field_min_fast_path_ineligibility_reason:
        Option<AggregateFieldExtremaIneligibilityReason>,
    pub(in crate::db::executor) field_max_fast_path_ineligibility_reason:
        Option<AggregateFieldExtremaIneligibilityReason>,
}

impl RouteCapabilityFacts {
    /// Return the ordered-load route mode selected by capability derivation.
    #[must_use]
    pub(in crate::db::executor) const fn load_order_route_mode(self) -> LoadOrderRouteMode {
        self.load_order_route_decision.mode()
    }

    /// Return the explanation for the ordered-load route mode decision.
    #[must_use]
    pub(in crate::db::executor) const fn load_order_route_reason(self) -> LoadOrderRouteReason {
        self.load_order_route_decision.reason()
    }

    /// Return whether the route retains a residual filter after access pushdown.
    #[must_use]
    pub(in crate::db::executor) const fn residual_filter_present(self) -> bool {
        self.residual_filter_present
    }
}
