//! Module: plan
//! Responsibility: small route-planning contract for style examples.
//! Does not own: catalog mutation admission or execution.
//! Boundary: names the route selected by an owner module.

mod route;

pub use route::{PlanRoute, PlanRouteKind};
