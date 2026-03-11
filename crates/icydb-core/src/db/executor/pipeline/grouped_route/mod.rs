//! Module: executor::pipeline::grouped_route
//! Responsibility: grouped route-stage derivation and layout invariant checks.
//! Does not own: grouped stream folding or grouped output materialization.
//! Boundary: planner handoff extraction + route observability normalization.

mod metrics;
mod resolve;
