//! Module: executor::pipeline::operators::post_access::coordinator::runtime
//! Responsibility: runtime-phase application for post-access execution coordination.
//! Does not own: budget-safety metadata derivation or terminal operator internals.
//! Boundary: executes post-access phases over one prepared plan wrapper.

mod guard_filter;
mod phases;
