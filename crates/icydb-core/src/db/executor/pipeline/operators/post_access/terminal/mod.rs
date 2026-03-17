//! Module: executor::pipeline::operators::post_access::terminal
//! Responsibility: terminal-phase ordering/paging/delete-limit application for post-access rows.
//! Does not own: predicate filtering, cursor-boundary execution, or planner policy authority.
//! Boundary: applies terminal materialization phases over rows from post-access coordination.

mod order;
mod paging_delete;

pub(in crate::db::executor::pipeline::operators::post_access) use order::apply_order_phase;
pub(in crate::db::executor::pipeline::operators::post_access) use paging_delete::apply_delete_limit_phase;
