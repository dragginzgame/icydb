//! Module: executor::pipeline::operators::post_access::contracts
//! Responsibility: row abstraction and stats contracts for post-access execution.
//! Does not own: post-access phase orchestration or terminal operator mechanics.
//! Boundary: exports post-access runtime/test contracts consumed by operators.

mod row;
mod stats;

pub(in crate::db::executor) use stats::PostAccessStats;
