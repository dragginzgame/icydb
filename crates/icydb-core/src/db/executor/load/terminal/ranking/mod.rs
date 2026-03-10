//! Module: executor::load::terminal::ranking
//! Responsibility: ranking terminal selection (`min/max` and `*_by`) for load execution.
//! Does not own: planner aggregate semantics or projection-expression evaluation.
//! Boundary: consumes planned slots and returns entity response terminals.

mod by_slot;
mod materialized;
mod take;
