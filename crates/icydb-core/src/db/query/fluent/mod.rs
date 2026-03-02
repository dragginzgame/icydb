//! Module: query::fluent
//! Responsibility: fluent session-bound query wrappers for load/delete paths.
//! Does not own: query planning internals or predicate semantics.
//! Boundary: ergonomic API layer over query intent/planned execution.

pub(crate) mod delete;
pub(crate) mod load;
