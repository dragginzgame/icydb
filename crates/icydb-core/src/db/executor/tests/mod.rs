//! Module: db::executor::tests
//! Collects executor integration-style unit tests across load, aggregate,
//! route, and mutation behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: wires the executor owner `tests/` suite and keeps support
//! fixtures in a dedicated support module.

mod aggregate_core;
mod aggregate_numeric;
mod aggregate_optimizations;
mod aggregate_path;
mod aggregate_projection;
mod aggregate_tail;
mod continuation_structure;
mod cursor_validation;
mod lifecycle;
mod live_state;
mod load_structure;
mod metrics;
mod mutation_save;
mod ordering;
mod pagination;
mod post_access;
mod reverse_index;
mod semantics;
mod set_access;
mod stale_secondary;
mod support;
