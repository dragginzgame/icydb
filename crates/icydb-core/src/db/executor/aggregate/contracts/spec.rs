//! Module: executor::aggregate::contracts::spec
//! Responsibility: aggregate spec/output contract types.
//! Does not own: grouped logical-spec validation or aggregate reducer state machines.
//! Boundary: declarative aggregate contracts consumed by state/grouped modules.

// -----------------------------------------------------------------------------
// Execution-Agnostic Guard
// -----------------------------------------------------------------------------
// This module must remain execution-agnostic.
// No imports from executor load/kernel/route are allowed.

pub(in crate::db::executor) use super::plan::AggregateKind;

impl AggregateKind {}
