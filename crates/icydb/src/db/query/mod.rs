//! Module: db::query
//!
//! Responsibility: public query facade re-exports.
//! Does not own: query planning, validation, or execution semantics.
//! Boundary: exposes stable core query DSL types through the facade crate.

//! Public facade query surface.
//!
//! Re-exports the typed query builders, semantic query types, and facade-only
//! helper modules used by downstream canister code.

pub use icydb_core::db::{
    AccessRequirementError, AccessRequirementViolation, AggregateExpr, CompareOp, CompiledQuery,
    ExplainAccessCandidateV1, ExplainAccessDecisionKind, ExplainAccessDecisionV1,
    ExplainEligibleAlternativeV1, ExplainPlan, ExplainRejectedIndexV1, ExplainResidualSummaryV1,
    ExplainSelectedAccessV1, FieldRef, FilterExpr, FilterValue, MissingRowPolicy,
    NumericProjectionExpr, OrderDirection, OrderExpr, OrderTerm, PlannedQuery, QueryTracePlan,
    RequiredAccessPath, RoundProjectionExpr, TextProjectionExpr, TraceExecutionFamily,
    TraceReuseArtifactClass, TraceReuseEvent, ValueProjectionExpr, add, asc, avg, contains, count,
    count_by, desc, div, ends_with, exists, field, first, last, left, length, lower, ltrim, max,
    max_by, min, min_by, mul, position, replace, right, round, round_expr, rtrim, starts_with, sub,
    substring, substring_with_length, sum, trim, upper,
};

// Low-level direct-query intent remains available to facade internals and
// diagnostics. Normal endpoint code should use `DbSession::load::<E>()` plus a
// semantic terminal instead of constructing `Query<E>` directly.
#[doc(hidden)]
pub use icydb_core::db::Query;
