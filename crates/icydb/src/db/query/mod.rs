//! Public facade query surface.
//!
//! Re-exports the typed query builders, semantic query types, and facade-only
//! helper modules used by downstream canister code.

pub use icydb_core::db::{
    AggregateExpr, CompareOp, CompiledQuery, ExplainPlan, FieldRef, FilterExpr, FilterValue,
    MissingRowPolicy, NumericProjectionExpr, OrderDirection, OrderExpr, OrderTerm, PlannedQuery,
    Query, QueryTracePlan, RoundProjectionExpr, TextProjectionExpr, TraceExecutionFamily,
    TraceReuseArtifactClass, TraceReuseEvent, ValueProjectionExpr, add, asc, avg, contains, count,
    count_by, desc, div, ends_with, exists, field, first, last, left, length, lower, ltrim, max,
    max_by, min, min_by, mul, position, replace, right, round, round_expr, rtrim, starts_with, sub,
    substring, substring_with_length, sum, trim, upper,
};
