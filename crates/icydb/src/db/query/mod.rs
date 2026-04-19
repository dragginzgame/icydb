//! Public facade query surface.
//!
//! Re-exports the typed query builders, semantic query types, and facade-only
//! helper modules used by downstream canister code.

pub mod expr;

pub use expr::{FilterExpr, OrderExpr, OrderTerm, asc, desc, field};
pub use icydb_core::db::{
    AggregateExpr, CompareOp, CompiledQuery, ExplainPlan, FieldRef, MissingRowPolicy,
    NumericProjectionExpr, OrderDirection, PlannedQuery, Query, QueryTracePlan,
    RoundProjectionExpr, TextProjectionExpr, TraceExecutionFamily, ValueProjectionExpr, add, avg,
    contains, count, count_by, div, ends_with, exists, first, last, left, length, lower, ltrim,
    max, max_by, min, min_by, mul, position, replace, right, round, round_expr, rtrim, starts_with,
    sub, substring, substring_with_length, sum, trim, upper,
};

/// Field-reference and aggregate helpers exposed by the facade query API.
pub mod builder {
    pub use icydb_core::db::{
        AggregateExpr, FieldRef, NumericProjectionExpr, RoundProjectionExpr, TextProjectionExpr,
        ValueProjectionExpr, add, avg, contains, count, count_by, div, ends_with, exists, first,
        last, left, length, lower, ltrim, max, max_by, min, min_by, mul, position, replace, right,
        round, round_expr, rtrim, starts_with, sub, substring, substring_with_length, sum, trim,
        upper,
    };
}
