//! Public facade query surface.
//!
//! Re-exports the typed query builders, semantic query types, and facade-only
//! helper modules used by downstream canister code.

pub mod expr;

pub use expr::{FilterExpr, OrderDirection, SortExpr};
pub use icydb_core::db::{
    AggregateExpr, CompareOp, CompiledQuery, ExplainPlan, FieldRef, MissingRowPolicy, PlannedQuery,
    Predicate, Query, QueryTracePlan, TextProjectionExpr, TextProjectionTransform,
    TraceExecutionFamily, avg, contains, count, count_by, ends_with, exists, first, last, left,
    length, lower, ltrim, max, max_by, min, min_by, position, replace, right, rtrim, starts_with,
    substring, substring_with_length, sum, trim, upper,
};

/// Field-reference and aggregate helpers exposed by the facade query API.
pub mod builder {
    pub use icydb_core::db::{
        AggregateExpr, FieldRef, TextProjectionExpr, TextProjectionTransform, avg, contains, count,
        count_by, ends_with, exists, first, last, left, length, lower, ltrim, max, max_by, min,
        min_by, position, replace, right, rtrim, starts_with, substring, substring_with_length,
        sum, trim, upper,
    };
}

/// Predicate type exposed at the facade query boundary.
pub mod predicate {
    pub use icydb_core::db::Predicate;
}
