//! Module: db::query
//!
//! Responsibility: public query facade re-exports.
//! Does not own: query planning, validation, or execution semantics.
//! Boundary: exposes stable core query DSL types through the facade crate.

//! Public facade query surface. Generated typed reads are a projection over
//! the accepted structural lane; the raw planner representation stays internal.

mod typed;

pub use typed::{
    ExhaustivePage, LivePage, MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES,
    MAX_TYPED_EXACT_KEY_BATCH_ITEMS, MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES,
    MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES, Query, TypedExhaustiveQueryError,
};

pub use icydb_core::db::{
    AggregateExpr, CollectionOperator, CompareOp, CompareOperator, ExplainAccessCandidate,
    ExplainAccessDecision, ExplainAccessDecisionKind, ExplainEligibleAlternative, ExplainPlan,
    ExplainRejectedIndex, ExplainResidualSummary, ExplainSelectedAccess, FieldCompareOperator,
    FieldRef, FilterExpr, FilterValue, JunctionOperator, MissingRowPolicy, NumericProjectionExpr,
    OrderDirection, OrderExpr, OrderTerm, RoundProjectionExpr, SetOperator, StateOperator,
    TextProjectionExpr, ValueProjectionExpr, add, asc, avg, contains, count, count_by, desc, div,
    ends_with, exists, field, first, last, left, length, lower, ltrim, max, max_by, min, min_by,
    mul, position, replace, right, round, round_expr, rtrim, starts_with, sub, substring,
    substring_with_length, sum, trim, upper,
};
