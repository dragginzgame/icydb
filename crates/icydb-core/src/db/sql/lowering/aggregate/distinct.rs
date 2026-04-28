use crate::db::{
    query::{
        builder::AggregateExpr,
        plan::{AggregateKind, expr::Expr},
    },
    sql::lowering::SqlLoweringError,
};

// Reject the currently unsupported aggregate DISTINCT + FILTER pairing in one
// owner so aggregate-call lowering does not grow parallel admission checks.
pub(in crate::db::sql::lowering::aggregate) const fn reject_distinct_filter_pairing(
    distinct: bool,
    filter_expr: Option<&Expr>,
) -> Result<(), SqlLoweringError> {
    if distinct && filter_expr.is_some() {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    Ok(())
}

// Preserve the parsed DISTINCT marker on the aggregate expression exactly once.
// Runtime strategy construction later decides whether that marker has observable
// reducer semantics for the specific aggregate family.
#[must_use]
pub(in crate::db::sql::lowering::aggregate) const fn apply_distinct_marker(
    aggregate: AggregateExpr,
    distinct: bool,
) -> AggregateExpr {
    if distinct {
        aggregate.distinct()
    } else {
        aggregate
    }
}

// Preserve only the DISTINCT families that have observable SQL aggregate
// semantics in the current runtime. MIN/MAX DISTINCT stays intentionally
// erased because it is equivalent under the current extrema reducers.
pub(in crate::db::sql::lowering::aggregate) const fn preserve_distinct_for_strategy(
    kind: AggregateKind,
    distinct: bool,
) -> bool {
    matches!(
        kind,
        AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg
    ) && distinct
}
