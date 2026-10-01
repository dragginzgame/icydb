use crate::db::query::builder::AggregateExpr;

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
