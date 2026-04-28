use crate::db::{query::plan::expr::Expr, sql::parser::SqlAggregateKind};

///
/// LoweredSqlAggregateShape
///
/// Locally validated aggregate-call shape used by SQL lowering to avoid
/// duplicating `(SqlAggregateKind, field)` validation across lowering lanes.
///
pub(in crate::db::sql::lowering::aggregate) enum LoweredSqlAggregateShape {
    CountRows {
        filter_expr: Option<Expr>,
    },
    CountField {
        field: String,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    FieldTarget {
        kind: SqlAggregateKind,
        field: String,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
    ExpressionInput {
        kind: SqlAggregateKind,
        input_expr: Expr,
        filter_expr: Option<Expr>,
        distinct: bool,
    },
}
