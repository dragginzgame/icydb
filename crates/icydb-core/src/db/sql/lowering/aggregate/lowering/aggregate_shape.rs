use crate::db::sql::{lowering::AnalyzedLoweredExpr, parser::SqlAggregateKind};

///
/// LoweredSqlAggregateShape
///
/// Locally validated aggregate-call shape used by SQL lowering to avoid
/// duplicating `(SqlAggregateKind, field)` validation across lowering lanes.
///
pub(in crate::db::sql::lowering::aggregate) enum LoweredSqlAggregateShape {
    CountRows {
        filter_expr: Option<AnalyzedLoweredExpr>,
    },
    CountField {
        field: String,
        filter_expr: Option<AnalyzedLoweredExpr>,
        distinct: bool,
    },
    FieldTarget {
        kind: SqlAggregateKind,
        field: String,
        filter_expr: Option<AnalyzedLoweredExpr>,
        distinct: bool,
    },
    ExpressionInput {
        kind: SqlAggregateKind,
        input_expr: AnalyzedLoweredExpr,
        filter_expr: Option<AnalyzedLoweredExpr>,
        distinct: bool,
    },
}

impl LoweredSqlAggregateShape {
    /// Borrow the lowered expression input analysis, when this aggregate owns
    /// an expression input instead of a field or row target.
    pub(in crate::db::sql::lowering::aggregate) const fn input_expr(
        &self,
    ) -> Option<&AnalyzedLoweredExpr> {
        match self {
            Self::ExpressionInput { input_expr, .. } => Some(input_expr),
            Self::CountRows { .. } | Self::CountField { .. } | Self::FieldTarget { .. } => None,
        }
    }

    /// Borrow the lowered aggregate FILTER analysis, when present.
    pub(in crate::db::sql::lowering::aggregate) const fn filter_expr(
        &self,
    ) -> Option<&AnalyzedLoweredExpr> {
        match self {
            Self::CountRows { filter_expr }
            | Self::CountField { filter_expr, .. }
            | Self::FieldTarget { filter_expr, .. }
            | Self::ExpressionInput { filter_expr, .. } => filter_expr.as_ref(),
        }
    }
}
