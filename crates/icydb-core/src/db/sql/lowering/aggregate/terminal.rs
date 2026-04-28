use crate::db::{
    query::{
        builder::AggregateExpr,
        plan::{AggregateKind, expr::Expr},
    },
    sql::lowering::SqlLoweringError,
};

///
/// AggregateInput
///
/// Canonical input shape for one executable SQL global aggregate terminal.
/// This separates aggregate function identity from the row, field, or scalar
/// expression input consumed by the aggregate runtime.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering) enum AggregateInput {
    Rows,
    Field(String),
    Expr(Expr),
}

///
/// SqlGlobalAggregateTerminal
///
/// Global SQL aggregate terminal currently executable through the dedicated
/// aggregate SQL entrypoint. The shape is intentionally data-driven so adding
/// a supported aggregate kind does not require another terminal variant family.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering) struct SqlGlobalAggregateTerminal {
    pub(in crate::db::sql::lowering::aggregate) kind: AggregateKind,
    pub(in crate::db::sql::lowering::aggregate) input: AggregateInput,
    pub(in crate::db::sql::lowering::aggregate) filter_expr: Option<Expr>,
    pub(in crate::db::sql::lowering::aggregate) distinct: bool,
}

impl SqlGlobalAggregateTerminal {
    // Build one terminal from the planner aggregate expression while preserving
    // the raw SQL aggregate facts. Semantic normalization happens later in the
    // aggregate semantics owner, not in this syntactic terminal.
    pub(in crate::db::sql::lowering::aggregate) fn from_aggregate_expr(
        aggregate_expr: &AggregateExpr,
    ) -> Result<Self, SqlLoweringError> {
        let kind = aggregate_expr.kind();
        let input = Self::resolve_input(aggregate_expr)?;

        Ok(Self {
            kind,
            input,
            filter_expr: aggregate_expr.filter_expr().cloned(),
            distinct: aggregate_expr.is_distinct(),
        })
    }

    // Resolve the aggregate target into the compact input model accepted by
    // the global aggregate execution lane.
    fn resolve_input(aggregate_expr: &AggregateExpr) -> Result<AggregateInput, SqlLoweringError> {
        let kind = aggregate_expr.kind();
        if matches!(
            kind,
            AggregateKind::Exists | AggregateKind::First | AggregateKind::Last
        ) {
            return Err(SqlLoweringError::unsupported_global_aggregate_projection());
        }
        if kind == AggregateKind::Count
            && aggregate_expr.target_field().is_none()
            && aggregate_expr.input_expr().is_none()
        {
            return Ok(AggregateInput::Rows);
        }

        if let Some(field) = aggregate_expr.target_field() {
            return Ok(AggregateInput::Field(field.to_string()));
        }
        if let Some(input_expr) = aggregate_expr.input_expr() {
            return Ok(AggregateInput::Expr(input_expr.clone()));
        }

        Err(SqlLoweringError::unsupported_global_aggregate_projection())
    }
}
