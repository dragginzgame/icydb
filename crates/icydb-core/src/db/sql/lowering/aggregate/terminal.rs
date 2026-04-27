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
    pub(super) kind: AggregateKind,
    pub(super) input: AggregateInput,
    pub(super) filter_expr: Option<Expr>,
    pub(super) distinct: bool,
}

impl SqlGlobalAggregateTerminal {
    // Build one terminal from the planner aggregate expression while preserving
    // the previous global-lane support matrix and MIN/MAX distinct erasure.
    pub(super) fn from_aggregate_expr(
        aggregate_expr: &AggregateExpr,
    ) -> Result<Self, SqlLoweringError> {
        let kind = aggregate_expr.kind();
        let input = Self::resolve_input(aggregate_expr)?;
        let distinct = Self::preserved_distinct_flag(kind, aggregate_expr.is_distinct());

        Ok(Self {
            kind,
            input,
            filter_expr: aggregate_expr.filter_expr().cloned(),
            distinct,
        })
    }

    #[must_use]
    pub(super) const fn is_field(&self) -> bool {
        matches!(self.input, AggregateInput::Field(_))
    }

    #[must_use]
    pub(super) const fn is_expr(&self) -> bool {
        matches!(self.input, AggregateInput::Expr(_))
    }

    #[must_use]
    pub(super) const fn field(&self) -> Option<&str> {
        match &self.input {
            AggregateInput::Field(field) => Some(field.as_str()),
            AggregateInput::Rows | AggregateInput::Expr(_) => None,
        }
    }

    #[must_use]
    pub(super) const fn expr(&self) -> Option<&Expr> {
        match &self.input {
            AggregateInput::Expr(expr) => Some(expr),
            AggregateInput::Rows | AggregateInput::Field(_) => None,
        }
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

    const fn preserved_distinct_flag(kind: AggregateKind, distinct: bool) -> bool {
        matches!(
            kind,
            AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg
        ) && distinct
    }
}
