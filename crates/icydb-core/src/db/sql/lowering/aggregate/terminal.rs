use crate::db::{
    query::{
        builder::AggregateExpr,
        plan::{
            AggregateKind, AggregateSemanticKey,
            expr::aggregate_count_input_expr_is_non_null_literal,
        },
    },
    sql::lowering::{AnalyzedLoweredExpr, SqlLoweringError},
};

///
/// LoweredAggregateInput
///
/// Canonical input shape for one lowered SQL global aggregate terminal.
/// Expression inputs keep the lowering analysis derived from their exact
/// expression tree so later model-bound validation does not re-analyze them.
///
#[derive(Clone, Debug)]
pub(in crate::db::sql::lowering::aggregate) enum LoweredAggregateInput {
    Rows,
    Field(String),
    Expr(AnalyzedLoweredExpr),
}

///
/// LoweredSqlGlobalAggregateTerminal
///
/// Global SQL aggregate terminal prepared before model binding. It keeps the
/// semantic de-dup key aligned with the analyzed input/filter expressions that
/// strategy binding validates against the accepted schema.
///
#[derive(Clone, Debug)]
pub(in crate::db::sql::lowering::aggregate) struct LoweredSqlGlobalAggregateTerminal {
    semantic_key: AggregateSemanticKey,
    input: LoweredAggregateInput,
    filter_expr: Option<AnalyzedLoweredExpr>,
}

impl LoweredSqlGlobalAggregateTerminal {
    pub(in crate::db::sql::lowering::aggregate) fn count_rows() -> Self {
        let aggregate_expr = crate::db::query::builder::aggregate::count();
        let semantic_key = AggregateSemanticKey::from_aggregate_expr(&aggregate_expr);

        Self {
            semantic_key,
            input: LoweredAggregateInput::Rows,
            filter_expr: None,
        }
    }

    // Build one terminal from the planner aggregate expression. Normalization
    // stays limited to executor-equivalent COUNT row inputs and is mirrored by
    // aggregate identity so projection lookup and runtime terminals agree.
    pub(in crate::db::sql::lowering::aggregate) fn from_aggregate_expr_with_semantic_key(
        aggregate_expr: &AggregateExpr,
        semantic_key: AggregateSemanticKey,
    ) -> Result<Self, SqlLoweringError> {
        debug_assert_eq!(
            semantic_key,
            AggregateSemanticKey::from_aggregate_expr(aggregate_expr),
            "global aggregate terminal semantic key must match its aggregate expression",
        );

        let input = Self::resolve_input(aggregate_expr)?;
        let filter_expr = aggregate_expr
            .filter_expr()
            .cloned()
            .map(|expr| AnalyzedLoweredExpr::new(expr, None));

        Ok(Self {
            semantic_key,
            input,
            filter_expr,
        })
    }

    pub(in crate::db::sql::lowering::aggregate) fn into_parts(
        self,
    ) -> (
        AggregateSemanticKey,
        LoweredAggregateInput,
        Option<AnalyzedLoweredExpr>,
    ) {
        let Self {
            semantic_key,
            input,
            filter_expr,
        } = self;

        (semantic_key, input, filter_expr)
    }

    // Resolve the aggregate target into the compact input model accepted by
    // the global aggregate execution lane.
    fn resolve_input(
        aggregate_expr: &AggregateExpr,
    ) -> Result<LoweredAggregateInput, SqlLoweringError> {
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
            return Ok(LoweredAggregateInput::Rows);
        }
        if kind == AggregateKind::Count
            && !aggregate_expr.is_distinct()
            && aggregate_expr
                .input_expr()
                .is_some_and(aggregate_count_input_expr_is_non_null_literal)
        {
            return Ok(LoweredAggregateInput::Rows);
        }

        if let Some(field) = aggregate_expr.target_field() {
            return Ok(LoweredAggregateInput::Field(field.to_string()));
        }
        if let Some(input_expr) = aggregate_expr.input_expr() {
            return Ok(LoweredAggregateInput::Expr(AnalyzedLoweredExpr::new(
                input_expr.clone(),
                None,
            )));
        }

        Err(SqlLoweringError::unsupported_global_aggregate_projection())
    }
}
