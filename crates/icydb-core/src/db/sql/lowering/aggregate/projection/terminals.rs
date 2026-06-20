use crate::db::{
    query::plan::{
        expr::{Alias, Expr, ProjectionField, ProjectionSpec},
        lower_global_aggregate_projection,
    },
    sql::{
        lowering::{
            SqlLoweringError,
            aggregate::{
                projection::remap::collect_global_aggregate_terminals_from_analysis,
                semantics::AggregateTerminalSemanticKey, terminal::SqlGlobalAggregateTerminal,
            },
            expr::SqlExprPhase,
            select::lower_analyzed_select_item_expr,
        },
        parser::SqlProjection,
    },
};

///
/// LoweredSqlGlobalAggregateTerminals
///
/// Canonical global aggregate lowering result that keeps only unique
/// executable terminals plus the canonical projection spec that preserves
/// original SQL projection order.
///
pub(in crate::db::sql::lowering::aggregate) struct LoweredSqlGlobalAggregateTerminals {
    pub(in crate::db::sql::lowering::aggregate) terminals: Vec<SqlGlobalAggregateTerminal>,
    terminal_semantic_keys: Vec<AggregateTerminalSemanticKey>,
    pub(in crate::db::sql::lowering::aggregate) projection: ProjectionSpec,
    #[cfg(test)]
    pub(in crate::db::sql::lowering::aggregate) output_remap: Vec<usize>,
}

impl LoweredSqlGlobalAggregateTerminals {
    /// Lower one SQL projection into unique executable aggregate terminals plus
    /// the output remap needed to preserve original projection order.
    pub(in crate::db::sql::lowering::aggregate) fn from_projection(
        projection: SqlProjection,
        projection_aliases: &[Option<String>],
    ) -> Result<Self, SqlLoweringError> {
        let SqlProjection::Items(items) = projection else {
            return Err(SqlLoweringError::unsupported_global_aggregate_projection());
        };
        if items.is_empty() {
            return Err(SqlLoweringError::unsupported_global_aggregate_projection());
        }

        let mut terminals = Vec::<SqlGlobalAggregateTerminal>::with_capacity(items.len());
        let mut terminal_semantic_keys =
            Vec::<AggregateTerminalSemanticKey>::with_capacity(items.len());
        #[cfg(test)]
        let mut output_remap = Vec::<usize>::with_capacity(items.len());
        let mut fields = Vec::<ProjectionField>::with_capacity(items.len());
        #[cfg(test)]
        let mut saw_wrapped_projection = false;

        for (index, item) in items.into_iter().enumerate() {
            let analyzed =
                lower_analyzed_select_item_expr(&item, SqlExprPhase::PostAggregate, None)?;
            let analysis = analyzed.analysis();
            if !analysis.contains_aggregate() || analysis.references_direct_fields() {
                return Err(SqlLoweringError::unsupported_global_aggregate_projection());
            }

            let direct_terminal_index = collect_global_aggregate_terminals_from_analysis(
                analysis.aggregate_refs(),
                matches!(analyzed.expr(), Expr::Aggregate(_)),
                &mut terminals,
                &mut terminal_semantic_keys,
            )?;
            #[cfg(test)]
            match direct_terminal_index {
                Some(unique_index) => output_remap.push(unique_index),
                None => {
                    saw_wrapped_projection = true;
                }
            }
            #[cfg(not(test))]
            let _ = direct_terminal_index;

            fields.push(ProjectionField::Scalar {
                expr: analyzed.into_expr(),
                alias: projection_aliases
                    .get(index)
                    .and_then(Option::as_deref)
                    .map(Alias::new),
            });
        }

        Ok(Self {
            terminals,
            terminal_semantic_keys,
            projection: lower_global_aggregate_projection(fields),
            #[cfg(test)]
            output_remap: if saw_wrapped_projection {
                Vec::new()
            } else {
                output_remap
            },
        })
    }

    pub(in crate::db::sql::lowering::aggregate) fn intern_having_terminal_index(
        &mut self,
        aggregate_expr: &crate::db::query::builder::AggregateExpr,
    ) -> Result<usize, SqlLoweringError> {
        super::remap::intern_global_aggregate_terminal_index(
            &mut self.terminals,
            &mut self.terminal_semantic_keys,
            aggregate_expr,
        )
    }
}
