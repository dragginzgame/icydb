use crate::db::{
    query::plan::{
        expr::{Alias, Expr, ProjectionField, ProjectionSpec},
        lower_global_aggregate_projection,
    },
    sql::{
        lowering::{
            SqlLoweringError,
            aggregate::{
                projection::remap::GlobalAggregateTerminalInterner,
                terminal::LoweredSqlGlobalAggregateTerminal,
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
    terminal_interner: GlobalAggregateTerminalInterner,
    pub(in crate::db::sql::lowering::aggregate) projection: ProjectionSpec,
    #[cfg(test)]
    pub(in crate::db::sql::lowering::aggregate) output_remap: Vec<usize>,
}

pub(in crate::db::sql::lowering::aggregate) struct LoweredSqlGlobalAggregateTerminalParts {
    pub(in crate::db::sql::lowering::aggregate) terminals: Vec<LoweredSqlGlobalAggregateTerminal>,
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

        let mut terminal_interner = GlobalAggregateTerminalInterner::with_capacity(items.len());
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

            let direct_terminal_index = terminal_interner.collect_from_analysis(
                analysis.aggregate_refs(),
                matches!(analyzed.expr(), Expr::Aggregate(_)),
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
            terminal_interner,
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
        self.terminal_interner.intern(aggregate_expr)
    }

    pub(in crate::db::sql::lowering::aggregate) fn into_parts(
        self,
    ) -> LoweredSqlGlobalAggregateTerminalParts {
        LoweredSqlGlobalAggregateTerminalParts {
            terminals: self.terminal_interner.into_terminals(),
            projection: self.projection,
            #[cfg(test)]
            output_remap: self.output_remap,
        }
    }
}
