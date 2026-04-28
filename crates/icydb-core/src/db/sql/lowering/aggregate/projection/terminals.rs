use crate::db::{
    query::plan::{
        expr::{Alias, ProjectionField, ProjectionSpec},
        lower_global_aggregate_projection,
    },
    sql::{
        lowering::{
            SqlLoweringError,
            aggregate::{
                projection::remap::collect_global_aggregate_terminals_from_expr,
                terminal::SqlGlobalAggregateTerminal,
            },
            analyze_lowered_expr,
            expr::SqlExprPhase,
            select::lower_select_item_expr,
        },
        parser::SqlProjection,
    },
};

///
/// LoweredSqlGlobalAggregateTerminals
///
/// Canonical global aggregate lowering result that keeps only unique
/// executable terminals plus one remap back to original SQL projection order.
///
pub(in crate::db::sql::lowering::aggregate) struct LoweredSqlGlobalAggregateTerminals {
    pub(in crate::db::sql::lowering::aggregate) terminals: Vec<SqlGlobalAggregateTerminal>,
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
        #[cfg(test)]
        let mut output_remap = Vec::<usize>::with_capacity(items.len());
        let mut fields = Vec::<ProjectionField>::with_capacity(items.len());
        #[cfg(test)]
        let mut saw_wrapped_projection = false;

        for (index, item) in items.into_iter().enumerate() {
            let expr = lower_select_item_expr(&item, SqlExprPhase::PostAggregate)?;
            let analysis = analyze_lowered_expr(&expr, None);
            if !analysis.contains_aggregate() || analysis.references_direct_fields() {
                return Err(SqlLoweringError::unsupported_global_aggregate_projection());
            }

            let direct_terminal_index =
                collect_global_aggregate_terminals_from_expr(&expr, &mut terminals)?;
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
                expr,
                alias: projection_aliases
                    .get(index)
                    .and_then(Option::as_deref)
                    .map(Alias::new),
            });
        }

        Ok(Self {
            terminals,
            projection: lower_global_aggregate_projection(fields),
            #[cfg(test)]
            output_remap: if saw_wrapped_projection {
                Vec::new()
            } else {
                output_remap
            },
        })
    }
}
