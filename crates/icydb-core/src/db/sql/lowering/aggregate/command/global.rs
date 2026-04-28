use crate::db::{
    query::plan::expr::{Expr, ProjectionSpec},
    sql::{
        lowering::{
            LoweredBaseQueryShape, LoweredSqlFilter, SqlLoweringError,
            aggregate::projection::{
                LoweredSqlGlobalAggregateTerminals, resolve_having_global_aggregate_terminal_index,
                strip_inert_global_aggregate_output_order_terms,
            },
            predicate::{lower_sql_where_bool_expr, lower_sql_where_expr},
            select::{lower_global_aggregate_having_expr, lower_order_terms},
        },
        parser::SqlSelectStatement,
    },
};

///
/// LoweredSqlGlobalAggregateCommand
///
/// Generic-free global aggregate command shape prepared before typed query
/// binding.
/// This keeps aggregate SQL lowering shared across entities until the final
/// execution boundary converts the base query shape into `Query<E>`.
///
#[derive(Clone, Debug)]
pub(in crate::db::sql::lowering) struct LoweredSqlGlobalAggregateCommand {
    pub(in crate::db::sql::lowering::aggregate::command) query: LoweredBaseQueryShape,
    pub(in crate::db::sql::lowering::aggregate::command) terminals:
        Vec<crate::db::sql::lowering::aggregate::terminal::SqlGlobalAggregateTerminal>,
    pub(in crate::db::sql::lowering::aggregate::command) projection: ProjectionSpec,
    pub(in crate::db::sql::lowering::aggregate::command) having: Option<Expr>,
    #[cfg(test)]
    pub(in crate::db::sql::lowering::aggregate::command) output_remap: Vec<usize>,
}

impl LoweredSqlGlobalAggregateCommand {
    /// Lower one constrained global aggregate select into the generic-free
    /// command shape shared by typed and structural aggregate binders.
    fn from_select_statement(statement: SqlSelectStatement) -> Result<Self, SqlLoweringError> {
        let SqlSelectStatement {
            projection,
            projection_aliases,
            predicate,
            distinct,
            group_by,
            having,
            order_by,
            limit,
            offset,
            entity: _,
            table_alias: _,
        } = statement;

        if distinct {
            return Err(SqlLoweringError::unsupported_select_distinct());
        }
        if !group_by.is_empty() {
            return Err(SqlLoweringError::global_aggregate_does_not_support_group_by());
        }
        let projection_for_having = projection.clone();
        let order_by = strip_inert_global_aggregate_output_order_terms(
            order_by,
            &projection_for_having,
            projection_aliases.as_slice(),
        )?;

        let mut lowered_terminals =
            LoweredSqlGlobalAggregateTerminals::from_projection(projection, &projection_aliases)?;
        let having =
            lower_global_aggregate_having_expr(having, &projection_for_having, |aggregate| {
                resolve_having_global_aggregate_terminal_index(
                    &mut lowered_terminals.terminals,
                    aggregate,
                )
            })?;

        Ok(Self {
            query: LoweredBaseQueryShape {
                filter: predicate
                    .as_ref()
                    .map(|expr| {
                        Ok::<_, SqlLoweringError>(
                            LoweredSqlFilter::from_visible_expr_and_predicate_subset(
                                lower_sql_where_bool_expr(expr)?,
                                lower_sql_where_expr(expr)?,
                            ),
                        )
                    })
                    .transpose()?,
                order_by: lower_order_terms(order_by)?,
                limit,
                offset,
            },
            terminals: lowered_terminals.terminals,
            projection: lowered_terminals.projection,
            having,
            #[cfg(test)]
            output_remap: lowered_terminals.output_remap,
        })
    }
}

pub(in crate::db::sql::lowering) fn lower_global_aggregate_select_shape(
    statement: SqlSelectStatement,
) -> Result<LoweredSqlGlobalAggregateCommand, SqlLoweringError> {
    LoweredSqlGlobalAggregateCommand::from_select_statement(statement)
}
