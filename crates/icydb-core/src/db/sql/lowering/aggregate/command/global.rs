use crate::db::{
    query::{
        builder::aggregate::count,
        plan::{
            AggregateKind,
            expr::{Alias, Expr, ProjectionField, ProjectionSpec},
            lower_global_aggregate_projection,
        },
    },
    sql::{
        lowering::{
            LoweredBaseQueryShape, LoweredSqlFilter, SqlLoweringError,
            aggregate::projection::{
                LoweredSqlGlobalAggregateTerminals, strip_inert_global_aggregate_output_order_terms,
            },
            aggregate::terminal::{AggregateInput, SqlGlobalAggregateTerminal},
            select::{lower_global_aggregate_having_expr, lower_order_terms},
        },
        parser::{
            SqlAggregateCall, SqlAggregateKind, SqlExpr, SqlOrderTerm, SqlProjection,
            SqlSelectItem, SqlSelectStatement,
        },
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
    pub(in crate::db::sql::lowering::aggregate::command) terminals: Vec<SqlGlobalAggregateTerminal>,
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
        if having.is_empty() && order_by.is_empty() && is_direct_count_rows_projection(&projection)
        {
            return Self::from_direct_count_rows_select(
                projection_aliases,
                predicate,
                limit,
                offset,
            );
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
                lowered_terminals.intern_having_terminal_index(aggregate)
            })?;

        Ok(Self {
            query: lower_global_aggregate_base_query_shape(predicate, order_by, limit, offset)?,
            terminals: lowered_terminals.terminals,
            projection: lowered_terminals.projection,
            having,
            #[cfg(test)]
            output_remap: lowered_terminals.output_remap,
        })
    }

    fn from_direct_count_rows_select(
        projection_aliases: Vec<Option<String>>,
        predicate: Option<SqlExpr>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Self, SqlLoweringError> {
        let alias = projection_aliases
            .into_iter()
            .next()
            .flatten()
            .map(Alias::new);

        Ok(Self {
            query: lower_global_aggregate_base_query_shape(predicate, Vec::new(), limit, offset)?,
            terminals: vec![SqlGlobalAggregateTerminal {
                kind: AggregateKind::Count,
                input: AggregateInput::Rows,
                filter_expr: None,
                distinct: false,
            }],
            projection: lower_global_aggregate_projection(vec![ProjectionField::Scalar {
                expr: Expr::Aggregate(count()),
                alias,
            }]),
            having: None,
            #[cfg(test)]
            output_remap: vec![0],
        })
    }
}

fn is_direct_count_rows_projection(projection: &SqlProjection) -> bool {
    let SqlProjection::Items(items) = projection else {
        return false;
    };

    matches!(
        items.as_slice(),
        [SqlSelectItem::Aggregate(SqlAggregateCall {
            kind: SqlAggregateKind::Count,
            input: None,
            filter_expr: None,
            distinct: false,
        })]
    )
}

fn lower_global_aggregate_base_query_shape(
    predicate: Option<SqlExpr>,
    order_by: Vec<SqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
) -> Result<LoweredBaseQueryShape, SqlLoweringError> {
    Ok(LoweredBaseQueryShape {
        filter: predicate.map(lower_global_aggregate_filter).transpose()?,
        order_by: lower_order_terms(order_by)?,
        limit,
        offset,
    })
}

fn lower_global_aggregate_filter(expr: SqlExpr) -> Result<LoweredSqlFilter, SqlLoweringError> {
    LoweredSqlFilter::from_where_expr_requiring_predicate_subset(&expr)
}

pub(in crate::db::sql::lowering) fn lower_global_aggregate_select_shape(
    statement: SqlSelectStatement,
) -> Result<LoweredSqlGlobalAggregateCommand, SqlLoweringError> {
    LoweredSqlGlobalAggregateCommand::from_select_statement(statement)
}
