//! Checked, value-independent SELECT structure for WHERE-only parameters.
//! Operand admission and physical planning remain invocation-local.

use crate::{
    db::{
        MissingRowPolicy, QueryError,
        query::{intent::StructuralQuery, plan::expr::Expr, preparation::PreparationWork},
        schema::SchemaInfo,
        sql::{
            lowering::{
                SqlGlobalAggregateCommand,
                aggregate::{SqlGlobalAggregateTemplate, lower_global_aggregate_select_shape},
                bind_lowered_sql_select_query_structural_with_schema,
                bindings::bind_prepared_where,
                prepare::prepare_statement,
                select::{LoweredSqlFilter, lower_select_shape_with_schema},
            },
            parser::{SqlExpr, SqlStatement},
        },
    },
    value::InputValue,
};

#[derive(Clone, Debug)]
pub(in crate::db) struct CheckedSqlSelectTemplate {
    predicate: SqlExpr,
    body: SelectTemplateBody,
}

#[derive(Clone, Debug)]
enum SelectTemplateBody {
    Select {
        query: Box<StructuralQuery>,
        grouped: bool,
    },
    GlobalAggregate(Box<SqlGlobalAggregateTemplate>),
}

/// Only this bound result can be converted into an execution command.
pub(in crate::db) enum BoundSqlSelect {
    Select(StructuralQuery),
    GlobalAggregate(SqlGlobalAggregateCommand),
}

impl CheckedSqlSelectTemplate {
    pub(in crate::db) fn compile(
        statement: &SqlStatement,
        schema: &SchemaInfo,
        bindings: &[InputValue],
        work: &PreparationWork<'_>,
    ) -> Result<(Self, BoundSqlSelect), QueryError> {
        let entity = schema.entity_name().ok_or_else(QueryError::invariant)?;
        let mut prepared = prepare_statement(statement, entity, work)
            .map_err(QueryError::from_sql_lowering_error)?;
        let SqlStatement::Select(select) = &mut prepared else {
            return Err(QueryError::invariant());
        };
        let predicate = select.predicate.take().ok_or_else(QueryError::invariant)?;
        // Preserve first-call error precedence: bound WHERE operands are admitted
        // before unrelated projection/lane lowering, just as on an uncached call.
        let bound_predicate = bind_prepared_where(&predicate, schema, bindings, work)?;
        let global = prepared
            .is_global_aggregate_lane_shape(work)
            .map_err(QueryError::from_sql_lowering_error)?;
        let SqlStatement::Select(select) = prepared else {
            return Err(QueryError::invariant());
        };
        let body = if global {
            SelectTemplateBody::GlobalAggregate(Box::new(
                lower_global_aggregate_select_shape(select, work)
                    .and_then(|lowered| lowered.into_parameter_template(schema))
                    .map_err(QueryError::from_sql_lowering_error)?,
            ))
        } else {
            let grouped = !select.group_by.is_empty();
            let lowered = lower_select_shape_with_schema(select, schema, work)
                .map_err(QueryError::from_sql_lowering_error)?;
            let query = bind_lowered_sql_select_query_structural_with_schema(
                lowered,
                MissingRowPolicy::Ignore,
                schema,
                work,
            )
            .map_err(QueryError::from_sql_lowering_error)?;
            SelectTemplateBody::Select {
                query: Box::new(query),
                grouped,
            }
        };
        let template = Self { predicate, body };
        let bound = template.bind_prepared_predicate(bound_predicate, schema, work)?;
        Ok((template, bound))
    }

    pub(in crate::db) fn bind(
        &self,
        schema: &SchemaInfo,
        bindings: &[InputValue],
        work: &PreparationWork<'_>,
    ) -> Result<BoundSqlSelect, QueryError> {
        let predicate = bind_prepared_where(&self.predicate, schema, bindings, work)?;
        self.bind_prepared_predicate(predicate, schema, work)
    }

    fn bind_prepared_predicate(
        &self,
        predicate: Expr,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<BoundSqlSelect, QueryError> {
        match &self.body {
            SelectTemplateBody::Select { query, grouped } => {
                let filter = LoweredSqlFilter::from_lowered_where_expr(predicate, !*grouped, work)
                    .map_err(QueryError::from_sql_lowering_error)?;
                Ok(BoundSqlSelect::Select(filter.apply_to_query(
                    query.as_ref().clone(),
                    schema,
                    work,
                )?))
            }
            SelectTemplateBody::GlobalAggregate(template) => template
                .bind(predicate, schema, work)
                .map(BoundSqlSelect::GlobalAggregate),
        }
    }
}

crate::retained::retained_fields!(CheckedSqlSelectTemplate {
    Self { predicate, body } => [predicate, body],
});
crate::retained::retained_fields!(SelectTemplateBody {
    Self::Select { query, grouped: _ } => [query],
    Self::GlobalAggregate(template) => [template],
});
