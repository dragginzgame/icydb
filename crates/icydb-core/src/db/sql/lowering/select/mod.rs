mod aggregate;
mod binding;
mod order;
mod projection;

use crate::db::sql::lowering::{
    SqlLoweringError,
    aggregate::{grouped_projection_aggregate_calls, lower_aggregate_call},
};
use crate::{
    db::{
        QueryError,
        predicate::{MissingRowPolicy, Predicate},
        query::{
            intent::{Query, StructuralQuery},
            plan::{
                GroupHavingExpr, GroupHavingValueExpr,
                canonicalize_grouped_having_numeric_literal_for_field_kind,
                expr::ProjectionSelection, resolve_group_field_slot,
            },
        },
        sql::parser::{SqlAggregateCall, SqlDeleteStatement, SqlOrderTerm, SqlSelectStatement},
    },
    model::entity::EntityModel,
    traits::EntityKind,
};

use crate::db::sql::lowering::select::{
    aggregate::{
        ResolvedHavingClause, ResolvedHavingExpr, ResolvedHavingValueExpr,
        extend_grouped_having_aggregate_calls, lower_having_clauses,
    },
    order::apply_order_terms_structural,
    projection::{lower_grouped_projection_selection, lower_scalar_projection_selection},
};

pub(in crate::db) use binding::{
    canonicalize_sql_predicate_for_model, canonicalize_strict_sql_literal_for_kind,
};
pub(in crate::db::sql::lowering) use projection::lower_select_item_expr;

///
/// LoweredSelectShape
///
/// Entity-agnostic lowered SQL SELECT shape prepared for typed `Query<E>`
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredSelectShape {
    projection_selection: ProjectionSelection,
    grouped_aggregates: Vec<SqlAggregateCall>,
    group_by_fields: Vec<String>,
    distinct: bool,
    having: Vec<ResolvedHavingClause>,
    predicate: Option<Predicate>,
    order_by: Vec<SqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
}

///
/// LoweredBaseQueryShape
///
/// Generic-free filter/order/window query modifiers shared by delete and
/// global-aggregate SQL lowering.
/// This keeps common SQL query-shape lowering shared before typed query
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredBaseQueryShape {
    pub(in crate::db::sql::lowering) predicate: Option<Predicate>,
    pub(in crate::db::sql::lowering) order_by: Vec<SqlOrderTerm>,
    pub(in crate::db::sql::lowering) limit: Option<u32>,
    pub(in crate::db::sql::lowering) offset: Option<u32>,
}

#[inline(never)]
pub(in crate::db::sql::lowering) fn lower_select_shape(
    statement: SqlSelectStatement,
    model: &'static EntityModel,
) -> Result<LoweredSelectShape, SqlLoweringError> {
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
    } = statement;
    let projection_for_having = projection.clone();

    // Phase 1: resolve scalar/grouped projection shape.
    let is_grouped = !group_by.is_empty();
    let (projection_selection, grouped_aggregates, normalized_distinct) = if is_grouped {
        let projection_aggregates =
            grouped_projection_aggregate_calls(&projection, group_by.as_slice(), model)?;
        let mut grouped_aggregates = projection_aggregates.clone();
        extend_grouped_having_aggregate_calls(&mut grouped_aggregates, having.as_slice());
        let projection_selection = lower_grouped_projection_selection(
            projection,
            projection_aliases.as_slice(),
            group_by.as_slice(),
            projection_aggregates.len() == grouped_aggregates.len(),
            model,
        )?;
        (projection_selection, grouped_aggregates, false)
    } else {
        let projection_selection =
            lower_scalar_projection_selection(projection, projection_aliases.as_slice(), distinct)?;
        (projection_selection, Vec::new(), distinct)
    };

    // Phase 2: resolve HAVING symbols against grouped projection authority.
    let having = lower_having_clauses(
        having,
        &projection_for_having,
        group_by.as_slice(),
        grouped_aggregates.as_slice(),
    )?;

    Ok(LoweredSelectShape {
        projection_selection,
        grouped_aggregates,
        group_by_fields: group_by,
        distinct: normalized_distinct,
        having,
        predicate,
        order_by,
        limit,
        offset,
    })
}

#[inline(never)]
pub(in crate::db) fn apply_lowered_select_shape(
    mut query: StructuralQuery,
    lowered: LoweredSelectShape,
) -> Result<StructuralQuery, SqlLoweringError> {
    let LoweredSelectShape {
        projection_selection,
        grouped_aggregates,
        group_by_fields,
        distinct,
        having,
        predicate,
        order_by,
        limit,
        offset,
    } = lowered;
    let model = query.model();

    // Phase 1: apply grouped declaration semantics.
    for field in group_by_fields {
        query = query.group_by(field)?;
    }

    // Phase 2: apply scalar DISTINCT and projection contracts.
    if distinct {
        query = query.distinct();
    }
    query = query.projection_selection(projection_selection);
    for aggregate in grouped_aggregates {
        query = query.aggregate(lower_aggregate_call(aggregate)?);
    }

    // Phase 3: bind resolved HAVING expressions against grouped terminals.
    for clause in having {
        query = query.having_expr(resolve_grouped_having_expr(model, clause.into_expr())?)?;
    }

    // Phase 4: attach the shared filter/order/page tail through the base-query lane.
    Ok(apply_lowered_base_query_shape(
        query,
        LoweredBaseQueryShape {
            predicate: predicate
                .map(|predicate| canonicalize_sql_predicate_for_model(model, predicate)),
            order_by,
            limit,
            offset,
        },
    ))
}

pub(in crate::db::sql::lowering) fn apply_lowered_base_query_shape(
    mut query: StructuralQuery,
    lowered: LoweredBaseQueryShape,
) -> StructuralQuery {
    if let Some(predicate) = lowered.predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms_structural(query, lowered.order_by);
    if let Some(limit) = lowered.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = lowered.offset {
        query = query.offset(offset);
    }

    query
}

pub(in crate::db) fn bind_lowered_sql_query_structural(
    model: &'static EntityModel,
    lowered: crate::db::sql::lowering::LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<StructuralQuery, SqlLoweringError> {
    match lowered {
        crate::db::sql::lowering::LoweredSqlQuery::Select(select) => {
            bind_lowered_sql_select_query_structural(model, select, consistency)
        }
        crate::db::sql::lowering::LoweredSqlQuery::Delete(delete) => Ok(
            bind_lowered_sql_delete_query_structural(model, delete, consistency),
        ),
    }
}

/// Bind one lowered SQL SELECT shape onto the structural query surface.
///
/// This keeps the field-only SQL read lane narrow and owner-local: any caller
/// that already resolved entity authority can reuse the same lowered-SELECT to
/// structural-query boundary without reopening SQL shape application itself.
pub(in crate::db) fn bind_lowered_sql_select_query_structural(
    model: &'static EntityModel,
    select: LoweredSelectShape,
    consistency: MissingRowPolicy,
) -> Result<StructuralQuery, SqlLoweringError> {
    apply_lowered_select_shape(StructuralQuery::new(model, consistency), select)
}

pub(in crate::db) fn bind_lowered_sql_delete_query_structural(
    model: &'static EntityModel,
    delete: LoweredBaseQueryShape,
    consistency: MissingRowPolicy,
) -> StructuralQuery {
    let delete = LoweredBaseQueryShape {
        predicate: delete
            .predicate
            .map(|predicate| canonicalize_sql_predicate_for_model(model, predicate)),
        order_by: delete.order_by,
        limit: delete.limit,
        offset: delete.offset,
    };

    apply_lowered_base_query_shape(StructuralQuery::new(model, consistency).delete(), delete)
}

pub(in crate::db) fn bind_lowered_sql_query<E: EntityKind>(
    lowered: crate::db::sql::lowering::LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    let structural = bind_lowered_sql_query_structural(E::MODEL, lowered, consistency)?;

    Ok(Query::from_inner(structural))
}

pub(in crate::db::sql::lowering) fn lower_delete_shape(
    statement: SqlDeleteStatement,
) -> LoweredBaseQueryShape {
    let SqlDeleteStatement {
        predicate,
        order_by,
        limit,
        offset,
        entity: _,
        returning: _,
    } = statement;

    LoweredBaseQueryShape {
        predicate,
        order_by,
        limit,
        offset,
    }
}

fn resolve_grouped_having_expr(
    model: &'static EntityModel,
    expr: ResolvedHavingExpr,
) -> Result<GroupHavingExpr, SqlLoweringError> {
    match expr {
        ResolvedHavingExpr::Compare { left, op, right } => {
            let left = resolve_grouped_having_value_expr(model, left)?;
            let right = resolve_grouped_having_value_expr(model, right)?;
            let (left, right) = canonicalize_grouped_having_compare_literals(left, right);

            Ok(GroupHavingExpr::Compare { left, op, right })
        }
    }
}

// Keep grouped SQL HAVING field/literal compares aligned with the fluent
// grouped HAVING boundary when the numeric conversion is lossless. This only
// canonicalizes the narrow Int<->Uint drift for direct grouped key compares.
fn canonicalize_grouped_having_compare_literals(
    left: GroupHavingValueExpr,
    right: GroupHavingValueExpr,
) -> (GroupHavingValueExpr, GroupHavingValueExpr) {
    match (&left, &right) {
        (GroupHavingValueExpr::GroupField(field_slot), GroupHavingValueExpr::Literal(value)) => {
            let canonical = canonicalize_grouped_having_numeric_literal_for_field_kind(
                field_slot.kind(),
                value,
            );
            (
                left,
                canonical
                    .map(GroupHavingValueExpr::Literal)
                    .unwrap_or(right),
            )
        }
        (GroupHavingValueExpr::Literal(value), GroupHavingValueExpr::GroupField(field_slot)) => {
            let canonical = canonicalize_grouped_having_numeric_literal_for_field_kind(
                field_slot.kind(),
                value,
            );
            (
                canonical.map(GroupHavingValueExpr::Literal).unwrap_or(left),
                right,
            )
        }
        _ => (left, right),
    }
}

fn resolve_grouped_having_value_expr(
    model: &'static EntityModel,
    expr: ResolvedHavingValueExpr,
) -> Result<GroupHavingValueExpr, SqlLoweringError> {
    match expr {
        ResolvedHavingValueExpr::GroupField(field) => Ok(GroupHavingValueExpr::GroupField(
            resolve_group_field_slot(model, &field).map_err(QueryError::from)?,
        )),
        ResolvedHavingValueExpr::AggregateIndex(index) => {
            Ok(GroupHavingValueExpr::AggregateIndex(index))
        }
        ResolvedHavingValueExpr::Literal(value) => Ok(GroupHavingValueExpr::Literal(value)),
        ResolvedHavingValueExpr::FunctionCall { function, args } => {
            Ok(GroupHavingValueExpr::FunctionCall {
                function,
                args: args
                    .into_iter()
                    .map(|arg| resolve_grouped_having_value_expr(model, arg))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        ResolvedHavingValueExpr::Binary { op, left, right } => Ok(GroupHavingValueExpr::Binary {
            op,
            left: Box::new(resolve_grouped_having_value_expr(model, *left)?),
            right: Box::new(resolve_grouped_having_value_expr(model, *right)?),
        }),
    }
}
