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
        predicate::{MissingRowPolicy, Predicate},
        query::{
            intent::{Query, StructuralQuery},
            plan::expr::ProjectionSelection,
        },
        sql::parser::{SqlAggregateCall, SqlDeleteStatement, SqlOrderTerm, SqlSelectStatement},
    },
    model::entity::EntityModel,
    traits::EntityKind,
};

use crate::db::sql::lowering::select::{
    aggregate::{ResolvedHavingClause, lower_having_clauses},
    binding::model_field_kind,
    order::apply_order_terms_structural,
    projection::{lower_grouped_projection_selection, lower_scalar_projection_selection},
};

pub(in crate::db) use binding::canonicalize_sql_predicate_for_model;

///
/// LoweredSelectShape
///
/// Entity-agnostic lowered SQL SELECT shape prepared for typed `Query<E>`
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredSelectShape {
    projection_selection: ProjectionSelection,
    grouped_projection_aggregates: Vec<SqlAggregateCall>,
    group_by_fields: Vec<String>,
    distinct: bool,
    having: Vec<ResolvedHavingClause>,
    predicate: Option<Predicate>,
    order_by: Vec<SqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl LoweredSelectShape {
    // Report whether this lowered select shape carries grouped execution state.
    pub(in crate::db::sql::lowering) const fn has_grouping(&self) -> bool {
        !self.group_by_fields.is_empty()
    }
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
    let has_grouping = !group_by.is_empty();
    let (projection_selection, grouped_projection_aggregates, normalized_distinct) = if has_grouping
    {
        let projection_selection = lower_grouped_projection_selection(
            projection.clone(),
            projection_aliases.as_slice(),
            group_by.as_slice(),
        )?;
        let grouped_projection_aggregates =
            grouped_projection_aggregate_calls(&projection, group_by.as_slice())?;
        (projection_selection, grouped_projection_aggregates, false)
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
        grouped_projection_aggregates.as_slice(),
    )?;

    Ok(LoweredSelectShape {
        projection_selection,
        grouped_projection_aggregates,
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
        grouped_projection_aggregates,
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
    for aggregate in grouped_projection_aggregates {
        query = query.aggregate(lower_aggregate_call(aggregate)?);
    }

    // Phase 3: bind resolved HAVING clauses against grouped terminals.
    for clause in having {
        match clause {
            ResolvedHavingClause::GroupField { field, op, value } => {
                let value = model_field_kind(model, &field)
                    .and_then(|field_kind| {
                        binding::canonicalize_strict_sql_numeric_value_for_kind(&field_kind, &value)
                    })
                    .unwrap_or(value);
                query = query.having_group(field, op, value)?;
            }
            ResolvedHavingClause::Aggregate {
                aggregate_index,
                op,
                value,
            } => {
                query = query.having_aggregate(aggregate_index, op, value)?;
            }
        }
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
