mod aggregate;
mod binding;
mod order;
mod projection;

use crate::db::sql::lowering::{
    SqlLoweringError,
    aggregate::{grouped_projection_aggregate_calls, lower_grouped_aggregate_call},
    predicate::{lower_sql_scalar_where_bool_expr, lower_sql_where_bool_expr},
};
use crate::{
    db::{
        predicate::{MissingRowPolicy, Predicate},
        query::{
            intent::{Query, StructuralQuery},
            plan::expr::{Expr, ProjectionSelection, derive_normalized_bool_expr_predicate_subset},
        },
        sql::parser::{SqlAggregateCall, SqlDeleteStatement, SqlSelectStatement},
    },
    model::entity::EntityModel,
    traits::EntityKind,
};

use crate::db::sql::lowering::select::{
    aggregate::{extend_grouped_having_aggregate_calls, lower_having_clauses},
    order::{LoweredSqlOrderTerm, apply_order_terms_structural},
    projection::{
        lower_grouped_projection_selection, lower_scalar_projection_selection,
        validate_distinct_order_terms_against_projection,
    },
};

pub(in crate::db) use crate::db::query::plan::canonicalize_strict_sql_literal_for_kind;
pub(in crate::db::sql::lowering) use aggregate::lower_global_aggregate_having_expr;
pub(in crate::db) use binding::{
    canonicalize_sql_filter_expr_for_model, canonicalize_sql_predicate_for_model,
};
pub(in crate::db::sql::lowering) use projection::lower_select_item_expr;

pub(in crate::db::sql::lowering) fn lower_order_terms(
    order_by: Vec<crate::db::sql::parser::SqlOrderTerm>,
) -> Result<Vec<LoweredSqlOrderTerm>, SqlLoweringError> {
    order::lower_order_terms(order_by)
}

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
    having: Vec<crate::db::query::plan::expr::Expr>,
    filter_expr: Option<Expr>,
    predicate: Option<Predicate>,
    order_by: Vec<LoweredSqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[cfg(test)]
impl LoweredSelectShape {
    /// Borrow grouped key fields in declaration order for lowering tests.
    #[must_use]
    pub(crate) fn group_by_fields_for_test(&self) -> &[String] {
        self.group_by_fields.as_slice()
    }

    /// Render normalized ORDER BY terms back into stable SQL labels for tests.
    #[must_use]
    pub(crate) fn order_labels_for_test(&self) -> Vec<String> {
        self.order_by
            .iter()
            .map(|term| {
                crate::db::query::builder::scalar_projection::render_scalar_projection_expr_sql_label(
                    &term.expr,
                )
            })
            .collect()
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
    pub(in crate::db::sql::lowering) filter_expr: Option<Expr>,
    pub(in crate::db::sql::lowering) predicate: Option<Predicate>,
    pub(in crate::db::sql::lowering) order_by: Vec<LoweredSqlOrderTerm>,
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
    if !is_grouped && !having.is_empty() {
        return Err(SqlLoweringError::having_requires_group_by());
    }

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

    // Phase 1b: keep SQL DISTINCT ordering fail-closed to the projected tuple.
    let order_by = lower_order_terms(order_by)?;
    if normalized_distinct {
        validate_distinct_order_terms_against_projection(
            &projection_selection,
            order_by.as_slice(),
            model,
        )?;
    }

    // Phase 2: resolve HAVING symbols against grouped projection authority.
    let having = lower_having_clauses(
        having,
        &projection_for_having,
        group_by.as_slice(),
        grouped_aggregates.as_slice(),
        model,
    )?;

    let (filter_expr, predicate) = match predicate.as_ref() {
        Some(expr) => {
            let filter_expr = if is_grouped {
                lower_sql_where_bool_expr(expr)?
            } else {
                lower_sql_scalar_where_bool_expr(expr)?
            };
            let predicate = derive_normalized_bool_expr_predicate_subset(&filter_expr)
                .unwrap_or(Predicate::True);

            (Some(filter_expr), Some(predicate))
        }
        None => (None, None),
    };

    Ok(LoweredSelectShape {
        projection_selection,
        grouped_aggregates,
        group_by_fields: group_by,
        distinct: normalized_distinct,
        having,
        filter_expr,
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
        filter_expr,
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
        query = query.aggregate(lower_grouped_aggregate_call(model, aggregate)?);
    }

    // Phase 3: bind resolved HAVING expressions against grouped terminals.
    for clause in having {
        query = query.having_expr_preserving_shape(clause)?;
    }

    // Phase 4: attach the shared filter/order/page tail through the base-query lane.
    Ok(apply_lowered_base_query_shape(
        query,
        LoweredBaseQueryShape {
            filter_expr,
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
    let model = query.model();

    if let Some(filter_expr) = lowered.filter_expr {
        let filter_expr = canonicalize_sql_filter_expr_for_model(model, filter_expr);
        let predicate = lowered
            .predicate
            .map(|predicate| canonicalize_sql_predicate_for_model(model, predicate))
            .expect("lowered SQL filter expression must carry one derived predicate");

        query = query.filter_expr_with_normalized_predicate(filter_expr, predicate);
    } else if let Some(predicate) = lowered.predicate {
        query = query.filter_predicate(canonicalize_sql_predicate_for_model(model, predicate));
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
        crate::db::sql::lowering::LoweredSqlQuery::Delete(delete) => {
            let delete = LoweredBaseQueryShape {
                filter_expr: delete.filter_expr,
                predicate: delete
                    .predicate
                    .map(|predicate| canonicalize_sql_predicate_for_model(model, predicate)),
                order_by: delete.order_by,
                limit: delete.limit,
                offset: delete.offset,
            };

            Ok(apply_lowered_base_query_shape(
                StructuralQuery::new(model, consistency).delete(),
                delete,
            ))
        }
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

pub(in crate::db) fn bind_lowered_sql_query<E: EntityKind>(
    lowered: crate::db::sql::lowering::LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    let structural = bind_lowered_sql_query_structural(E::MODEL, lowered, consistency)?;

    Ok(Query::from_inner(structural))
}

pub(in crate::db::sql::lowering) fn lower_delete_shape(
    statement: SqlDeleteStatement,
) -> Result<LoweredBaseQueryShape, SqlLoweringError> {
    let SqlDeleteStatement {
        predicate,
        order_by,
        limit,
        offset,
        entity: _,
        returning: _,
    } = statement;
    let (filter_expr, predicate) = match predicate.as_ref() {
        Some(expr) => {
            let filter_expr = lower_sql_scalar_where_bool_expr(expr)?;
            let predicate = derive_normalized_bool_expr_predicate_subset(&filter_expr)
                .unwrap_or(Predicate::True);

            (Some(filter_expr), Some(predicate))
        }
        None => (None, None),
    };

    Ok(LoweredBaseQueryShape {
        filter_expr,
        predicate,
        order_by: lower_order_terms(order_by)?,
        limit,
        offset,
    })
}
