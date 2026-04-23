use crate::db::sql::lowering::{
    LoweredExprAnalysis, SqlLoweringError, analyze_lowered_expr,
    expr::{SqlExprPhase, lower_sql_expr},
};
use crate::{
    db::{
        query::plan::expr::{Alias, Expr, FieldId, ProjectionField, ProjectionSelection},
        sql::lowering::select::order::LoweredSqlOrderTerm,
        sql::parser::{SqlProjection, SqlSelectItem},
    },
    model::entity::EntityModel,
};

pub(super) fn lower_scalar_projection_selection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
    distinct: bool,
) -> Result<ProjectionSelection, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(ProjectionSelection::All);
    };

    if items.iter().any(SqlSelectItem::contains_aggregate) {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    if let Some(field_ids) = direct_scalar_field_selection(items.as_slice(), projection_aliases) {
        return Ok(ProjectionSelection::Fields(field_ids));
    }

    let fields = items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            lower_projection_field(
                item,
                projection_aliases.get(index).and_then(Option::as_deref),
                SqlExprPhase::Scalar,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    if distinct && fields.is_empty() {
        return Ok(ProjectionSelection::Exprs(fields));
    }

    Ok(ProjectionSelection::Exprs(fields))
}

pub(super) fn lower_grouped_projection_selection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
    group_by: &[String],
    allow_identity_fast_path: bool,
    model: &'static EntityModel,
) -> Result<ProjectionSelection, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::grouped_projection_requires_explicit_list());
    };
    let grouped_field_names = group_by.iter().map(String::as_str).collect::<Vec<_>>();

    let mut seen_aggregate = false;
    let mut fields = Vec::with_capacity(items.len());

    for (index, item) in items.into_iter().enumerate() {
        let expr = lower_select_item_expr(&item, SqlExprPhase::PostAggregate)?;
        let analysis = analyze_lowered_expr(&expr, Some(model));
        let contains_aggregate = analysis.contains_aggregate();
        if seen_aggregate && !contains_aggregate {
            return Err(SqlLoweringError::grouped_projection_scalar_after_aggregate(
                index,
            ));
        }
        validate_grouped_projection_expr(index, &expr, grouped_field_names.as_slice(), &analysis)?;
        seen_aggregate |= contains_aggregate;

        fields.push(ProjectionField::Scalar {
            expr,
            alias: projection_aliases
                .get(index)
                .and_then(Option::as_deref)
                .map(Alias::new),
        });
    }

    if !seen_aggregate {
        return Err(SqlLoweringError::grouped_projection_requires_aggregate());
    }

    if allow_identity_fast_path
        && projection_aliases.iter().all(Option::is_none)
        && grouped_projection_is_canonical_identity(fields.as_slice(), group_by)
    {
        return Ok(ProjectionSelection::All);
    }

    Ok(ProjectionSelection::Exprs(fields))
}

// Validate one grouped projection expression against grouped-key authority
// while preserving specific unknown-field diagnostics.
fn validate_grouped_projection_expr(
    index: usize,
    expr: &Expr,
    grouped_field_names: &[&str],
    analysis: &LoweredExprAnalysis,
) -> Result<(), SqlLoweringError> {
    if let Some(field) = analysis.first_unknown_field() {
        return Err(SqlLoweringError::unknown_field(field));
    }
    if !expr.references_only_fields(grouped_field_names) {
        return Err(SqlLoweringError::grouped_projection_references_non_group_field(index));
    }

    Ok(())
}

// Preserve the older grouped `ProjectionSelection::All` fast path only for
// the canonical identity shape where projected grouped fields match `GROUP BY`
// exactly and aggregate terminals follow in declaration order.
fn grouped_projection_is_canonical_identity(
    fields: &[ProjectionField],
    group_by: &[String],
) -> bool {
    if fields.len() < group_by.len() {
        return false;
    }

    let Some((group_fields, aggregate_fields)) = fields.split_at_checked(group_by.len()) else {
        return false;
    };

    group_fields
        .iter()
        .zip(group_by.iter())
        .all(|(field, group_by)| {
            matches!(
                field,
                ProjectionField::Scalar {
                    expr: Expr::Field(field_id),
                    alias: None,
                } if field_id.as_str() == group_by
            )
        })
        && aggregate_fields.iter().all(|field| {
            matches!(
                field,
                ProjectionField::Scalar {
                    expr: Expr::Aggregate(_),
                    alias: None,
                }
            )
        })
}

// Enforce the SQL DISTINCT rule at lowering time: every ORDER BY term must be
// fully expressible as a function of the outward projected distinct tuple
// rather than from hidden base-row fields.
pub(in crate::db::sql::lowering) fn validate_distinct_order_terms_against_projection(
    projection: &ProjectionSelection,
    order_by: &[LoweredSqlOrderTerm],
    model: &'static EntityModel,
) -> Result<(), SqlLoweringError> {
    if order_by
        .iter()
        .all(|term| distinct_order_term_is_derivable_from_projection(projection, &term.expr, model))
    {
        return Ok(());
    }

    Err(SqlLoweringError::distinct_order_by_requires_projected_tuple())
}

// Keep DISTINCT ORDER BY derivation intentionally narrow:
// - exact projected expressions are always admissible
// - otherwise the term may reference only direct projected fields
// - `SELECT DISTINCT *` exposes the full entity field set
// This is a field-level proof only; it does not admit hidden payloads or a
// broader symbolic derivation model.
fn distinct_order_term_is_derivable_from_projection(
    projection: &ProjectionSelection,
    order_expr: &Expr,
    model: &'static EntityModel,
) -> bool {
    match projection {
        ProjectionSelection::All => {
            let projected_fields = model
                .fields()
                .iter()
                .map(crate::model::field::FieldModel::name)
                .collect::<Vec<_>>();

            order_expr.references_only_fields(projected_fields.as_slice())
        }
        ProjectionSelection::Fields(field_ids) => {
            let projected_fields = field_ids.iter().map(FieldId::as_str).collect::<Vec<_>>();

            order_expr.references_only_fields(projected_fields.as_slice())
        }
        ProjectionSelection::Exprs(fields) => {
            if fields.iter().any(|field| match field {
                ProjectionField::Scalar { expr, .. } => expr == order_expr,
            }) {
                return true;
            }

            let projected_fields = fields
                .iter()
                .filter_map(ProjectionField::direct_field_name)
                .collect::<Vec<_>>();

            order_expr.references_only_fields(projected_fields.as_slice())
        }
    }
}

pub(super) fn direct_scalar_field_selection(
    items: &[SqlSelectItem],
    projection_aliases: &[Option<String>],
) -> Option<Vec<FieldId>> {
    if !projection_aliases.iter().all(Option::is_none) {
        return None;
    }

    items
        .iter()
        .map(|item| match item {
            SqlSelectItem::Field(field) => Some(FieldId::new(field.clone())),
            SqlSelectItem::Aggregate(_) | SqlSelectItem::Expr(_) => None,
        })
        .collect()
}

fn lower_projection_field(
    item: SqlSelectItem,
    alias: Option<&str>,
    phase: SqlExprPhase,
) -> Result<ProjectionField, SqlLoweringError> {
    Ok(ProjectionField::Scalar {
        expr: lower_select_item_expr(&item, phase)?,
        alias: alias.map(Alias::new),
    })
}

pub(in crate::db::sql::lowering) fn lower_select_item_expr(
    item: &SqlSelectItem,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_expr(
        &crate::db::sql::parser::SqlExpr::from_select_item(item),
        phase,
    )
}
