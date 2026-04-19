use crate::db::sql::lowering::{
    SqlLoweringError,
    expr::{SqlExprPhase, lower_sql_expr, sql_expr_contains_aggregate},
};
use crate::{
    db::{
        query::plan::expr::{
            Alias, Expr, FieldId, ProjectionField, ProjectionSelection, expr_references_only_fields,
        },
        sql::parser::{SqlProjection, SqlSelectItem},
    },
    model::entity::{EntityModel, resolve_field_slot},
};

pub(super) fn lower_scalar_projection_selection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
    distinct: bool,
) -> Result<ProjectionSelection, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(ProjectionSelection::All);
    };

    if items.iter().any(select_item_contains_aggregate) {
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
        let contains_aggregate = expr_contains_aggregate(&expr);
        if seen_aggregate && !contains_aggregate {
            return Err(SqlLoweringError::grouped_projection_scalar_after_aggregate(
                index,
            ));
        }
        validate_grouped_projection_expr(model, index, &expr, grouped_field_names.as_slice())?;
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
    model: &EntityModel,
    index: usize,
    expr: &Expr,
    grouped_field_names: &[&str],
) -> Result<(), SqlLoweringError> {
    if let Some(field) = first_unknown_field_in_expr(expr, model) {
        return Err(SqlLoweringError::unknown_field(field));
    }
    if !expr_references_only_fields(expr, grouped_field_names) {
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

// Return the first unknown field referenced anywhere inside one projection
// expression so grouped SQL lowering can keep field-resolution errors specific.
fn first_unknown_field_in_expr(expr: &Expr, model: &EntityModel) -> Option<String> {
    match expr {
        Expr::Field(field) => (resolve_field_slot(model, field.as_str()).is_none())
            .then(|| field.as_str().to_string()),
        Expr::Literal(_) | Expr::Aggregate(_) => None,
        Expr::FunctionCall { args, .. } => args
            .iter()
            .find_map(|arg| first_unknown_field_in_expr(arg, model)),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => when_then_arms
            .iter()
            .find_map(|arm| {
                first_unknown_field_in_expr(arm.condition(), model)
                    .or_else(|| first_unknown_field_in_expr(arm.result(), model))
            })
            .or_else(|| first_unknown_field_in_expr(else_expr, model)),
        Expr::Binary { left, right, .. } => first_unknown_field_in_expr(left, model)
            .or_else(|| first_unknown_field_in_expr(right, model)),
        Expr::Unary { expr, .. } => first_unknown_field_in_expr(expr, model),
        #[cfg(test)]
        Expr::Alias { expr, .. } => first_unknown_field_in_expr(expr, model),
    }
}

// Keep grouped non-aggregate projection widening narrow: grouped key-side
// expressions may depend on grouped fields, but they may not carry aggregate
// leaves because aggregate projection remains explicit in the grouped runtime
// handoff.
pub(in crate::db::sql::lowering) fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_) => true,
        Expr::Field(_) | Expr::Literal(_) => false,
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_aggregate),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().any(|arm| {
                expr_contains_aggregate(arm.condition()) || expr_contains_aggregate(arm.result())
            }) || expr_contains_aggregate(else_expr)
        }
        Expr::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::Unary { expr, .. } => expr_contains_aggregate(expr),
        #[cfg(test)]
        Expr::Alias { expr, .. } => expr_contains_aggregate(expr),
    }
}

pub(in crate::db::sql::lowering) fn select_item_contains_aggregate(item: &SqlSelectItem) -> bool {
    sql_expr_contains_aggregate(&crate::db::sql::parser::SqlExpr::from_select_item(item))
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
