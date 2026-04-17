//! Module: db::session::sql::projection::labels
//! Responsibility: derive stable outward SQL projection column labels from
//! structural plans, prepared projection specs, and computed SQL surfaces.
//! Does not own: projection execution or projection payload storage.
//! Boundary: keeps SQL projection naming policy at the session boundary.

use crate::{
    db::{
        executor::{KernelRow, projection::prepare_projection_shape_from_plan},
        query::builder::scalar_projection::render_scalar_projection_expr_sql_label,
        query::{
            builder::aggregate::AggregateExpr,
            explain::ExplainExecutionNodeDescriptor,
            plan::{
                AccessPlannedQuery,
                expr::{Expr, ProjectionField, ProjectionSpec},
            },
        },
    },
    error::InternalError,
    model::{entity::EntityModel, field::FieldModel},
    value::Value,
};

// Render one aggregate expression into a canonical projection column label.
fn projection_label_from_aggregate(aggregate: &AggregateExpr) -> String {
    let kind = aggregate.kind().sql_label();
    let distinct = if aggregate.is_distinct() {
        "DISTINCT "
    } else {
        ""
    };

    if let Some(input_expr) = aggregate.input_expr() {
        let input = render_scalar_projection_expr_sql_label(input_expr);

        return format!("{kind}({distinct}{input})");
    }

    format!("{kind}({distinct}*)")
}

// Render one projection expression into a canonical output label.
fn projection_label_from_expr(expr: &Expr, ordinal: usize) -> String {
    #[cfg(not(test))]
    let _ = ordinal;

    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::Literal(_) | Expr::FunctionCall { .. } | Expr::Binary { .. } => {
            render_scalar_projection_expr_sql_label(expr)
        }
        Expr::Aggregate(aggregate) => projection_label_from_aggregate(aggregate),
        #[cfg(test)]
        Expr::Alias { name, .. } => name.as_str().to_string(),
        #[cfg(test)]
        Expr::Unary { .. } => {
            format!("expr_{ordinal}")
        }
    }
}

// Render canonical projection labels from one projection spec regardless of
// whether the caller arrived from a typed or structural query shell.
pub(in crate::db::session::sql) fn projection_labels_from_projection_spec(
    projection: &ProjectionSpec,
) -> Vec<String> {
    let mut labels = Vec::with_capacity(projection.len());

    for (ordinal, field) in projection.fields().enumerate() {
        match field {
            ProjectionField::Scalar {
                expr: _,
                alias: Some(alias),
            } => labels.push(alias.as_str().to_string()),
            ProjectionField::Scalar { expr, alias: None } => {
                labels.push(projection_label_from_expr(expr, ordinal));
            }
        }
    }

    labels
}

// Derive fixed decimal display scales for outward SQL projection columns.
// This preserves `ROUND(..., scale)` display semantics even when the outward
// SQL column label is aliased and no longer exposes the original function
// text to downstream renderers.
pub(in crate::db::session::sql) fn projection_fixed_scales_from_projection_spec(
    projection: &ProjectionSpec,
) -> Vec<Option<u32>> {
    projection
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar { expr, .. } => round_scale_from_expr(expr),
        })
        .collect()
}

fn round_scale_from_expr(expr: &Expr) -> Option<u32> {
    let Expr::FunctionCall { function, args } = expr else {
        return None;
    };
    if !matches!(function, crate::db::query::plan::expr::Function::Round) {
        return None;
    }

    match args.get(1) {
        Some(Expr::Literal(Value::Uint(scale))) => u32::try_from(*scale).ok(),
        Some(Expr::Literal(Value::Int(scale))) if *scale >= 0 => u32::try_from(*scale).ok(),
        _ => None,
    }
}

// Attach SQL-facing projection labels and shell-facing projection runtime hints
// only at the session SQL boundary so executor-owned EXPLAIN assembly stays
// structural.
pub(in crate::db::session::sql) fn annotate_sql_projection_debug_on_execution_descriptor(
    descriptor: &mut ExplainExecutionNodeDescriptor,
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    projection: &ProjectionSpec,
) {
    let labels = projection_labels_from_projection_spec(projection)
        .into_iter()
        .map(Value::from)
        .collect();
    descriptor
        .node_properties
        .insert("proj_fields", Value::List(labels));

    if let Some(materialization) = sql_projection_materialization_label(descriptor, model, plan) {
        descriptor
            .node_properties
            .insert("proj_materialization", Value::from(materialization));
    }
}

fn sql_projection_materialization_label(
    descriptor: &ExplainExecutionNodeDescriptor,
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
) -> Option<&'static str> {
    if !plan.scalar_plan().mode.is_load()
        || plan.grouped_plan().is_some()
        || plan.scalar_projection_plan().is_none()
    {
        return None;
    }
    if descriptor.covering_scan() == Some(true) {
        return Some("covering_read");
    }

    let prepared_projection = prepare_projection_shape_from_plan(model, plan);
    if prepared_projection
        .retained_slot_direct_projection_field_slots()
        .is_some()
    {
        return Some("direct_slot_row");
    }

    Some("scalar_projection")
}

// Derive canonical full-entity projection labels in declared model order.
pub(in crate::db::session::sql) fn projection_labels_from_fields(
    fields: &'static [FieldModel],
) -> Vec<String> {
    fields
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

// Materialize structural kernel rows into canonical SQL projection rows at the
// session boundary instead of inside executor delete paths.
pub(in crate::db::session::sql) fn sql_projection_rows_from_kernel_rows(
    rows: Vec<KernelRow>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    rows.into_iter()
        .map(|row| {
            Ok(row
                .into_slots()?
                .into_iter()
                .map(|value| value.unwrap_or(Value::Null))
                .collect::<Vec<_>>())
        })
        .collect()
}
