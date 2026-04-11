//! Module: db::session::sql::projection::labels
//! Responsibility: derive stable outward SQL projection column labels from
//! structural plans, prepared projection specs, and computed SQL surfaces.
//! Does not own: projection execution or projection payload storage.
//! Boundary: keeps SQL projection naming policy at the session boundary.

use crate::{
    db::{
        executor::KernelRow,
        query::{
            builder::aggregate::AggregateExpr,
            plan::expr::{Expr, ProjectionField, ProjectionSpec},
        },
    },
    error::InternalError,
    model::field::FieldModel,
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

    if let Some(field) = aggregate.target_field() {
        return format!("{kind}({distinct}{field})");
    }

    format!("{kind}({distinct}*)")
}

// Render one projection expression into a canonical output label.
#[cfg(not(test))]
fn projection_label_from_expr(expr: &Expr, _: usize) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::Aggregate(aggregate) => projection_label_from_aggregate(aggregate),
    }
}

// Render one projection expression into a canonical output label.
#[cfg(test)]
fn projection_label_from_expr(expr: &Expr, ordinal: usize) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::Aggregate(aggregate) => projection_label_from_aggregate(aggregate),
        Expr::Alias { name, .. } => name.as_str().to_string(),
        Expr::Literal(_) | Expr::Unary { .. } | Expr::Binary { .. } => {
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
