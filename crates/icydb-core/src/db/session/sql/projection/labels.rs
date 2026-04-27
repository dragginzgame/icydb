//! Module: db::session::sql::projection::labels
//! Responsibility: derive stable outward SQL projection column labels from
//! structural plans, prepared projection specs, and computed SQL surfaces.
//! Does not own: projection execution or projection payload storage.
//! Boundary: keeps SQL projection naming policy at the session boundary.

use crate::{
    db::{
        query::builder::scalar_projection::render_scalar_projection_expr_plan_label,
        query::{
            explain::ExplainExecutionNodeDescriptor,
            plan::{
                AccessPlannedQuery,
                expr::{Expr, ProjectionField, ProjectionSpec},
            },
        },
    },
    model::field::FieldModel,
    value::Value,
};

// Render canonical projection labels from one projection spec regardless of
// whether the caller arrived from a typed or structural query shell.
pub(in crate::db::session::sql) fn projection_labels_from_projection_spec(
    projection: &ProjectionSpec,
) -> Vec<String> {
    let mut labels = Vec::with_capacity(projection.len());

    // Derive outward labels directly from the frozen projection spec so the
    // session boundary does not bounce through one-expression helper wrappers.
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar {
                expr: _,
                alias: Some(alias),
            } => labels.push(alias.as_str().to_string()),
            ProjectionField::Scalar { expr, alias: None } => {
                labels.push(match expr {
                    Expr::Field(field) => field.as_str().to_string(),
                    Expr::Aggregate(aggregate) => {
                        let kind = aggregate.kind().canonical_label();
                        let distinct = if aggregate.is_distinct() {
                            "DISTINCT "
                        } else {
                            ""
                        };
                        if let Some(input_expr) = aggregate.input_expr() {
                            let input = render_scalar_projection_expr_plan_label(input_expr);

                            format!("{kind}({distinct}{input})")
                        } else {
                            format!("{kind}({distinct}*)")
                        }
                    }
                    #[cfg(test)]
                    Expr::Alias { name, .. } => name.as_str().to_string(),
                    Expr::Literal(_)
                    | Expr::FunctionCall { .. }
                    | Expr::Case { .. }
                    | Expr::Binary { .. }
                    | Expr::Unary { .. } => render_scalar_projection_expr_plan_label(expr),
                });
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
            // Recover fixed ROUND(...) display scales directly from the frozen
            // projection expression instead of bouncing through a one-call
            // extractor helper.
            ProjectionField::Scalar { expr, .. } => {
                let Expr::FunctionCall { function, args } = expr else {
                    return None;
                };
                function.fixed_decimal_scale(args)
            }
        })
        .collect()
}

// Attach SQL-facing projection labels and shell-facing projection runtime hints
// only at the session SQL boundary so executor-owned EXPLAIN assembly stays
// structural.
pub(in crate::db::session::sql) fn annotate_sql_projection_debug_on_execution_descriptor(
    descriptor: &mut ExplainExecutionNodeDescriptor,
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

    // Classify the materialization mode from the frozen planner contract
    // directly here so SQL EXPLAIN does not round-trip through a single-use
    // wrapper just to attach one stable debug label.
    let materialization = if !plan.scalar_plan().mode.is_load()
        || plan.grouped_plan().is_some()
        || plan.scalar_projection_plan().is_none()
    {
        None
    } else if descriptor.covering_scan() == Some(true) {
        Some("covering_read")
    } else {
        // Recognize the retained-slot direct projection shape directly from
        // the planner-frozen projection metadata instead of routing through a
        // single-use predicate helper.
        let direct_slot_projection =
            plan.frozen_direct_projection_slots()
                .is_some_and(|direct_projection_slots| {
                    let projection = plan.frozen_projection_spec();
                    projection.len() == direct_projection_slots.len()
                        && projection
                            .fields()
                            .all(|field| field.direct_field_name().is_some())
                });

        if direct_slot_projection {
            Some("direct_slot_row")
        } else {
            Some("scalar_projection")
        }
    };

    if let Some(materialization) = materialization {
        descriptor
            .node_properties
            .insert("proj_materialization", Value::from(materialization));
    }
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
