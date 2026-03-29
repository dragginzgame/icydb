use crate::{
    db::{
        QueryError,
        executor::KernelRow,
        query::{
            builder::aggregate::AggregateExpr,
            intent::StructuralQuery,
            plan::{
                AggregateKind,
                expr::{Expr, ProjectionField, ProjectionSpec},
            },
        },
    },
    model::EntityModel,
    value::Value,
};

// Render one aggregate expression into a canonical projection column label.
fn projection_label_from_aggregate(aggregate: &AggregateExpr) -> String {
    let kind = match aggregate.kind() {
        AggregateKind::Count => "COUNT",
        AggregateKind::Sum => "SUM",
        AggregateKind::Avg => "AVG",
        AggregateKind::Exists => "EXISTS",
        AggregateKind::First => "FIRST",
        AggregateKind::Last => "LAST",
        AggregateKind::Min => "MIN",
        AggregateKind::Max => "MAX",
    };
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

// Derive canonical projection column labels from one structural query projection spec.
pub(in crate::db::session::sql) fn projection_labels_from_structural_query(
    query: &StructuralQuery,
) -> Result<Vec<String>, QueryError> {
    let projection = query.build_plan()?.projection_spec(query.model());

    Ok(projection_labels_from_projection_spec(&projection))
}

// Render canonical projection labels from one projection spec regardless of
// whether the caller arrived from a typed or structural query shell.
fn projection_labels_from_projection_spec(projection: &ProjectionSpec) -> Vec<String> {
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
pub(in crate::db::session::sql) fn projection_labels_from_entity_model(
    model: &'static EntityModel,
) -> Vec<String> {
    model
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect()
}

// Materialize structural kernel rows into canonical SQL projection rows at the
// session boundary instead of inside executor delete paths.
pub(in crate::db::session::sql) fn sql_projection_rows_from_kernel_rows(
    rows: Vec<KernelRow>,
) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            row.into_slots()
                .into_iter()
                .map(|value| value.unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}
