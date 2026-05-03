use crate::{
    db::query::{
        builder::aggregate::AggregateExpr,
        plan::{
            AggregateKind, PlanError,
            expr::{
                BinaryOp, Expr, NumericSubtype,
                type_inference::{ExprType, infer_expr_type, source::render_field_path},
            },
            validate::ExprPlanError,
        },
    },
    db::schema::SchemaInfo,
};

pub(super) fn infer_aggregate_expr_type(
    aggregate: &AggregateExpr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let kind = aggregate.kind();
    let input_expr = aggregate.input_expr();

    match kind {
        AggregateKind::Count => Ok(ExprType::Numeric(NumericSubtype::Integer)),
        AggregateKind::Exists => Ok(ExprType::Bool),
        AggregateKind::Sum => infer_sum_aggregate_type(input_expr, schema, "sum"),
        AggregateKind::Avg => infer_sum_aggregate_type(input_expr, schema, "avg"),
        AggregateKind::Min | AggregateKind::Max | AggregateKind::First | AggregateKind::Last => {
            infer_target_field_aggregate_type(input_expr, schema)
        }
    }
}

fn infer_sum_aggregate_type(
    input_expr: Option<&Expr>,
    schema: &SchemaInfo,
    aggregate_name: &str,
) -> Result<ExprType, PlanError> {
    let Some(input_expr) = input_expr else {
        return Err(PlanError::from(ExprPlanError::aggregate_target_required(
            aggregate_name,
        )));
    };

    let inferred = infer_expr_type(input_expr, schema)?;

    match (input_expr, &inferred) {
        (Expr::Field(_), ExprType::Numeric(_)) => {}
        (Expr::Field(field), _) => {
            return Err(PlanError::from(
                ExprPlanError::non_numeric_aggregate_target(aggregate_name, field.as_str()),
            ));
        }
        (_, ExprType::Numeric(_)) => {}
        _ => {
            return Err(PlanError::from(
                ExprPlanError::non_numeric_aggregate_target(
                    aggregate_name,
                    render_aggregate_input_expr_label(input_expr).as_str(),
                ),
            ));
        }
    }

    Ok(inferred)
}

fn infer_target_field_aggregate_type(
    input_expr: Option<&Expr>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(input_expr) = input_expr else {
        // Bootstrap behavior: target-less extrema/value terminals stay unresolved.
        return Ok(ExprType::Unknown);
    };

    infer_expr_type(input_expr, schema)
}

fn render_aggregate_input_expr_label(expr: &Expr) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::FieldPath(path) => render_field_path(path),
        Expr::Literal(value) => format!("{value:?}"),
        Expr::FunctionCall { function, args } => {
            let rendered_args = args
                .iter()
                .map(render_aggregate_input_expr_label)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({rendered_args})", function.canonical_label())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            let mut rendered = String::from("CASE");
            for arm in when_then_arms {
                rendered.push_str(" WHEN ");
                rendered.push_str(render_aggregate_input_expr_label(arm.condition()).as_str());
                rendered.push_str(" THEN ");
                rendered.push_str(render_aggregate_input_expr_label(arm.result()).as_str());
            }
            rendered.push_str(" ELSE ");
            rendered.push_str(render_aggregate_input_expr_label(else_expr).as_str());
            rendered.push_str(" END");
            rendered
        }
        Expr::Binary { op, left, right } => {
            let left = render_aggregate_input_expr_label(left);
            let right = render_aggregate_input_expr_label(right);
            let op = match op {
                BinaryOp::Or => "OR",
                BinaryOp::And => "AND",
                BinaryOp::Eq => "=",
                BinaryOp::Ne => "!=",
                BinaryOp::Lt => "<",
                BinaryOp::Lte => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::Gte => ">=",
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
            };

            format!("{left} {op} {right}")
        }
        Expr::Aggregate(_) => "aggregate".to_string(),
        #[cfg(test)]
        Expr::Alias { expr, .. } => render_aggregate_input_expr_label(expr),
        Expr::Unary { expr, .. } => render_aggregate_input_expr_label(expr),
    }
}
