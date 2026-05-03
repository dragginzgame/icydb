use crate::{
    db::{
        predicate::{CoercionId, CoercionSpec, CompareOp, Predicate},
        query::plan::expr::{BinaryOp, CaseWhenArm, Expr},
        schema::SchemaInfo,
    },
    value::Value,
};

// Canonicalize strict numeric SQL predicate literals onto the resolved model
// field kind so unsigned-width fields keep strict/indexable semantics even
// though reduced SQL integer tokens parse through one generic numeric value
// variant first.
// Canonicalize strict numeric SQL predicate literals through the provided
// schema view. Session SQL compile paths pass the accepted schema projection
// here so top-level read predicates line up with live schema reconciliation.
pub(in crate::db) fn canonicalize_sql_predicate_for_schema(
    schema: &SchemaInfo,
    predicate: Predicate,
) -> Predicate {
    match predicate {
        Predicate::And(children) => Predicate::And(
            children
                .into_iter()
                .map(|child| canonicalize_sql_predicate_for_schema(schema, child))
                .collect(),
        ),
        Predicate::Or(children) => Predicate::Or(
            children
                .into_iter()
                .map(|child| canonicalize_sql_predicate_for_schema(schema, child))
                .collect(),
        ),
        Predicate::Not(inner) => Predicate::Not(Box::new(canonicalize_sql_predicate_for_schema(
            schema, *inner,
        ))),
        Predicate::Compare(mut cmp) => {
            canonicalize_sql_compare_for_schema(schema, &mut cmp);
            Predicate::Compare(cmp)
        }
        Predicate::CompareFields(cmp) => Predicate::CompareFields(cmp),
        Predicate::True
        | Predicate::False
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => predicate,
    }
}

/// Canonicalize one lowered SQL filter expression against model-owned strict
/// literal rules so the expression shell and derived predicate stay in sync.
/// Canonicalize one lowered SQL filter expression through a schema view.
///
/// This keeps the expression shell and derived predicate in sync after strict
/// literal conversion, while allowing session execution to use the accepted
/// schema instead of generated metadata for top-level fields.
#[must_use]
pub(in crate::db) fn canonicalize_sql_filter_expr_for_schema(
    schema: &SchemaInfo,
    expr: Expr,
) -> Expr {
    match expr {
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) | Expr::Aggregate(_) => expr,
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(canonicalize_sql_filter_expr_for_schema(schema, *expr)),
        },
        Expr::Binary { op, left, right } => {
            canonicalize_sql_binary_expr_for_schema(schema, op, *left, *right)
        }
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(|arg| canonicalize_sql_filter_expr_for_schema(schema, arg))
                .collect(),
        },
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        canonicalize_sql_filter_expr_for_schema(schema, arm.condition().clone()),
                        canonicalize_sql_filter_expr_for_schema(schema, arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(canonicalize_sql_filter_expr_for_schema(schema, *else_expr)),
        },
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(canonicalize_sql_filter_expr_for_schema(schema, *expr)),
            name,
        },
    }
}

// Keep SQL-only strict literal canonicalization narrow:
// - only direct field predicates are eligible
// - text operators stay on raw text literals
// - field-kind-owned rewrites stay local to SQL lowering
fn canonicalize_sql_compare_for_schema(
    schema: &SchemaInfo,
    cmp: &mut crate::db::predicate::ComparePredicate,
) {
    match cmp.op {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => {
            if let Some((value, coercion)) = canonicalize_sql_compare_literal_for_schema(
                schema,
                cmp.field.as_str(),
                cmp.op,
                &cmp.value,
                cmp.coercion.id,
            ) {
                cmp.value = value;
                cmp.coercion = coercion;
            }
        }
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(items) = &cmp.value else {
                return;
            };

            if let Some((items, coercion)) = canonicalize_sql_compare_list_for_schema(
                schema,
                cmp.field.as_str(),
                cmp.op,
                items.as_slice(),
                cmp.coercion.id,
            ) {
                cmp.value = Value::List(items);
                cmp.coercion = coercion;
            }
        }
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => {}
    }
}

// Keep SQL filter-expression literal rewriting aligned with the predicate
// canonicalizer so planned residual filter expressions do not drift from the
// canonical predicate shell on converted literals.
fn canonicalize_sql_binary_expr_for_schema(
    schema: &SchemaInfo,
    op: BinaryOp,
    left: Expr,
    right: Expr,
) -> Expr {
    let left = canonicalize_sql_filter_expr_for_schema(schema, left);
    let right = canonicalize_sql_filter_expr_for_schema(schema, right);

    match (left, right, op) {
        (Expr::Field(field), Expr::Literal(value), op)
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Lte
                    | BinaryOp::Gt
                    | BinaryOp::Gte
            ) =>
        {
            let value = schema
                .canonicalize_strict_sql_literal(field.as_str(), &value)
                .unwrap_or(value);

            Expr::Binary {
                op,
                left: Box::new(Expr::Field(field)),
                right: Box::new(Expr::Literal(value)),
            }
        }
        (Expr::Literal(value), Expr::Field(field), op)
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Lte
                    | BinaryOp::Gt
                    | BinaryOp::Gte
            ) =>
        {
            let value = schema
                .canonicalize_strict_sql_literal(field.as_str(), &value)
                .unwrap_or(value);

            Expr::Binary {
                op,
                left: Box::new(Expr::Literal(value)),
                right: Box::new(Expr::Field(field)),
            }
        }
        (left, right, op) => Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

fn canonicalize_sql_compare_literal_for_schema(
    schema: &SchemaInfo,
    field: &str,
    op: CompareOp,
    value: &Value,
    coercion: CoercionId,
) -> Option<(Value, CoercionSpec)> {
    let value = schema.canonicalize_strict_sql_literal(field, value)?;
    let coercion = match coercion {
        CoercionId::Strict | CoercionId::NumericWiden
            if matches!(
                op,
                CompareOp::Eq
                    | CompareOp::Ne
                    | CompareOp::Lt
                    | CompareOp::Lte
                    | CompareOp::Gt
                    | CompareOp::Gte
            ) =>
        {
            CoercionSpec::new(CoercionId::Strict)
        }
        _ => return None,
    };

    Some((value, coercion))
}

fn canonicalize_sql_compare_list_for_schema(
    schema: &SchemaInfo,
    field: &str,
    op: CompareOp,
    items: &[Value],
    coercion: CoercionId,
) -> Option<(Vec<Value>, CoercionSpec)> {
    let coercion = match (coercion, op) {
        (CoercionId::Strict, _) => CoercionSpec::new(CoercionId::Strict),
        (CoercionId::NumericWiden, CompareOp::In | CompareOp::NotIn) => {
            CoercionSpec::new(CoercionId::Strict)
        }
        _ => return None,
    };
    let items = items
        .iter()
        .map(|item| schema.canonicalize_strict_sql_literal(field, item))
        .collect::<Option<Vec<_>>>()?;

    Some((items, coercion))
}
