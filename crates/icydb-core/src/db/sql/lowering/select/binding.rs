use crate::{
    db::{
        predicate::{CoercionId, CoercionSpec, CompareOp, Predicate},
        query::plan::expr::{BinaryOp, Expr, Function},
        schema::SchemaInfo,
    },
    value::{Value, canonicalize_value_set},
};

// Canonicalize strict numeric SQL predicate literals onto the resolved model
// field kind so unsigned-width fields keep strict/indexable semantics even
// though reduced SQL integer tokens parse through one generic numeric value
// variant first.
// Canonicalize strict numeric SQL predicate literals through the provided
// schema view. Session SQL compile paths pass the accepted schema projection
// here so top-level read predicates line up with live schema reconciliation.
pub(super) fn canonicalize_sql_predicate_for_schema(
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
pub(super) fn canonicalize_sql_filter_expr_for_schema(schema: &SchemaInfo, mut expr: Expr) -> Expr {
    match &mut expr {
        Expr::Binary { op, left, right } => {
            canonicalize_sql_binary_expr_for_schema(schema, *op, left.take(), right.take())
        }
        Expr::FunctionCall { function, args } => {
            canonicalize_sql_filter_function_for_schema(schema, *function, std::mem::take(args))
        }
        _ => {
            expr.map_scalar_children(|child| {
                canonicalize_sql_filter_expr_for_schema(schema, child)
            });
            expr
        }
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

fn canonicalize_sql_filter_function_for_schema(
    schema: &SchemaInfo,
    function: Function,
    args: Vec<Expr>,
) -> Expr {
    let args = args
        .into_iter()
        .map(|arg| canonicalize_sql_filter_expr_for_schema(schema, arg))
        .collect::<Vec<_>>();

    match function {
        Function::InList => canonicalize_sql_in_list_expr_for_schema(schema, args.as_slice())
            .unwrap_or(Expr::FunctionCall { function, args }),
        _ => Expr::FunctionCall { function, args },
    }
}

fn canonicalize_sql_in_list_expr_for_schema(schema: &SchemaInfo, args: &[Expr]) -> Option<Expr> {
    let [Expr::Field(field), Expr::Literal(Value::List(items))] = args else {
        return None;
    };

    let (items, _) = canonicalize_sql_compare_list_for_schema(
        schema,
        field.as_str(),
        CompareOp::In,
        items.as_slice(),
        CoercionId::NumericWiden,
    )?;

    Some(Expr::membership(Expr::Field(field.clone()), items, false))
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
    let mut left = canonicalize_sql_filter_expr_for_schema(schema, left);
    let mut right = canonicalize_sql_filter_expr_for_schema(schema, right);
    if matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte
    ) {
        let literal = match (&mut left, &mut right) {
            (Expr::Field(field), Expr::Literal(value))
            | (Expr::Literal(value), Expr::Field(field)) => Some((field, value)),
            _ => None,
        };
        if let Some((field, value)) = literal
            && let Some(canonical) = schema.canonicalize_strict_sql_literal(field.as_str(), value)
        {
            *value = canonical;
        }
    }

    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
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
    let mut items = items
        .iter()
        .map(|item| schema.canonicalize_strict_sql_literal(field, item))
        .collect::<Option<Vec<_>>>()?;
    canonicalize_value_set(&mut items);

    Some((items, coercion))
}
