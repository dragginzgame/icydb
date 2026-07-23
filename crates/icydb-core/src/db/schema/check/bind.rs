//! Structured proposal binding and canonicalization for `CheckExprV1`.

use crate::{
    db::{
        CompareOp, Predicate,
        data::encode_input_value_for_candidate_field_contract,
        schema::{
            AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckLiteralV1,
            AcceptedCheckValueExprV1, AcceptedCompositeCatalog, AcceptedEnumCatalog,
            AcceptedFieldDecodeContract, AcceptedFieldKind, PersistedSchemaSnapshot,
            ValueAdmissionBudget,
            check::{
                AcceptedCheckExprV1Error, MAX_CHECK_EXPR_V1_MEMBERSHIP_ITEMS, nat64_codec,
                nat64_kind,
            },
            input_value_from_strict_sql_literal_for_persisted_kind,
        },
    },
    model::field::{FieldStorageDecode, LeafCodec},
    value::{InputValue, Value},
};

#[cfg(feature = "sql")]
use crate::db::{
    schema::PersistedFieldSnapshot,
    sql::parser::{SqlExpr, SqlExprBinaryOp, SqlExprUnaryOp, SqlScalarFunction},
};

/// Structured frontend-neutral proposal for one row-local check expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CheckExprV1Input {
    True,
    False,
    Not(Box<Self>),
    And(Vec<Self>),
    Or(Vec<Self>),
    Compare {
        left: CheckValueExprV1Input,
        op: AcceptedCheckCompareOpV1,
        right: CheckValueExprV1Input,
    },
    IsNull(CheckValueExprV1Input),
    IsNotNull(CheckValueExprV1Input),
    #[cfg(test)]
    Between {
        value: CheckValueExprV1Input,
        lower: InputValue,
        upper: InputValue,
    },
    EnumIn {
        field: String,
        members: Vec<InputValue>,
    },
}

/// Unbound value operand used by generated and SQL check frontends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CheckValueExprV1Input {
    Field(String),
    Literal(InputValue),
    CharLength(String),
    OctetLength(String),
    Cardinality(String),
}

#[derive(Clone, Copy)]
struct ValueBinding<'a> {
    kind: &'a AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

struct BoundValue<'a> {
    expression: AcceptedCheckValueExprV1,
    binding: ValueBinding<'a>,
}

/// Bind names, admit literals, lower sugar, and return one canonical AST.
pub(in crate::db) fn bind_check_expr_v1(
    input: CheckExprV1Input,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    let expression = bind_expression(input, snapshot, enum_catalog, composite_catalog)?;
    expression.validate(snapshot)?;
    Ok(expression)
}

/// Bind one structured generated predicate to accepted check semantics.
pub(in crate::db) fn bind_generated_check_predicate(
    predicate: &Predicate,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    let input = generated_predicate_input(predicate, snapshot)?;
    bind_check_expr_v1(input, snapshot, enum_catalog, composite_catalog)
}

/// Bind one parser-owned SQL expression into the same accepted check AST used
/// by generated declarations.
#[cfg(feature = "sql")]
pub(in crate::db) fn bind_sql_check_expr(
    expression: &SqlExpr,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    let input = sql_check_expr_input(expression, snapshot)?;
    bind_check_expr_v1(input, snapshot, enum_catalog, composite_catalog)
}

#[cfg(feature = "sql")]
fn sql_check_expr_input(
    expression: &SqlExpr,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<CheckExprV1Input, AcceptedCheckExprV1Error> {
    match expression {
        SqlExpr::Literal(Value::Bool(true)) => Ok(CheckExprV1Input::True),
        SqlExpr::Literal(Value::Bool(false)) => Ok(CheckExprV1Input::False),
        SqlExpr::Unary {
            op: SqlExprUnaryOp::Not,
            expr,
        } => sql_check_expr_input(expr, snapshot)
            .map(Box::new)
            .map(CheckExprV1Input::Not),
        SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left,
            right,
        } => Ok(CheckExprV1Input::And(vec![
            sql_check_expr_input(left, snapshot)?,
            sql_check_expr_input(right, snapshot)?,
        ])),
        SqlExpr::Binary {
            op: SqlExprBinaryOp::Or,
            left,
            right,
        } => Ok(CheckExprV1Input::Or(vec![
            sql_check_expr_input(left, snapshot)?,
            sql_check_expr_input(right, snapshot)?,
        ])),
        SqlExpr::Binary { op, left, right } => Ok(CheckExprV1Input::Compare {
            left: sql_check_value_input(left, Some(right), snapshot)?,
            op: sql_check_compare_op(*op)?,
            right: sql_check_value_input(right, Some(left), snapshot)?,
        }),
        SqlExpr::NullTest { expr, negated } => {
            let value = sql_check_value_input(expr, None, snapshot)?;
            Ok(if *negated {
                CheckExprV1Input::IsNotNull(value)
            } else {
                CheckExprV1Input::IsNull(value)
            })
        }
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => {
            let SqlExpr::Field(field_name) = expr.as_ref() else {
                return Err(AcceptedCheckExprV1Error::UnsupportedOperator);
            };
            let field = snapshot
                .fields()
                .iter()
                .find(|field| field.name() == field_name)
                .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
            let members = values
                .iter()
                .map(|value| {
                    input_value_from_strict_sql_literal_for_persisted_kind(field.kind(), value)
                        .ok_or(AcceptedCheckExprV1Error::LiteralAdmissionRejected)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let membership = CheckExprV1Input::EnumIn {
                field: field_name.clone(),
                members,
            };
            Ok(if *negated {
                CheckExprV1Input::Not(Box::new(membership))
            } else {
                membership
            })
        }
        SqlExpr::Field(_)
        | SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Case { .. } => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
    }
}

#[cfg(feature = "sql")]
fn sql_check_value_input(
    expression: &SqlExpr,
    counterpart: Option<&SqlExpr>,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<CheckValueExprV1Input, AcceptedCheckExprV1Error> {
    match expression {
        SqlExpr::Field(field_name) => Ok(CheckValueExprV1Input::Field(field_name.clone())),
        SqlExpr::Literal(value) => {
            let expected = counterpart
                .and_then(|other| sql_check_operand_kind(other, snapshot))
                .ok_or(AcceptedCheckExprV1Error::LiteralRequiresExpectedKind)?;
            input_value_from_strict_sql_literal_for_persisted_kind(expected, value)
                .map(CheckValueExprV1Input::Literal)
                .ok_or(AcceptedCheckExprV1Error::LiteralAdmissionRejected)
        }
        SqlExpr::FunctionCall { function, args } => {
            let [SqlExpr::Field(field_name)] = args.as_slice() else {
                return Err(AcceptedCheckExprV1Error::UnsupportedOperator);
            };
            match function {
                SqlScalarFunction::Length => {
                    Ok(CheckValueExprV1Input::CharLength(field_name.clone()))
                }
                SqlScalarFunction::OctetLength => {
                    Ok(CheckValueExprV1Input::OctetLength(field_name.clone()))
                }
                _ => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
            }
        }
        SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Param { .. }
        | SqlExpr::Membership { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Binary { .. }
        | SqlExpr::Case { .. } => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
    }
}

#[cfg(feature = "sql")]
fn sql_check_operand_kind<'a>(
    expression: &SqlExpr,
    snapshot: &'a PersistedSchemaSnapshot,
) -> Option<&'a AcceptedFieldKind> {
    match expression {
        SqlExpr::Field(field_name) => snapshot
            .fields()
            .iter()
            .find(|field| field.name() == field_name)
            .map(PersistedFieldSnapshot::kind),
        SqlExpr::FunctionCall { function, args }
            if matches!(
                (function, args.as_slice()),
                (
                    SqlScalarFunction::Length | SqlScalarFunction::OctetLength,
                    [SqlExpr::Field(_)]
                )
            ) =>
        {
            Some(nat64_kind())
        }
        _ => None,
    }
}

#[cfg(feature = "sql")]
const fn sql_check_compare_op(
    operation: SqlExprBinaryOp,
) -> Result<AcceptedCheckCompareOpV1, AcceptedCheckExprV1Error> {
    match operation {
        SqlExprBinaryOp::Eq => Ok(AcceptedCheckCompareOpV1::Eq),
        SqlExprBinaryOp::Ne => Ok(AcceptedCheckCompareOpV1::Ne),
        SqlExprBinaryOp::Lt => Ok(AcceptedCheckCompareOpV1::Lt),
        SqlExprBinaryOp::Lte => Ok(AcceptedCheckCompareOpV1::Lte),
        SqlExprBinaryOp::Gt => Ok(AcceptedCheckCompareOpV1::Gt),
        SqlExprBinaryOp::Gte => Ok(AcceptedCheckCompareOpV1::Gte),
        SqlExprBinaryOp::Or
        | SqlExprBinaryOp::And
        | SqlExprBinaryOp::Add
        | SqlExprBinaryOp::Sub
        | SqlExprBinaryOp::Mul
        | SqlExprBinaryOp::Div => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
    }
}

fn generated_predicate_input(
    predicate: &Predicate,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<CheckExprV1Input, AcceptedCheckExprV1Error> {
    match predicate {
        Predicate::True => Ok(CheckExprV1Input::True),
        Predicate::False => Ok(CheckExprV1Input::False),
        Predicate::And(children) => children
            .iter()
            .map(|child| generated_predicate_input(child, snapshot))
            .collect::<Result<Vec<_>, _>>()
            .map(CheckExprV1Input::And),
        Predicate::Or(children) => children
            .iter()
            .map(|child| generated_predicate_input(child, snapshot))
            .collect::<Result<Vec<_>, _>>()
            .map(CheckExprV1Input::Or),
        Predicate::Not(inner) => generated_predicate_input(inner, snapshot)
            .map(Box::new)
            .map(CheckExprV1Input::Not),
        Predicate::Compare(compare) => {
            generated_compare_input(compare.field(), compare.op(), compare.value(), snapshot)
        }
        Predicate::CompareFields(compare) => Ok(CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field(compare.left_field().to_string()),
            op: accepted_compare_op(compare.op())?,
            right: CheckValueExprV1Input::Field(compare.right_field().to_string()),
        }),
        Predicate::IsNull { field } => Ok(CheckExprV1Input::IsNull(CheckValueExprV1Input::Field(
            field.clone(),
        ))),
        Predicate::IsNotNull { field } => Ok(CheckExprV1Input::IsNotNull(
            CheckValueExprV1Input::Field(field.clone()),
        )),
        Predicate::IsEmpty { field } => generated_empty_compare(field, false, snapshot),
        Predicate::IsNotEmpty { field } => generated_empty_compare(field, true, snapshot),
        Predicate::IsMissing { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
    }
}

fn generated_compare_input(
    field_name: &str,
    op: CompareOp,
    value: &Value,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<CheckExprV1Input, AcceptedCheckExprV1Error> {
    if matches!(op, CompareOp::In | CompareOp::NotIn) {
        let Value::List(members) = value else {
            return Err(AcceptedCheckExprV1Error::LiteralAdmissionRejected);
        };
        let members = members
            .iter()
            .map(|member| generated_literal_input(field_name, member, snapshot))
            .collect::<Result<Vec<_>, _>>()?;
        let membership = CheckExprV1Input::EnumIn {
            field: field_name.to_string(),
            members,
        };
        return Ok(if matches!(op, CompareOp::NotIn) {
            CheckExprV1Input::Not(Box::new(membership))
        } else {
            membership
        });
    }

    Ok(CheckExprV1Input::Compare {
        left: CheckValueExprV1Input::Field(field_name.to_string()),
        op: accepted_compare_op(op)?,
        right: CheckValueExprV1Input::Literal(generated_literal_input(
            field_name, value, snapshot,
        )?),
    })
}

fn generated_literal_input(
    field_name: &str,
    value: &Value,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<InputValue, AcceptedCheckExprV1Error> {
    let field = snapshot
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
    input_value_from_strict_sql_literal_for_persisted_kind(field.kind(), value)
        .ok_or(AcceptedCheckExprV1Error::LiteralAdmissionRejected)
}

fn generated_empty_compare(
    field_name: &str,
    negated: bool,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<CheckExprV1Input, AcceptedCheckExprV1Error> {
    let field = snapshot
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
    let left = match field.kind() {
        AcceptedFieldKind::Text { .. } => CheckValueExprV1Input::CharLength(field_name.to_string()),
        AcceptedFieldKind::Blob { .. } => {
            CheckValueExprV1Input::OctetLength(field_name.to_string())
        }
        AcceptedFieldKind::List(_) | AcceptedFieldKind::Set(_) | AcceptedFieldKind::Map { .. } => {
            CheckValueExprV1Input::Cardinality(field_name.to_string())
        }
        _ => return Err(AcceptedCheckExprV1Error::LengthOperationKindMismatch),
    };
    Ok(CheckExprV1Input::Compare {
        left,
        op: if negated {
            AcceptedCheckCompareOpV1::Ne
        } else {
            AcceptedCheckCompareOpV1::Eq
        },
        right: CheckValueExprV1Input::Literal(InputValue::Nat64(0)),
    })
}

const fn accepted_compare_op(
    op: CompareOp,
) -> Result<AcceptedCheckCompareOpV1, AcceptedCheckExprV1Error> {
    match op {
        CompareOp::Eq => Ok(AcceptedCheckCompareOpV1::Eq),
        CompareOp::Ne => Ok(AcceptedCheckCompareOpV1::Ne),
        CompareOp::Lt => Ok(AcceptedCheckCompareOpV1::Lt),
        CompareOp::Lte => Ok(AcceptedCheckCompareOpV1::Lte),
        CompareOp::Gt => Ok(AcceptedCheckCompareOpV1::Gt),
        CompareOp::Gte => Ok(AcceptedCheckCompareOpV1::Gte),
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
    }
}

fn bind_expression(
    input: CheckExprV1Input,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    match input {
        CheckExprV1Input::True => Ok(AcceptedCheckExprV1::True),
        CheckExprV1Input::False => Ok(AcceptedCheckExprV1::False),
        CheckExprV1Input::Not(inner) => Ok(AcceptedCheckExprV1::Not(Box::new(bind_expression(
            *inner,
            snapshot,
            enum_catalog,
            composite_catalog,
        )?))),
        CheckExprV1Input::And(children) => children
            .into_iter()
            .map(|child| bind_expression(child, snapshot, enum_catalog, composite_catalog))
            .collect::<Result<Vec<_>, _>>()
            .and_then(AcceptedCheckExprV1::canonicalized_and),
        CheckExprV1Input::Or(children) => children
            .into_iter()
            .map(|child| bind_expression(child, snapshot, enum_catalog, composite_catalog))
            .collect::<Result<Vec<_>, _>>()
            .and_then(AcceptedCheckExprV1::canonicalized_or),
        CheckExprV1Input::Compare { left, op, right } => {
            bind_compare(left, op, right, snapshot, enum_catalog, composite_catalog)
        }
        CheckExprV1Input::IsNull(value) => bind_non_literal_value(value, snapshot)
            .map(|value| AcceptedCheckExprV1::IsNull(value.expression)),
        CheckExprV1Input::IsNotNull(value) => bind_non_literal_value(value, snapshot)
            .map(|value| AcceptedCheckExprV1::IsNotNull(value.expression)),
        #[cfg(test)]
        CheckExprV1Input::Between {
            value,
            lower,
            upper,
        } => {
            let lower = bind_compare(
                value.clone(),
                AcceptedCheckCompareOpV1::Gte,
                CheckValueExprV1Input::Literal(lower),
                snapshot,
                enum_catalog,
                composite_catalog,
            )?;
            let upper = bind_compare(
                value,
                AcceptedCheckCompareOpV1::Lte,
                CheckValueExprV1Input::Literal(upper),
                snapshot,
                enum_catalog,
                composite_catalog,
            )?;
            AcceptedCheckExprV1::canonicalized_and(vec![lower, upper])
        }
        CheckExprV1Input::EnumIn { field, members } => {
            bind_enum_membership(field, members, snapshot, enum_catalog, composite_catalog)
        }
    }
}

fn bind_compare(
    left: CheckValueExprV1Input,
    op: AcceptedCheckCompareOpV1,
    right: CheckValueExprV1Input,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    let (left, right) = match (left, right) {
        (CheckValueExprV1Input::Literal(_), CheckValueExprV1Input::Literal(_)) => {
            return Err(AcceptedCheckExprV1Error::LiteralRequiresExpectedKind);
        }
        (CheckValueExprV1Input::Literal(literal), right) => {
            let right = bind_non_literal_value(right, snapshot)?;
            let left = AcceptedCheckValueExprV1::Literal(bind_literal(
                literal,
                right.binding,
                enum_catalog,
                composite_catalog,
            )?);
            (left, right.expression)
        }
        (left, CheckValueExprV1Input::Literal(literal)) => {
            let left = bind_non_literal_value(left, snapshot)?;
            let right = AcceptedCheckValueExprV1::Literal(bind_literal(
                literal,
                left.binding,
                enum_catalog,
                composite_catalog,
            )?);
            (left.expression, right)
        }
        (left, right) => {
            let left = bind_non_literal_value(left, snapshot)?;
            let right = bind_non_literal_value(right, snapshot)?;
            if left.binding.kind != right.binding.kind {
                return Err(AcceptedCheckExprV1Error::OperandKindMismatch);
            }
            (left.expression, right.expression)
        }
    };

    Ok(AcceptedCheckExprV1::Compare { left, op, right })
}

fn bind_non_literal_value(
    input: CheckValueExprV1Input,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<BoundValue<'_>, AcceptedCheckExprV1Error> {
    let (field_name, operation) = match input {
        CheckValueExprV1Input::Field(name) => (name, 0_u8),
        CheckValueExprV1Input::CharLength(name) => (name, 1),
        CheckValueExprV1Input::OctetLength(name) => (name, 2),
        CheckValueExprV1Input::Cardinality(name) => (name, 3),
        CheckValueExprV1Input::Literal(_) => {
            return Err(AcceptedCheckExprV1Error::LiteralRequiresExpectedKind);
        }
    };
    let field = snapshot
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
    if matches!(
        field.kind(),
        AcceptedFieldKind::Relation { .. } | AcceptedFieldKind::Composite { .. }
    ) {
        return Err(AcceptedCheckExprV1Error::UnsupportedFieldKind);
    }

    let (expression, binding) = match operation {
        0 => (
            AcceptedCheckValueExprV1::Field(field.id()),
            ValueBinding {
                kind: field.kind(),
                storage_decode: field.storage_decode(),
                leaf_codec: field.leaf_codec(),
            },
        ),
        1 if matches!(field.kind(), AcceptedFieldKind::Text { .. }) => (
            AcceptedCheckValueExprV1::CharLength(field.id()),
            computed_length_binding(),
        ),
        2 if matches!(field.kind(), AcceptedFieldKind::Blob { .. }) => (
            AcceptedCheckValueExprV1::OctetLength(field.id()),
            computed_length_binding(),
        ),
        3 if matches!(
            field.kind(),
            AcceptedFieldKind::List(_) | AcceptedFieldKind::Set(_) | AcceptedFieldKind::Map { .. }
        ) =>
        {
            (
                AcceptedCheckValueExprV1::Cardinality(field.id()),
                computed_length_binding(),
            )
        }
        _ => return Err(AcceptedCheckExprV1Error::LengthOperationKindMismatch),
    };

    Ok(BoundValue {
        expression,
        binding,
    })
}

const fn computed_length_binding() -> ValueBinding<'static> {
    let (storage_decode, leaf_codec) = nat64_codec();
    ValueBinding {
        kind: nat64_kind(),
        storage_decode,
        leaf_codec,
    }
}

fn bind_literal(
    input: InputValue,
    expected: ValueBinding<'_>,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckLiteralV1, AcceptedCheckExprV1Error> {
    if matches!(input, InputValue::Null) {
        return Err(AcceptedCheckExprV1Error::NullLiteralUnsupported);
    }
    let field = AcceptedFieldDecodeContract::new(
        "__icydb_check_literal",
        expected.kind,
        false,
        expected.storage_decode,
        expected.leaf_codec,
    );
    let mut budget = ValueAdmissionBudget::standard();
    let payload = encode_input_value_for_candidate_field_contract(
        enum_catalog,
        composite_catalog,
        field,
        input,
        &mut budget,
    )
    .map_err(|_| AcceptedCheckExprV1Error::LiteralAdmissionRejected)?;

    Ok(AcceptedCheckLiteralV1::from_accepted_parts(
        expected.kind.clone(),
        expected.storage_decode,
        expected.leaf_codec,
        payload,
    ))
}

fn bind_enum_membership(
    field_name: String,
    members: Vec<InputValue>,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    if members.is_empty() {
        return Err(AcceptedCheckExprV1Error::MembershipEmpty);
    }
    if members.len() > MAX_CHECK_EXPR_V1_MEMBERSHIP_ITEMS {
        return Err(AcceptedCheckExprV1Error::MembershipTooWide);
    }
    let field = snapshot
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
    if !matches!(field.kind(), AcceptedFieldKind::Enum { .. }) {
        return Err(AcceptedCheckExprV1Error::MembershipRequiresEnumField);
    }
    let field_id = field.id();
    let binding = ValueBinding {
        kind: field.kind(),
        storage_decode: field.storage_decode(),
        leaf_codec: field.leaf_codec(),
    };
    let comparisons = members
        .into_iter()
        .map(|member| {
            Ok(AcceptedCheckExprV1::Compare {
                left: AcceptedCheckValueExprV1::Field(field_id),
                op: AcceptedCheckCompareOpV1::Eq,
                right: AcceptedCheckValueExprV1::Literal(bind_literal(
                    member,
                    binding,
                    enum_catalog,
                    composite_catalog,
                )?),
            })
        })
        .collect::<Result<Vec<_>, AcceptedCheckExprV1Error>>()?;

    AcceptedCheckExprV1::canonicalized_or(comparisons)
}
