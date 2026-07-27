//! Structured proposal binding and canonicalization for `CheckExprV1`.

#[cfg(any(test, feature = "sql"))]
use crate::db::schema::check::MAX_CHECK_EXPR_V1_MEMBERSHIP_ITEMS;
use crate::{
    db::{
        data::encode_input_value_for_candidate_field_contract,
        schema::{
            AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckLiteralV1,
            AcceptedCheckValueExprV1, AcceptedCompositeCatalog, AcceptedEnumCatalog,
            AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedSourceBindingCatalog,
            PersistedSchemaSnapshot, ValueAdmissionBudget,
            check::{AcceptedCheckExprV1Error, nat64_codec, nat64_kind},
        },
    },
    model::field::{FieldStorageDecode, LeafCodec},
    types::EntityTag,
    value::{InputValue, InputValueEnum},
};
use icydb_schema::{ScalarLiteral, SourceCheckExpr, SourceCheckInstruction};

#[cfg(feature = "sql")]
use crate::db::{
    schema::{PersistedFieldSnapshot, input_value_from_strict_sql_literal_for_persisted_kind},
    sql::parser::{SqlExpr, SqlExprBinaryOp, SqlExprUnaryOp, SqlScalarFunction},
};
#[cfg(feature = "sql")]
use crate::value::Value;

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
    #[cfg(any(test, feature = "sql"))]
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

#[derive(Clone)]
struct ValueBinding {
    kind: AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

struct BoundValue {
    expression: AcceptedCheckValueExprV1,
    binding: ValueBinding,
}

/// Bind names, admit literals, lower sugar, and return one canonical AST.
pub(in crate::db) fn bind_check_expr_v1(
    input: CheckExprV1Input,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    let expression = bind_expression(input, snapshot, enum_catalog, composite_catalog)?;
    expression.validate(snapshot, composite_catalog)?;
    Ok(expression)
}

/// Bind one immutable-source-key expression through the same accepted
/// frontend-neutral compiler used by generated and SQL declarations.
pub(in crate::db::schema) fn bind_source_check_expr(
    expression: &SourceCheckExpr,
    entity: EntityTag,
    bindings: &AcceptedSourceBindingCatalog,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    let mut stack = Vec::new();
    for instruction in expression.instructions() {
        match instruction {
            SourceCheckInstruction::Field(source) => {
                let field_id = bindings
                    .field(entity, source)
                    .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
                let field = snapshot
                    .fields()
                    .iter()
                    .find(|field| field.id() == field_id)
                    .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
                let kind = composite_catalog
                    .resolve_newtype_value_kind(field.kind())
                    .ok_or(AcceptedCheckExprV1Error::UnsupportedFieldKind)?;
                stack.push(SourceCheckNode::Value(SourceCheckValue::Field {
                    name: field.name().to_string(),
                    kind,
                }));
            }
            SourceCheckInstruction::Literal(literal) => stack.push(SourceCheckNode::Value(
                SourceCheckValue::Literal(literal.clone()),
            )),
            SourceCheckInstruction::Equal
            | SourceCheckInstruction::NotEqual
            | SourceCheckInstruction::LessThan
            | SourceCheckInstruction::LessThanOrEqual
            | SourceCheckInstruction::GreaterThan
            | SourceCheckInstruction::GreaterThanOrEqual => {
                let right = pop_source_value(&mut stack)?;
                let left = pop_source_value(&mut stack)?;
                let (left, right) = source_comparison_values(left, right, bindings, enum_catalog)?;
                stack.push(SourceCheckNode::Boolean(CheckExprV1Input::Compare {
                    left,
                    op: source_compare_op(instruction)
                        .ok_or(AcceptedCheckExprV1Error::UnsupportedOperator)?,
                    right,
                }));
            }
            SourceCheckInstruction::And | SourceCheckInstruction::Or => {
                let right = pop_source_boolean(&mut stack)?;
                let left = pop_source_boolean(&mut stack)?;
                stack.push(SourceCheckNode::Boolean(match instruction {
                    SourceCheckInstruction::And => CheckExprV1Input::And(vec![left, right]),
                    SourceCheckInstruction::Or => CheckExprV1Input::Or(vec![left, right]),
                    _ => return Err(AcceptedCheckExprV1Error::UnsupportedOperator),
                }));
            }
            SourceCheckInstruction::Not => {
                let inner = pop_source_boolean(&mut stack)?;
                stack.push(SourceCheckNode::Boolean(CheckExprV1Input::Not(Box::new(
                    inner,
                ))));
            }
            SourceCheckInstruction::IsNull | SourceCheckInstruction::IsNotNull => {
                let value = source_value_without_literal(pop_source_value(&mut stack)?)?;
                stack.push(SourceCheckNode::Boolean(match instruction {
                    SourceCheckInstruction::IsNull => CheckExprV1Input::IsNull(value),
                    SourceCheckInstruction::IsNotNull => CheckExprV1Input::IsNotNull(value),
                    _ => return Err(AcceptedCheckExprV1Error::UnsupportedOperator),
                }));
            }
            SourceCheckInstruction::Length => {
                let value = pop_source_value(&mut stack)?;
                let SourceCheckValue::Field { name, kind } = value else {
                    return Err(AcceptedCheckExprV1Error::UnsupportedOperator);
                };
                let expression = match kind {
                    AcceptedFieldKind::Text { .. } => CheckValueExprV1Input::CharLength(name),
                    AcceptedFieldKind::Blob { .. } => CheckValueExprV1Input::OctetLength(name),
                    AcceptedFieldKind::List(_)
                    | AcceptedFieldKind::Set(_)
                    | AcceptedFieldKind::Map { .. } => CheckValueExprV1Input::Cardinality(name),
                    _ => return Err(AcceptedCheckExprV1Error::UnsupportedOperator),
                };
                stack.push(SourceCheckNode::Value(SourceCheckValue::Bound {
                    expression,
                    kind: AcceptedFieldKind::Nat64,
                }));
            }
        }
    }
    let [result] = stack.as_slice() else {
        return Err(AcceptedCheckExprV1Error::UnsupportedOperator);
    };
    let input = source_node_boolean(result.clone())?;
    bind_check_expr_v1(input, snapshot, enum_catalog, composite_catalog)
}

#[derive(Clone)]
enum SourceCheckNode {
    Boolean(CheckExprV1Input),
    Value(SourceCheckValue),
}

#[derive(Clone)]
enum SourceCheckValue {
    Field {
        name: String,
        kind: AcceptedFieldKind,
    },
    Bound {
        expression: CheckValueExprV1Input,
        kind: AcceptedFieldKind,
    },
    Literal(ScalarLiteral),
}

fn pop_source_value(
    stack: &mut Vec<SourceCheckNode>,
) -> Result<SourceCheckValue, AcceptedCheckExprV1Error> {
    match stack.pop() {
        Some(SourceCheckNode::Value(value)) => Ok(value),
        Some(SourceCheckNode::Boolean(_)) | None => {
            Err(AcceptedCheckExprV1Error::UnsupportedOperator)
        }
    }
}

fn pop_source_boolean(
    stack: &mut Vec<SourceCheckNode>,
) -> Result<CheckExprV1Input, AcceptedCheckExprV1Error> {
    let node = stack
        .pop()
        .ok_or(AcceptedCheckExprV1Error::UnsupportedOperator)?;
    source_node_boolean(node)
}

fn source_node_boolean(
    node: SourceCheckNode,
) -> Result<CheckExprV1Input, AcceptedCheckExprV1Error> {
    match node {
        SourceCheckNode::Boolean(expression) => Ok(expression),
        SourceCheckNode::Value(SourceCheckValue::Literal(ScalarLiteral::Bool(true))) => {
            Ok(CheckExprV1Input::True)
        }
        SourceCheckNode::Value(SourceCheckValue::Literal(ScalarLiteral::Bool(false))) => {
            Ok(CheckExprV1Input::False)
        }
        SourceCheckNode::Value(_) => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
    }
}

fn source_comparison_values(
    left: SourceCheckValue,
    right: SourceCheckValue,
    bindings: &AcceptedSourceBindingCatalog,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<(CheckValueExprV1Input, CheckValueExprV1Input), AcceptedCheckExprV1Error> {
    let left_kind = source_value_kind(&left);
    let right_kind = source_value_kind(&right);
    let left = source_value_input(left, right_kind.as_ref(), bindings, enum_catalog)?;
    let right = source_value_input(right, left_kind.as_ref(), bindings, enum_catalog)?;
    Ok((left, right))
}

fn source_value_kind(value: &SourceCheckValue) -> Option<AcceptedFieldKind> {
    match value {
        SourceCheckValue::Field { kind, .. } | SourceCheckValue::Bound { kind, .. } => {
            Some(kind.clone())
        }
        SourceCheckValue::Literal(_) => None,
    }
}

fn source_value_input(
    value: SourceCheckValue,
    expected: Option<&AcceptedFieldKind>,
    bindings: &AcceptedSourceBindingCatalog,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<CheckValueExprV1Input, AcceptedCheckExprV1Error> {
    match value {
        SourceCheckValue::Field { name, .. } => Ok(CheckValueExprV1Input::Field(name)),
        SourceCheckValue::Bound { expression, .. } => Ok(expression),
        SourceCheckValue::Literal(literal) => source_literal_input(
            &literal,
            expected.ok_or(AcceptedCheckExprV1Error::LiteralAdmissionRejected)?,
            bindings,
            enum_catalog,
        )
        .map(CheckValueExprV1Input::Literal),
    }
}

fn source_value_without_literal(
    value: SourceCheckValue,
) -> Result<CheckValueExprV1Input, AcceptedCheckExprV1Error> {
    match value {
        SourceCheckValue::Field { name, .. } => Ok(CheckValueExprV1Input::Field(name)),
        SourceCheckValue::Bound { expression, .. } => Ok(expression),
        SourceCheckValue::Literal(_) => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
    }
}

const fn source_compare_op(
    instruction: &SourceCheckInstruction,
) -> Option<AcceptedCheckCompareOpV1> {
    match instruction {
        SourceCheckInstruction::Equal => Some(AcceptedCheckCompareOpV1::Eq),
        SourceCheckInstruction::NotEqual => Some(AcceptedCheckCompareOpV1::Ne),
        SourceCheckInstruction::LessThan => Some(AcceptedCheckCompareOpV1::Lt),
        SourceCheckInstruction::LessThanOrEqual => Some(AcceptedCheckCompareOpV1::Lte),
        SourceCheckInstruction::GreaterThan => Some(AcceptedCheckCompareOpV1::Gt),
        SourceCheckInstruction::GreaterThanOrEqual => Some(AcceptedCheckCompareOpV1::Gte),
        _ => None,
    }
}

pub(in crate::db::schema) fn source_literal_input(
    literal: &ScalarLiteral,
    expected: &AcceptedFieldKind,
    bindings: &AcceptedSourceBindingCatalog,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<InputValue, AcceptedCheckExprV1Error> {
    let value = match literal {
        ScalarLiteral::Account(value) => InputValue::Account(*value),
        ScalarLiteral::Blob(value) => InputValue::Blob(value.to_vec()),
        ScalarLiteral::Bool(value) => InputValue::Bool(*value),
        ScalarLiteral::Date(value) => InputValue::Date(*value),
        ScalarLiteral::Decimal(value) => InputValue::Decimal(*value),
        ScalarLiteral::Duration(value) => InputValue::Duration(*value),
        ScalarLiteral::EnumUnit { enum_type, variant } => {
            let Some(crate::db::schema::AcceptedNamedTypeIdentity::Enum(type_id)) =
                bindings.named_type(enum_type)
            else {
                return Err(AcceptedCheckExprV1Error::LiteralAdmissionRejected);
            };
            let variant_id = bindings
                .enum_variant(type_id, variant)
                .ok_or(AcceptedCheckExprV1Error::LiteralAdmissionRejected)?;
            let definition = enum_catalog
                .enum_type(type_id)
                .ok_or(AcceptedCheckExprV1Error::LiteralAdmissionRejected)?;
            let accepted_variant = definition
                .variant(variant_id)
                .ok_or(AcceptedCheckExprV1Error::LiteralAdmissionRejected)?;
            if !matches!(expected, AcceptedFieldKind::Enum { type_id: expected } if *expected == type_id)
            {
                return Err(AcceptedCheckExprV1Error::LiteralAdmissionRejected);
            }
            InputValue::Enum(InputValueEnum::new(
                accepted_variant.name(),
                Some(definition.path()),
            ))
        }
        ScalarLiteral::Float32(value) => InputValue::Float32(*value),
        ScalarLiteral::Float64(value) => InputValue::Float64(*value),
        ScalarLiteral::Int(value) => match expected {
            AcceptedFieldKind::Int8
            | AcceptedFieldKind::Int16
            | AcceptedFieldKind::Int32
            | AcceptedFieldKind::Int64 => InputValue::Int64(
                i64::try_from(*value)
                    .map_err(|_| AcceptedCheckExprV1Error::LiteralAdmissionRejected)?,
            ),
            AcceptedFieldKind::Int128 => InputValue::Int128(*value),
            _ => return Err(AcceptedCheckExprV1Error::LiteralAdmissionRejected),
        },
        ScalarLiteral::IntBig(value) => InputValue::IntBig(value.clone()),
        ScalarLiteral::Nat(value) => match expected {
            AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64 => InputValue::Nat64(
                u64::try_from(*value)
                    .map_err(|_| AcceptedCheckExprV1Error::LiteralAdmissionRejected)?,
            ),
            AcceptedFieldKind::Nat128 => InputValue::Nat128(*value),
            _ => return Err(AcceptedCheckExprV1Error::LiteralAdmissionRejected),
        },
        ScalarLiteral::NatBig(value) => InputValue::NatBig(value.clone()),
        ScalarLiteral::Principal(value) => InputValue::Principal(*value),
        ScalarLiteral::Subaccount(value) => InputValue::Subaccount(*value),
        ScalarLiteral::Text(value) => InputValue::Text(value.clone()),
        ScalarLiteral::Timestamp(value) => InputValue::Timestamp(*value),
        ScalarLiteral::Ulid(value) => InputValue::Ulid(*value),
        ScalarLiteral::Unit(_) => InputValue::Unit,
    };
    Ok(value)
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
        CheckExprV1Input::IsNull(value) => {
            bind_non_literal_value(value, snapshot, composite_catalog)
                .map(|value| AcceptedCheckExprV1::IsNull(value.expression))
        }
        CheckExprV1Input::IsNotNull(value) => {
            bind_non_literal_value(value, snapshot, composite_catalog)
                .map(|value| AcceptedCheckExprV1::IsNotNull(value.expression))
        }
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
        #[cfg(any(test, feature = "sql"))]
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
            let right = bind_non_literal_value(right, snapshot, composite_catalog)?;
            let left = AcceptedCheckValueExprV1::Literal(bind_literal(
                literal,
                right.binding,
                enum_catalog,
                composite_catalog,
            )?);
            (left, right.expression)
        }
        (left, CheckValueExprV1Input::Literal(literal)) => {
            let left = bind_non_literal_value(left, snapshot, composite_catalog)?;
            let right = AcceptedCheckValueExprV1::Literal(bind_literal(
                literal,
                left.binding,
                enum_catalog,
                composite_catalog,
            )?);
            (left.expression, right)
        }
        (left, right) => {
            let left = bind_non_literal_value(left, snapshot, composite_catalog)?;
            let right = bind_non_literal_value(right, snapshot, composite_catalog)?;
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
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<BoundValue, AcceptedCheckExprV1Error> {
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
    if matches!(field.kind(), AcceptedFieldKind::Relation { .. }) {
        return Err(AcceptedCheckExprV1Error::UnsupportedFieldKind);
    }
    let resolved_kind = composite_catalog
        .resolve_newtype_value_kind(field.kind())
        .ok_or(AcceptedCheckExprV1Error::UnsupportedFieldKind)?;

    let (expression, binding) = match operation {
        0 => {
            let binding = if matches!(field.kind(), AcceptedFieldKind::Composite { .. }) {
                value_binding_for_resolved_kind(resolved_kind)
            } else {
                ValueBinding {
                    kind: field.kind().clone(),
                    storage_decode: field.storage_decode(),
                    leaf_codec: field.leaf_codec(),
                }
            };
            (AcceptedCheckValueExprV1::Field(field.id()), binding)
        }
        1 if matches!(resolved_kind, AcceptedFieldKind::Text { .. }) => (
            AcceptedCheckValueExprV1::CharLength(field.id()),
            computed_length_binding(),
        ),
        2 if matches!(resolved_kind, AcceptedFieldKind::Blob { .. }) => (
            AcceptedCheckValueExprV1::OctetLength(field.id()),
            computed_length_binding(),
        ),
        3 if matches!(
            resolved_kind,
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

const fn value_binding_for_resolved_kind(kind: AcceptedFieldKind) -> ValueBinding {
    let storage_decode = FieldStorageDecode::ByKind;
    let leaf_codec = kind.leaf_codec_for_storage(storage_decode);
    ValueBinding {
        kind,
        storage_decode,
        leaf_codec,
    }
}

fn computed_length_binding() -> ValueBinding {
    let (storage_decode, leaf_codec) = nat64_codec();
    ValueBinding {
        kind: nat64_kind().clone(),
        storage_decode,
        leaf_codec,
    }
}

fn bind_literal(
    input: InputValue,
    expected: ValueBinding,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckLiteralV1, AcceptedCheckExprV1Error> {
    if matches!(input, InputValue::Null) {
        return Err(AcceptedCheckExprV1Error::NullLiteralUnsupported);
    }
    let field = AcceptedFieldDecodeContract::new(
        "__icydb_check_literal",
        &expected.kind,
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
        expected.kind,
        expected.storage_decode,
        expected.leaf_codec,
        payload,
    ))
}

#[cfg(any(test, feature = "sql"))]
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
        kind: field.kind().clone(),
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
                    binding.clone(),
                    enum_catalog,
                    composite_catalog,
                )?),
            })
        })
        .collect::<Result<Vec<_>, AcceptedCheckExprV1Error>>()?;

    AcceptedCheckExprV1::canonicalized_or(comparisons)
}
