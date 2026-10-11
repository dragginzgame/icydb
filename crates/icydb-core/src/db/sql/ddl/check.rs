//! Lower SQL CHECK syntax into the sole accepted-schema check binder.

use crate::{
    db::{
        schema::{
            AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckExprV1Error,
            AcceptedCompositeCatalog, AcceptedEnumCatalog, AcceptedFieldKind, CheckExprV1Input,
            CheckValueExprV1Input, PersistedSchemaSnapshot, bind_check_expr_v1,
            input_value_from_strict_sql_literal_for_persisted_kind,
        },
        sql::parser::{SqlExpr, SqlExprBinaryOp, SqlExprUnaryOp, SqlScalarFunction},
    },
    value::Value,
};

pub(in crate::db) fn bind_sql_check_expr(
    expression: &SqlExpr,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    let input = sql_check_expr_input(expression, snapshot, composite_catalog)?;
    bind_check_expr_v1(input, snapshot, enum_catalog, composite_catalog)
}

fn sql_check_expr_input(
    expression: &SqlExpr,
    snapshot: &PersistedSchemaSnapshot,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<CheckExprV1Input, AcceptedCheckExprV1Error> {
    match expression {
        SqlExpr::Literal(Value::Bool(true)) => Ok(CheckExprV1Input::True),
        SqlExpr::Literal(Value::Bool(false)) => Ok(CheckExprV1Input::False),
        SqlExpr::Unary {
            op: SqlExprUnaryOp::Not,
            expr,
        } => sql_check_expr_input(expr, snapshot, composite_catalog)
            .map(Box::new)
            .map(CheckExprV1Input::Not),
        SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left,
            right,
        } => Ok(CheckExprV1Input::And(vec![
            sql_check_expr_input(left, snapshot, composite_catalog)?,
            sql_check_expr_input(right, snapshot, composite_catalog)?,
        ])),
        SqlExpr::Binary {
            op: SqlExprBinaryOp::Or,
            left,
            right,
        } => Ok(CheckExprV1Input::Or(vec![
            sql_check_expr_input(left, snapshot, composite_catalog)?,
            sql_check_expr_input(right, snapshot, composite_catalog)?,
        ])),
        SqlExpr::Binary { op, left, right } => Ok(CheckExprV1Input::Compare {
            left: sql_check_value_input(left, Some(right), snapshot, composite_catalog)?,
            op: sql_check_compare_op(*op)?,
            right: sql_check_value_input(right, Some(left), snapshot, composite_catalog)?,
        }),
        SqlExpr::BooleanTest {
            expr,
            value,
            negated,
        } => {
            // CHECK admits UNKNOWN, so totalize the Boolean test before its
            // accepted expression reaches either checks or index membership.
            let literal = SqlExpr::Literal(Value::Bool(*value));
            let operand = sql_check_value_input(expr, Some(&literal), snapshot, composite_catalog)?;
            let comparison = CheckExprV1Input::Compare {
                left: operand.clone(),
                op: AcceptedCheckCompareOpV1::Eq,
                right: sql_check_value_input(&literal, Some(expr), snapshot, composite_catalog)?,
            };
            let total =
                CheckExprV1Input::And(vec![CheckExprV1Input::IsNotNull(operand), comparison]);
            Ok(if *negated {
                CheckExprV1Input::Not(Box::new(total))
            } else {
                total
            })
        }
        SqlExpr::NullTest { expr, negated } => {
            let value = sql_check_value_input(expr, None, snapshot, composite_catalog)?;
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
                    let crate::db::sql::parser::SqlMembershipValue::Literal(value) = value else {
                        return Err(AcceptedCheckExprV1Error::UnsupportedOperator);
                    };
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

fn sql_check_value_input(
    expression: &SqlExpr,
    counterpart: Option<&SqlExpr>,
    snapshot: &PersistedSchemaSnapshot,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Result<CheckValueExprV1Input, AcceptedCheckExprV1Error> {
    match expression {
        SqlExpr::Field(field_name) => Ok(CheckValueExprV1Input::Field(field_name.clone())),
        SqlExpr::Literal(value) => {
            let expected = counterpart
                .and_then(|other| sql_check_operand_kind(other, snapshot, composite_catalog))
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
                SqlScalarFunction::Cardinality => {
                    Ok(CheckValueExprV1Input::Cardinality(field_name.clone()))
                }
                _ => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
            }
        }
        SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Param { .. }
        | SqlExpr::Membership { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::BooleanTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Binary { .. }
        | SqlExpr::Case { .. } => Err(AcceptedCheckExprV1Error::UnsupportedOperator),
    }
}

fn sql_check_operand_kind<'a>(
    expression: &SqlExpr,
    snapshot: &'a PersistedSchemaSnapshot,
    composite_catalog: &'a AcceptedCompositeCatalog,
) -> Option<&'a AcceptedFieldKind> {
    match expression {
        // Literal admission must observe the same accepted value kind as the
        // frontend-neutral binder, including nominal newtype wrappers.
        SqlExpr::Field(field_name) => snapshot
            .fields()
            .iter()
            .find(|field| field.name() == field_name)
            .and_then(|field| composite_catalog.resolve_newtype_value_kind(field.kind())),
        SqlExpr::FunctionCall { function, args }
            if matches!(
                (function, args.as_slice()),
                (
                    SqlScalarFunction::Length
                        | SqlScalarFunction::OctetLength
                        | SqlScalarFunction::Cardinality,
                    [SqlExpr::Field(_)]
                )
            ) =>
        {
            Some(&AcceptedFieldKind::Nat64)
        }
        _ => None,
    }
}

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
