//! Module: db::schema::check::render
//! Responsibility: deterministic SQL rendering of accepted check semantics.
//! Does not own: source SQL retention, check binding, or runtime evaluation.
//! Boundary: projects the field-ID-bound accepted AST through current accepted names.

use super::{
    AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckLiteralV1,
    AcceptedCheckValueExprV1, compile::decode_literal,
};
use crate::{
    db::{
        schema::{AcceptedValueCatalogHandle, FieldId, PersistedSchemaSnapshot},
        sql_shared::render_scalar_sql_value,
    },
    error::InternalError,
    value::Value,
};

/// Render one accepted check expression through current accepted field names.
pub(in crate::db) fn render_accepted_check_expr_sql(
    expression: &AcceptedCheckExprV1,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<String, InternalError> {
    render_expression(expression, snapshot, value_catalog)
}

fn render_expression(
    expression: &AcceptedCheckExprV1,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<String, InternalError> {
    match expression {
        AcceptedCheckExprV1::True => Ok("TRUE".to_string()),
        AcceptedCheckExprV1::False => Ok("FALSE".to_string()),
        AcceptedCheckExprV1::Not(inner) => Ok(format!(
            "NOT ({})",
            render_expression(inner, snapshot, value_catalog)?
        )),
        AcceptedCheckExprV1::And(children) => {
            render_children(children, "AND", snapshot, value_catalog)
        }
        AcceptedCheckExprV1::Or(children) => {
            render_children(children, "OR", snapshot, value_catalog)
        }
        AcceptedCheckExprV1::Compare { left, op, right } => Ok(format!(
            "{} {} {}",
            render_value(left, snapshot, value_catalog)?,
            compare_operator(*op),
            render_value(right, snapshot, value_catalog)?,
        )),
        AcceptedCheckExprV1::IsNull(value) => Ok(format!(
            "{} IS NULL",
            render_value(value, snapshot, value_catalog)?
        )),
        AcceptedCheckExprV1::IsNotNull(value) => Ok(format!(
            "{} IS NOT NULL",
            render_value(value, snapshot, value_catalog)?
        )),
        AcceptedCheckExprV1::MultipleOf { value, factor } => Ok(format!(
            "MULTIPLE_OF({}, {})",
            render_value(value, snapshot, value_catalog)?,
            render_literal(factor, value_catalog)?,
        )),
    }
}

fn render_children(
    children: &[AcceptedCheckExprV1],
    operator: &str,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<String, InternalError> {
    children
        .iter()
        .map(|child| {
            render_expression(child, snapshot, value_catalog)
                .map(|rendered| format!("({rendered})"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|children| children.join(format!(" {operator} ").as_str()))
}

fn render_value(
    value: &AcceptedCheckValueExprV1,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<String, InternalError> {
    match value {
        AcceptedCheckValueExprV1::Field(field_id) => accepted_field_name(snapshot, *field_id),
        AcceptedCheckValueExprV1::Literal(literal) => render_literal(literal, value_catalog),
        AcceptedCheckValueExprV1::CharLength(field_id) => Ok(format!(
            "LENGTH({})",
            accepted_field_name(snapshot, *field_id)?
        )),
        AcceptedCheckValueExprV1::OctetLength(field_id) => Ok(format!(
            "OCTET_LENGTH({})",
            accepted_field_name(snapshot, *field_id)?
        )),
        AcceptedCheckValueExprV1::Cardinality(field_id) => Ok(format!(
            "CARDINALITY({})",
            accepted_field_name(snapshot, *field_id)?
        )),
    }
}

fn render_literal(
    literal: &AcceptedCheckLiteralV1,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<String, InternalError> {
    let value = decode_literal(literal, value_catalog)
        .map_err(|_| InternalError::accepted_row_constraint_program_corrupt())?;
    let sql_value = match value {
        Value::Enum(value) if value.payload().is_none() => {
            let variant = value_catalog
                .enum_catalog()
                .resolve_value(value.canonical())
                .map_err(|_| InternalError::accepted_row_constraint_program_corrupt())?
                .variant_name()
                .to_string();
            Value::Text(variant)
        }
        Value::Ulid(value) => Value::Text(value.to_string()),
        value => value,
    };
    render_scalar_sql_value(&sql_value)
        .ok_or_else(InternalError::accepted_row_constraint_program_corrupt)
}

fn accepted_field_name(
    snapshot: &PersistedSchemaSnapshot,
    field_id: FieldId,
) -> Result<String, InternalError> {
    snapshot
        .fields()
        .iter()
        .find(|field| field.id() == field_id)
        .map(|field| field.name().to_string())
        .ok_or_else(InternalError::accepted_row_constraint_program_corrupt)
}

const fn compare_operator(operator: AcceptedCheckCompareOpV1) -> &'static str {
    match operator {
        AcceptedCheckCompareOpV1::Eq => "=",
        AcceptedCheckCompareOpV1::Ne => "!=",
        AcceptedCheckCompareOpV1::Lt => "<",
        AcceptedCheckCompareOpV1::Lte => "<=",
        AcceptedCheckCompareOpV1::Gt => ">",
        AcceptedCheckCompareOpV1::Gte => ">=",
    }
}
