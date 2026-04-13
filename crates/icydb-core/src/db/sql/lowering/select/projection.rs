use crate::db::sql::lowering::{SqlLoweringError, aggregate::lower_aggregate_call};
use crate::{
    db::{
        QueryError,
        query::{
            builder::TextProjectionExpr,
            plan::expr::{Alias, Expr, FieldId, Function, ProjectionField, ProjectionSelection},
        },
        sql::parser::{SqlProjection, SqlSelectItem, SqlTextFunction, SqlTextFunctionCall},
    },
    value::Value,
};

pub(super) fn lower_scalar_projection_selection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
    distinct: bool,
) -> Result<ProjectionSelection, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        let _ = distinct;
        return Ok(ProjectionSelection::All);
    };

    let has_aggregate = items
        .iter()
        .any(|item| matches!(item, SqlSelectItem::Aggregate(_)));
    if has_aggregate {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    if let Some(field_ids) = direct_scalar_field_selection(items.as_slice(), projection_aliases) {
        return Ok(ProjectionSelection::Fields(field_ids));
    }

    let fields = items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            lower_projection_field(
                item,
                projection_aliases.get(index).and_then(Option::as_deref),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    if distinct && fields.is_empty() {
        return Ok(ProjectionSelection::Exprs(fields));
    }

    Ok(ProjectionSelection::Exprs(fields))
}

pub(super) fn lower_grouped_projection_selection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
    group_by: &[String],
) -> Result<ProjectionSelection, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::unsupported_select_group_by());
    };

    let mut projected_group_fields = Vec::new();
    let mut seen_aggregate = false;
    let mut fields = Vec::with_capacity(items.len());

    for (index, item) in items.into_iter().enumerate() {
        match &item {
            SqlSelectItem::Field(field) => {
                if seen_aggregate {
                    return Err(SqlLoweringError::unsupported_select_group_by());
                }

                projected_group_fields.push(field.clone());
            }
            SqlSelectItem::TextFunction(_) => {
                return Err(SqlLoweringError::unsupported_select_group_by());
            }
            SqlSelectItem::Aggregate(_) => {
                seen_aggregate = true;
            }
        }

        fields.push(lower_projection_field(
            item,
            projection_aliases.get(index).and_then(Option::as_deref),
        )?);
    }

    if !seen_aggregate || projected_group_fields.as_slice() != group_by {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    if projection_aliases.iter().all(Option::is_none) {
        return Ok(ProjectionSelection::All);
    }

    Ok(ProjectionSelection::Exprs(fields))
}

pub(super) fn direct_scalar_field_selection(
    items: &[SqlSelectItem],
    projection_aliases: &[Option<String>],
) -> Option<Vec<FieldId>> {
    if !projection_aliases.iter().all(Option::is_none) {
        return None;
    }

    items
        .iter()
        .map(|item| match item {
            SqlSelectItem::Field(field) => Some(FieldId::new(field.clone())),
            SqlSelectItem::Aggregate(_) | SqlSelectItem::TextFunction(_) => None,
        })
        .collect()
}

fn lower_projection_field(
    item: SqlSelectItem,
    alias: Option<&str>,
) -> Result<ProjectionField, SqlLoweringError> {
    Ok(ProjectionField::Scalar {
        expr: match item {
            SqlSelectItem::Field(field) => Expr::Field(FieldId::new(field)),
            SqlSelectItem::Aggregate(aggregate) => {
                Expr::Aggregate(lower_aggregate_call(aggregate)?)
            }
            SqlSelectItem::TextFunction(call) => lower_text_function_expr(&call)?,
        },
        alias: alias.map(Alias::new),
    })
}

fn lower_text_function_expr(call: &SqlTextFunctionCall) -> Result<Expr, SqlLoweringError> {
    validate_text_function_literal_contract(call)?;

    let projection = match call.function {
        SqlTextFunction::Trim => TextProjectionExpr::unary(call.field.clone(), Function::Trim),
        SqlTextFunction::Ltrim => TextProjectionExpr::unary(call.field.clone(), Function::Ltrim),
        SqlTextFunction::Rtrim => TextProjectionExpr::unary(call.field.clone(), Function::Rtrim),
        SqlTextFunction::Lower => TextProjectionExpr::unary(call.field.clone(), Function::Lower),
        SqlTextFunction::Upper => TextProjectionExpr::unary(call.field.clone(), Function::Upper),
        SqlTextFunction::Length => TextProjectionExpr::unary(call.field.clone(), Function::Length),
        SqlTextFunction::Left => TextProjectionExpr::with_literal(
            call.field.clone(),
            Function::Left,
            call.literal.clone().unwrap_or(Value::Null),
        ),
        SqlTextFunction::Right => TextProjectionExpr::with_literal(
            call.field.clone(),
            Function::Right,
            call.literal.clone().unwrap_or(Value::Null),
        ),
        SqlTextFunction::StartsWith => TextProjectionExpr::with_literal(
            call.field.clone(),
            Function::StartsWith,
            call.literal.clone().unwrap_or(Value::Null),
        ),
        SqlTextFunction::EndsWith => TextProjectionExpr::with_literal(
            call.field.clone(),
            Function::EndsWith,
            call.literal.clone().unwrap_or(Value::Null),
        ),
        SqlTextFunction::Contains => TextProjectionExpr::with_literal(
            call.field.clone(),
            Function::Contains,
            call.literal.clone().unwrap_or(Value::Null),
        ),
        SqlTextFunction::Position => TextProjectionExpr::position(
            call.field.clone(),
            call.literal.clone().unwrap_or(Value::Null),
        ),
        SqlTextFunction::Replace => TextProjectionExpr::with_two_literals(
            call.field.clone(),
            Function::Replace,
            call.literal.clone().unwrap_or(Value::Null),
            call.literal2.clone().unwrap_or(Value::Null),
        ),
        SqlTextFunction::Substring => match call.literal2.clone() {
            Some(length) => TextProjectionExpr::with_two_literals(
                call.field.clone(),
                Function::Substring,
                call.literal.clone().unwrap_or(Value::Null),
                length,
            ),
            None => TextProjectionExpr::with_literal(
                call.field.clone(),
                Function::Substring,
                call.literal.clone().unwrap_or(Value::Null),
            ),
        },
    };

    Ok(projection.expr().clone())
}

fn validate_text_function_literal_contract(
    call: &SqlTextFunctionCall,
) -> Result<(), SqlLoweringError> {
    validate_text_function_primary_literal(
        call.function,
        call.field.as_str(),
        call.literal.as_ref(),
    )?;
    validate_text_function_second_literal(
        call.function,
        call.field.as_str(),
        call.literal2.as_ref(),
    )?;
    validate_text_function_numeric_literals(
        call.function,
        call.field.as_str(),
        call.literal.as_ref(),
        call.literal2.as_ref(),
        call.literal3.as_ref(),
    )?;

    Ok(())
}

fn validate_text_function_primary_literal(
    function: SqlTextFunction,
    field: &str,
    literal: Option<&Value>,
) -> Result<(), SqlLoweringError> {
    if matches!(
        function,
        SqlTextFunction::Substring | SqlTextFunction::Left | SqlTextFunction::Right
    ) {
        return Ok(());
    }

    match literal {
        None | Some(Value::Null | Value::Text(_)) => Ok(()),
        Some(other) => Err(QueryError::unsupported_query(format!(
            "{}({field}, ...) requires text or NULL literal argument, found {other:?}",
            sql_text_function_to_function(function).sql_label(),
        ))
        .into()),
    }
}

fn validate_text_function_second_literal(
    function: SqlTextFunction,
    field: &str,
    literal: Option<&Value>,
) -> Result<(), SqlLoweringError> {
    match (function, literal) {
        (SqlTextFunction::Replace, Some(Value::Null | Value::Text(_)))
        | (SqlTextFunction::Substring, _) => Ok(()),
        (SqlTextFunction::Replace, Some(other)) => Err(QueryError::unsupported_query(format!(
            "REPLACE({field}, ..., ...) requires text or NULL replacement literal, found {other:?}",
        ))
        .into()),
        (SqlTextFunction::Replace, None) => Err(QueryError::invariant(
            "REPLACE projection item was missing its replacement literal",
        )
        .into()),
        (_, None) => Ok(()),
        (_, Some(_)) => Err(QueryError::invariant(
            "only REPLACE and SUBSTRING should carry a second projection literal",
        )
        .into()),
    }
}

fn validate_text_function_numeric_literals(
    function: SqlTextFunction,
    field: &str,
    start: Option<&Value>,
    len: Option<&Value>,
    extra: Option<&Value>,
) -> Result<(), SqlLoweringError> {
    if !matches!(
        function,
        SqlTextFunction::Substring | SqlTextFunction::Left | SqlTextFunction::Right
    ) {
        if extra.is_some() {
            return Err(QueryError::invariant(
                "only numeric text projection helpers should carry extra literal arguments",
            )
            .into());
        }

        return Ok(());
    }

    if matches!(function, SqlTextFunction::Left | SqlTextFunction::Right) {
        let function_name = sql_text_function_to_function(function).sql_label();

        validate_numeric_projection_literal(function_name, field, "length", start, true)?;
        if len.is_some() || extra.is_some() {
            return Err(QueryError::invariant(format!(
                "{function_name} projection item carried unexpected extra literal arguments",
            ))
            .into());
        }

        return Ok(());
    }

    let function_name = sql_text_function_to_function(function).sql_label();

    validate_numeric_projection_literal(function_name, field, "start", start, true)?;
    validate_numeric_projection_literal(function_name, field, "length", len, false)?;
    if extra.is_some() {
        return Err(QueryError::invariant(
            "SUBSTRING projection item carried an unexpected extra literal",
        )
        .into());
    }

    Ok(())
}

fn validate_numeric_projection_literal(
    function_name: &str,
    field: &str,
    label: &str,
    value: Option<&Value>,
    required: bool,
) -> Result<(), SqlLoweringError> {
    match value {
        Some(Value::Null | Value::Int(_) | Value::Uint(_)) => Ok(()),
        Some(other) => Err(QueryError::unsupported_query(format!(
            "{function_name}({field}, ...) requires integer or NULL {label}, found {other:?}",
        ))
        .into()),
        None if required => Err(QueryError::invariant(format!(
            "{function_name} projection item was missing its {label} literal",
        ))
        .into()),
        None => Ok(()),
    }
}

const fn sql_text_function_to_function(function: SqlTextFunction) -> Function {
    match function {
        SqlTextFunction::Trim => Function::Trim,
        SqlTextFunction::Ltrim => Function::Ltrim,
        SqlTextFunction::Rtrim => Function::Rtrim,
        SqlTextFunction::Lower => Function::Lower,
        SqlTextFunction::Upper => Function::Upper,
        SqlTextFunction::Length => Function::Length,
        SqlTextFunction::Left => Function::Left,
        SqlTextFunction::Right => Function::Right,
        SqlTextFunction::StartsWith => Function::StartsWith,
        SqlTextFunction::EndsWith => Function::EndsWith,
        SqlTextFunction::Contains => Function::Contains,
        SqlTextFunction::Position => Function::Position,
        SqlTextFunction::Replace => Function::Replace,
        SqlTextFunction::Substring => Function::Substring,
    }
}
