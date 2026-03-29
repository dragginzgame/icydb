use crate::{
    db::{
        QueryError,
        session::sql::computed_projection::model::{
            SqlComputedProjectionItem, SqlComputedProjectionPlan,
        },
        sql::parser::{
            SqlExplainMode, SqlExplainStatement, SqlExplainTarget, SqlProjection, SqlSelectItem,
            SqlStatement, SqlTextFunction, SqlTextFunctionCall,
        },
    },
    value::Value,
};

// Validate one integer-like literal used by the narrow numeric text
// projection helpers.
fn validate_numeric_projection_literal(
    function_name: &str,
    field: &str,
    label: &str,
    value: Option<&Value>,
    required: bool,
) -> Result<(), QueryError> {
    match value {
        Some(Value::Null | Value::Int(_) | Value::Uint(_)) => Ok(()),
        Some(other) => Err(QueryError::unsupported_query(format!(
            "{function_name}({field}, ...) requires integer or NULL {label}, found {other:?}",
        ))),
        None if required => Err(QueryError::invariant(format!(
            "{function_name} projection item was missing its {label} literal",
        ))),
        None => Ok(()),
    }
}

// Validate the narrow literal contract for binary text projection helpers.
fn validate_computed_sql_projection_literal(
    function: SqlTextFunction,
    field: &str,
    literal: Option<&Value>,
) -> Result<(), QueryError> {
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
            function.projection_label(),
        ))),
    }
}

// Validate the second literal used only by `REPLACE`.
fn validate_computed_sql_projection_second_literal(
    function: SqlTextFunction,
    field: &str,
    literal: Option<&Value>,
) -> Result<(), QueryError> {
    match (function, literal) {
        (SqlTextFunction::Replace, Some(Value::Null | Value::Text(_)))
        | (SqlTextFunction::Substring, _) => Ok(()),
        (SqlTextFunction::Replace, Some(other)) => Err(QueryError::unsupported_query(format!(
            "REPLACE({field}, ..., ...) requires text or NULL replacement literal, found {other:?}",
        ))),
        (SqlTextFunction::Replace, None) => Err(QueryError::invariant(
            "REPLACE projection item was missing its replacement literal",
        )),
        (_, None) => Ok(()),
        (_, Some(_)) => Err(QueryError::invariant(
            "only REPLACE and SUBSTRING should carry a second projection literal",
        )),
    }
}

// Validate the integer-like literal arguments used by the numeric text
// projection helpers on the session-owned lane.
fn validate_computed_sql_projection_numeric_literals(
    function: SqlTextFunction,
    field: &str,
    start: Option<&Value>,
    len: Option<&Value>,
    extra: Option<&Value>,
) -> Result<(), QueryError> {
    if !matches!(
        function,
        SqlTextFunction::Substring | SqlTextFunction::Left | SqlTextFunction::Right
    ) {
        if extra.is_some() {
            return Err(QueryError::invariant(
                "only numeric text projection helpers should carry extra literal arguments",
            ));
        }

        return Ok(());
    }

    if matches!(function, SqlTextFunction::Left | SqlTextFunction::Right) {
        let function_name = function.projection_label();

        validate_numeric_projection_literal(function_name, field, "length", start, true)?;
        if len.is_some() || extra.is_some() {
            return Err(QueryError::invariant(format!(
                "{function_name} projection item carried unexpected extra literal arguments",
            )));
        }

        return Ok(());
    }

    let function_name = function.projection_label();

    validate_numeric_projection_literal(function_name, field, "start", start, true)?;
    validate_numeric_projection_literal(function_name, field, "length", len, false)?;
    if extra.is_some() {
        return Err(QueryError::invariant(
            "SUBSTRING projection item carried an unexpected extra literal",
        ));
    }

    Ok(())
}

// Validate and build one text-function projection item for the narrow
// session-owned computed projection lane.
fn computed_sql_projection_text_function_item(
    call: &SqlTextFunctionCall,
) -> Result<SqlComputedProjectionItem, QueryError> {
    validate_computed_sql_projection_literal(
        call.function,
        call.field.as_str(),
        call.literal.as_ref(),
    )?;
    validate_computed_sql_projection_second_literal(
        call.function,
        call.field.as_str(),
        call.literal2.as_ref(),
    )?;
    validate_computed_sql_projection_numeric_literals(
        call.function,
        call.field.as_str(),
        call.literal.as_ref(),
        call.literal2.as_ref(),
        call.literal3.as_ref(),
    )?;

    Ok(SqlComputedProjectionItem::text_function(
        call.function,
        call.field.clone(),
        call.literal.clone(),
        call.literal2.clone(),
        call.literal3.clone(),
    ))
}

// Build one narrow computed SQL projection plan when the parsed statement uses
// the currently shipped text projection forms on the session-owned lane.
pub(in crate::db::session::sql::computed_projection) fn computed_sql_projection_plan(
    statement: &SqlStatement,
) -> Result<Option<SqlComputedProjectionPlan>, QueryError> {
    let SqlStatement::Select(select) = statement else {
        return Ok(None);
    };
    let SqlProjection::Items(items) = &select.projection else {
        return Ok(None);
    };
    if !items
        .iter()
        .any(|item| matches!(item, SqlSelectItem::TextFunction(_)))
    {
        return Ok(None);
    }

    // Phase 1: fence this lane to the small scalar projection subset so it
    // does not silently broaden SQL semantics through the session boundary.
    if select.distinct || !select.group_by.is_empty() || !select.having.is_empty() {
        return Err(QueryError::unsupported_query(
            "computed SQL projection currently supports only scalar SELECT field lists plus TRIM(...) / LTRIM(...) / RTRIM(...) / LOWER(...) / UPPER(...) / LENGTH(...) / LEFT(...) / RIGHT(...) / STARTS_WITH(...) / ENDS_WITH(...) / CONTAINS(...) / POSITION(...) / REPLACE(...) / SUBSTRING(...) without DISTINCT or GROUP BY",
        ));
    }

    let mut base_items = Vec::with_capacity(items.len());
    let mut computed_items = Vec::with_capacity(items.len());

    // Phase 2: derive base field projection plus the post-load transform plan
    // in the same declaration order.
    for item in items {
        match item {
            SqlSelectItem::Field(field) => {
                base_items.push(SqlSelectItem::Field(field.clone()));
                computed_items.push(SqlComputedProjectionItem::field(field.clone()));
            }
            SqlSelectItem::TextFunction(call) => {
                base_items.push(SqlSelectItem::Field(call.field.clone()));
                computed_items.push(computed_sql_projection_text_function_item(call)?);
            }
            SqlSelectItem::Aggregate(_) => {
                return Err(QueryError::unsupported_query(
                    "computed SQL projection does not support aggregate projection items",
                ));
            }
        }
    }

    let mut base_select = select.clone();
    base_select.projection = SqlProjection::Items(base_items);

    Ok(Some(SqlComputedProjectionPlan {
        base_statement: SqlStatement::Select(base_select),
        items: computed_items,
    }))
}

// Build one narrow computed SQL projection plan from an EXPLAIN-wrapped SELECT
// when the wrapped statement uses the staged text projection surface.
pub(in crate::db::session::sql::computed_projection) fn computed_sql_projection_explain_plan(
    statement: &SqlStatement,
) -> Result<Option<(SqlExplainMode, SqlComputedProjectionPlan)>, QueryError> {
    let SqlStatement::Explain(SqlExplainStatement { mode, statement }) = statement else {
        return Ok(None);
    };
    let SqlExplainTarget::Select(select) = statement else {
        return Ok(None);
    };

    computed_sql_projection_plan(&SqlStatement::Select(select.clone()))
        .map(|plan| plan.map(|plan| (*mode, plan)))
}
