//! Module: db::session::sql::computed_projection::plan
//! Responsibility: validate parser output and build the narrow computed SQL
//! projection plan used by session-owned text transforms.
//! Does not own: SQL parsing, row evaluation, or payload rendering.
//! Boundary: accepts parser statements and emits only validated computed
//! projection metadata.

use crate::{
    db::{
        QueryError,
        session::sql::computed_projection::model::{
            SqlComputedProjectionItem, SqlComputedProjectionPlan,
        },
        sql::parser::{
            SqlAggregateCall, SqlAggregateKind, SqlExplainMode, SqlExplainStatement,
            SqlExplainTarget, SqlProjection, SqlSelectItem, SqlSelectStatement, SqlStatement,
            SqlTextFunction, SqlTextFunctionCall,
        },
    },
    value::Value,
};

// Resolve one computed-projection `ORDER BY <alias>` onto the already-shipped
// order target family before this lane rewrites the base projection to plain
// fields. This keeps computed execution alias-neutral once the base statement
// enters shared SQL lowering.
fn rewrite_computed_projection_order_aliases(
    select: &SqlSelectStatement,
    order_by: &mut [crate::db::sql::parser::SqlOrderTerm],
) -> Result<(), QueryError> {
    let SqlProjection::Items(items) = &select.projection else {
        return Ok(());
    };

    for term in order_by.iter_mut() {
        for (item, alias) in items.iter().zip(select.projection_aliases.iter()) {
            let Some(alias) = alias.as_deref() else {
                continue;
            };
            if !alias.eq_ignore_ascii_case(term.field.as_str()) {
                continue;
            }

            term.field = match item {
                SqlSelectItem::Field(field) => field.clone(),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Lower,
                    field,
                    literal: None,
                    literal2: None,
                    literal3: None,
                }) => format!("LOWER({field})"),
                SqlSelectItem::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Upper,
                    field,
                    literal: None,
                    literal2: None,
                    literal3: None,
                }) => format!("UPPER({field})"),
                SqlSelectItem::Aggregate(_) | SqlSelectItem::TextFunction(_) => {
                    return Err(QueryError::unsupported_query(format!(
                        "ORDER BY alias '{alias}' does not resolve to a supported order target",
                    )));
                }
            };

            break;
        }
    }

    Ok(())
}

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

// Render one grouped aggregate output label for the session-owned computed
// projection lane so grouped SQL execution and grouped direct execution keep the
// same outward column contract.
fn computed_sql_grouped_aggregate_label(aggregate: &SqlAggregateCall) -> String {
    let kind = match aggregate.kind {
        SqlAggregateKind::Count => "COUNT",
        SqlAggregateKind::Sum => "SUM",
        SqlAggregateKind::Avg => "AVG",
        SqlAggregateKind::Min => "MIN",
        SqlAggregateKind::Max => "MAX",
    };

    match aggregate.field.as_deref() {
        Some(field) if aggregate.distinct => format!("{kind}(DISTINCT {field})"),
        Some(field) => format!("{kind}({field})"),
        None => format!("{kind}(*)"),
    }
}

// Resolve one computed projection output label from parser-owned alias
// metadata while keeping generic structural planning alias-neutral.
fn computed_sql_output_label(
    select: &crate::db::sql::parser::SqlSelectStatement,
    index: usize,
    fallback: impl FnOnce() -> String,
) -> String {
    select
        .projection_alias(index)
        .map_or_else(fallback, str::to_string)
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

    let grouped_surface = !select.group_by.is_empty();

    // Phase 1: fence this lane to the currently admitted scalar and grouped
    // projection subsets so it does not silently broaden SQL semantics
    // through the session boundary.
    if (!grouped_surface && (select.distinct || !select.having.is_empty()))
        || (grouped_surface && !select.having.is_empty())
    {
        return Err(QueryError::unsupported_query(
            "computed SQL projection currently supports only scalar SELECT field lists plus TRIM(...) / LTRIM(...) / RTRIM(...) / LOWER(...) / UPPER(...) / LENGTH(...) / LEFT(...) / RIGHT(...) / STARTS_WITH(...) / ENDS_WITH(...) / CONTAINS(...) / POSITION(...) / REPLACE(...) / SUBSTRING(...), or grouped SELECT lists where those text functions wrap grouped fields before aggregate outputs",
        ));
    }

    let mut base_items = Vec::with_capacity(items.len());
    let mut computed_items = Vec::with_capacity(items.len());
    let mut projected_group_fields = Vec::new();
    let mut seen_aggregate = false;

    // Phase 2: derive base field projection plus the post-load transform plan
    // in the same declaration order.
    for (index, item) in items.iter().enumerate() {
        match item {
            SqlSelectItem::Field(field) => {
                if grouped_surface {
                    if seen_aggregate {
                        return Err(QueryError::unsupported_query(
                            "grouped computed SQL projection requires grouped fields before aggregate outputs",
                        ));
                    }
                    projected_group_fields.push(field.clone());
                }
                base_items.push(SqlSelectItem::Field(field.clone()));
                let mut computed = SqlComputedProjectionItem::field(field.clone());
                computed.output_label = computed_sql_output_label(select, index, || field.clone());
                computed_items.push(computed);
            }
            SqlSelectItem::TextFunction(call) => {
                if grouped_surface {
                    if seen_aggregate {
                        return Err(QueryError::unsupported_query(
                            "grouped computed SQL projection requires grouped fields before aggregate outputs",
                        ));
                    }
                    projected_group_fields.push(call.field.clone());
                }
                base_items.push(SqlSelectItem::Field(call.field.clone()));
                let mut computed = computed_sql_projection_text_function_item(call)?;
                computed.output_label =
                    computed_sql_output_label(select, index, || computed.output_label.clone());
                computed_items.push(computed);
            }
            SqlSelectItem::Aggregate(aggregate) => {
                if !grouped_surface {
                    return Err(QueryError::unsupported_query(
                        "computed SQL projection does not support aggregate projection items",
                    ));
                }
                seen_aggregate = true;
                base_items.push(SqlSelectItem::Aggregate(aggregate.clone()));
                computed_items.push(SqlComputedProjectionItem::passthrough(
                    computed_sql_output_label(select, index, || {
                        computed_sql_grouped_aggregate_label(aggregate)
                    }),
                ));
            }
        }
    }

    // Phase 3: keep grouped computed projection on the same grouped SQL
    // ownership boundary as the admitted grouped lowering lane.
    if grouped_surface
        && (!seen_aggregate || projected_group_fields.as_slice() != select.group_by.as_slice())
    {
        return Err(QueryError::unsupported_query(
            "grouped computed SQL projection currently supports only grouped fields or text functions over those grouped fields followed by aggregate outputs",
        ));
    }

    let mut base_select = select.clone();
    if grouped_surface {
        // Top-level DISTINCT is redundant for the admitted grouped SQL lane,
        // so keep the computed rewrite on the same normalized grouped query.
        base_select.distinct = false;
    }

    rewrite_computed_projection_order_aliases(select, &mut base_select.order_by)?;
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
