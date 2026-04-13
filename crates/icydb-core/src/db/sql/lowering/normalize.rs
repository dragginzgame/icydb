use crate::db::sql::lowering::SqlLoweringError;
use crate::db::{
    predicate::Predicate,
    query::plan::expr::{
        Expr, FieldId, Function, parse_supported_order_expr, render_supported_order_expr,
        rewrite_supported_order_expr_field, supported_order_expr_field,
    },
    sql::{
        identifier::{
            identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
            rewrite_field_identifiers,
        },
        parser::{
            SqlAggregateCall, SqlHavingClause, SqlHavingSymbol, SqlOrderTerm, SqlProjection,
            SqlSelectItem, SqlSelectStatement, SqlTextFunction, SqlTextFunctionCall,
        },
    },
};

pub(in crate::db::sql::lowering) fn normalize_select_statement_to_expected_entity(
    mut statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    // Re-scope parsed identifiers onto the resolved entity surface after the
    // caller has already established entity ownership for this statement.
    let entity_scope = sql_entity_scope_candidates(statement.entity.as_str(), expected_entity);
    statement.projection =
        normalize_projection_identifiers(statement.projection, entity_scope.as_slice());
    statement.group_by = normalize_identifier_list(statement.group_by, entity_scope.as_slice());
    statement.predicate = statement
        .predicate
        .map(|predicate| adapt_predicate_identifiers_to_scope(predicate, entity_scope.as_slice()));
    statement.order_by = normalize_select_order_terms(
        statement.order_by,
        &statement.projection,
        statement.projection_aliases.as_slice(),
        entity_scope.as_slice(),
    )?;
    statement.having = normalize_having_clauses(statement.having, entity_scope.as_slice());

    Ok(statement)
}

pub(in crate::db::sql::lowering) fn normalize_having_clauses(
    clauses: Vec<SqlHavingClause>,
    entity_scope: &[String],
) -> Vec<SqlHavingClause> {
    clauses
        .into_iter()
        .map(|clause| SqlHavingClause {
            symbol: normalize_having_symbol(clause.symbol, entity_scope),
            op: clause.op,
            value: clause.value,
        })
        .collect()
}

fn normalize_having_symbol(symbol: SqlHavingSymbol, entity_scope: &[String]) -> SqlHavingSymbol {
    match symbol {
        SqlHavingSymbol::Field(field) => {
            SqlHavingSymbol::Field(normalize_identifier_to_scope(field, entity_scope))
        }
        SqlHavingSymbol::Aggregate(aggregate) => SqlHavingSymbol::Aggregate(
            normalize_aggregate_call_identifiers(aggregate, entity_scope),
        ),
    }
}

fn normalize_aggregate_call_identifiers(
    aggregate: SqlAggregateCall,
    entity_scope: &[String],
) -> SqlAggregateCall {
    SqlAggregateCall {
        kind: aggregate.kind,
        field: aggregate
            .field
            .map(|field| normalize_identifier_to_scope(field, entity_scope)),
        distinct: aggregate.distinct,
    }
}

// Build one identifier scope used for reducing SQL-qualified field references
// (`entity.field`, `schema.entity.field`) into canonical planner field names.
pub(in crate::db::sql::lowering) fn sql_entity_scope_candidates(
    sql_entity: &str,
    expected_entity: &'static str,
) -> Vec<String> {
    let mut out = Vec::new();
    out.push(sql_entity.to_string());
    out.push(expected_entity.to_string());

    if let Some(last) = identifier_last_segment(sql_entity) {
        out.push(last.to_string());
    }
    if let Some(last) = identifier_last_segment(expected_entity) {
        out.push(last.to_string());
    }

    out
}

fn normalize_projection_identifiers(
    projection: SqlProjection,
    entity_scope: &[String],
) -> SqlProjection {
    match projection {
        SqlProjection::All => SqlProjection::All,
        SqlProjection::Items(items) => SqlProjection::Items(
            items
                .into_iter()
                .map(|item| match item {
                    SqlSelectItem::Field(field) => {
                        SqlSelectItem::Field(normalize_identifier(field, entity_scope))
                    }
                    SqlSelectItem::Aggregate(aggregate) => {
                        SqlSelectItem::Aggregate(SqlAggregateCall {
                            kind: aggregate.kind,
                            field: aggregate
                                .field
                                .map(|field| normalize_identifier(field, entity_scope)),
                            distinct: aggregate.distinct,
                        })
                    }
                    SqlSelectItem::TextFunction(SqlTextFunctionCall {
                        function,
                        field,
                        literal,
                        literal2,
                        literal3,
                    }) => SqlSelectItem::TextFunction(SqlTextFunctionCall {
                        function,
                        field: normalize_identifier(field, entity_scope),
                        literal,
                        literal2,
                        literal3,
                    }),
                })
                .collect(),
        ),
    }
}

// Normalize `ORDER BY` targets after projection normalization so alias
// rewrites stay parser/session-owned and planner order semantics remain
// canonical.
fn normalize_select_order_terms(
    terms: Vec<SqlOrderTerm>,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
    entity_scope: &[String],
) -> Result<Vec<SqlOrderTerm>, SqlLoweringError> {
    terms
        .into_iter()
        .map(|term| {
            let field = match resolve_projection_order_alias(
                term.field.as_str(),
                projection,
                projection_aliases,
            )? {
                Some(rewritten) => rewritten,
                None => term.field,
            };

            Ok(SqlOrderTerm {
                field: normalize_order_term_identifier(field, entity_scope),
                direction: term.direction,
            })
        })
        .collect()
}

// Resolve one `ORDER BY <alias>` target onto one already-supported projection
// order target. Unsupported aliases fail closed here rather than leaking new
// order semantics into planner lowering.
fn resolve_projection_order_alias(
    order_target: &str,
    projection: &SqlProjection,
    projection_aliases: &[Option<String>],
) -> Result<Option<String>, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(None);
    };

    for (item, alias) in items.iter().zip(projection_aliases.iter()) {
        let Some(alias) = alias.as_deref() else {
            continue;
        };
        if !alias.eq_ignore_ascii_case(order_target) {
            continue;
        }

        let Some(target) = order_target_from_projection_item(item) else {
            return Err(SqlLoweringError::unsupported_order_by_alias(order_target));
        };

        return Ok(Some(target));
    }

    Ok(None)
}

// Restrict alias rewrites to the exact order target family already accepted by
// the reduced SQL parser: plain fields and LOWER/UPPER(field) expressions.
fn order_target_from_projection_item(item: &SqlSelectItem) -> Option<String> {
    match item {
        SqlSelectItem::Field(field) => Some(field.clone()),
        SqlSelectItem::TextFunction(SqlTextFunctionCall {
            function: SqlTextFunction::Lower,
            field,
            literal: None,
            literal2: None,
            literal3: None,
        }) => render_supported_order_expr(&Expr::FunctionCall {
            function: Function::Lower,
            args: vec![Expr::Field(FieldId::new(field.clone()))],
        }),
        SqlSelectItem::TextFunction(SqlTextFunctionCall {
            function: SqlTextFunction::Upper,
            field,
            literal: None,
            literal2: None,
            literal3: None,
        }) => render_supported_order_expr(&Expr::FunctionCall {
            function: Function::Upper,
            args: vec![Expr::Field(FieldId::new(field.clone()))],
        }),
        SqlSelectItem::Aggregate(_) | SqlSelectItem::TextFunction(_) => None,
    }
}

pub(in crate::db::sql::lowering) fn normalize_order_terms(
    terms: Vec<SqlOrderTerm>,
    entity_scope: &[String],
) -> Vec<SqlOrderTerm> {
    terms
        .into_iter()
        .map(|term| SqlOrderTerm {
            field: normalize_order_term_identifier(term.field, entity_scope),
            direction: term.direction,
        })
        .collect()
}

fn normalize_order_term_identifier(identifier: String, entity_scope: &[String]) -> String {
    let Some(expression) = parse_supported_order_expr(identifier.as_str()) else {
        return normalize_identifier(identifier, entity_scope);
    };
    let normalized_field = normalize_identifier(
        supported_order_expr_field(&expression)
            .expect("supported order expression parsing must preserve one field argument")
            .as_str()
            .to_string(),
        entity_scope,
    );
    let rewritten = rewrite_supported_order_expr_field(&expression, normalized_field)
        .expect("supported order expression rewrite must preserve the admitted order function");

    render_supported_order_expr(&rewritten)
        .expect("supported order expression rendering must preserve the admitted order function")
}

pub(in crate::db::sql::lowering) fn normalize_identifier_list(
    fields: Vec<String>,
    entity_scope: &[String],
) -> Vec<String> {
    fields
        .into_iter()
        .map(|field| normalize_identifier(field, entity_scope))
        .collect()
}

// SQL lowering only adapts identifier qualification (`entity.field` -> `field`)
// and delegates predicate-tree traversal ownership to `db::predicate`.
pub(in crate::db::sql::lowering) fn adapt_predicate_identifiers_to_scope(
    predicate: Predicate,
    entity_scope: &[String],
) -> Predicate {
    rewrite_field_identifiers(predicate, |field| normalize_identifier(field, entity_scope))
}

fn normalize_identifier(identifier: String, entity_scope: &[String]) -> String {
    normalize_identifier_to_scope(identifier, entity_scope)
}

pub(in crate::db::sql::lowering) fn ensure_entity_matches_expected(
    sql_entity: &str,
    expected_entity: &'static str,
) -> Result<(), SqlLoweringError> {
    if identifiers_tail_match(sql_entity, expected_entity) {
        return Ok(());
    }

    Err(SqlLoweringError::entity_mismatch(
        sql_entity,
        expected_entity,
    ))
}
