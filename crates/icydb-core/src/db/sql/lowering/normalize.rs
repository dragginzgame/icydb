use crate::db::sql::lowering::SqlLoweringError;
use crate::db::{
    predicate::Predicate,
    query::plan::ExpressionOrderTerm,
    sql::{
        identifier::{
            identifier_last_segment, identifiers_tail_match, normalize_identifier_to_scope,
            rewrite_field_identifiers,
        },
        parser::{
            SqlAggregateCall, SqlHavingClause, SqlHavingSymbol, SqlOrderTerm, SqlProjection,
            SqlSelectItem, SqlSelectStatement, SqlTextFunctionCall,
        },
    },
};

pub(in crate::db::sql::lowering) fn normalize_select_statement_to_expected_entity(
    mut statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> SqlSelectStatement {
    // Re-scope parsed identifiers onto the resolved entity surface after the
    // caller has already established entity ownership for this statement.
    let entity_scope = sql_entity_scope_candidates(statement.entity.as_str(), expected_entity);
    statement.projection =
        normalize_projection_identifiers(statement.projection, entity_scope.as_slice());
    statement.group_by = normalize_identifier_list(statement.group_by, entity_scope.as_slice());
    statement.predicate = statement
        .predicate
        .map(|predicate| adapt_predicate_identifiers_to_scope(predicate, entity_scope.as_slice()));
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());
    statement.having = normalize_having_clauses(statement.having, entity_scope.as_slice());

    statement
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
    let Some(expression) = ExpressionOrderTerm::parse(identifier.as_str()) else {
        return normalize_identifier(identifier, entity_scope);
    };
    let normalized_field = normalize_identifier(expression.field().to_string(), entity_scope);

    expression.canonical_text_with_field(normalized_field.as_str())
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
