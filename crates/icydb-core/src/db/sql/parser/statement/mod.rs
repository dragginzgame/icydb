//! Module: db::sql::parser::statement
//! Responsibility: reduced SQL statement-shell parsing and clause-order diagnostics.
//! Does not own: projection item parsing, clause helper internals, or execution semantics.
//! Boundary: keeps statement entry routing and statement-local clause sequencing out of the parser root.

mod delete;
mod insert;
mod select;
mod update;

use crate::db::{
    predicate::Predicate,
    sql::identifier::{
        identifier_last_segment, normalize_identifier_to_scope, rewrite_field_identifiers,
    },
};
use crate::db::{
    reduced_sql::{Keyword, SqlParseError, TokenKind},
    sql::parser::{
        Parser, SqlAggregateCall, SqlArithmeticProjectionCall, SqlAssignment, SqlDeleteStatement,
        SqlDescribeStatement, SqlExplainMode, SqlExplainStatement, SqlExplainTarget,
        SqlHavingClause, SqlHavingSymbol, SqlOrderTerm, SqlProjection, SqlReturningProjection,
        SqlSelectItem, SqlSelectStatement, SqlShowColumnsStatement, SqlShowEntitiesStatement,
        SqlShowIndexesStatement, SqlStatement, SqlTextFunctionCall, SqlUpdateStatement,
    },
};

impl Parser {
    pub(super) fn parse_statement(&mut self) -> Result<SqlStatement, SqlParseError> {
        if self.eat_keyword(Keyword::Select) {
            return Ok(SqlStatement::Select(self.parse_select_statement()?));
        }
        if self.eat_keyword(Keyword::Delete) {
            return Ok(SqlStatement::Delete(self.parse_delete_statement()?));
        }
        if self.eat_keyword(Keyword::Insert) {
            return Ok(SqlStatement::Insert(self.parse_insert_statement()?));
        }
        if self.eat_keyword(Keyword::Update) {
            return Ok(SqlStatement::Update(self.parse_update_statement()?));
        }
        if self.eat_keyword(Keyword::Explain) {
            return Ok(SqlStatement::Explain(self.parse_explain_statement()?));
        }
        if self.eat_keyword(Keyword::Describe) {
            return Ok(SqlStatement::Describe(self.parse_describe_statement()?));
        }
        if self.eat_keyword(Keyword::Show) {
            return self.parse_show_statement();
        }

        if let Some(feature) = self.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        }

        Err(SqlParseError::expected(
            "one of SELECT, DELETE, INSERT, UPDATE, EXPLAIN, DESCRIBE, SHOW",
            self.peek_kind(),
        ))
    }

    // Classify one trailing token as a likely out-of-order clause mistake so
    // callers get an actionable parser diagnostic instead of generic EOI.
    pub(super) fn trailing_clause_order_error(
        &self,
        statement: &SqlStatement,
    ) -> Option<SqlParseError> {
        match statement {
            SqlStatement::Select(select) => self.select_clause_order_error(select),
            SqlStatement::Delete(delete) => self.delete_clause_order_error(delete),
            SqlStatement::Insert(_) => None,
            SqlStatement::Update(update) => self.update_clause_order_error(update),
            SqlStatement::Explain(explain) => match &explain.statement {
                SqlExplainTarget::Select(select) => self.select_clause_order_error(select),
                SqlExplainTarget::Delete(delete) => self.delete_clause_order_error(delete),
            },
            SqlStatement::Describe(_) => {
                Some(SqlParseError::unsupported_feature("DESCRIBE modifiers"))
            }
            SqlStatement::ShowIndexes(_) => {
                Some(SqlParseError::unsupported_feature("SHOW INDEXES modifiers"))
            }
            SqlStatement::ShowColumns(_) => {
                Some(SqlParseError::unsupported_feature("SHOW COLUMNS modifiers"))
            }
            SqlStatement::ShowEntities(_) => Some(SqlParseError::unsupported_feature(
                "SHOW ENTITIES modifiers",
            )),
        }
    }

    fn parse_show_statement(&mut self) -> Result<SqlStatement, SqlParseError> {
        if self.eat_keyword(Keyword::Indexes) {
            return Ok(SqlStatement::ShowIndexes(
                self.parse_show_indexes_statement()?,
            ));
        }
        if self.eat_keyword(Keyword::Columns) {
            return Ok(SqlStatement::ShowColumns(
                self.parse_show_columns_statement()?,
            ));
        }
        if self.eat_keyword(Keyword::Entities) {
            return Ok(SqlStatement::ShowEntities(SqlShowEntitiesStatement));
        }
        if self.eat_keyword(Keyword::Tables) {
            return Ok(SqlStatement::ShowEntities(SqlShowEntitiesStatement));
        }

        Err(SqlParseError::unsupported_feature(
            "SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES/SHOW TABLES",
        ))
    }

    fn parse_explain_statement(&mut self) -> Result<SqlExplainStatement, SqlParseError> {
        let mode = if self.eat_keyword(Keyword::Execution) {
            SqlExplainMode::Execution
        } else if self.eat_keyword(Keyword::Json) {
            SqlExplainMode::Json
        } else {
            SqlExplainMode::Plan
        };

        let statement = if self.eat_keyword(Keyword::Select) {
            SqlExplainTarget::Select(self.parse_select_statement()?)
        } else if self.eat_keyword(Keyword::Delete) {
            SqlExplainTarget::Delete(self.parse_delete_statement()?)
        } else if let Some(feature) = self.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        } else {
            return Err(SqlParseError::expected(
                "one of SELECT, DELETE",
                self.peek_kind(),
            ));
        };

        Ok(SqlExplainStatement { mode, statement })
    }

    fn select_clause_order_error(&self, statement: &SqlSelectStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order)
            && (statement.limit.is_some() || statement.offset.is_some())
        {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT/OFFSET",
            ));
        }

        None
    }

    fn delete_clause_order_error(&self, statement: &SqlDeleteStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order) && statement.limit.is_some() {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT in DELETE",
            ));
        }

        None
    }

    fn update_clause_order_error(&self, statement: &SqlUpdateStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order)
            && (statement.limit.is_some() || statement.offset.is_some())
        {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT/OFFSET in UPDATE",
            ));
        }
        if self.peek_keyword(Keyword::Limit) && statement.offset.is_some() {
            return Some(SqlParseError::invalid_syntax(
                "LIMIT must appear before OFFSET in UPDATE",
            ));
        }

        None
    }

    pub(super) fn parse_optional_table_alias(&mut self) -> Result<Option<String>, SqlParseError> {
        if self.eat_keyword(Keyword::As) {
            return self.expect_identifier().map(Some);
        }

        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            let Some(TokenKind::Identifier(value)) = self.peek_kind() else {
                unreachable!();
            };
            if matches!(
                value.as_str().to_ascii_uppercase().as_str(),
                "SET" | "VALUES"
            ) {
                return Ok(None);
            }

            return self.expect_identifier().map(Some);
        }

        Ok(None)
    }

    fn parse_describe_statement(&mut self) -> Result<SqlDescribeStatement, SqlParseError> {
        let entity = self.expect_identifier()?;

        Ok(SqlDescribeStatement { entity })
    }

    fn parse_show_indexes_statement(&mut self) -> Result<SqlShowIndexesStatement, SqlParseError> {
        let entity = self.expect_identifier()?;

        Ok(SqlShowIndexesStatement { entity })
    }

    fn parse_show_columns_statement(&mut self) -> Result<SqlShowColumnsStatement, SqlParseError> {
        let entity = self.expect_identifier()?;

        Ok(SqlShowColumnsStatement { entity })
    }
}

// Normalize one admitted write-lane `RETURNING` field list back onto the
// canonical entity namespace after one single-table alias is admitted.
pub(super) fn normalize_returning_projection_for_table_alias(
    projection: SqlReturningProjection,
    entity: &str,
    alias: &str,
) -> SqlReturningProjection {
    match projection {
        SqlReturningProjection::All => SqlReturningProjection::All,
        SqlReturningProjection::Fields(fields) => SqlReturningProjection::Fields(
            normalize_identifier_list_for_table_alias(fields, entity, alias),
        ),
    }
}

// Build the identifier scope admitted for one single-table alias surface.
fn table_alias_scope(entity: &str, alias: &str) -> Vec<String> {
    let mut scope = vec![entity.to_string(), alias.to_string()];

    if let Some(last) = identifier_last_segment(entity) {
        scope.push(last.to_string());
    }

    scope
}

// Reduce one parsed field/projection tree back onto canonical entity-local
// field identifiers when the statement admitted one table alias.
pub(super) fn normalize_projection_for_table_alias(
    projection: SqlProjection,
    entity: &str,
    alias: &str,
) -> SqlProjection {
    let scope = table_alias_scope(entity, alias);

    match projection {
        SqlProjection::All => SqlProjection::All,
        SqlProjection::Items(items) => SqlProjection::Items(
            items
                .into_iter()
                .map(|item| match item {
                    SqlSelectItem::Field(field) => {
                        SqlSelectItem::Field(normalize_identifier_to_scope(field, scope.as_slice()))
                    }
                    SqlSelectItem::Aggregate(aggregate) => SqlSelectItem::Aggregate(
                        normalize_aggregate_call_for_table_alias(aggregate, scope.as_slice()),
                    ),
                    SqlSelectItem::TextFunction(call) => SqlSelectItem::TextFunction(
                        normalize_text_function_call_for_table_alias(call, scope.as_slice()),
                    ),
                    SqlSelectItem::Arithmetic(call) => {
                        SqlSelectItem::Arithmetic(SqlArithmeticProjectionCall {
                            field: normalize_identifier_to_scope(call.field, scope.as_slice()),
                            op: call.op,
                            literal: call.literal,
                        })
                    }
                })
                .collect(),
        ),
    }
}

// Reduce one parsed predicate tree onto canonical entity-local identifiers so
// alias-qualified WHERE clauses stay planner-neutral.
pub(super) fn normalize_predicate_for_table_alias(
    predicate: Predicate,
    entity: &str,
    alias: &str,
) -> Predicate {
    let scope = table_alias_scope(entity, alias);

    rewrite_field_identifiers(predicate, |field| {
        normalize_identifier_to_scope(field, scope.as_slice())
    })
}

// Reduce one identifier list such as GROUP BY onto canonical entity-local
// field names for the admitted alias scope.
pub(super) fn normalize_identifier_list_for_table_alias(
    fields: Vec<String>,
    entity: &str,
    alias: &str,
) -> Vec<String> {
    let scope = table_alias_scope(entity, alias);

    fields
        .into_iter()
        .map(|field| normalize_identifier_to_scope(field, scope.as_slice()))
        .collect()
}

// Reduce one UPDATE assignment list onto canonical entity-local field names so
// single-table alias use stays parser-local before dispatch.
pub(super) fn normalize_assignments_for_table_alias(
    assignments: Vec<SqlAssignment>,
    entity: &str,
    alias: &str,
) -> Vec<SqlAssignment> {
    let scope = table_alias_scope(entity, alias);

    assignments
        .into_iter()
        .map(|assignment| SqlAssignment {
            field: normalize_identifier_to_scope(assignment.field, scope.as_slice()),
            value: assignment.value,
        })
        .collect()
}

// Reduce grouped HAVING references onto canonical entity-local fields while
// preserving grouped aggregate payloads.
pub(super) fn normalize_having_for_table_alias(
    clauses: Vec<SqlHavingClause>,
    entity: &str,
    alias: &str,
) -> Vec<SqlHavingClause> {
    let scope = table_alias_scope(entity, alias);

    clauses
        .into_iter()
        .map(|clause| SqlHavingClause {
            symbol: match clause.symbol {
                SqlHavingSymbol::Field(field) => {
                    SqlHavingSymbol::Field(normalize_identifier_to_scope(field, scope.as_slice()))
                }
                SqlHavingSymbol::Aggregate(aggregate) => SqlHavingSymbol::Aggregate(
                    normalize_aggregate_call_for_table_alias(aggregate, scope.as_slice()),
                ),
            },
            op: clause.op,
            value: clause.value,
        })
        .collect()
}

// Reduce ORDER BY fields and the admitted LOWER/UPPER(field) forms onto
// canonical entity-local targets before lowering sees the statement.
pub(super) fn normalize_order_terms_for_table_alias(
    terms: Vec<SqlOrderTerm>,
    entity: &str,
    alias: &str,
) -> Vec<SqlOrderTerm> {
    let scope = table_alias_scope(entity, alias);

    terms
        .into_iter()
        .map(|term| SqlOrderTerm {
            field: normalize_order_term_for_table_alias(term.field, scope.as_slice()),
            direction: term.direction,
        })
        .collect()
}

fn normalize_aggregate_call_for_table_alias(
    aggregate: SqlAggregateCall,
    scope: &[String],
) -> SqlAggregateCall {
    SqlAggregateCall {
        kind: aggregate.kind,
        field: aggregate
            .field
            .map(|field| normalize_identifier_to_scope(field, scope)),
        distinct: aggregate.distinct,
    }
}

fn normalize_text_function_call_for_table_alias(
    call: SqlTextFunctionCall,
    scope: &[String],
) -> SqlTextFunctionCall {
    SqlTextFunctionCall {
        function: call.function,
        field: normalize_identifier_to_scope(call.field, scope),
        literal: call.literal,
        literal2: call.literal2,
        literal3: call.literal3,
    }
}

fn normalize_order_term_for_table_alias(field: String, scope: &[String]) -> String {
    if let Some(inner) = field
        .strip_prefix("LOWER(")
        .and_then(|tail| tail.strip_suffix(')'))
    {
        return format!(
            "LOWER({})",
            normalize_identifier_to_scope(inner.to_string(), scope)
        );
    }

    if let Some(inner) = field
        .strip_prefix("UPPER(")
        .and_then(|tail| tail.strip_suffix(')'))
    {
        return format!(
            "UPPER({})",
            normalize_identifier_to_scope(inner.to_string(), scope)
        );
    }

    normalize_identifier_to_scope(field, scope)
}
