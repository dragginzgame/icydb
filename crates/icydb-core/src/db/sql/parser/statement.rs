//! Module: db::sql::parser::statement
//! Responsibility: reduced SQL statement-shell parsing and clause-order diagnostics.
//! Does not own: projection item parsing, clause helper internals, or execution semantics.
//! Boundary: keeps statement entry routing and statement-local clause sequencing out of the parser root.

use crate::db::{
    predicate::Predicate,
    reduced_sql::{Keyword, SqlParseError},
    sql::identifier::{
        identifier_last_segment, normalize_identifier_to_scope, rewrite_field_identifiers,
    },
    sql::parser::{
        Parser, SqlAggregateCall, SqlAssignment, SqlDeleteStatement, SqlDescribeStatement,
        SqlExplainMode, SqlExplainStatement, SqlExplainTarget, SqlHavingClause, SqlHavingSymbol,
        SqlInsertSource, SqlInsertStatement, SqlOrderTerm, SqlProjection, SqlReturningProjection,
        SqlSelectItem, SqlSelectStatement, SqlShowColumnsStatement, SqlShowEntitiesStatement,
        SqlShowIndexesStatement, SqlStatement, SqlTextFunctionCall, SqlUpdateStatement,
    },
};
use crate::value::Value;

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

    fn parse_select_statement(&mut self) -> Result<SqlSelectStatement, SqlParseError> {
        let distinct = self.eat_keyword(Keyword::Distinct);
        let (projection, projection_aliases) = self.parse_projection()?;
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;

        // Phase 1: parse predicate and grouping clauses in canonical sequence.
        let mut predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let mut group_by = if self.eat_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            self.parse_identifier_list()?
        } else {
            Vec::new()
        };

        let mut having = if self.eat_keyword(Keyword::Having) {
            self.parse_having_clauses()?
        } else {
            Vec::new()
        };

        // Phase 2: parse ordering and window clauses.
        let mut order_by = if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_terms()?
        } else {
            Vec::new()
        };

        let limit = if self.eat_keyword(Keyword::Limit) {
            Some(self.parse_u32_literal("LIMIT")?)
        } else {
            None
        };

        let offset = if self.eat_keyword(Keyword::Offset) {
            Some(self.parse_u32_literal("OFFSET")?)
        } else {
            None
        };

        // Phase 3: collapse one admitted single-table alias back onto the
        // canonical entity field namespace so downstream lowering stays
        // alias-neutral.
        let projection = match table_alias.as_deref() {
            Some(alias) => normalize_projection_for_table_alias(projection, entity.as_str(), alias),
            None => projection,
        };
        if let Some(alias) = table_alias.as_deref() {
            predicate = predicate.map(|predicate| {
                normalize_predicate_for_table_alias(predicate, entity.as_str(), alias)
            });
            group_by = normalize_identifier_list_for_table_alias(group_by, entity.as_str(), alias);
            having = normalize_having_for_table_alias(having, entity.as_str(), alias);
            order_by = normalize_order_terms_for_table_alias(order_by, entity.as_str(), alias);
        }

        Ok(SqlSelectStatement {
            entity,
            projection,
            projection_aliases,
            predicate,
            distinct,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_delete_statement(&mut self) -> Result<SqlDeleteStatement, SqlParseError> {
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;

        let mut predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let mut order_by = if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_terms()?
        } else {
            Vec::new()
        };

        let limit = if self.eat_keyword(Keyword::Limit) {
            Some(self.parse_u32_literal("LIMIT")?)
        } else {
            None
        };

        let offset = if self.eat_keyword(Keyword::Offset) {
            Some(self.parse_u32_literal("OFFSET")?)
        } else {
            None
        };
        let mut returning = if self.eat_keyword(Keyword::Returning) {
            Some(self.parse_returning_projection()?)
        } else {
            None
        };

        if let Some(alias) = table_alias.as_deref() {
            predicate = predicate.map(|predicate| {
                normalize_predicate_for_table_alias(predicate, entity.as_str(), alias)
            });
            order_by = normalize_order_terms_for_table_alias(order_by, entity.as_str(), alias);
            returning = returning.map(|returning| {
                normalize_returning_projection_for_table_alias(returning, entity.as_str(), alias)
            });
        }

        Ok(SqlDeleteStatement {
            entity,
            predicate,
            order_by,
            limit,
            offset,
            returning,
        })
    }

    fn parse_insert_statement(&mut self) -> Result<SqlInsertStatement, SqlParseError> {
        self.expect_identifier_keyword("INTO")?;
        let entity = self.expect_identifier()?;
        let _table_alias = self.parse_optional_table_alias()?;

        let columns = if self.peek_lparen() {
            self.expect_lparen()?;
            let columns = self.parse_identifier_list()?;
            self.expect_rparen()?;
            columns
        } else {
            Vec::new()
        };
        let source = if self.eat_keyword(Keyword::Select) {
            SqlInsertSource::Select(Box::new(self.parse_select_statement()?))
        } else {
            self.expect_identifier_keyword("VALUES")?;
            let values =
                self.parse_insert_values_tuples((!columns.is_empty()).then_some(columns.len()))?;

            SqlInsertSource::Values(values)
        };
        let returning = if self.eat_keyword(Keyword::Returning) {
            Some(self.parse_returning_projection()?)
        } else {
            None
        };

        Ok(SqlInsertStatement {
            entity,
            columns,
            source,
            returning,
        })
    }

    fn parse_optional_table_alias(&mut self) -> Result<Option<String>, SqlParseError> {
        if self.eat_keyword(Keyword::As) {
            return self.expect_identifier().map(Some);
        }

        if matches!(
            self.peek_kind(),
            Some(crate::db::reduced_sql::TokenKind::Identifier(_))
        ) {
            let Some(crate::db::reduced_sql::TokenKind::Identifier(value)) = self.peek_kind()
            else {
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

    // Parse one or more reduced SQL VALUES tuples while keeping tuple arity
    // aligned with the explicit INSERT column list.
    fn parse_insert_values_tuples(
        &mut self,
        expected_columns: Option<usize>,
    ) -> Result<Vec<Vec<Value>>, SqlParseError> {
        let mut tuples = Vec::new();

        loop {
            self.expect_lparen()?;
            let tuple = self.parse_insert_values_tuple(expected_columns)?;
            self.expect_rparen()?;
            tuples.push(tuple);

            if !self.eat_comma() {
                break;
            }
        }

        Ok(tuples)
    }

    fn parse_insert_values_tuple(
        &mut self,
        expected_columns: Option<usize>,
    ) -> Result<Vec<Value>, SqlParseError> {
        let mut values = Vec::new();
        loop {
            values.push(self.parse_literal()?);

            if self.eat_comma() {
                continue;
            }

            break;
        }

        if let Some(expected_columns) = expected_columns
            && expected_columns != values.len()
        {
            return Err(SqlParseError::invalid_syntax(
                "INSERT column list and VALUES tuple length must match",
            ));
        }

        Ok(values)
    }

    fn parse_update_statement(&mut self) -> Result<SqlUpdateStatement, SqlParseError> {
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;
        self.expect_identifier_keyword("SET")?;
        let mut assignments = self.parse_update_assignments()?;

        // Phase 1: parse the reduced predicate before any bounded windowing.
        let mut predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        // Phase 2: parse the bounded ordered window admitted on the narrowed
        // SQL UPDATE lane.
        let mut order_by = if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_terms()?
        } else {
            Vec::new()
        };

        let limit = if self.eat_keyword(Keyword::Limit) {
            Some(self.parse_u32_literal("LIMIT")?)
        } else {
            None
        };

        let offset = if self.eat_keyword(Keyword::Offset) {
            Some(self.parse_u32_literal("OFFSET")?)
        } else {
            None
        };
        let mut returning = if self.eat_keyword(Keyword::Returning) {
            Some(self.parse_returning_projection()?)
        } else {
            None
        };

        // Phase 3: collapse the admitted single-table alias back onto the
        // canonical entity field namespace so the write selector stays
        // alias-neutral downstream.
        if let Some(alias) = table_alias.as_deref() {
            assignments =
                normalize_assignments_for_table_alias(assignments, entity.as_str(), alias);
            predicate = predicate.map(|predicate| {
                normalize_predicate_for_table_alias(predicate, entity.as_str(), alias)
            });
            order_by = normalize_order_terms_for_table_alias(order_by, entity.as_str(), alias);
            returning = returning.map(|returning| {
                normalize_returning_projection_for_table_alias(returning, entity.as_str(), alias)
            });
        }

        Ok(SqlUpdateStatement {
            entity,
            assignments,
            predicate,
            order_by,
            limit,
            offset,
            returning,
        })
    }

    fn parse_update_assignments(&mut self) -> Result<Vec<SqlAssignment>, SqlParseError> {
        let mut assignments = Vec::new();
        loop {
            let field = self.expect_identifier()?;
            self.expect_assignment_eq()?;
            let value = self.parse_literal()?;
            assignments.push(SqlAssignment { field, value });

            if self.eat_comma() {
                continue;
            }

            break;
        }

        if assignments.is_empty() {
            return Err(SqlParseError::expected(
                "one UPDATE assignment",
                self.peek_kind(),
            ));
        }

        Ok(assignments)
    }

    fn parse_returning_projection(&mut self) -> Result<SqlReturningProjection, SqlParseError> {
        if self.eat_star() {
            return Ok(SqlReturningProjection::All);
        }

        let mut fields = vec![self.expect_identifier()?];
        if self.peek_lparen() {
            return Err(SqlParseError::unsupported_feature(
                "SQL function namespace beyond supported aggregate or scalar text projection forms",
            ));
        }

        while self.eat_comma() {
            let field = self.expect_identifier()?;
            if self.peek_lparen() {
                return Err(SqlParseError::unsupported_feature(
                    "SQL function namespace beyond supported aggregate or scalar text projection forms",
                ));
            }
            fields.push(field);
        }

        Ok(SqlReturningProjection::Fields(fields))
    }

    fn expect_assignment_eq(&mut self) -> Result<(), SqlParseError> {
        if matches!(
            self.peek_kind(),
            Some(crate::db::reduced_sql::TokenKind::Eq)
        ) {
            let _ = self.cursor.advance();
            return Ok(());
        }

        Err(SqlParseError::expected(
            "'=' in UPDATE assignment",
            self.peek_kind(),
        ))
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
fn normalize_returning_projection_for_table_alias(
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
fn normalize_projection_for_table_alias(
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
                })
                .collect(),
        ),
    }
}

// Reduce one parsed predicate tree onto canonical entity-local identifiers so
// alias-qualified WHERE clauses stay planner-neutral.
fn normalize_predicate_for_table_alias(
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
fn normalize_identifier_list_for_table_alias(
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
fn normalize_assignments_for_table_alias(
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
fn normalize_having_for_table_alias(
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
fn normalize_order_terms_for_table_alias(
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
