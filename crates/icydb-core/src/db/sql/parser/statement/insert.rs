use crate::db::{
    sql::parser::{
        Parser, SqlInsertSource, SqlInsertStatement, SqlReturningProjection, SqlWriteValue,
    },
    sql_shared::{Keyword, SqlParseError, SqlSyntaxErrorKind},
};
use icydb_diagnostic_code::SqlFeatureCode;

impl Parser {
    pub(super) fn parse_insert_statement(&mut self) -> Result<SqlInsertStatement, SqlParseError> {
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
        } else if columns.is_empty() && self.eat_keyword(Keyword::Default) {
            self.expect_identifier_keyword("VALUES")?;
            SqlInsertSource::DefaultValues
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

    // Parse one or more reduced SQL VALUES tuples while keeping tuple arity
    // aligned with the explicit INSERT column list.
    fn parse_insert_values_tuples(
        &mut self,
        expected_columns: Option<usize>,
    ) -> Result<Vec<Vec<SqlWriteValue>>, SqlParseError> {
        let mut tuples = Vec::new();

        loop {
            self.expect_lparen()?;
            let mut tuple = Vec::new();
            loop {
                tuple.push(if self.eat_keyword(Keyword::Default) {
                    SqlWriteValue::Default
                } else {
                    SqlWriteValue::Literal(self.parse_literal()?)
                });

                if self.eat_comma() {
                    continue;
                }

                break;
            }
            self.expect_rparen()?;
            if let Some(expected_columns) = expected_columns
                && expected_columns != tuple.len()
            {
                return Err(SqlParseError::invalid_syntax(
                    SqlSyntaxErrorKind::InsertValuesTupleLengthMismatch,
                ));
            }
            tuples.push(tuple);

            if !self.eat_comma() {
                break;
            }
        }

        Ok(tuples)
    }

    pub(super) fn parse_returning_projection(
        &mut self,
    ) -> Result<SqlReturningProjection, SqlParseError> {
        if self.eat_star() {
            return Ok(SqlReturningProjection::All);
        }

        let mut fields = vec![self.expect_identifier()?];
        if self.peek_lparen() {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::UnsupportedFunctionNamespace,
            ));
        }

        while self.eat_comma() {
            let field = self.expect_identifier()?;
            if self.peek_lparen() {
                return Err(SqlParseError::unsupported_feature(
                    SqlFeatureCode::UnsupportedFunctionNamespace,
                ));
            }
            fields.push(field);
        }

        Ok(SqlReturningProjection::Fields(fields))
    }
}
