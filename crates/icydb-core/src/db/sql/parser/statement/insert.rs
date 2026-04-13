use crate::db::{
    reduced_sql::SqlParseError,
    sql::parser::{Parser, SqlInsertSource, SqlInsertStatement, SqlReturningProjection},
};
use crate::value::Value;

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
        let source = if self.eat_keyword(crate::db::reduced_sql::Keyword::Select) {
            SqlInsertSource::Select(Box::new(self.parse_select_statement()?))
        } else {
            self.expect_identifier_keyword("VALUES")?;
            let values =
                self.parse_insert_values_tuples((!columns.is_empty()).then_some(columns.len()))?;

            SqlInsertSource::Values(values)
        };
        let returning = if self.eat_keyword(crate::db::reduced_sql::Keyword::Returning) {
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

    pub(super) fn parse_returning_projection(
        &mut self,
    ) -> Result<SqlReturningProjection, SqlParseError> {
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
}
