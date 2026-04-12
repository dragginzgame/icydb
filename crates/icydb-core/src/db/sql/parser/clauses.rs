//! Module: db::sql::parser::clauses
//! Responsibility: reduced SQL clause parsing shared by statement shells.
//! Does not own: statement routing, projection parsing, or predicate semantics.
//! Boundary: keeps ordering/grouping/HAVING helpers out of the parser root.

use crate::{
    db::{
        predicate::CompareOp,
        reduced_sql::{Keyword, SqlParseError},
        sql::parser::{
            Parser, SqlHavingClause, SqlHavingSymbol, SqlOrderDirection, SqlOrderTerm,
            SqlTextFunction,
        },
    },
    value::Value,
};

impl Parser {
    pub(super) fn parse_order_terms(&mut self) -> Result<Vec<SqlOrderTerm>, SqlParseError> {
        let mut terms = Vec::new();
        loop {
            let field = self.parse_order_term_target()?;
            let direction = if self.eat_keyword(Keyword::Desc) {
                SqlOrderDirection::Desc
            } else {
                self.eat_keyword(Keyword::Asc);
                SqlOrderDirection::Asc
            };

            terms.push(SqlOrderTerm { field, direction });
            if !self.eat_comma() {
                break;
            }
        }

        Ok(terms)
    }

    fn parse_order_term_target(&mut self) -> Result<String, SqlParseError> {
        let field = self.expect_identifier()?;
        if !self.peek_lparen() {
            return Ok(field);
        }

        let Some(function) = SqlTextFunction::from_identifier(field.as_str()) else {
            return Err(SqlParseError::unsupported_feature(
                "ORDER BY functions beyond supported LOWER(...) or UPPER(...) forms",
            ));
        };

        match function {
            SqlTextFunction::Lower | SqlTextFunction::Upper => {
                self.expect_lparen()?;
                let field = self.expect_identifier()?;
                self.expect_rparen()?;

                Ok(match function {
                    SqlTextFunction::Lower => format!("LOWER({field})"),
                    SqlTextFunction::Upper => format!("UPPER({field})"),
                    SqlTextFunction::Trim
                    | SqlTextFunction::Ltrim
                    | SqlTextFunction::Rtrim
                    | SqlTextFunction::Length
                    | SqlTextFunction::Left
                    | SqlTextFunction::Right
                    | SqlTextFunction::StartsWith
                    | SqlTextFunction::EndsWith
                    | SqlTextFunction::Contains
                    | SqlTextFunction::Position
                    | SqlTextFunction::Replace
                    | SqlTextFunction::Substring => unreachable!(),
                })
            }
            SqlTextFunction::Trim
            | SqlTextFunction::Ltrim
            | SqlTextFunction::Rtrim
            | SqlTextFunction::Length
            | SqlTextFunction::Left
            | SqlTextFunction::Right
            | SqlTextFunction::StartsWith
            | SqlTextFunction::EndsWith
            | SqlTextFunction::Contains
            | SqlTextFunction::Position
            | SqlTextFunction::Replace
            | SqlTextFunction::Substring => Err(SqlParseError::unsupported_feature(
                "ORDER BY functions beyond supported LOWER(...) or UPPER(...) forms",
            )),
        }
    }

    pub(super) fn parse_having_clauses(&mut self) -> Result<Vec<SqlHavingClause>, SqlParseError> {
        let mut clauses = vec![self.parse_having_clause()?];
        while self.eat_keyword(Keyword::And) {
            clauses.push(self.parse_having_clause()?);
        }

        if self.peek_keyword(Keyword::Or) || self.peek_keyword(Keyword::Not) {
            return Err(SqlParseError::unsupported_feature(
                "HAVING boolean operators beyond AND",
            ));
        }

        Ok(clauses)
    }

    pub(super) fn parse_identifier_list(&mut self) -> Result<Vec<String>, SqlParseError> {
        let mut fields = vec![self.expect_identifier()?];
        while self.eat_comma() {
            fields.push(self.expect_identifier()?);
        }

        Ok(fields)
    }

    fn parse_having_clause(&mut self) -> Result<SqlHavingClause, SqlParseError> {
        let symbol = self.parse_having_symbol()?;

        if self.eat_keyword(Keyword::Is) {
            let is_not = self.eat_keyword(Keyword::Not);
            self.expect_keyword(Keyword::Null)?;

            return Ok(SqlHavingClause {
                symbol,
                op: if is_not { CompareOp::Ne } else { CompareOp::Eq },
                value: Value::Null,
            });
        }

        let op = self.parse_compare_operator()?;
        let value = self.parse_literal()?;

        Ok(SqlHavingClause { symbol, op, value })
    }

    fn parse_having_symbol(&mut self) -> Result<SqlHavingSymbol, SqlParseError> {
        if let Some(kind) = self.parse_aggregate_kind() {
            return Ok(SqlHavingSymbol::Aggregate(self.parse_aggregate_call(kind)?));
        }

        let field = self.expect_identifier()?;
        if self.peek_lparen() {
            return Err(SqlParseError::unsupported_feature(
                "SQL function namespace beyond supported aggregate forms",
            ));
        }

        Ok(SqlHavingSymbol::Field(field))
    }
}
