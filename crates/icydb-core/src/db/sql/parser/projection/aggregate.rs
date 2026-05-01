use crate::db::{
    sql::parser::{
        Parser, SqlAggregateCall, SqlAggregateKind, SqlExpr, projection::SqlExprParseSurface,
    },
    sql_shared::{Keyword, SqlParseError, TokenKind},
};

impl Parser {
    pub(in crate::db::sql::parser) fn parse_aggregate_kind(&self) -> Option<SqlAggregateKind> {
        match self.peek_kind() {
            Some(TokenKind::Keyword(Keyword::Count)) => Some(SqlAggregateKind::Count),
            Some(TokenKind::Keyword(Keyword::Sum)) => Some(SqlAggregateKind::Sum),
            Some(TokenKind::Keyword(Keyword::Avg)) => Some(SqlAggregateKind::Avg),
            Some(TokenKind::Keyword(Keyword::Min)) => Some(SqlAggregateKind::Min),
            Some(TokenKind::Keyword(Keyword::Max)) => Some(SqlAggregateKind::Max),
            _ => None,
        }
    }

    pub(in crate::db::sql::parser) fn parse_aggregate_call(
        &mut self,
        kind: SqlAggregateKind,
    ) -> Result<SqlAggregateCall, SqlParseError> {
        let _ = self.cursor.advance();
        self.expect_lparen()?;
        let distinct = self.eat_keyword(Keyword::Distinct);

        let input = if kind.supports_star_input() && self.eat_star() {
            None
        } else {
            Some(self.parse_aggregate_input_expr()?)
        };

        self.expect_rparen()?;
        let filter_expr = self.parse_aggregate_filter_clause()?;

        Ok(SqlAggregateCall {
            kind,
            input: input.map(Box::new),
            filter_expr: filter_expr.map(Box::new),
            distinct,
        })
    }

    // Parse one aggregate-owned FILTER predicate directly onto the aggregate
    // call instead of rewriting it through CASE or a clause-local wrapper.
    fn parse_aggregate_filter_clause(&mut self) -> Result<Option<SqlExpr>, SqlParseError> {
        if !self.eat_keyword(Keyword::Filter) {
            return Ok(None);
        }

        self.expect_lparen()?;
        self.expect_keyword(Keyword::Where)?;
        let expr = self.record_predicate_parse_stage(|parser| {
            parser.parse_sql_expr(SqlExprParseSurface::Where, 0)
        })?;
        self.expect_rparen()?;

        Ok(Some(expr))
    }

    fn parse_aggregate_input_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let expr = self.parse_sql_expr(SqlExprParseSurface::AggregateInput, 0)?;

        if matches!(expr, SqlExpr::Aggregate(_)) {
            return Err(SqlParseError::unsupported_feature(
                "nested aggregate references inside aggregate input expressions",
            ));
        }

        Ok(expr)
    }
}
