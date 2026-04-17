mod field;
mod literal;

use crate::{
    db::predicate::parser::{
        expression::atom::{
            field::parse_field_predicate, literal::parse_literal_leading_predicate,
        },
        operand::{parse_starts_with_predicate, predicate_literal_starts},
    },
    db::{
        predicate::Predicate,
        sql_shared::{SqlParseError, SqlTokenCursor, TokenKind},
    },
};

// Parse one parenthesized predicate or one field/operator predicate atom.
pub(in crate::db::predicate::parser::expression) fn parse_predicate_primary(
    cursor: &mut SqlTokenCursor,
) -> Result<Predicate, SqlParseError> {
    if cursor.eat_lparen() {
        let predicate =
            crate::db::predicate::parser::expression::parse_predicate_from_cursor(cursor)?;
        cursor.expect_rparen()?;

        return Ok(predicate);
    }

    if cursor.peek_identifier_keyword("STARTS_WITH")
        && matches!(cursor.peek_next_kind(), Some(TokenKind::LParen))
    {
        return parse_starts_with_predicate(cursor);
    }

    if predicate_literal_starts(cursor.peek_kind()) {
        return parse_literal_leading_predicate(cursor);
    }

    parse_field_predicate(cursor)
}
