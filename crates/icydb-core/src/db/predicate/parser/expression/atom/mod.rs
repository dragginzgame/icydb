mod field;
mod literal;

use crate::{
    db::predicate::parser::{
        expression::atom::{
            field::parse_field_predicate, literal::parse_literal_leading_predicate,
        },
        expression::{
            ParsedPredicate, descend_predicate_parse_depth, parse_predicate_from_cursor_at_depth,
        },
        operand::{parse_starts_with_predicate, predicate_literal_starts},
    },
    db::sql_shared::{SqlParseError, SqlTokenCursor, TokenKind},
};

// Parse one parenthesized predicate or one field/operator predicate atom.
pub(in crate::db::predicate::parser::expression) fn parse_predicate_primary(
    cursor: &mut SqlTokenCursor,
    parse_depth: usize,
) -> Result<ParsedPredicate, SqlParseError> {
    if cursor.eat_lparen() {
        let child_parse_depth = descend_predicate_parse_depth(parse_depth)?;
        let predicate = parse_predicate_from_cursor_at_depth(cursor, child_parse_depth)?;
        cursor.expect_rparen()?;

        return Ok(predicate);
    }

    if cursor.peek_identifier_keyword("STARTS_WITH")
        && matches!(cursor.peek_next_kind(), Some(TokenKind::LParen))
    {
        return parse_starts_with_predicate(cursor).map(|predicate| (predicate, 1));
    }

    if predicate_literal_starts(cursor.peek_kind()) {
        return parse_literal_leading_predicate(cursor).map(|predicate| (predicate, 1));
    }

    parse_field_predicate(cursor).map(|predicate| (predicate, 1))
}
