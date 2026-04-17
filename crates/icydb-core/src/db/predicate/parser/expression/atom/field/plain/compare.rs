use crate::{
    db::predicate::parser::lowering::{predicate_compare, predicate_compare_fields},
    db::{
        predicate::Predicate,
        sql_shared::{SqlParseError, SqlTokenCursor, TokenKind},
    },
};

// Parse the plain-field compare lane once all reduced SQL special forms have
// been ruled out by the owning plain-field dispatcher.
pub(in crate::db::predicate::parser::expression::atom::field::plain) fn parse_plain_compare_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
) -> Result<Predicate, SqlParseError> {
    let op = cursor.parse_compare_operator()?;
    if matches!(cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
        let right_field = cursor.expect_identifier()?;
        return Ok(predicate_compare_fields(field, op, right_field));
    }

    let value = cursor.parse_literal()?;

    Ok(predicate_compare(field, op, value))
}
