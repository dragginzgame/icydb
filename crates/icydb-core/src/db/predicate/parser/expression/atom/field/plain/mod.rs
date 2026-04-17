mod compare;
mod special;

use crate::{
    db::predicate::parser::expression::atom::field::plain::{
        compare::parse_plain_compare_predicate, special::parse_plain_special_predicate,
    },
    db::{
        predicate::Predicate,
        sql_shared::{Keyword, SqlParseError, SqlTokenCursor},
    },
};

// Parse one plain-field predicate family, including reduced SQL special forms.
pub(in crate::db::predicate::parser::expression::atom::field) fn parse_plain_field_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
) -> Result<Predicate, SqlParseError> {
    if cursor.peek_keyword(Keyword::Is)
        || cursor.peek_keyword(Keyword::Not)
        || cursor.peek_keyword(Keyword::In)
        || cursor.peek_keyword(Keyword::Between)
    {
        return parse_plain_special_predicate(cursor, field);
    }

    parse_plain_compare_predicate(cursor, field)
}
