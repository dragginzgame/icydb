mod atom;
mod chain;

use crate::{
    db::predicate::Predicate,
    db::sql_shared::{SqlParseError, SqlTokenCursor},
};

/// Parse one full predicate tree from the shared reduced-SQL token cursor.
pub(in crate::db) fn parse_predicate_from_cursor(
    cursor: &mut SqlTokenCursor,
) -> Result<Predicate, SqlParseError> {
    crate::db::predicate::parser::expression::chain::parse_or_predicate(cursor)
}
