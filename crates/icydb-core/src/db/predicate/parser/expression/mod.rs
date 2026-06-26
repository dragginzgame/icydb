mod atom;
mod chain;

use crate::{
    db::predicate::Predicate,
    db::sql_shared::{
        MAX_SQL_EXPR_DEPTH, SqlParseError, SqlTokenCursor, sql_expr_depth_limit_error,
    },
};

pub(in crate::db::predicate::parser::expression) type ParsedPredicate = (Predicate, usize);

/// Parse one full predicate tree from the shared reduced-SQL token cursor.
pub(in crate::db::predicate::parser) fn parse_predicate_from_cursor(
    cursor: &mut SqlTokenCursor,
) -> Result<Predicate, SqlParseError> {
    let (predicate, _) = parse_predicate_from_cursor_at_depth(cursor, 1)?;

    Ok(predicate)
}

pub(in crate::db::predicate::parser::expression) fn parse_predicate_from_cursor_at_depth(
    cursor: &mut SqlTokenCursor,
    parse_depth: usize,
) -> Result<ParsedPredicate, SqlParseError> {
    crate::db::predicate::parser::expression::chain::parse_or_predicate(cursor, parse_depth)
}

pub(in crate::db::predicate::parser::expression) fn descend_predicate_parse_depth(
    parse_depth: usize,
) -> Result<usize, SqlParseError> {
    if parse_depth >= MAX_SQL_EXPR_DEPTH {
        return Err(sql_expr_depth_limit_error());
    }

    Ok(parse_depth.saturating_add(1))
}

pub(in crate::db::predicate::parser::expression) fn validate_predicate_tree_depth(
    tree_depth: usize,
) -> Result<(), SqlParseError> {
    if tree_depth > MAX_SQL_EXPR_DEPTH {
        return Err(sql_expr_depth_limit_error());
    }

    Ok(())
}
