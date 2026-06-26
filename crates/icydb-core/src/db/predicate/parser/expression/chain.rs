use crate::db::{
    predicate::{
        Predicate,
        parser::expression::{
            ParsedPredicate, descend_predicate_parse_depth, validate_predicate_tree_depth,
        },
    },
    sql_shared::{Keyword, SqlParseError, SqlTokenCursor},
};

// Parse OR chains with left-associative reduced SQL predicate semantics.
pub(in crate::db::predicate::parser::expression) fn parse_or_predicate(
    cursor: &mut SqlTokenCursor,
    parse_depth: usize,
) -> Result<ParsedPredicate, SqlParseError> {
    let (mut left, mut left_depth) = parse_and_predicate(cursor, parse_depth)?;
    while cursor.eat_keyword(Keyword::Or) {
        let (right, right_depth) = parse_and_predicate(cursor, parse_depth)?;
        let next_depth = left_depth.max(right_depth).saturating_add(1);
        validate_predicate_tree_depth(next_depth)?;
        left = Predicate::Or(vec![left, right]);
        left_depth = next_depth;
    }

    Ok((left, left_depth))
}

// Parse AND chains with stronger precedence than OR.
fn parse_and_predicate(
    cursor: &mut SqlTokenCursor,
    parse_depth: usize,
) -> Result<ParsedPredicate, SqlParseError> {
    let (mut left, mut left_depth) = parse_not_predicate(cursor, parse_depth)?;
    while cursor.eat_keyword(Keyword::And) {
        let (right, right_depth) = parse_not_predicate(cursor, parse_depth)?;
        let next_depth = left_depth.max(right_depth).saturating_add(1);
        validate_predicate_tree_depth(next_depth)?;
        left = Predicate::And(vec![left, right]);
        left_depth = next_depth;
    }

    Ok((left, left_depth))
}

// Parse unary NOT before falling through to one primary predicate atom.
fn parse_not_predicate(
    cursor: &mut SqlTokenCursor,
    parse_depth: usize,
) -> Result<ParsedPredicate, SqlParseError> {
    if cursor.eat_keyword(Keyword::Not) {
        let child_parse_depth = descend_predicate_parse_depth(parse_depth)?;
        let (predicate, child_depth) = parse_not_predicate(cursor, child_parse_depth)?;
        let tree_depth = child_depth.saturating_add(1);
        validate_predicate_tree_depth(tree_depth)?;

        return Ok((Predicate::Not(Box::new(predicate)), tree_depth));
    }

    crate::db::predicate::parser::expression::atom::parse_predicate_primary(cursor, parse_depth)
}
