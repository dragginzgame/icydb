use crate::db::{
    predicate::{
        Predicate,
        parser::expression::{
            ParsedPredicate, descend_predicate_parse_depth, validate_predicate_source_depth,
        },
    },
    sql_shared::{Keyword, SqlParseError, SqlTokenCursor},
};

// Build one OR container for the authored chain, without flattening parentheses.
pub(in crate::db::predicate::parser::expression) fn parse_or_predicate(
    cursor: &mut SqlTokenCursor,
    parse_depth: usize,
) -> Result<ParsedPredicate, SqlParseError> {
    let (first, mut source_depth) = parse_and_predicate(cursor, parse_depth)?;
    if !cursor.eat_keyword(Keyword::Or) {
        return Ok((first, source_depth));
    }
    let mut children = Vec::with_capacity(2);
    children.push(first);
    loop {
        let (right, right_depth) = parse_and_predicate(cursor, parse_depth)?;
        source_depth = source_depth.max(right_depth).saturating_add(1);
        validate_predicate_source_depth(source_depth)?;
        children.push(right);
        if !cursor.eat_keyword(Keyword::Or) {
            break;
        }
    }

    Ok((Predicate::Or(children), source_depth))
}

// Parse AND chains with stronger precedence than OR.
fn parse_and_predicate(
    cursor: &mut SqlTokenCursor,
    parse_depth: usize,
) -> Result<ParsedPredicate, SqlParseError> {
    let (first, mut source_depth) = parse_not_predicate(cursor, parse_depth)?;
    if !cursor.eat_keyword(Keyword::And) {
        return Ok((first, source_depth));
    }
    let mut children = Vec::with_capacity(2);
    children.push(first);
    loop {
        let (right, right_depth) = parse_not_predicate(cursor, parse_depth)?;
        source_depth = source_depth.max(right_depth).saturating_add(1);
        validate_predicate_source_depth(source_depth)?;
        children.push(right);
        if !cursor.eat_keyword(Keyword::And) {
            break;
        }
    }

    Ok((Predicate::And(children), source_depth))
}

// Parse unary NOT before falling through to one primary predicate atom.
fn parse_not_predicate(
    cursor: &mut SqlTokenCursor,
    parse_depth: usize,
) -> Result<ParsedPredicate, SqlParseError> {
    if cursor.eat_keyword(Keyword::Not) {
        let child_parse_depth = descend_predicate_parse_depth(parse_depth)?;
        let (predicate, child_depth) = parse_not_predicate(cursor, child_parse_depth)?;
        let source_depth = child_depth.saturating_add(1);
        validate_predicate_source_depth(source_depth)?;

        return Ok((Predicate::Not(Box::new(predicate)), source_depth));
    }

    crate::db::predicate::parser::expression::atom::parse_predicate_primary(cursor, parse_depth)
}
