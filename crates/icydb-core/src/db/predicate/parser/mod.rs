//! Module: predicate::parser
//! Responsibility: reduced SQL predicate parsing for core predicate semantics.
//! Does not own: statement routing, SQL frontend dispatch, or executor behavior.
//! Boundary: schema/index/core code consumes standalone predicate parsing here.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        reduced_sql::{Keyword, SqlParseError, SqlTokenCursor, TokenKind, tokenize_sql},
    },
    value::Value,
};

/// Parse one SQL predicate expression.
///
/// This is the core predicate parsing boundary used by schema/index contracts
/// that need predicate semantics without a full SQL statement wrapper.
pub(crate) fn parse_sql_predicate(sql: &str) -> Result<Predicate, SqlParseError> {
    let tokens = tokenize_sql(sql)?;
    let mut cursor = SqlTokenCursor::new(tokens);
    let predicate = parse_predicate_from_cursor(&mut cursor)?;

    if cursor.eat_semicolon() && !cursor.is_eof() {
        return Err(SqlParseError::unsupported_feature(
            "multi-statement SQL input",
        ));
    }

    if !cursor.is_eof() {
        if let Some(feature) = cursor.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        }

        return Err(SqlParseError::expected_end_of_input(cursor.peek_kind()));
    }

    Ok(predicate)
}

// Parse one full predicate tree from the shared reduced-SQL token cursor.
pub(in crate::db) fn parse_predicate_from_cursor(
    cursor: &mut SqlTokenCursor,
) -> Result<Predicate, SqlParseError> {
    parse_or_predicate(cursor)
}

// Parse OR chains with left-associative reduced SQL predicate semantics.
fn parse_or_predicate(cursor: &mut SqlTokenCursor) -> Result<Predicate, SqlParseError> {
    let mut left = parse_and_predicate(cursor)?;
    while cursor.eat_keyword(Keyword::Or) {
        let right = parse_and_predicate(cursor)?;
        left = Predicate::Or(vec![left, right]);
    }

    Ok(left)
}

// Parse AND chains with stronger precedence than OR.
fn parse_and_predicate(cursor: &mut SqlTokenCursor) -> Result<Predicate, SqlParseError> {
    let mut left = parse_not_predicate(cursor)?;
    while cursor.eat_keyword(Keyword::And) {
        let right = parse_not_predicate(cursor)?;
        left = Predicate::And(vec![left, right]);
    }

    Ok(left)
}

// Parse unary NOT before falling through to one primary predicate atom.
fn parse_not_predicate(cursor: &mut SqlTokenCursor) -> Result<Predicate, SqlParseError> {
    if cursor.eat_keyword(Keyword::Not) {
        return Ok(Predicate::Not(Box::new(parse_not_predicate(cursor)?)));
    }

    parse_predicate_primary(cursor)
}

// Parse one parenthesized predicate or one field/operator predicate atom.
fn parse_predicate_primary(cursor: &mut SqlTokenCursor) -> Result<Predicate, SqlParseError> {
    if cursor.eat_lparen() {
        let predicate = parse_predicate_from_cursor(cursor)?;
        cursor.expect_rparen()?;

        return Ok(predicate);
    }

    parse_field_predicate(cursor)
}

// Parse one field predicate family, including reduced SQL special forms.
fn parse_field_predicate(cursor: &mut SqlTokenCursor) -> Result<Predicate, SqlParseError> {
    let (field, lower_expr) = parse_predicate_field_operand(cursor)?;
    if lower_expr {
        if cursor.eat_identifier_keyword("LIKE") {
            return parse_lower_like_prefix_predicate(cursor, field);
        }

        return Err(SqlParseError::unsupported_feature(
            "LOWER(field) predicate forms beyond LIKE 'prefix%'",
        ));
    }

    if cursor.eat_identifier_keyword("LIKE") {
        return Err(SqlParseError::unsupported_feature(
            "LIKE predicates beyond LOWER(field) LIKE 'prefix%'",
        ));
    }

    if cursor.eat_keyword(Keyword::Is) {
        let is_not = cursor.eat_keyword(Keyword::Not);
        cursor.expect_keyword(Keyword::Null)?;

        return Ok(if is_not {
            Predicate::IsNotNull { field }
        } else {
            Predicate::IsNull { field }
        });
    }

    if cursor.eat_keyword(Keyword::Not) {
        if cursor.eat_keyword(Keyword::In) {
            return parse_in_predicate(cursor, field, true);
        }

        return Err(SqlParseError::expected("IN after NOT", cursor.peek_kind()));
    }

    if cursor.eat_keyword(Keyword::In) {
        return parse_in_predicate(cursor, field, false);
    }

    if cursor.eat_keyword(Keyword::Between) {
        return parse_between_predicate(cursor, field);
    }

    let op = cursor.parse_compare_operator()?;
    let value = cursor.parse_literal()?;

    Ok(predicate_compare(field, op, value))
}

// Parse one predicate field operand.
// Reduced SQL supports plain fields and one bounded expression form:
// LOWER(<field>) for casefold LIKE-prefix lowering.
fn parse_predicate_field_operand(
    cursor: &mut SqlTokenCursor,
) -> Result<(String, bool), SqlParseError> {
    if cursor.peek_identifier_keyword("LOWER")
        && matches!(cursor.peek_next_kind(), Some(TokenKind::LParen))
    {
        let _ = cursor.bump();
        cursor.expect_lparen()?;
        let field = cursor.expect_identifier()?;
        cursor.expect_rparen()?;
        return Ok((field, true));
    }

    Ok((cursor.expect_identifier()?, false))
}

// Parse one bounded LOWER(field) LIKE 'prefix%' predicate family.
fn parse_lower_like_prefix_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
) -> Result<Predicate, SqlParseError> {
    let Some(TokenKind::StringLiteral(pattern)) = cursor.bump() else {
        return Err(SqlParseError::expected(
            "string literal pattern after LIKE",
            cursor.peek_kind(),
        ));
    };
    let Some(prefix) = like_prefix_from_pattern(pattern.as_str()) else {
        return Err(SqlParseError::unsupported_feature(
            "LOWER(field) LIKE patterns beyond trailing '%' prefix form",
        ));
    };

    Ok(Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::StartsWith,
        Value::Text(prefix.to_string()),
        CoercionId::TextCasefold,
    )))
}

// Parse one IN / NOT IN list predicate into one canonical predicate compare.
fn parse_in_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
    negated: bool,
) -> Result<Predicate, SqlParseError> {
    cursor.expect_lparen()?;

    let mut values = Vec::new();
    loop {
        values.push(cursor.parse_literal()?);
        if !cursor.eat_comma() {
            break;
        }
    }
    cursor.expect_rparen()?;

    let op = if negated {
        CompareOp::NotIn
    } else {
        CompareOp::In
    };

    Ok(Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        Value::List(values),
        CoercionId::Strict,
    )))
}

// Parse one BETWEEN range into two canonical compare clauses joined by AND.
fn parse_between_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
) -> Result<Predicate, SqlParseError> {
    let lower = cursor.parse_literal()?;
    cursor.expect_keyword(Keyword::And)?;
    let upper = cursor.parse_literal()?;

    Ok(Predicate::And(vec![
        predicate_compare(field.clone(), CompareOp::Gte, lower),
        predicate_compare(field, CompareOp::Lte, upper),
    ]))
}

fn like_prefix_from_pattern(pattern: &str) -> Option<&str> {
    if !pattern.ends_with('%') {
        return None;
    }

    let prefix = &pattern[..pattern.len() - 1];
    if prefix.contains('%') || prefix.contains('_') {
        return None;
    }

    Some(prefix)
}

fn predicate_compare(field: String, op: CompareOp, value: Value) -> Predicate {
    let coercion = match op {
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => CoercionId::NumericWiden,
        _ => CoercionId::Strict,
    };

    Predicate::Compare(ComparePredicate::with_coercion(field, op, value, coercion))
}
