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

// Track the accepted reduced-SQL text wrappers that lower onto shared
// casefolded text predicate semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TextPredicateWrapper {
    Lower,
    Upper,
}

impl TextPredicateWrapper {
    const fn unsupported_feature(self) -> &'static str {
        match self {
            Self::Lower => "LOWER(field) predicate forms beyond LIKE 'prefix%'",
            Self::Upper => "UPPER(field) predicate forms beyond LIKE 'prefix%'",
        }
    }
}

// Track whether one parsed predicate field operand is raw-field strict text or
// wrapped casefold text for the reduced SQL `LIKE` lowering boundary.
#[derive(Debug, Eq, PartialEq)]
enum PredicateFieldOperand {
    Plain(String),
    Wrapped {
        field: String,
        wrapper: TextPredicateWrapper,
    },
}

impl PredicateFieldOperand {
    fn into_plain_field(self) -> Result<String, SqlParseError> {
        match self {
            Self::Plain(field) => Ok(field),
            Self::Wrapped { wrapper, .. } => Err(SqlParseError::unsupported_feature(
                wrapper.unsupported_feature(),
            )),
        }
    }
}

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
    let operand = parse_predicate_field_operand(cursor)?;
    if cursor.eat_identifier_keyword("LIKE") {
        return parse_like_prefix_predicate(cursor, operand);
    }

    let field = operand.into_plain_field()?;

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
// Reduced SQL supports plain fields plus bounded `LOWER(<field>)` /
// `UPPER(<field>)` wrappers for casefold LIKE-prefix lowering.
fn parse_predicate_field_operand(
    cursor: &mut SqlTokenCursor,
) -> Result<PredicateFieldOperand, SqlParseError> {
    if cursor.peek_identifier_keyword("LOWER")
        && matches!(cursor.peek_next_kind(), Some(TokenKind::LParen))
    {
        return parse_wrapped_field_operand(cursor, TextPredicateWrapper::Lower);
    }

    if cursor.peek_identifier_keyword("UPPER")
        && matches!(cursor.peek_next_kind(), Some(TokenKind::LParen))
    {
        return parse_wrapped_field_operand(cursor, TextPredicateWrapper::Upper);
    }

    Ok(PredicateFieldOperand::Plain(cursor.expect_identifier()?))
}

// Parse one bounded LOWER/UPPER(field) or raw-field LIKE 'prefix%' predicate family.
fn parse_like_prefix_predicate(
    cursor: &mut SqlTokenCursor,
    operand: PredicateFieldOperand,
) -> Result<Predicate, SqlParseError> {
    let Some(TokenKind::StringLiteral(pattern)) = cursor.bump() else {
        return Err(SqlParseError::expected(
            "string literal pattern after LIKE",
            cursor.peek_kind(),
        ));
    };
    let Some(prefix) = like_prefix_from_pattern(pattern.as_str()) else {
        return Err(SqlParseError::unsupported_feature(
            "LIKE patterns beyond trailing '%' prefix form",
        ));
    };
    let (field, coercion) = match operand {
        PredicateFieldOperand::Plain(field) => (field, CoercionId::Strict),
        PredicateFieldOperand::Wrapped { field, .. } => (field, CoercionId::TextCasefold),
    };

    Ok(Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::StartsWith,
        Value::Text(prefix.to_string()),
        coercion,
    )))
}

fn parse_wrapped_field_operand(
    cursor: &mut SqlTokenCursor,
    wrapper: TextPredicateWrapper,
) -> Result<PredicateFieldOperand, SqlParseError> {
    let _ = cursor.bump();
    cursor.expect_lparen()?;
    let field = cursor.expect_identifier()?;
    cursor.expect_rparen()?;

    Ok(PredicateFieldOperand::Wrapped { field, wrapper })
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
