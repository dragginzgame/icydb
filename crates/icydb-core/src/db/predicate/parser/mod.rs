//! Module: predicate::parser
//! Responsibility: reduced SQL predicate parsing for core predicate semantics.
//! Does not own: statement routing, SQL frontend dispatch, or executor behavior.
//! Boundary: schema/index/core code consumes standalone predicate parsing here.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
        sql_shared::{Keyword, SqlParseError, SqlTokenCursor, TokenKind, tokenize_sql},
    },
    value::Value,
};

const DIRECT_STARTS_WITH_NON_FIELD_FEATURE: &str =
    "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers";

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
            Self::Lower => {
                "LOWER(field) predicate forms beyond LIKE 'prefix%' or ordered text bounds"
            }
            Self::Upper => {
                "UPPER(field) predicate forms beyond LIKE 'prefix%' or ordered text bounds"
            }
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
    // Map one bounded predicate operand to its canonical field/coercion pair.
    fn into_field_and_coercion(self) -> (String, CoercionId) {
        match self {
            Self::Plain(field) => (field, CoercionId::Strict),
            Self::Wrapped { field, .. } => (field, CoercionId::TextCasefold),
        }
    }
}

// Track the bounded prefix-text predicate spellings that lower onto the shared
// STARTS_WITH compare seam.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrefixTextPredicateOperator {
    Like,
    Ilike,
}

impl PrefixTextPredicateOperator {
    const fn literal_context(self) -> &'static str {
        match self {
            Self::Like => "string literal pattern after LIKE",
            Self::Ilike => "string literal pattern after ILIKE",
        }
    }

    const fn result_coercion(self, operand_coercion: CoercionId) -> CoercionId {
        match self {
            Self::Like => operand_coercion,
            Self::Ilike => CoercionId::TextCasefold,
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

// Parse one field predicate family, including reduced SQL special forms.
fn parse_field_predicate(cursor: &mut SqlTokenCursor) -> Result<Predicate, SqlParseError> {
    let operand = parse_predicate_field_operand(cursor)?;
    if let Some((operator, negated)) = eat_prefix_text_predicate_operator(cursor) {
        let predicate = parse_prefix_text_predicate(cursor, operand, operator)?;

        return Ok(if negated {
            Predicate::not(predicate)
        } else {
            predicate
        });
    }

    match operand {
        PredicateFieldOperand::Plain(field) => parse_plain_field_predicate(cursor, field),
        PredicateFieldOperand::Wrapped { field, wrapper } => {
            parse_wrapped_field_predicate(cursor, field, wrapper)
        }
    }
}

// Parse one plain-field predicate family, including reduced SQL special forms.
fn parse_plain_field_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
) -> Result<Predicate, SqlParseError> {
    if cursor.eat_keyword(Keyword::Is) {
        if cursor.eat_keyword(Keyword::Not) {
            if cursor.eat_keyword(Keyword::Null) {
                return Ok(Predicate::IsNotNull { field });
            }
            if cursor.eat_keyword(Keyword::True) {
                return Ok(Predicate::not(predicate_compare(
                    field,
                    CompareOp::Eq,
                    Value::Bool(true),
                )));
            }
            if cursor.eat_keyword(Keyword::False) {
                return Ok(Predicate::not(predicate_compare(
                    field,
                    CompareOp::Eq,
                    Value::Bool(false),
                )));
            }

            return Err(SqlParseError::expected(
                "NULL, TRUE, or FALSE after IS NOT",
                cursor.peek_kind(),
            ));
        }

        if cursor.eat_keyword(Keyword::Null) {
            return Ok(Predicate::IsNull { field });
        }

        if cursor.eat_keyword(Keyword::True) {
            return Ok(predicate_compare(field, CompareOp::Eq, Value::Bool(true)));
        }

        if cursor.eat_keyword(Keyword::False) {
            return Ok(predicate_compare(field, CompareOp::Eq, Value::Bool(false)));
        }

        return Err(SqlParseError::expected(
            "NULL, TRUE, or FALSE after IS",
            cursor.peek_kind(),
        ));
    }

    if cursor.eat_keyword(Keyword::Not) {
        if cursor.eat_keyword(Keyword::In) {
            return parse_in_predicate(cursor, field, true);
        }

        if cursor.eat_keyword(Keyword::Between) {
            return parse_between_predicate(cursor, field, true);
        }

        return Err(SqlParseError::expected(
            "IN or BETWEEN after NOT",
            cursor.peek_kind(),
        ));
    }

    if cursor.eat_keyword(Keyword::In) {
        return parse_in_predicate(cursor, field, false);
    }

    if cursor.eat_keyword(Keyword::Between) {
        return parse_between_predicate(cursor, field, false);
    }

    let op = cursor.parse_compare_operator()?;
    if matches!(cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
        let right_field = cursor.expect_identifier()?;
        return Ok(predicate_compare_fields(field, op, right_field));
    }

    let value = cursor.parse_literal()?;

    Ok(predicate_compare(field, op, value))
}

// Parse one symmetric literal-leading compare and normalize it back onto the
// canonical field-first predicate seam.
fn parse_literal_leading_predicate(
    cursor: &mut SqlTokenCursor,
) -> Result<Predicate, SqlParseError> {
    let literal = cursor.parse_literal()?;
    let op = cursor.parse_compare_operator()?;
    let operand = parse_predicate_field_operand(cursor)?;
    let flipped = op.flipped();

    match operand {
        PredicateFieldOperand::Plain(field) => Ok(predicate_compare(field, flipped, literal)),
        PredicateFieldOperand::Wrapped { field, wrapper } => {
            if !matches!(
                flipped,
                CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
            ) || !matches!(literal, Value::Text(_))
            {
                return Err(SqlParseError::unsupported_feature(
                    wrapper.unsupported_feature(),
                ));
            }

            Ok(predicate_compare_with_coercion(
                field,
                flipped,
                literal,
                CoercionId::TextCasefold,
            ))
        }
    }
}

// Parse the intentionally narrow wrapped-field predicate family.
// Reduced SQL only accepts ordered text bounds on LOWER/UPPER(field) wrappers,
// and it lowers those bounds onto the same TextCasefold compare contract that
// expression-prefix planning already uses.
fn parse_wrapped_field_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
    wrapper: TextPredicateWrapper,
) -> Result<Predicate, SqlParseError> {
    if cursor.eat_keyword(Keyword::Is)
        || cursor.eat_keyword(Keyword::Not)
        || cursor.eat_keyword(Keyword::In)
        || cursor.eat_keyword(Keyword::Between)
    {
        return Err(SqlParseError::unsupported_feature(
            wrapper.unsupported_feature(),
        ));
    }

    let op = cursor.parse_compare_operator()?;
    if !matches!(
        op,
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
    ) {
        return Err(SqlParseError::unsupported_feature(
            wrapper.unsupported_feature(),
        ));
    }

    let value = cursor.parse_literal()?;
    if !matches!(value, Value::Text(_)) {
        return Err(SqlParseError::unsupported_feature(
            wrapper.unsupported_feature(),
        ));
    }

    Ok(predicate_compare_with_coercion(
        field,
        op,
        value,
        CoercionId::TextCasefold,
    ))
}

const fn predicate_literal_starts(kind: Option<&TokenKind>) -> bool {
    matches!(
        kind,
        Some(
            TokenKind::StringLiteral(_)
                | TokenKind::Number(_)
                | TokenKind::Minus
                | TokenKind::Keyword(Keyword::Null | Keyword::True | Keyword::False,)
        )
    )
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

// Parse one bounded LIKE/ILIKE 'prefix%' predicate family and lower it onto
// the shared STARTS_WITH compare seam.
fn parse_prefix_text_predicate(
    cursor: &mut SqlTokenCursor,
    operand: PredicateFieldOperand,
    operator: PrefixTextPredicateOperator,
) -> Result<Predicate, SqlParseError> {
    let Some(TokenKind::StringLiteral(pattern)) = cursor.peek_kind() else {
        return Err(SqlParseError::expected(
            operator.literal_context(),
            cursor.peek_kind(),
        ));
    };
    let Some(prefix) = like_prefix_from_pattern(pattern.as_str()) else {
        return Err(SqlParseError::unsupported_feature(
            "LIKE patterns beyond trailing '%' prefix form",
        ));
    };
    let prefix = prefix.to_string();
    let _ = cursor.advance();
    let (field, coercion) = operand.into_field_and_coercion();

    Ok(Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::StartsWith,
        Value::Text(prefix),
        operator.result_coercion(coercion),
    )))
}

// Parse one bounded direct `STARTS_WITH(...)` predicate spelling.
// This remains intentionally narrow: it accepts only plain fields plus the
// same LOWER/UPPER casefold wrappers already supported on the reduced `LIKE`
// prefix family, and it does not open generic SQL function predicates.
fn parse_starts_with_predicate(cursor: &mut SqlTokenCursor) -> Result<Predicate, SqlParseError> {
    let _ = cursor.eat_identifier_keyword("STARTS_WITH");
    cursor.expect_lparen()?;

    // Keep the direct spelling exact and structural: the first argument may be
    // one plain field identifier or one bounded LOWER/UPPER field wrapper.
    let operand = parse_predicate_field_operand(cursor)?;

    if matches!(cursor.peek_kind(), Some(TokenKind::LParen)) {
        return Err(SqlParseError::unsupported_feature(
            DIRECT_STARTS_WITH_NON_FIELD_FEATURE,
        ));
    }
    expect_predicate_argument_comma(cursor, "',' between STARTS_WITH arguments")?;

    let Some(TokenKind::StringLiteral(prefix)) = cursor.peek_kind() else {
        return Err(SqlParseError::expected(
            "string literal second argument to STARTS_WITH",
            cursor.peek_kind(),
        ));
    };
    let prefix = prefix.clone();
    let _ = cursor.advance();
    cursor.expect_rparen()?;
    let (field, coercion) = operand.into_field_and_coercion();

    Ok(Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::StartsWith,
        Value::Text(prefix),
        coercion,
    )))
}

fn parse_wrapped_field_operand(
    cursor: &mut SqlTokenCursor,
    wrapper: TextPredicateWrapper,
) -> Result<PredicateFieldOperand, SqlParseError> {
    let _ = cursor.advance();
    cursor.expect_lparen()?;
    let field = cursor.expect_identifier()?;
    cursor.expect_rparen()?;

    Ok(PredicateFieldOperand::Wrapped { field, wrapper })
}

fn expect_predicate_argument_comma(
    cursor: &mut SqlTokenCursor,
    context: &'static str,
) -> Result<(), SqlParseError> {
    if cursor.eat_comma() {
        return Ok(());
    }

    Err(SqlParseError::expected(context, cursor.peek_kind()))
}

// Detect and consume the bounded prefix-text operators without stealing the
// broader plain-field `NOT IN` / `NOT BETWEEN` surface.
fn eat_prefix_text_predicate_operator(
    cursor: &mut SqlTokenCursor,
) -> Option<(PrefixTextPredicateOperator, bool)> {
    if cursor.eat_identifier_keyword("LIKE") {
        return Some((PrefixTextPredicateOperator::Like, false));
    }
    if cursor.eat_identifier_keyword("ILIKE") {
        return Some((PrefixTextPredicateOperator::Ilike, false));
    }
    if peek_not_identifier_keyword(cursor, "LIKE") {
        let _ = cursor.eat_keyword(Keyword::Not);
        let _ = cursor.eat_identifier_keyword("LIKE");

        return Some((PrefixTextPredicateOperator::Like, true));
    }
    if peek_not_identifier_keyword(cursor, "ILIKE") {
        let _ = cursor.eat_keyword(Keyword::Not);
        let _ = cursor.eat_identifier_keyword("ILIKE");

        return Some((PrefixTextPredicateOperator::Ilike, true));
    }

    None
}

fn peek_not_identifier_keyword(cursor: &SqlTokenCursor, keyword: &str) -> bool {
    matches!(cursor.peek_kind(), Some(TokenKind::Keyword(Keyword::Not)))
        && matches!(
            cursor.peek_next_kind(),
            Some(TokenKind::Identifier(value)) if value.eq_ignore_ascii_case(keyword)
        )
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
        if matches!(cursor.peek_kind(), Some(TokenKind::RParen)) {
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
    negated: bool,
) -> Result<Predicate, SqlParseError> {
    let lower = parse_between_bound(cursor)?;
    cursor.expect_keyword(Keyword::And)?;
    let upper = parse_between_bound(cursor)?;

    Ok(if negated {
        Predicate::Or(vec![
            predicate_between_bound(field.clone(), CompareOp::Lt, lower),
            predicate_between_bound(field, CompareOp::Gt, upper),
        ])
    } else {
        Predicate::And(vec![
            predicate_between_bound(field.clone(), CompareOp::Gte, lower),
            predicate_between_bound(field, CompareOp::Lte, upper),
        ])
    })
}

// Track one bounded BETWEEN endpoint while keeping the surface limited to
// plain literals or plain field references.
enum BetweenBound {
    Literal(Value),
    Field(String),
}

// Parse one BETWEEN endpoint without widening into generic expression bounds.
fn parse_between_bound(cursor: &mut SqlTokenCursor) -> Result<BetweenBound, SqlParseError> {
    if matches!(cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
        return cursor.expect_identifier().map(BetweenBound::Field);
    }

    cursor.parse_literal().map(BetweenBound::Literal)
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
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            if matches!(value, Value::Text(_)) {
                CoercionId::Strict
            } else {
                CoercionId::NumericWiden
            }
        }
        _ => CoercionId::Strict,
    };

    predicate_compare_with_coercion(field, op, value, coercion)
}

fn predicate_compare_with_coercion(
    field: String,
    op: CompareOp,
    value: Value,
    coercion: CoercionId,
) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(field, op, value, coercion))
}

fn predicate_compare_fields(left_field: String, op: CompareOp, right_field: String) -> Predicate {
    let coercion = match op {
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => CoercionId::NumericWiden,
        _ => CoercionId::Strict,
    };

    Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
        left_field,
        op,
        right_field,
        coercion,
    ))
}

fn predicate_between_bound(field: String, op: CompareOp, bound: BetweenBound) -> Predicate {
    match bound {
        BetweenBound::Literal(value) => predicate_compare(field, op, value),
        BetweenBound::Field(other_field) => predicate_compare_fields(field, op, other_field),
    }
}
