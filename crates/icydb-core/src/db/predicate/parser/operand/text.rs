use crate::{
    db::{
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate,
            parser::operand::field::{PredicateFieldOperand, parse_predicate_field_operand},
        },
        sql_shared::{Keyword, SqlParseError, SqlTokenCursor, TokenKind},
    },
    value::Value,
};

const DIRECT_STARTS_WITH_NON_FIELD_FEATURE: &str =
    "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers";

///
/// PrefixTextPredicateOperator
///
/// Tracks the bounded prefix-text spellings that lower onto the shared
/// `STARTS_WITH` compare seam while preserving the correct coercion choice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::predicate::parser) enum PrefixTextPredicateOperator {
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

pub(in crate::db::predicate::parser) const fn predicate_literal_starts(
    kind: Option<&TokenKind>,
) -> bool {
    matches!(
        kind,
        Some(
            TokenKind::StringLiteral(_)
                | TokenKind::BlobLiteral(_)
                | TokenKind::Number(_)
                | TokenKind::Minus
                | TokenKind::Keyword(Keyword::Null | Keyword::True | Keyword::False,)
        )
    )
}

// Parse one bounded LIKE/ILIKE 'prefix%' predicate family and lower it onto
// the shared STARTS_WITH compare seam.
pub(in crate::db::predicate::parser) fn parse_prefix_text_predicate(
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
pub(in crate::db::predicate::parser) fn parse_starts_with_predicate(
    cursor: &mut SqlTokenCursor,
) -> Result<Predicate, SqlParseError> {
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
pub(in crate::db::predicate::parser) fn eat_prefix_text_predicate_operator(
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
