use crate::db::{
    predicate::CoercionId,
    sql_shared::{SqlParseError, SqlTokenCursor, TokenKind},
};

///
/// TextPredicateWrapper
///
/// Tracks the bounded wrapper spellings that the reduced predicate parser
/// accepts so wrapped text predicates lower onto shared casefold semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::predicate::parser) enum TextPredicateWrapper {
    Lower,
    Upper,
}

impl TextPredicateWrapper {
    pub(in crate::db::predicate::parser) const fn unsupported_feature(self) -> &'static str {
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

///
/// PredicateFieldOperand
///
/// Tracks whether one parsed field operand is a plain field or one bounded
/// casefold wrapper so prefix-text forms can share one lowering boundary.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::predicate::parser) enum PredicateFieldOperand {
    Plain(String),
    Wrapped {
        field: String,
        wrapper: TextPredicateWrapper,
    },
}

impl PredicateFieldOperand {
    // Map one bounded predicate operand to its canonical field/coercion pair.
    pub(in crate::db::predicate::parser) fn into_field_and_coercion(self) -> (String, CoercionId) {
        match self {
            Self::Plain(field) => (field, CoercionId::Strict),
            Self::Wrapped { field, .. } => (field, CoercionId::TextCasefold),
        }
    }
}

// Parse one predicate field operand.
// Reduced SQL supports plain fields plus bounded `LOWER(<field>)` /
// `UPPER(<field>)` wrappers for casefold LIKE-prefix lowering.
pub(in crate::db::predicate::parser) fn parse_predicate_field_operand(
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
