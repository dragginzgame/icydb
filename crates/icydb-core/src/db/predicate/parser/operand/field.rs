use crate::db::{
    predicate::CoercionId,
    sql_shared::{SqlParseError, SqlTokenCursor, TokenKind},
};
use icydb_diagnostic_code::SqlFeatureCode;

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
    pub(in crate::db::predicate::parser) const fn unsupported_feature(self) -> SqlFeatureCode {
        match self {
            Self::Lower => SqlFeatureCode::LowerFieldPredicateUnsupported,
            Self::Upper => SqlFeatureCode::UpperFieldPredicateUnsupported,
        }
    }
}

///
/// PredicateFieldOperand
///
/// Tracks whether one parsed field operand is a plain field or one bounded
/// text wrapper so prefix-text forms can share one fail-closed lowering boundary.
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
    pub(in crate::db::predicate::parser) fn into_field_and_coercion(
        self,
    ) -> Result<(String, CoercionId), SqlParseError> {
        match self {
            Self::Plain(field) => Ok((field, CoercionId::Strict)),
            Self::Wrapped {
                field,
                wrapper: TextPredicateWrapper::Lower,
            } => Ok((field, CoercionId::TextCasefold)),
            Self::Wrapped {
                wrapper: TextPredicateWrapper::Upper,
                ..
            } => Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::UpperFieldPredicateUnsupported,
            )),
        }
    }
}

// Parse one predicate field operand. Reduced SQL recognizes LOWER/UPPER so it
// can lower LOWER exactly and reject UPPER without changing its semantics.
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
