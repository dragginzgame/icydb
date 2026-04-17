use crate::{
    db::predicate::parser::{
        lowering::{
            parse_between_predicate, parse_in_predicate, predicate_compare,
            predicate_compare_fields, predicate_compare_with_coercion,
        },
        operand::{
            PredicateFieldOperand, TextPredicateWrapper, eat_prefix_text_predicate_operator,
            parse_predicate_field_operand, parse_prefix_text_predicate,
        },
    },
    db::{
        predicate::{CoercionId, CompareOp, Predicate},
        sql_shared::{Keyword, SqlParseError, SqlTokenCursor, TokenKind},
    },
    value::Value,
};

// Parse one field predicate family, including reduced SQL special forms.
pub(in crate::db::predicate::parser::expression::atom) fn parse_field_predicate(
    cursor: &mut SqlTokenCursor,
) -> Result<Predicate, SqlParseError> {
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
