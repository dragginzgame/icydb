use crate::{
    db::{
        predicate::{
            CoercionId, CompareOp, Predicate,
            parser::{
                lowering::predicate_compare_with_coercion, operand::TextPredicateWrapper,
                parse_compare_operator,
            },
        },
        sql_shared::{Keyword, SqlParseError, SqlTokenCursor},
    },
    value::Value,
};

// Parse the intentionally narrow wrapped-field predicate family.
// Reduced SQL only accepts ordered text bounds on LOWER/UPPER(field) wrappers,
// and it lowers those bounds onto the same TextCasefold compare contract that
// expression-prefix planning already uses.
pub(in crate::db::predicate::parser::expression::atom::field) fn parse_wrapped_field_predicate(
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

    let op = parse_compare_operator(cursor)?;
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
