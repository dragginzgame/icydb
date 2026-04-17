use crate::{
    db::predicate::parser::{
        lowering::{predicate_compare, predicate_compare_with_coercion},
        operand::{PredicateFieldOperand, parse_predicate_field_operand},
    },
    db::{
        predicate::{CoercionId, CompareOp, Predicate},
        sql_shared::{SqlParseError, SqlTokenCursor},
    },
    value::Value,
};

// Parse one symmetric literal-leading compare and normalize it back onto the
// canonical field-first predicate seam.
pub(in crate::db::predicate::parser::expression::atom) fn parse_literal_leading_predicate(
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
