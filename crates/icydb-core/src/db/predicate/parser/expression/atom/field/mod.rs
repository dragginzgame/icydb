mod plain;
mod wrapped;

use crate::{
    db::predicate::parser::{
        expression::atom::field::{
            plain::parse_plain_field_predicate, wrapped::parse_wrapped_field_predicate,
        },
        operand::{
            PredicateFieldOperand, eat_prefix_text_predicate_operator,
            parse_predicate_field_operand, parse_prefix_text_predicate,
        },
    },
    db::{
        predicate::Predicate,
        sql_shared::{SqlParseError, SqlTokenCursor},
    },
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
