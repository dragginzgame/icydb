use crate::{
    db::predicate::parser::lowering::{
        parse_between_predicate, parse_in_predicate, predicate_compare,
    },
    db::{
        predicate::{CompareOp, Predicate},
        sql_shared::{Keyword, SqlParseError, SqlTokenCursor},
    },
    value::Value,
};

// Parse the reduced SQL plain-field special forms that are not handled by the
// generic compare-operator lane.
pub(in crate::db::predicate::parser::expression::atom::field::plain) fn parse_plain_special_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
) -> Result<Predicate, SqlParseError> {
    if cursor.eat_keyword(Keyword::Is) {
        return parse_plain_is_predicate(cursor, field);
    }

    if cursor.eat_keyword(Keyword::Not) {
        return parse_plain_not_predicate(cursor, field);
    }

    if cursor.eat_keyword(Keyword::In) {
        return parse_in_predicate(cursor, field, false);
    }

    if cursor.eat_keyword(Keyword::Between) {
        return parse_between_predicate(cursor, field, false);
    }

    Err(SqlParseError::expected(
        "IS, NOT, IN, or BETWEEN after field",
        cursor.peek_kind(),
    ))
}

fn parse_plain_is_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
) -> Result<Predicate, SqlParseError> {
    let negated = cursor.eat_keyword(Keyword::Not);

    parse_plain_is_terminal_predicate(cursor, field, negated)
}

fn parse_plain_not_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
) -> Result<Predicate, SqlParseError> {
    if cursor.eat_keyword(Keyword::In) {
        return parse_in_predicate(cursor, field, true);
    }

    if cursor.eat_keyword(Keyword::Between) {
        return parse_between_predicate(cursor, field, true);
    }

    Err(SqlParseError::expected(
        "IN or BETWEEN after NOT",
        cursor.peek_kind(),
    ))
}

// Parse the bounded `IS [NOT] { NULL | TRUE | FALSE }` family once the owning
// special-form dispatcher has already consumed `IS`.
fn parse_plain_is_terminal_predicate(
    cursor: &mut SqlTokenCursor,
    field: String,
    negated: bool,
) -> Result<Predicate, SqlParseError> {
    if cursor.eat_keyword(Keyword::Null) {
        return Ok(if negated {
            Predicate::IsNotNull { field }
        } else {
            Predicate::IsNull { field }
        });
    }

    if cursor.eat_keyword(Keyword::True) {
        return Ok(plain_bool_is_predicate(field, true, negated));
    }

    if cursor.eat_keyword(Keyword::False) {
        return Ok(plain_bool_is_predicate(field, false, negated));
    }

    Err(SqlParseError::expected(
        if negated {
            "NULL, TRUE, or FALSE after IS NOT"
        } else {
            "NULL, TRUE, or FALSE after IS"
        },
        cursor.peek_kind(),
    ))
}

fn plain_bool_is_predicate(field: String, value: bool, negated: bool) -> Predicate {
    let predicate = predicate_compare(field, CompareOp::Eq, Value::Bool(value));

    if negated {
        Predicate::not(predicate)
    } else {
        predicate
    }
}
