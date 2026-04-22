use crate::{
    db::{
        predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
        sql_shared::{Keyword, SqlParseError, SqlTokenCursor, TokenKind},
    },
    value::Value,
};

///
/// BetweenBound
///
/// Tracks one bounded `BETWEEN` endpoint so the parser can lower range syntax
/// onto compare predicates without widening the accepted surface to expressions.
///
enum BetweenBound {
    Literal(Value),
    Field(String),
}

// Parse one IN / NOT IN list predicate into one canonical predicate compare.
pub(in crate::db::predicate::parser) fn parse_in_predicate(
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
pub(in crate::db::predicate::parser) fn parse_between_predicate(
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

// Parse one BETWEEN endpoint without widening into generic expression bounds.
fn parse_between_bound(cursor: &mut SqlTokenCursor) -> Result<BetweenBound, SqlParseError> {
    if matches!(cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
        return cursor.expect_identifier().map(BetweenBound::Field);
    }

    cursor.parse_literal().map(BetweenBound::Literal)
}

// Build one compare predicate and assign the parser's coercion policy for
// ordered numeric and ordered text comparisons.
pub(in crate::db::predicate::parser) fn predicate_compare(
    field: String,
    op: CompareOp,
    value: Value,
) -> Predicate {
    let coercion = if op.is_ordering_family() {
        if matches!(value, Value::Text(_)) {
            CoercionId::Strict
        } else {
            CoercionId::NumericWiden
        }
    } else {
        CoercionId::Strict
    };

    predicate_compare_with_coercion(field, op, value, coercion)
}

// Build one compare predicate after the parser has already selected the
// canonical coercion contract for this reduced-SQL spelling.
pub(in crate::db::predicate::parser) fn predicate_compare_with_coercion(
    field: String,
    op: CompareOp,
    value: Value,
    coercion: CoercionId,
) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(field, op, value, coercion))
}

// Build one field-to-field compare predicate and keep the parser's widening
// policy aligned with the scalar compare lowering path.
pub(in crate::db::predicate::parser) fn predicate_compare_fields(
    left_field: String,
    op: CompareOp,
    right_field: String,
) -> Predicate {
    let coercion = if op.is_ordering_family() {
        CoercionId::NumericWiden
    } else {
        CoercionId::Strict
    };

    Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
        left_field,
        op,
        right_field,
        coercion,
    ))
}

// Lower one parsed BETWEEN endpoint back onto the shared compare helpers so
// literal and field-bound ranges stay on the same canonical predicate seam.
fn predicate_between_bound(field: String, op: CompareOp, bound: BetweenBound) -> Predicate {
    match bound {
        BetweenBound::Literal(value) => predicate_compare(field, op, value),
        BetweenBound::Field(other_field) => predicate_compare_fields(field, op, other_field),
    }
}
