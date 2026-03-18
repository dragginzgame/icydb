//! Module: db::predicate::parser::tests
//! Responsibility: module-local ownership and contracts for standalone predicate parsing.
//! Does not own: SQL statement parsing or lowering.
//! Boundary: verifies the predicate-owned reduced SQL parser contract.

use crate::{
    db::predicate::{
        CoercionId, CompareOp, ComparePredicate, Predicate, SqlParseError, parse_sql_predicate,
    },
    value::Value,
};

#[test]
fn parse_sql_predicate_parses_expression_without_statement_wrapper() {
    let predicate = parse_sql_predicate("active = true AND age >= 21")
        .expect("predicate-only SQL should parse");

    assert_eq!(
        predicate,
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "active",
                CompareOp::Eq,
                Value::Bool(true),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gte,
                Value::Int(21),
                CoercionId::NumericWiden,
            )),
        ]),
    );
}

#[test]
fn parse_sql_predicate_rejects_trailing_unsupported_clause() {
    let err = parse_sql_predicate("active = true ORDER BY age")
        .expect_err("predicate parser should reject trailing unsupported clauses");

    assert!(matches!(err, SqlParseError::InvalidSyntax { .. }));
}
