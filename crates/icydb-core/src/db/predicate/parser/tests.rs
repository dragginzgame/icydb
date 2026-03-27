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

#[test]
fn parse_sql_predicate_like_prefix_lowering_respects_operand_text_mode() {
    let plain = parse_sql_predicate("name LIKE 'Al%'").expect("plain LIKE prefix should parse");
    let lower =
        parse_sql_predicate("LOWER(name) LIKE 'Al%'").expect("LOWER(field) LIKE should parse");
    let upper =
        parse_sql_predicate("UPPER(name) LIKE 'AL%'").expect("UPPER(field) LIKE should parse");

    assert_eq!(
        plain,
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        ))
    );
    assert_eq!(
        lower,
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::TextCasefold,
        ))
    );
    assert_eq!(
        upper,
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        ))
    );
}
