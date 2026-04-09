//! Module: db::predicate::parser::tests
//! Responsibility: module-local ownership and contracts for standalone predicate parsing.
//! Does not own: SQL statement parsing or lowering.
//! Boundary: verifies the predicate-owned reduced SQL parser contract.

use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, parse_sql_predicate},
        reduced_sql::SqlParseError,
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

#[test]
fn parse_sql_predicate_ordered_text_compares_stay_strict() {
    let predicate =
        parse_sql_predicate("name >= 'Al' AND name < 'Am'").expect("text range should parse");

    assert_eq!(
        predicate,
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Gte,
                Value::Text("Al".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Lt,
                Value::Text("Am".to_string()),
                CoercionId::Strict,
            )),
        ]),
    );
}

#[test]
fn parse_sql_predicate_wrapped_ordered_text_compares_lower_to_text_casefold() {
    let lower = parse_sql_predicate("LOWER(name) >= 'Al' AND LOWER(name) < 'Am'")
        .expect("LOWER(field) ordered text range should parse");
    let upper = parse_sql_predicate("UPPER(name) >= 'AL' AND UPPER(name) < 'AM'")
        .expect("UPPER(field) ordered text range should parse");

    assert_eq!(
        lower,
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Gte,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Lt,
                Value::Text("Am".to_string()),
                CoercionId::TextCasefold,
            )),
        ]),
    );
    assert_eq!(
        upper,
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Gte,
                Value::Text("AL".to_string()),
                CoercionId::TextCasefold,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Lt,
                Value::Text("AM".to_string()),
                CoercionId::TextCasefold,
            )),
        ]),
    );
}

#[test]
fn parse_sql_predicate_wrapped_equality_remains_fail_closed() {
    let err = parse_sql_predicate("LOWER(name) = 'Al'").expect_err(
        "wrapped equality should stay outside the reduced SQL expression predicate slice",
    );

    assert_eq!(
        err,
        SqlParseError::UnsupportedFeature {
            feature: "LOWER(field) predicate forms beyond LIKE 'prefix%' or ordered text bounds",
        }
    );
}

#[test]
fn parse_sql_predicate_direct_starts_with_lowers_to_strict_starts_with_intent() {
    let predicate = parse_sql_predicate("STARTS_WITH(name, 'Al')")
        .expect("direct STARTS_WITH predicate should parse");

    assert_eq!(
        predicate,
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        ))
    );
}

#[test]
fn parse_sql_predicate_direct_wrapped_starts_with_lowers_to_casefold_intent() {
    let lower = parse_sql_predicate("STARTS_WITH(LOWER(name), 'Al')")
        .expect("direct LOWER(field) STARTS_WITH predicate should parse");
    let upper = parse_sql_predicate("STARTS_WITH(UPPER(name), 'AL')")
        .expect("direct UPPER(field) STARTS_WITH predicate should parse");

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

#[test]
fn parse_sql_predicate_direct_starts_with_rejects_non_casefold_wrapper_argument() {
    let err = parse_sql_predicate("STARTS_WITH(TRIM(name), 'Al')")
        .expect_err("non-casefold direct STARTS_WITH first argument should stay fail-closed");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
        }
    );
}
