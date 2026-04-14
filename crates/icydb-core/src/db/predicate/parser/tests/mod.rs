//! Module: db::predicate::parser::tests
//! Covers standalone predicate parsing behavior and parse-error boundaries.
//! Does not own: SQL statement parsing or lowering.
//! Boundary: verifies the predicate-owned reduced SQL parser contract.

use crate::{
    db::{
        predicate::{
            CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate,
            parse_sql_predicate,
        },
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
fn parse_sql_predicate_not_equal_angle_brackets_lowers_to_ne() {
    let predicate = parse_sql_predicate("active <> true").expect("predicate-only <> should parse");

    assert_eq!(
        predicate,
        Predicate::Compare(ComparePredicate::with_coercion(
            "active",
            CompareOp::Ne,
            Value::Bool(true),
            CoercionId::Strict,
        )),
    );
}

#[test]
fn parse_sql_predicate_in_and_not_in_allow_one_trailing_comma() {
    let in_predicate =
        parse_sql_predicate("age IN (10, 20, 30,)").expect("IN with trailing comma should parse");
    let not_in_predicate = parse_sql_predicate("age NOT IN (10, 20, 30,)")
        .expect("NOT IN with trailing comma should parse");

    assert_eq!(
        in_predicate,
        Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::In,
            Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
            CoercionId::Strict,
        )),
    );
    assert_eq!(
        not_in_predicate,
        Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::NotIn,
            Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]),
            CoercionId::Strict,
        )),
    );
}

#[test]
fn parse_sql_predicate_is_true_and_is_false_lower_to_strict_bool_equality() {
    let is_true = parse_sql_predicate("active IS TRUE").expect("IS TRUE predicate should parse");
    let is_false = parse_sql_predicate("active IS FALSE").expect("IS FALSE predicate should parse");

    assert_eq!(
        is_true,
        Predicate::Compare(ComparePredicate::with_coercion(
            "active",
            CompareOp::Eq,
            Value::Bool(true),
            CoercionId::Strict,
        )),
    );
    assert_eq!(
        is_false,
        Predicate::Compare(ComparePredicate::with_coercion(
            "active",
            CompareOp::Eq,
            Value::Bool(false),
            CoercionId::Strict,
        )),
    );
}

#[test]
fn parse_sql_predicate_is_not_true_and_is_not_false_lower_to_negated_bool_equality() {
    let is_not_true =
        parse_sql_predicate("active IS NOT TRUE").expect("IS NOT TRUE predicate should parse");
    let is_not_false =
        parse_sql_predicate("active IS NOT FALSE").expect("IS NOT FALSE predicate should parse");

    assert_eq!(
        is_not_true,
        Predicate::Not(Box::new(Predicate::Compare(
            ComparePredicate::with_coercion(
                "active",
                CompareOp::Eq,
                Value::Bool(true),
                CoercionId::Strict,
            ),
        ))),
    );
    assert_eq!(
        is_not_false,
        Predicate::Not(Box::new(Predicate::Compare(
            ComparePredicate::with_coercion(
                "active",
                CompareOp::Eq,
                Value::Bool(false),
                CoercionId::Strict,
            ),
        ))),
    );
}

#[test]
fn parse_sql_predicate_rejects_empty_or_double_comma_in_lists() {
    for sql in ["age IN ()", "age IN (10,, 20)", "age NOT IN (10,, 20)"] {
        let err = parse_sql_predicate(sql).expect_err("invalid list shape should stay rejected");

        assert!(matches!(err, SqlParseError::InvalidSyntax { .. }));
    }
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
fn parse_sql_predicate_not_like_prefix_lowering_respects_operand_text_mode() {
    let plain =
        parse_sql_predicate("name NOT LIKE 'Al%'").expect("plain NOT LIKE prefix should parse");
    let lower = parse_sql_predicate("LOWER(name) NOT LIKE 'Al%'")
        .expect("LOWER(field) NOT LIKE should parse");
    let upper = parse_sql_predicate("UPPER(name) NOT LIKE 'AL%'")
        .expect("UPPER(field) NOT LIKE should parse");

    assert_eq!(
        plain,
        Predicate::Not(Box::new(Predicate::Compare(
            ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::Strict,
            )
        )))
    );
    assert_eq!(
        lower,
        Predicate::Not(Box::new(Predicate::Compare(
            ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            )
        )))
    );
    assert_eq!(
        upper,
        Predicate::Not(Box::new(Predicate::Compare(
            ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("AL".to_string()),
                CoercionId::TextCasefold,
            )
        )))
    );
}

#[test]
fn parse_sql_predicate_ilike_prefix_lowering_stays_casefolded() {
    let plain = parse_sql_predicate("name ILIKE 'al%'").expect("plain ILIKE prefix should parse");
    let lower =
        parse_sql_predicate("LOWER(name) ILIKE 'al%'").expect("LOWER(field) ILIKE should parse");
    let upper =
        parse_sql_predicate("UPPER(name) ILIKE 'AL%'").expect("UPPER(field) ILIKE should parse");

    let expected_plain = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("al".to_string()),
        CoercionId::TextCasefold,
    ));
    let expected_upper = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("AL".to_string()),
        CoercionId::TextCasefold,
    ));

    assert_eq!(plain, expected_plain);
    assert_eq!(lower, expected_plain);
    assert_eq!(upper, expected_upper);
}

#[test]
fn parse_sql_predicate_not_ilike_prefix_lowering_stays_casefolded() {
    let plain =
        parse_sql_predicate("name NOT ILIKE 'al%'").expect("plain NOT ILIKE prefix should parse");
    let lower = parse_sql_predicate("LOWER(name) NOT ILIKE 'al%'")
        .expect("LOWER(field) NOT ILIKE should parse");
    let upper = parse_sql_predicate("UPPER(name) NOT ILIKE 'AL%'")
        .expect("UPPER(field) NOT ILIKE should parse");

    let expected_plain = Predicate::Not(Box::new(Predicate::Compare(
        ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("al".to_string()),
            CoercionId::TextCasefold,
        ),
    )));
    let expected_upper = Predicate::Not(Box::new(Predicate::Compare(
        ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        ),
    )));

    assert_eq!(plain, expected_plain);
    assert_eq!(lower, expected_plain);
    assert_eq!(upper, expected_upper);
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
fn parse_sql_predicate_not_between_lowers_to_outside_range_disjunction() {
    let predicate =
        parse_sql_predicate("age NOT BETWEEN 10 AND 20").expect("NOT BETWEEN should parse");

    assert_eq!(
        predicate,
        Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Lt,
                Value::Int(10),
                CoercionId::NumericWiden,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gt,
                Value::Int(20),
                CoercionId::NumericWiden,
            )),
        ]),
    );
}

#[test]
fn parse_sql_predicate_field_bound_between_and_not_between_lower_to_compare_fields() {
    let between = parse_sql_predicate("age BETWEEN min_age AND max_age")
        .expect("field-bound BETWEEN should parse");
    let not_between = parse_sql_predicate("age NOT BETWEEN min_age AND max_age")
        .expect("field-bound NOT BETWEEN should parse");

    assert_eq!(
        between,
        Predicate::And(vec![
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                "age",
                CompareOp::Gte,
                "min_age",
                CoercionId::NumericWiden,
            )),
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                "age",
                CompareOp::Lte,
                "max_age",
                CoercionId::NumericWiden,
            )),
        ]),
    );
    assert_eq!(
        not_between,
        Predicate::Or(vec![
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                "age",
                CompareOp::Lt,
                "min_age",
                CoercionId::NumericWiden,
            )),
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                "age",
                CompareOp::Gt,
                "max_age",
                CoercionId::NumericWiden,
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

#[test]
fn parse_sql_predicate_parses_field_to_field_compare_leaves() {
    let predicate = parse_sql_predicate("age > rank AND name = label")
        .expect("field-to-field predicate leaves should parse");

    assert_eq!(
        predicate,
        Predicate::And(vec![
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                "age",
                CompareOp::Gt,
                "rank",
                CoercionId::NumericWiden,
            )),
            Predicate::eq_fields("name".to_string(), "label".to_string()),
        ]),
    );
}

#[test]
fn parse_sql_predicate_normalizes_literal_leading_compare_to_field_first() {
    let predicate = parse_sql_predicate("5 < age").expect("literal-leading compare should parse");

    assert_eq!(
        predicate,
        Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gt,
            Value::Int(5),
            CoercionId::NumericWiden,
        )),
    );
}

#[test]
fn parse_sql_predicate_normalizes_swapped_field_equality_to_deterministic_order() {
    let predicate =
        parse_sql_predicate("dexterity = strength").expect("swapped field equality should parse");

    assert_eq!(
        predicate,
        Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "strength",
            CompareOp::Eq,
            "dexterity",
            CoercionId::Strict,
        )),
    );
}
