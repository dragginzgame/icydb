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
        sql_shared::{MAX_SQL_EXPR_DEPTH, SqlParseError, SqlSyntaxErrorKind},
    },
    value::Value,
};
use icydb_diagnostic_code::SqlFeatureCode;

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
                Value::Int64(21),
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
            Value::List(vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)]),
            CoercionId::Strict,
        )),
    );
    assert_eq!(
        not_in_predicate,
        Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::NotIn,
            Value::List(vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)]),
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

        std::assert_matches!(err, SqlParseError::InvalidSyntax { .. });
    }
}

#[test]
fn parse_sql_predicate_rejects_trailing_unsupported_clause() {
    let err = parse_sql_predicate("active = true ORDER BY age")
        .expect_err("predicate parser should reject trailing unsupported clauses");

    std::assert_matches!(err, SqlParseError::InvalidSyntax { .. });
}

#[test]
fn parse_sql_predicate_like_prefix_lowering_respects_operand_text_mode() {
    assert_prefix_text_predicate("name LIKE 'Al%'", "Al", CoercionId::Strict, false);
    assert_prefix_text_predicate(
        "LOWER(name) LIKE 'Al%'",
        "Al",
        CoercionId::TextCasefold,
        false,
    );
}

#[test]
fn parse_sql_predicate_not_like_prefix_lowering_respects_operand_text_mode() {
    assert_prefix_text_predicate("name NOT LIKE 'Al%'", "Al", CoercionId::Strict, true);
    assert_prefix_text_predicate(
        "LOWER(name) NOT LIKE 'Al%'",
        "Al",
        CoercionId::TextCasefold,
        true,
    );
}

#[test]
fn parse_sql_predicate_ilike_prefix_lowering_stays_casefolded() {
    assert_prefix_text_predicate("name ILIKE 'al%'", "al", CoercionId::TextCasefold, false);
    assert_prefix_text_predicate(
        "LOWER(name) ILIKE 'al%'",
        "al",
        CoercionId::TextCasefold,
        false,
    );
}

#[test]
fn parse_sql_predicate_not_ilike_prefix_lowering_stays_casefolded() {
    assert_prefix_text_predicate("name NOT ILIKE 'al%'", "al", CoercionId::TextCasefold, true);
    assert_prefix_text_predicate(
        "LOWER(name) NOT ILIKE 'al%'",
        "al",
        CoercionId::TextCasefold,
        true,
    );
}

#[test]
fn parse_sql_predicate_rejects_upper_wrappers_without_reinterpreting_them_as_casefold() {
    for sql in [
        "UPPER(name) LIKE 'AL%'",
        "UPPER(name) NOT LIKE 'AL%'",
        "UPPER(name) ILIKE 'AL%'",
        "UPPER(name) NOT ILIKE 'AL%'",
        "UPPER(name) >= 'AL'",
        "'AL' <= UPPER(name)",
        "STARTS_WITH(UPPER(name), 'AL')",
    ] {
        assert_eq!(
            parse_sql_predicate(sql),
            Err(SqlParseError::UnsupportedFeature {
                feature: SqlFeatureCode::UpperFieldPredicateUnsupported,
            }),
            "{sql}",
        );
    }
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
                Value::Int64(10),
                CoercionId::NumericWiden,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gt,
                Value::Int64(20),
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
fn parse_sql_predicate_lower_ordered_text_compares_lower_to_text_casefold() {
    let lower = parse_sql_predicate("LOWER(name) >= 'Al' AND LOWER(name) < 'Am'")
        .expect("LOWER(field) ordered text range should parse");

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
}

#[test]
fn parse_sql_predicate_wrapped_equality_remains_fail_closed() {
    let err = parse_sql_predicate("LOWER(name) = 'Al'").expect_err(
        "wrapped equality should stay outside the reduced SQL expression predicate slice",
    );

    assert_eq!(
        err,
        SqlParseError::UnsupportedFeature {
            feature: SqlFeatureCode::LowerFieldPredicateUnsupported,
        }
    );
}

#[test]
fn parse_sql_predicate_direct_starts_with_lowers_to_strict_starts_with_intent() {
    assert_prefix_text_predicate("STARTS_WITH(name, 'Al')", "Al", CoercionId::Strict, false);
}

#[test]
fn parse_sql_predicate_direct_lower_starts_with_lowers_to_casefold_intent() {
    assert_prefix_text_predicate(
        "STARTS_WITH(LOWER(name), 'Al')",
        "Al",
        CoercionId::TextCasefold,
        false,
    );
}

#[test]
fn parse_sql_predicate_direct_starts_with_rejects_non_casefold_wrapper_argument() {
    let err = parse_sql_predicate("STARTS_WITH(TRIM(name), 'Al')")
        .expect_err("non-casefold direct STARTS_WITH first argument should stay fail-closed");

    assert_eq!(
        err,
        super::SqlParseError::UnsupportedFeature {
            feature: SqlFeatureCode::PredicateStartsWithFirstArgument,
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
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                "name",
                CompareOp::Eq,
                "label",
                CoercionId::Strict,
            )),
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
            Value::Int64(5),
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

#[test]
fn parse_sql_predicate_rejects_excessive_not_depth() {
    let mut sql = String::new();
    for _ in 0..140 {
        sql.push_str("NOT ");
    }
    sql.push_str("active = true");

    let err = parse_sql_predicate(sql.as_str()).expect_err("deep NOT predicates should reject");

    assert_eq!(
        err,
        SqlParseError::InvalidSyntax {
            kind: SqlSyntaxErrorKind::ExpressionDepthLimit {
                max_depth: MAX_SQL_EXPR_DEPTH
            },
        }
    );
}

#[test]
fn parse_sql_predicate_builds_flat_authored_boolean_chains() {
    for conjunction in [false, true] {
        for width in [2, 3, 16, MAX_SQL_EXPR_DEPTH] {
            let terms = (0..width)
                .map(|i| format!("field_{i} = 'value_{i}'"))
                .collect::<Vec<_>>();
            let leaves = terms
                .iter()
                .map(|term| parse_sql_predicate(term).unwrap())
                .collect();
            let expected = if conjunction {
                Predicate::And(leaves)
            } else {
                Predicate::Or(leaves)
            };
            let parsed =
                parse_sql_predicate(&terms.join(if conjunction { " AND " } else { " OR " }))
                    .unwrap();
            assert_eq!(parsed, expected);
        }
    }
}

#[test]
fn parse_sql_predicate_preserves_precedence_and_explicit_groups() {
    let leaf = |field: &str| parse_sql_predicate(&format!("{field} = true")).unwrap();
    assert_eq!(
        parse_sql_predicate(
            "a = true OR b = true AND c = true AND d = true OR NOT (e = true OR f = true)"
        )
        .unwrap(),
        Predicate::Or(vec![
            leaf("a"),
            Predicate::And(vec![leaf("b"), leaf("c"), leaf("d")]),
            Predicate::Not(Box::new(Predicate::Or(vec![leaf("e"), leaf("f")]))),
        ]),
    );
    for (sql, expected) in [
        (
            "(a = true OR b = true) OR c = true",
            Predicate::Or(vec![Predicate::Or(vec![leaf("a"), leaf("b")]), leaf("c")]),
        ),
        (
            "a = true AND (b = true AND c = true)",
            Predicate::And(vec![leaf("a"), Predicate::And(vec![leaf("b"), leaf("c")])]),
        ),
    ] {
        assert_eq!(parse_sql_predicate(sql).unwrap(), expected);
    }
}

#[test]
fn parse_sql_predicate_keeps_source_admission_independent_of_flat_output() {
    let depth_error = SqlParseError::InvalidSyntax {
        kind: SqlSyntaxErrorKind::ExpressionDepthLimit {
            max_depth: MAX_SQL_EXPR_DEPTH,
        },
    };
    for operator in [" AND ", " OR "] {
        let at_limit = vec!["active = true"; MAX_SQL_EXPR_DEPTH].join(operator);
        assert!(parse_sql_predicate(&at_limit).is_ok());
        for sql in [
            format!("{at_limit}{operator}active = true"),
            format!("NOT ({at_limit})"),
            format!("active = true{operator}({at_limit})"),
        ] {
            assert_eq!(parse_sql_predicate(&sql).unwrap_err(), depth_error);
        }
    }
    for depth in [MAX_SQL_EXPR_DEPTH - 1, MAX_SQL_EXPR_DEPTH] {
        let sql = format!("{}active = true{}", "(".repeat(depth), ")".repeat(depth));
        if depth < MAX_SQL_EXPR_DEPTH {
            assert!(parse_sql_predicate(&sql).is_ok());
        } else {
            assert_eq!(parse_sql_predicate(&sql).unwrap_err(), depth_error);
        }
    }
    for sql in [
        "active = true AND",
        "active = true OR",
        "active = true AND OR active = false",
    ] {
        assert!(parse_sql_predicate(sql).is_err());
    }
}

#[test]
fn parse_sql_predicate_flat_or_has_the_membership_canonical_identity() {
    use crate::db::{
        executor::budget::MaintenanceConstructionBudget,
        predicate::{normalize, predicate_fingerprint_normalized},
    };

    let disjunction = normalize(
        parse_sql_predicate("name = 'Ada' OR name = 'Grace' OR name = 'Lin' OR name = 'Ada'")
            .unwrap(),
    );
    let membership = normalize(parse_sql_predicate("name IN ('Ada', 'Grace', 'Lin')").unwrap());
    assert_eq!(disjunction, membership);
    assert_eq!(normalize(disjunction.clone()), disjunction);
    let work = MaintenanceConstructionBudget::new();
    assert_eq!(
        predicate_fingerprint_normalized(&disjunction, &work).unwrap(),
        predicate_fingerprint_normalized(&membership, &work).unwrap(),
    );
}

fn assert_prefix_text_predicate(sql: &str, prefix: &str, coercion: CoercionId, negated: bool) {
    let predicate = parse_sql_predicate(sql).expect("prefix predicate should parse");

    assert_eq!(
        predicate,
        expected_prefix_text_predicate(prefix, coercion, negated),
    );
}

fn expected_prefix_text_predicate(prefix: &str, coercion: CoercionId, negated: bool) -> Predicate {
    let compare = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text(prefix.to_string()),
        coercion,
    ));

    if negated {
        Predicate::Not(Box::new(compare))
    } else {
        compare
    }
}
