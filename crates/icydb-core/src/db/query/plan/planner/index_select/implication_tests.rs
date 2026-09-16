//! Conservative implication and borrowed short-circuit traversal contracts.

use super::{
    CompareClauseMode, ComparisonRef, ImplicationClause, access_bound_lower_range_clause,
    access_bound_text_prefix_range_implies_required, access_bound_upper_range_clause,
    predicate_implies_clause_for_planner, predicate_implies_predicate_for_planner,
    strip_query_clauses_satisfied_by_filtered_guard, visit_implication_clauses,
};
use crate::db::query::preparation::with_preparation_work;
use crate::{
    db::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
    retained::RetainedBytes,
    value::Value,
};
use std::ops::{Bound, ControlFlow};

#[test]
fn text_prefix_proof_preserves_range_endpoints_and_rejects_unrelated_bounds() {
    let a = Value::Text("a".into());
    let aa = Value::Text("aa".into());
    let b = Value::Text("b".into());
    let before = Value::Text("0".into());
    let wrong_type = Value::Nat64(1);
    let predicate = ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        a.clone(),
        CoercionId::Strict,
    );
    for (field, lower, upper, expected) in [
        (
            "name",
            Some((CompareOp::Gte, &a)),
            Some((CompareOp::Lt, &b)),
            true,
        ),
        (
            "name",
            Some((CompareOp::Gt, &a)),
            Some((CompareOp::Lt, &b)),
            true,
        ),
        (
            "name",
            Some((CompareOp::Gte, &aa)),
            Some((CompareOp::Lte, &aa)),
            true,
        ),
        (
            "name",
            Some((CompareOp::Gte, &a)),
            Some((CompareOp::Lte, &b)),
            false,
        ),
        (
            "name",
            Some((CompareOp::Gt, &before)),
            Some((CompareOp::Lt, &b)),
            false,
        ),
        ("name", None, Some((CompareOp::Lt, &b)), false),
        ("name", Some((CompareOp::Gte, &a)), None, false),
        ("name", None, None, false),
        (
            "other",
            Some((CompareOp::Gte, &a)),
            Some((CompareOp::Lt, &b)),
            false,
        ),
        (
            "name",
            Some((CompareOp::Gte, &wrong_type)),
            Some((CompareOp::Lt, &b)),
            false,
        ),
    ] {
        let ranges = [lower, upper]
            .map(|bound| bound.map(|(op, value)| ComparisonRef::strict(field, op, value)));
        assert_eq!(
            with_preparation_work(|budget| {
                access_bound_text_prefix_range_implies_required(&ranges, &predicate, budget)
            })
            .unwrap(),
            expected,
            "field={field}, lower={lower:?}, upper={upper:?}",
        );
    }

    let ranges = [
        Some(ComparisonRef::strict("name", CompareOp::Gte, &a)),
        Some(ComparisonRef::strict("name", CompareOp::Lt, &b)),
    ];
    for (op, value, coercion) in [
        (
            CompareOp::StartsWith,
            Value::Text(String::new()),
            CoercionId::Strict,
        ),
        (CompareOp::StartsWith, a.clone(), CoercionId::TextCasefold),
        (CompareOp::StartsWith, wrong_type, CoercionId::Strict),
        (CompareOp::EndsWith, a.clone(), CoercionId::Strict),
    ] {
        let rejected = ComparePredicate::with_coercion("name", op, value, coercion);
        assert!(
            !with_preparation_work(|budget| {
                access_bound_text_prefix_range_implies_required(&ranges, &rejected, budget)
            })
            .unwrap()
        );
    }
    assert_eq!(predicate.value(), &a);
}

#[test]
fn text_prefix_proof_matches_unicode_component_bounds() {
    use crate::db::index::{TextPrefixBoundMode, starts_with_component_bounds};

    for (prefix, successor) in [
        ("a", Some("b")),
        ("a\0b", Some("a\0c")),
        ("λ", Some("μ")),
        ("\u{7f}", Some("\u{80}")),
        ("\u{7ff}", Some("\u{800}")),
        ("\u{d7ff}", Some("\u{e000}")),
        ("\u{ffff}", Some("\u{10000}")),
        ("a\u{10ffff}", Some("b")),
        ("\u{10ffff}\u{10ffff}", None),
    ] {
        let predicate = ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text(prefix.into()),
            CoercionId::Strict,
        );
        let (lower, upper) =
            starts_with_component_bounds(prefix, TextPrefixBoundMode::Strict).unwrap();
        assert_eq!(
            upper,
            successor.map_or(Bound::Unbounded, |text| Bound::Excluded(Value::Text(
                text.into()
            )))
        );
        let lower = access_bound_lower_range_clause("name", &lower);
        let upper = access_bound_upper_range_clause("name", &upper);
        assert!(
            with_preparation_work(|budget| {
                access_bound_text_prefix_range_implies_required(&[lower, upper], &predicate, budget)
            })
            .unwrap()
        );
        assert_eq!(
            with_preparation_work(|budget| {
                access_bound_text_prefix_range_implies_required(&[lower, None], &predicate, budget)
            })
            .unwrap(),
            successor.is_none(),
        );
        assert!(
            !with_preparation_work(|budget| {
                access_bound_text_prefix_range_implies_required(&[None, upper], &predicate, budget)
            })
            .unwrap()
        );
    }
}

fn equal(field: &str, value: u64) -> Predicate {
    Predicate::eq(field.into(), Value::Nat64(value))
}

#[test]
fn flat_reduced_or_guard_keeps_membership_proofs_conservative() {
    use crate::db::predicate::{normalize, parse_sql_predicate};

    let guard =
        normalize(parse_sql_predicate("name = 'Ada' OR name = 'Grace' OR name = 'Lin'").unwrap());
    let membership = normalize(parse_sql_predicate("name IN ('Ada', 'Grace', 'Lin')").unwrap());
    assert_eq!(guard, membership);
    // Required IN guards are intentionally unsupported by this proof owner.
    // The parser hard cut must not silently broaden index eligibility.
    for sql in [
        "name = 'Ada'",
        "name = 'Grace'",
        "name = 'Lin'",
        "name = 'Other'",
        "name IN ('Ada', 'Lin')",
        "name IN ('Ada', 'Other')",
    ] {
        let query = normalize(parse_sql_predicate(sql).unwrap());
        assert_eq!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&query, &guard, budget)
            })
            .unwrap(),
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&query, &membership, budget)
            })
            .unwrap(),
            "{sql}",
        );
        assert!(
            !with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&query, &guard, budget)
            })
            .unwrap()
        );
    }
    let non_null = Predicate::IsNotNull {
        field: "name".into(),
    };
    assert!(
        with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(&guard, &non_null, budget)
        })
        .unwrap()
    );
}

#[test]
fn implication_classification_preserves_unknown_and_contradictory_boundaries() {
    let known = equal("id", 7);
    let unknown = Predicate::IsNull { field: "id".into() };
    let nested_false = Predicate::And(vec![Predicate::False]);
    let cases = [
        (Predicate::True, Predicate::True, true),
        (Predicate::True, Predicate::False, false),
        (Predicate::False, Predicate::False, true),
        (Predicate::False, known.clone(), true),
        (Predicate::False, unknown.clone(), false),
        (Predicate::False, nested_false, false),
        (unknown.clone(), Predicate::True, false),
        (Predicate::And(vec![unknown.clone()]), Predicate::True, true),
        (Predicate::And(vec![]), Predicate::True, true),
        (Predicate::Or(vec![]), unknown.clone(), true),
        (
            Predicate::And(vec![unknown.clone(), Predicate::False]),
            known.clone(),
            true,
        ),
        (
            Predicate::And(vec![Predicate::False, unknown.clone()]),
            known.clone(),
            true,
        ),
        (
            Predicate::Or(vec![Predicate::False, known.clone()]),
            known.clone(),
            true,
        ),
        (
            Predicate::Or(vec![known.clone(), unknown.clone()]),
            known.clone(),
            false,
        ),
        (
            Predicate::And(vec![known.clone(), unknown.clone()]),
            known.clone(),
            true,
        ),
        (
            known.clone(),
            Predicate::And(vec![known.clone(), unknown]),
            false,
        ),
        (
            Predicate::And(vec![Predicate::Or(vec![known.clone()])]),
            known,
            false,
        ),
    ];
    for (query, required, expected) in cases {
        assert_eq!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&query, &required, budget)
            })
            .unwrap(),
            expected,
            "query={query:?}, required={required:?}"
        );
    }
}

#[test]
fn disjunctive_proofs_preserve_shared_requirement_and_branch_boundaries() {
    let required = Predicate::And(
        (0..8)
            .map(|slot| {
                Predicate::Compare(ComparePredicate::with_coercion(
                    format!("field_{slot}"),
                    CompareOp::Gte,
                    Value::Nat64(7),
                    CoercionId::Strict,
                ))
            })
            .collect(),
    );
    let branch = Predicate::And(
        (0..8)
            .map(|slot| equal(&format!("field_{slot}"), 8))
            .collect(),
    );
    let matching = Predicate::Or(vec![Predicate::Or(vec![branch.clone(); 16]); 2]);
    assert!(
        with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(&matching, &required, budget)
        })
        .unwrap()
    );
    for branches in [
        vec![equal("field_0", 6), matching.clone()],
        vec![matching, equal("field_0", 6)],
    ] {
        assert!(
            !with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&Predicate::Or(branches), &required, budget)
            })
            .unwrap()
        );
    }

    let empty = Predicate::Or(vec![]);
    let vacuous = Predicate::Or(vec![empty.clone(), Predicate::Or(vec![empty.clone()])]);
    let unsupported = Predicate::IsNull {
        field: "field_0".into(),
    };
    for requirement in [
        required,
        Predicate::False,
        unsupported.clone(),
        Predicate::And(vec![Predicate::False]),
        Predicate::And(vec![branch.clone(), unsupported]),
    ] {
        assert!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&vacuous, &requirement, budget)
            })
            .unwrap()
        );
        // FALSE cannot rescue an unsupported requirement; empty OR can still
        // be skipped when another branch must provide a real proof.
        let mixed = Predicate::Or(vec![vacuous.clone(), Predicate::False]);
        assert_eq!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&mixed, &requirement, budget)
            })
            .unwrap(),
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&Predicate::False, &requirement, budget)
            })
            .unwrap(),
        );
    }
    assert!(
        with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(
                &Predicate::Or(vec![empty, branch.clone()]),
                &branch,
                budget,
            )
        })
        .unwrap()
    );
}

#[test]
fn borrowed_single_requirements_match_full_predicate_proofs() {
    let compares = [
        ComparePredicate::with_coercion("id", CompareOp::Eq, Value::Nat64(7), CoercionId::Strict),
        ComparePredicate::with_coercion(
            "id",
            CompareOp::Gte,
            Value::Int64(7),
            CoercionId::NumericWiden,
        ),
    ];
    let queries = [
        Predicate::True,
        Predicate::False,
        equal("id", 7),
        equal("other", 7),
        Predicate::IsNotNull { field: "id".into() },
        Predicate::in_("id".into(), vec![Value::Nat64(7), Value::Nat64(8)]),
        Predicate::And(vec![equal("id", 7), Predicate::False]),
        Predicate::Or(vec![equal("id", 7), equal("id", 8)]),
    ];
    for query in queries {
        for compare in &compares {
            assert_eq!(
                with_preparation_work(|budget| {
                    predicate_implies_clause_for_planner(
                        &query,
                        ImplicationClause::Compare(compare),
                        budget,
                    )
                })
                .unwrap(),
                with_preparation_work(|budget| {
                    predicate_implies_predicate_for_planner(
                        &query,
                        &Predicate::Compare(compare.clone()),
                        budget,
                    )
                })
                .unwrap(),
            );
        }
        for field in ["id", "other"] {
            assert_eq!(
                with_preparation_work(|budget| {
                    predicate_implies_clause_for_planner(
                        &query,
                        ImplicationClause::NonNull(field),
                        budget,
                    )
                })
                .unwrap(),
                with_preparation_work(|budget| {
                    predicate_implies_predicate_for_planner(
                        &query,
                        &Predicate::IsNotNull {
                            field: field.into(),
                        },
                        budget,
                    )
                })
                .unwrap(),
            );
        }
    }
}

#[test]
fn filtered_guard_pruning_keeps_stricter_and_unsupported_clauses() {
    let guard = Predicate::Or(vec![equal("id", 7), equal("id", 8)]);
    let non_null = Predicate::IsNotNull { field: "id".into() };
    let stricter = equal("id", 7);
    let opaque = Predicate::Or(vec![non_null.clone(), equal("other", 9)]);
    let query = Predicate::And(vec![
        non_null.clone(),
        Predicate::And(vec![Predicate::True, stricter.clone()]),
        non_null,
        opaque.clone(),
    ]);
    let before = query.clone();
    for _ in 0..3 {
        assert_eq!(
            with_preparation_work(|budget| {
                strip_query_clauses_satisfied_by_filtered_guard(query.clone(), &guard, budget)
            })
            .unwrap(),
            Some(Predicate::And(vec![stricter.clone(), opaque.clone()])),
        );
    }
    assert_eq!(query, before);
}

#[test]
fn implication_visitor_borrows_clauses_and_stops_before_later_siblings() {
    let query = Predicate::And(vec![
        equal("first", 1),
        Predicate::And(vec![equal("later", 2)]),
    ]);
    let Predicate::And(children) = &query else {
        unreachable!()
    };
    let Predicate::Compare(first) = &children[0] else {
        unreachable!()
    };
    let mut visits = 0;
    let result = with_preparation_work(|budget| {
        visit_implication_clauses(&query, CompareClauseMode::Query, budget, &mut |clause| {
            visits += 1;
            let ImplicationClause::Compare(compare) = clause else {
                unreachable!()
            };
            assert!(std::ptr::eq(compare, first));
            Ok(ControlFlow::Break(17))
        })
    })
    .unwrap();
    assert!(matches!(result, ControlFlow::Break(17)));
    assert_eq!(visits, 1);
}

#[test]
fn residual_pruning_reuses_owned_backing_and_counts_spare_capacity() {
    let field = "field".repeat(32);
    let text = "λ".repeat(128);
    let pointers = (field.as_ptr(), text.as_ptr());
    let payload_capacity = field.capacity() + text.capacity();
    let mut children = Vec::with_capacity(16);
    children.extend([
        Predicate::True,
        Predicate::TextContains {
            field,
            value: Value::Text(text),
        },
        Predicate::False,
    ]);
    let backing = children.as_ptr();
    let capacity = children.capacity();
    let residual = with_preparation_work(|budget| {
        strip_query_clauses_satisfied_by_filtered_guard(
            Predicate::And(children),
            &Predicate::True,
            budget,
        )
    })
    .unwrap()
    .unwrap();
    let Predicate::And(children) = &residual else {
        unreachable!()
    };
    assert_eq!(children.as_ptr(), backing);
    assert_eq!(children.capacity(), capacity);
    assert_eq!(children.len(), 2);
    let Predicate::TextContains {
        field,
        value: Value::Text(text),
    } = &children[0]
    else {
        unreachable!()
    };
    assert_eq!((field.as_ptr(), text.as_ptr()), pointers);
    let expected = size_of::<Predicate>() + capacity * size_of::<Predicate>() + payload_capacity;
    assert_eq!(RetainedBytes::measure(&residual, expected), Some(expected));
    assert_eq!(RetainedBytes::measure(&residual, expected - 1), None);
}

#[test]
fn residual_pruning_moves_single_children_and_removes_empty_conjunctions() {
    let field = "retained".repeat(32);
    let pointer = field.as_ptr();
    let query = Predicate::And(vec![
        Predicate::True,
        Predicate::And(vec![Predicate::IsNull { field }, Predicate::True]),
    ]);
    let residual = with_preparation_work(|budget| {
        strip_query_clauses_satisfied_by_filtered_guard(query, &Predicate::True, budget)
    })
    .unwrap()
    .unwrap();
    let Predicate::IsNull { field } = residual else {
        unreachable!()
    };
    assert_eq!(field.as_ptr(), pointer);
    assert_eq!(
        with_preparation_work(|budget| {
            strip_query_clauses_satisfied_by_filtered_guard(
                Predicate::And(vec![Predicate::True, Predicate::And(vec![])]),
                &Predicate::True,
                budget,
            )
        })
        .unwrap(),
        None
    );
}
