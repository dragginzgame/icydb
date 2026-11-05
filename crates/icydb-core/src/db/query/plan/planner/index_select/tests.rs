use super::{
    ComparisonRef, access_bound_lower_range_clause, access_bound_upper_range_clause,
    predicate_implies_predicate_for_planner, strip_query_clauses_satisfied_by_filtered_guard,
};
use crate::db::query::preparation::with_preparation_work;
use crate::{
    db::predicate::{CoercionId, CompareFieldsPredicate, CompareOp, Predicate},
    value::Value,
};

fn non_null(field: &str) -> Predicate {
    Predicate::is_not_null(field.to_string())
}

fn compare(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(crate::db::predicate::ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
}

#[test]
fn residual_bound_comparisons_borrow_operands_and_preserve_coercion() {
    use std::ops::Bound;

    let field = "λ".repeat(32);
    let predicate = crate::db::predicate::ComparePredicate::with_coercion(
        field.clone(),
        CompareOp::Eq,
        Value::Text("payload".repeat(128)),
        CoercionId::NumericWiden,
    );
    let view = ComparisonRef::from(&predicate);
    assert!(std::ptr::eq(view.field, predicate.field()));
    assert!(std::ptr::eq(view.value, predicate.value()));
    assert_eq!(view.coercion, CoercionId::NumericWiden);

    for (bound, lower_op, upper_op) in [
        (
            Bound::Included(predicate.value().clone()),
            CompareOp::Gte,
            CompareOp::Lte,
        ),
        (
            Bound::Excluded(predicate.value().clone()),
            CompareOp::Gt,
            CompareOp::Lt,
        ),
    ] {
        let value = match &bound {
            Bound::Included(value) | Bound::Excluded(value) => value,
            Bound::Unbounded => unreachable!(),
        };
        for (view, op) in [
            (
                access_bound_lower_range_clause(&field, &bound).unwrap(),
                lower_op,
            ),
            (
                access_bound_upper_range_clause(&field, &bound).unwrap(),
                upper_op,
            ),
        ] {
            assert!(std::ptr::eq(view.field, field.as_str()));
            assert!(std::ptr::eq(view.value, value));
            assert_eq!(view.op, op);
            assert_eq!(view.coercion, CoercionId::Strict);
        }
    }
    assert!(access_bound_lower_range_clause(&field, &Bound::Unbounded).is_none());
    assert!(access_bound_upper_range_clause(&field, &Bound::Unbounded).is_none());
}

#[test]
fn nullable_guard_implication_accepts_exact_and_non_null_scalar_comparisons() {
    let required = non_null("email");
    assert!(
        with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(&required, &required, budget)
        })
        .unwrap()
    );

    for op in [
        CompareOp::Eq,
        CompareOp::Gt,
        CompareOp::Gte,
        CompareOp::Lt,
        CompareOp::Lte,
    ] {
        assert!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(
                    &compare("email", op, Value::Text("a@example.com".to_string())),
                    &required,
                    budget,
                )
            })
            .unwrap()
        );
    }
}

#[test]
fn nullable_guard_implication_accepts_membership_and_text_prefix_but_not_null_members() {
    let required = non_null("email");
    let membership = compare(
        "email",
        CompareOp::In,
        Value::List(vec![
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ]),
    );
    let prefix = compare("email", CompareOp::StartsWith, Value::Text("a".to_string()));
    for query in [
        membership.clone(),
        prefix.clone(),
        Predicate::or(vec![membership, prefix]),
    ] {
        assert!(
            with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&query, &required, budget)
            })
            .unwrap()
        );
    }
    assert!(
        with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(
                &compare("unit", CompareOp::Eq, Value::Unit),
                &non_null("unit"),
                budget,
            )
        })
        .unwrap()
    );
    let nullable_membership = compare(
        "email",
        CompareOp::In,
        Value::List(vec![Value::Text("a".to_string()), Value::Null]),
    );
    assert!(
        !with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(&nullable_membership, &required, budget)
        })
        .unwrap()
    );
    let casefold = Predicate::Compare(crate::db::predicate::ComparePredicate::with_coercion(
        "email",
        CompareOp::Eq,
        Value::Text("A".to_string()),
        CoercionId::TextCasefold,
    ));
    assert!(
        with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(&casefold, &required, budget)
        })
        .unwrap()
    );
}

#[test]
fn nullable_guard_implication_requires_every_composite_guard_and_extra_filter() {
    let required = Predicate::and(vec![
        non_null("tenant"),
        non_null("email"),
        compare("active", CompareOp::Eq, Value::Bool(true)),
    ]);
    let complete = Predicate::and(vec![
        compare(
            "email",
            CompareOp::Eq,
            Value::Text("a@example.com".to_string()),
        ),
        non_null("tenant"),
        compare("active", CompareOp::Eq, Value::Bool(true)),
        Predicate::IsNotEmpty {
            field: "display_name".to_string(),
        },
    ]);
    assert!(
        with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(&complete, &required, budget)
        })
        .unwrap()
    );

    let missing = Predicate::and(vec![
        non_null("email"),
        compare("active", CompareOp::Eq, Value::Bool(true)),
    ]);
    assert!(
        !with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(&missing, &required, budget)
        })
        .unwrap()
    );
}

#[test]
fn nullable_guard_implication_keeps_unsupported_shapes_conservative() {
    let required = non_null("email");
    let cross_field = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
        "email",
        CompareOp::Eq,
        "backup_email",
        CoercionId::Strict,
    ));
    for query in [
        Predicate::or(vec![required.clone(), non_null("tenant")]),
        Predicate::not(Predicate::IsNull {
            field: "email".to_string(),
        }),
        cross_field,
        compare("email", CompareOp::Eq, Value::Null),
        compare(
            "lower_email",
            CompareOp::Eq,
            Value::Text("a@example.com".to_string()),
        ),
    ] {
        assert!(
            !with_preparation_work(|budget| {
                predicate_implies_predicate_for_planner(&query, &required, budget)
            })
            .unwrap()
        );
    }
}

#[test]
fn nullable_guard_implication_treats_unsatisfiable_query_as_vacuous_proof() {
    assert!(
        with_preparation_work(|budget| {
            predicate_implies_predicate_for_planner(&Predicate::False, &non_null("email"), budget)
        })
        .unwrap()
    );
}

#[test]
fn filtered_guard_stripping_removes_only_guaranteed_non_null_clause() {
    let guard = non_null("email");
    assert_eq!(
        with_preparation_work(|budget| {
            strip_query_clauses_satisfied_by_filtered_guard(guard.clone(), &guard, budget)
        })
        .unwrap(),
        None,
    );

    let query = Predicate::and(vec![guard.clone(), non_null("tenant")]);
    assert_eq!(
        with_preparation_work(|budget| {
            strip_query_clauses_satisfied_by_filtered_guard(query, &guard, budget)
        })
        .unwrap(),
        Some(non_null("tenant")),
    );
}
