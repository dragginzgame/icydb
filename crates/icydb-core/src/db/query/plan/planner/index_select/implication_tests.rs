//! Conservative implication and borrowed short-circuit traversal contracts.

use super::{
    CompareClauseMode, ImplicationClause, predicate_implies_clause_for_planner,
    predicate_implies_predicate_for_planner, strip_query_clauses_satisfied_by_filtered_guard,
    visit_implication_clauses,
};
use crate::{
    db::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
    retained::RetainedBytes,
    value::Value,
};
use std::ops::ControlFlow;

fn equal(field: &str, value: u64) -> Predicate {
    Predicate::eq(field.into(), Value::Nat64(value))
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
            predicate_implies_predicate_for_planner(&query, &required),
            expected,
            "query={query:?}, required={required:?}"
        );
    }
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
                predicate_implies_clause_for_planner(&query, ImplicationClause::Compare(compare)),
                predicate_implies_predicate_for_planner(
                    &query,
                    &Predicate::Compare(compare.clone())
                ),
            );
        }
        for field in ["id", "other"] {
            assert_eq!(
                predicate_implies_clause_for_planner(&query, ImplicationClause::NonNull(field)),
                predicate_implies_predicate_for_planner(
                    &query,
                    &Predicate::IsNotNull {
                        field: field.into()
                    }
                ),
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
            strip_query_clauses_satisfied_by_filtered_guard(query.clone(), &guard),
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
    let result = visit_implication_clauses(&query, CompareClauseMode::Query, &mut |clause| {
        visits += 1;
        let ImplicationClause::Compare(compare) = clause else {
            unreachable!()
        };
        assert!(std::ptr::eq(compare, first));
        ControlFlow::Break(17)
    });
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
    let residual =
        strip_query_clauses_satisfied_by_filtered_guard(Predicate::And(children), &Predicate::True)
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
    let residual =
        strip_query_clauses_satisfied_by_filtered_guard(query, &Predicate::True).unwrap();
    let Predicate::IsNull { field } = residual else {
        unreachable!()
    };
    assert_eq!(field.as_ptr(), pointer);
    assert_eq!(
        strip_query_clauses_satisfied_by_filtered_guard(
            Predicate::And(vec![Predicate::True, Predicate::And(vec![])]),
            &Predicate::True,
        ),
        None
    );
}
