//! Residual pruning uses borrowed bounds without widening access guarantees.

use super::{ENTITY_NAME, SqlStatementResult, initialize, insert_row, projection_rows};
use crate::db::query::preparation::with_preparation_work;
use crate::{
    db::{
        access::{AccessPlan, SemanticIndexAccessContract, SemanticIndexRangeSpec},
        index::{TextPrefixBoundMode, starts_with_component_bounds},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::residual_query_predicate_after_access_path_bounds,
    },
    value::{OutputValue, Value},
};
use std::ops::Bound;

fn index(name: &str) -> SemanticIndexAccessContract {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let accepted = catalog
        .accepted_schema_info()
        .field_path_indexes()
        .iter()
        .find(|index| index.name() == name)
        .unwrap();
    SemanticIndexAccessContract::from_accepted_field_path_index(accepted)
        .expect("valid accepted index fixture")
}

fn compare(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
}

fn assert_residual(access: &AccessPlan<Value>, query: &Predicate, expected: Option<&Predicate>) {
    let before = access.clone();
    for _ in 0..3 {
        assert_eq!(
            with_preparation_work(|budget| {
                residual_query_predicate_after_access_path_bounds(
                    access.as_path(),
                    query.clone(),
                    budget,
                )
            })
            .unwrap()
            .as_ref(),
            expected,
        );
    }
    assert_eq!(*access, before);
}

#[test]
fn residual_bounds_preserve_prefix_membership_and_branch_guarantees() {
    let index = index("b_wide_branch_idx");
    let first = index.key_field_at(0).unwrap();
    let second = index.key_field_at(1).unwrap();
    let fixed = Value::Text("λ".repeat(128));
    let branches = vec![Value::Int64(7), Value::Int64(9)];
    let prefix = compare(first, CompareOp::Eq, fixed.clone());
    let extra = Predicate::IsNotNull {
        field: "not_covered".into(),
    };
    let cases = [
        (
            AccessPlan::index_prefix_from_contract(index.clone(), vec![fixed.clone()]),
            prefix.clone(),
            compare(first, CompareOp::Eq, Value::Text("different".into())),
        ),
        (
            AccessPlan::index_multi_lookup_from_contract(index.clone(), branches.clone()),
            compare(
                first,
                CompareOp::In,
                Value::List(vec![Value::Nat64(9), Value::Nat64(7), Value::Nat64(11)]),
            ),
            compare(first, CompareOp::Eq, Value::Int64(7)),
        ),
        (
            AccessPlan::index_branch_set_from_contract(
                index.clone(),
                vec![fixed],
                branches.clone(),
            ),
            Predicate::And(vec![
                prefix,
                compare(second, CompareOp::In, Value::List(branches)),
            ]),
            compare(second, CompareOp::Eq, Value::Int64(7)),
        ),
    ];
    for (access, guaranteed, stricter) in cases {
        assert_residual(&access, &guaranteed, None);
        assert_residual(
            &access,
            &Predicate::And(vec![guaranteed, extra.clone()]),
            Some(&extra),
        );
        assert_residual(&access, &stricter, Some(&stricter));
    }
}

#[test]
fn residual_bounds_keep_stricter_range_siblings_and_endpoint_inclusion() {
    let index = index("a_common_idx");
    let field = index.key_field_at(0).unwrap();
    for (lower, admitted, stricter) in [
        (
            Bound::Included(Value::Int64(7)),
            CompareOp::Gte,
            CompareOp::Gt,
        ),
        (
            Bound::Excluded(Value::Int64(7)),
            CompareOp::Gt,
            CompareOp::Eq,
        ),
    ] {
        let access = AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
            index.clone(),
            vec![0],
            vec![],
            lower,
            Bound::Excluded(Value::Int64(11)),
        ));
        let guaranteed = Predicate::And(vec![
            compare(field, admitted, Value::Nat64(7)),
            compare(field, CompareOp::Lt, Value::Nat64(11)),
        ]);
        assert_residual(&access, &guaranteed, None);
        let stricter = compare(field, stricter, Value::Nat64(7));
        assert_residual(&access, &stricter, Some(&stricter));
        let upper = compare(field, CompareOp::Lt, Value::Nat64(10));
        assert_residual(&access, &upper, Some(&upper));
    }
}

#[test]
fn residual_bounds_reuse_text_prefix_proof_and_preserve_unbounded_paths() {
    let index = index("a_common_idx");
    let field = index.key_field_at(0).unwrap();
    for prefix in ["a", "λ", "a\0b", "\u{10ffff}"] {
        let (lower, upper) =
            starts_with_component_bounds(prefix, TextPrefixBoundMode::Strict).unwrap();
        let access = AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
            index.clone(),
            vec![0],
            vec![],
            lower,
            upper,
        ));
        let guaranteed = compare(field, CompareOp::StartsWith, Value::Text(prefix.into()));
        assert_residual(&access, &guaranteed, None);
        let empty_prefix = compare(field, CompareOp::StartsWith, Value::Text(String::new()));
        assert_residual(&access, &empty_prefix, Some(&empty_prefix));
    }
    let query = Predicate::And(vec![Predicate::True, Predicate::True]);
    for access in [
        AccessPlan::full_scan(),
        AccessPlan::index_prefix_from_contract(index.clone(), vec![]),
        AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
            index,
            vec![0],
            vec![],
            Bound::Unbounded,
            Bound::Unbounded,
        )),
    ] {
        // No proven clauses means the original shape is retained, not simplified.
        assert_residual(&access, &query, Some(&query));
    }
}

#[test]
fn residual_bounds_casefold_negations_preserve_indexed_rows_and_counts() {
    let session = initialize();
    for (id, common) in [(1, "Alice"), (2, "alice"), (3, "Bob")] {
        insert_row(&session, id, common, "group-a");
    }
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for (predicate, ids) in [
            ("common = 'Alice' AND LOWER(common) <> 'alice'", vec![]),
            (
                "common = 'Alice' AND LOWER(common) NOT IN ('alice')",
                vec![],
            ),
            (
                "common IN ('Alice', 'Bob') AND LOWER(common) <> 'alice'",
                vec![3],
            ),
            (
                "common IN ('Alice', 'Bob') AND LOWER(common) NOT IN ('alice')",
                vec![3],
            ),
        ] {
            let expected = ids
                .iter()
                .map(|id| vec![OutputValue::nat64(*id)])
                .collect::<Vec<_>>();
            for _ in 0..2 {
                // Arithmetic keeps the full expression residual as a control
                // for the fully predicate-covered optimized query.
                for condition in [predicate.into(), format!("({predicate}) AND id + 0 = id")] {
                    assert_eq!(
                        projection_rows(
                            &session,
                            &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                        ),
                        expected,
                        "{condition}",
                    );
                }
                assert_eq!(
                    projection_rows(
                        &session,
                        &format!("SELECT COUNT(*) FROM PlannerRow WHERE {predicate}")
                    ),
                    vec![vec![OutputValue::nat64(ids.len() as u64)]],
                    "COUNT: {predicate}",
                );
            }
        }
    }
}

#[test]
fn residual_bounds_exclusion_proofs_respect_comparison_domains() {
    let index = index("b_wide_branch_idx");
    let alice = Value::Text("Alice".into());
    let cases = [
        (
            AccessPlan::index_prefix_from_contract(index.clone(), vec![alice.clone()]),
            index.key_field_at(0).unwrap(),
        ),
        (
            AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
                index.clone(),
                vec![0, 1],
                vec![alice.clone()],
                Bound::Unbounded,
                Bound::Unbounded,
            )),
            index.key_field_at(0).unwrap(),
        ),
        (
            AccessPlan::index_multi_lookup_from_contract(
                index.clone(),
                vec![alice.clone(), Value::Text("Bob".into())],
            ),
            index.key_field_at(0).unwrap(),
        ),
        (
            AccessPlan::index_branch_set_from_contract(
                index.clone(),
                vec![Value::Text("all".into())],
                vec![alice, Value::Text("Bob".into())],
            ),
            index.key_field_at(1).unwrap(),
        ),
    ];
    for (access, field) in cases {
        for op in [CompareOp::Ne, CompareOp::NotIn] {
            let value = Value::Text("alice".into());
            let value = if op == CompareOp::NotIn {
                Value::List(vec![value])
            } else {
                value
            };
            let casefold = Predicate::Compare(ComparePredicate::with_coercion(
                field,
                op,
                value.clone(),
                CoercionId::TextCasefold,
            ));
            assert_residual(&access, &casefold, Some(&casefold));
            assert_residual(&access, &compare(field, op, value), None);
        }
    }
    let numeric = AccessPlan::index_multi_lookup_from_contract(
        index.clone(),
        vec![Value::Int64(7), Value::Int64(9)],
    );
    for coercion in [CoercionId::Strict, CoercionId::NumericWiden] {
        for (op, value) in [
            (CompareOp::Ne, Value::Nat64(11)),
            (CompareOp::NotIn, Value::List(vec![Value::Nat64(11)])),
        ] {
            let predicate = Predicate::Compare(ComparePredicate::with_coercion(
                index.key_field_at(0).unwrap(),
                op,
                value,
                coercion,
            ));
            assert_residual(&numeric, &predicate, None);
        }
    }
    // Strict equality remains sufficient for positive casefold membership.
    let positive = Predicate::Compare(ComparePredicate::with_coercion(
        index.key_field_at(0).unwrap(),
        CompareOp::In,
        Value::List(vec![Value::Text("Alice".into())]),
        CoercionId::TextCasefold,
    ));
    let prefix = AccessPlan::index_prefix_from_contract(index, vec![Value::Text("Alice".into())]);
    assert_residual(&prefix, &positive, None);
}

#[test]
fn residual_bounds_casefold_negations_preserve_mutation_selection() {
    for exclusion in ["<> 'alice'", "NOT IN ('alice')"] {
        for (bounds, expected) in [("= 'Alice'", 0), ("IN ('Alice', 'Bob')", 1)] {
            let session = initialize();
            // Heap-store recreation is a reinstall fixture: discard the
            // accepted runtime root retained from the previous case.
            session.invalidate_accepted_schema_runtime_root();
            for (id, common) in [(1, "Alice"), (2, "alice"), (3, "Bob")] {
                insert_row(&session, id, common, "group-a");
            }
            let predicate = format!("common {bounds} AND LOWER(common) {exclusion}");
            let result = session
                .execute_trusted_sql_exact_update(
                    &format!("UPDATE PlannerRow SET rare = 'marked' WHERE {predicate}"),
                    3,
                )
                .unwrap();
            assert!(
                matches!(result, SqlStatementResult::Count { row_count } if row_count == expected)
            );
            let marked = if expected == 0 {
                vec![]
            } else {
                vec![vec![OutputValue::nat64(3)]]
            };
            assert_eq!(
                projection_rows(&session, "SELECT id FROM PlannerRow WHERE rare = 'marked'"),
                marked,
                "UPDATE: {predicate}",
            );
            let result = session
                .execute_trusted_sql_mutation(&format!("DELETE FROM PlannerRow WHERE {predicate}"))
                .unwrap();
            assert!(
                matches!(result, SqlStatementResult::Count { row_count } if row_count == expected)
            );
            let ids = if expected == 0 {
                vec![1, 2, 3]
            } else {
                vec![1, 2]
            };
            assert_eq!(
                projection_rows(
                    &session,
                    "SELECT id FROM PlannerRow WHERE id > 0 ORDER BY id"
                ),
                ids.into_iter()
                    .map(|id| vec![OutputValue::nat64(id)])
                    .collect::<Vec<_>>(),
                "DELETE: {predicate}",
            );
        }
    }
}

#[test]
fn residual_bounds_casefold_exclusion_survives_wide_exact_count() {
    let session = initialize();
    let mut literals = Vec::new();
    for id in 0..20 {
        let name = format!("Name{id:02}");
        insert_row(&session, id, &name, "group-a");
        literals.push(format!("'{name}'"));
    }
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for exclusion in ["<> 'name00'", "NOT IN ('name00')"] {
            let predicate = format!(
                "common IN ({}) AND LOWER(common) {exclusion}",
                literals.join(",")
            );
            for _ in 0..2 {
                assert_eq!(
                    projection_rows(
                        &session,
                        &format!("SELECT COUNT(*) FROM PlannerRow WHERE {predicate}")
                    ),
                    vec![vec![OutputValue::nat64(19)]],
                );
                assert_eq!(
                    projection_rows(
                        &session,
                        &format!("SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id")
                    ),
                    (1..20)
                        .map(|id| vec![OutputValue::nat64(id)])
                        .collect::<Vec<_>>(),
                );
            }
        }
    }
}
