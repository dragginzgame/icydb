//! Residual pruning uses borrowed bounds without widening access guarantees.

use super::{ENTITY_NAME, initialize};
use crate::{
    db::{
        access::{AccessPlan, SemanticIndexAccessContract, SemanticIndexRangeSpec},
        index::{TextPrefixBoundMode, starts_with_component_bounds},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::residual_query_predicate_after_access_path_bounds,
    },
    value::Value,
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
            residual_query_predicate_after_access_path_bounds(access.as_path(), query.clone())
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
