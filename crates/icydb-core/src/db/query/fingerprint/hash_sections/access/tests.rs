use super::*;
use crate::db::query::{
    fingerprint::hash_sections::write_value, preparation::with_preparation_work,
};
use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::MissingRowPolicy,
    query::{plan::AccessPlannedQuery, preparation::PreparationWork},
};
use icydb_diagnostic_code::{DiagnosticExecutionLane as Lane, DiagnosticFactTag};
use sha2::Digest;

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn admitted_hash(
    access: &AccessPlan<Value>,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<[u8; 32], QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        let mut hasher = Sha256::new();
        hash_access_plan(&mut hasher, access, work).map_err(QueryError::execute)?;
        Ok(hasher.finalize().into())
    })
}

#[test]
fn access_hash_admission_is_cumulative_and_preserves_identity_on_retry() {
    let access = AccessPlan::Union(vec![
        AccessPlan::by_key(Value::Text("key".repeat(128))),
        AccessPlan::Intersection(vec![
            AccessPlan::by_keys(vec![Value::Map(vec![(Value::Nat64(1), Value::Bool(true))])]),
            AccessPlan::key_range(Value::Nat64(1), Value::Nat64(9)),
            AccessPlan::full_scan(),
        ]),
    ]);
    let before = access.clone();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let measured = request(Resource::PredicateExpressionSteps, 16_000_000);
        let expected = admitted_hash(&access, &measured, lane).unwrap();
        for resource in [
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
            Resource::TemporaryBytes,
        ] {
            let exact = measured.observed(resource);
            assert!(exact > 0);
            for limit in [exact - 1, exact, 2 * exact] {
                let root = request(resource, limit);
                for attempt in 1..=3 {
                    let result = admitted_hash(&access, &root, lane);
                    if attempt * exact <= limit {
                        assert_eq!(result.unwrap(), expected);
                    } else {
                        let facts = result.unwrap_err().diagnostic_facts();
                        assert!(
                            facts.contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                        assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                        break;
                    }
                }
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::QueryExecutions), 0);
            }
        }
    }
    assert_eq!(access, before);
}

#[test]
fn branch_shells_reject_before_descending_or_writing_partial_hash_bytes() {
    let access = AccessPlan::Union(vec![AccessPlan::Intersection(
        vec![AccessPlan::full_scan()],
    )]);
    for limit in [0, 3] {
        let root = request(Resource::PredicateExpressionSteps, limit);
        let result = admitted_hash(&access, &root, Lane::Diagnostic);
        assert_eq!(result.is_ok(), limit == 3);
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
    let root = request(Resource::PredicateExpressionSteps, 0);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        let mut hasher = Sha256::new();
        let before = hasher.clone().finalize();
        assert!(hash_access_plan(&mut hasher, &access, work).is_err());
        assert_eq!(hasher.finalize(), before);
        Ok(())
    })
    .unwrap();
}

#[test]
fn access_hash_exhaustion_prevents_a_continuation_contract() {
    let plan = AccessPlannedQuery::new(
        crate::db::access::AccessPath::ByKey(Value::Nat64(7)),
        MissingRowPolicy::Ignore,
    );
    let build = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
                .map_err(QueryError::execute)
        })
    };
    let denied = request(Resource::NestedValueSteps, 0);
    assert!(build(&denied).unwrap_err().diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::NestedValueSteps.raw(),
    )));
    let admitted = request(Resource::NestedValueSteps, 2);
    let first = build(&admitted).unwrap().unwrap().continuation_signature();
    assert_eq!(
        build(&admitted).unwrap().unwrap().continuation_signature(),
        first
    );
    assert!(build(&admitted).is_err());
    assert_eq!(
        build(&request(Resource::NestedValueSteps, 1))
            .unwrap()
            .unwrap()
            .continuation_signature(),
        first
    );
}

#[test]
fn indexed_access_hash_admits_labels_and_bounds_without_changing_framing() {
    use crate::db::query::fingerprint::hash_sections::{
        VALUE_BOUND_EXCLUDED_TAG, VALUE_BOUND_INCLUDED_TAG,
    };
    let name = "index_λ";
    let fields = ["root", "nested"];
    let values = [Value::Nat64(1), Value::Nat64(2)];
    let lower = Bound::Included(Value::Nat64(3));
    let upper = Bound::Excluded(Value::Nat64(4));
    for (route, tag, value_count) in [
        (0, ACCESS_TAG_INDEX_PREFIX, 1),
        (1, ACCESS_TAG_INDEX_MULTI_LOOKUP, 2),
        (2, ACCESS_TAG_INDEX_BRANCH_SET, 2),
        (3, ACCESS_TAG_INDEX_RANGE, 3),
    ] {
        // Pin wire framing independently of the admission-aware visitor.
        let mut expected = Sha256::new();
        write_tag(&mut expected, tag);
        write_str(&mut expected, name);
        write_u32(&mut expected, 2);
        for field in fields {
            write_str(&mut expected, field);
        }
        if matches!(route, 0 | 3) {
            write_u32(&mut expected, 1);
        }
        write_u32(&mut expected, if route == 1 { 2 } else { 1 });
        write_value(&mut expected, &values[0]).unwrap();
        if route == 1 {
            write_value(&mut expected, &values[1]).unwrap();
        }
        if route == 2 {
            write_u32(&mut expected, 1);
            write_value(&mut expected, &values[1]).unwrap();
        }
        if route == 3 {
            write_tag(&mut expected, VALUE_BOUND_INCLUDED_TAG);
            write_value(&mut expected, &Value::Nat64(3)).unwrap();
            write_tag(&mut expected, VALUE_BOUND_EXCLUDED_TAG);
            write_value(&mut expected, &Value::Nat64(4)).unwrap();
        }
        let expected = expected.finalize();
        let exact = (1 + name.len() + 2 + fields.iter().map(|field| field.len()).sum::<usize>())
            as u64
            + value_count * 128;
        for limit in [exact - 1, exact] {
            let root = request(Resource::PredicateExpressionSteps, limit);
            let result = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                let mut hasher = Sha256::new();
                let mut visitor = AccessFingerprintVisitor {
                    hasher: &mut hasher,
                    budget: work,
                };
                match route {
                    0 => visitor.index_prefix(name, fields.into_iter(), 1, &values[..1]),
                    1 => visitor.index_multi_lookup(name, fields.into_iter(), &values),
                    2 => visitor.index_branch_set(
                        name,
                        fields.into_iter(),
                        &values[..1],
                        &values[1..],
                    ),
                    _ => visitor.index_range(
                        name,
                        fields.into_iter(),
                        1,
                        &values[..1],
                        &lower,
                        &upper,
                    ),
                }
                .map_err(QueryError::execute)?;
                Ok(hasher.finalize())
            });
            if limit == exact {
                assert_eq!(result.unwrap(), expected);
                assert_eq!(root.observed(Resource::NestedValueSteps), value_count);
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                )));
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        }
    }
}

#[test]
fn nested_access_hash_propagates_failure() {
    use crate::value::{test_hash_budget_error, with_test_hash_override};
    let access = AccessPlan::Union(vec![AccessPlan::Intersection(vec![AccessPlan::by_key(
        Value::Nat64(7),
    )])]);
    with_test_hash_override(Err(test_hash_budget_error), || {
        let error =
            with_preparation_work(|work| hash_access_plan(&mut Sha256::new(), &access, work))
                .unwrap_err();
        assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
        assert_eq!(
            error.diagnostic_facts(),
            test_hash_budget_error().diagnostic_facts()
        );
    });
}

#[test]
fn access_key_hashes_preserve_payload_and_framing() {
    let first = Value::Text("start".repeat(1024));
    let last = Value::Text("stop".repeat(1024));
    let nested = Value::List(vec![Value::Map(vec![(
        Value::Text("payload".to_string()),
        Value::NatBig(crate::types::NatBig::from_biguint(
            num_bigint::BigUint::from(1_u8) << 4096_usize,
        )),
    )])]);
    // Raw access fixtures qualify hash framing, not key-type admission. Cover
    // payload-bearing values so the hash boundary cannot assume Copy keys.
    for (access, tag, values, framed_list) in [
        (
            AccessPlan::by_key(first.clone()),
            ACCESS_TAG_BY_KEY,
            vec![first.clone()],
            false,
        ),
        (
            AccessPlan::by_keys(vec![first.clone(), nested.clone(), first.clone()]),
            ACCESS_TAG_BY_KEYS,
            vec![first.clone(), nested, first.clone()],
            true,
        ),
        (
            AccessPlan::key_range(first.clone(), last.clone()),
            ACCESS_TAG_KEY_RANGE,
            vec![first, last],
            false,
        ),
    ] {
        let snapshot = access.clone();
        let mut expected = Sha256::new();
        write_tag(&mut expected, tag);
        if framed_list {
            write_u32(&mut expected, u32::try_from(values.len()).unwrap());
        }
        for value in &values {
            write_value(&mut expected, value).unwrap();
        }
        let expected = expected.finalize();
        for _ in 0..2 {
            let mut planned = Sha256::new();
            with_preparation_work(|work| hash_access_plan(&mut planned, &access, work)).unwrap();
            assert_eq!(planned.finalize(), expected);
        }
        assert_eq!(access, snapshot);
    }
}

#[test]
fn open_primary_key_range_hashes_frame_the_present_endpoint() {
    let value = Value::Nat64(40);
    let lower = AccessPlan::key_range_bounds(Some(value.clone()), None);
    let upper = AccessPlan::key_range_bounds(None, Some(value.clone()));
    let two_sided = AccessPlan::key_range(value.clone(), value);
    let root = request(Resource::PredicateExpressionSteps, 16_000_000);
    let lower_hash = admitted_hash(&lower, &root, Lane::Diagnostic).unwrap();
    let upper_hash = admitted_hash(&upper, &root, Lane::Diagnostic).unwrap();
    let two_sided_hash = admitted_hash(&two_sided, &root, Lane::Diagnostic).unwrap();
    assert_ne!(lower_hash, upper_hash);
    assert_ne!(lower_hash, two_sided_hash);
    assert_ne!(upper_hash, two_sided_hash);
}

#[test]
fn access_projection_hash_preserves_canonical_postorder() {
    let access = AccessPlan::Union(vec![
        AccessPlan::by_keys(vec![Value::Nat64(7), Value::Nat64(2)]),
        AccessPlan::Intersection(vec![AccessPlan::by_keys(vec![]), AccessPlan::Union(vec![])]),
    ]);
    // Pin the existing stream explicitly, independently of either walker.
    let mut expected = Sha256::new();
    write_tag(&mut expected, ACCESS_TAG_BY_KEYS);
    write_u32(&mut expected, 2);
    write_value(&mut expected, &Value::Nat64(7)).unwrap();
    write_value(&mut expected, &Value::Nat64(2)).unwrap();
    write_tag(&mut expected, ACCESS_TAG_BY_KEYS);
    write_u32(&mut expected, 0);
    write_tag(&mut expected, ACCESS_TAG_UNION);
    write_u32(&mut expected, 0);
    write_tag(&mut expected, ACCESS_TAG_INTERSECTION);
    write_u32(&mut expected, 2);
    write_tag(&mut expected, ACCESS_TAG_UNION);
    write_u32(&mut expected, 2);
    let mut planned = Sha256::new();
    with_preparation_work(|work| hash_access_plan(&mut planned, &access, work)).unwrap();
    assert_eq!(planned.finalize(), expected.clone().finalize());
}
