//! Normalized cache identity keeps canonical bytes and admits its temporary backing.

use super::{hash_predicate, hash_predicate_structural, predicate_fingerprint_normalized};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        codec::{finalize_hash_sha256, new_hash_sha256},
        predicate::{
            CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate,
            encoding::{
                normalized_predicate_key_capacity, raw_predicate_key_capacity,
                write_normalized_predicate_sort_key, write_predicate_sort_key,
            },
        },
        query::preparation::PreparationWork,
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Principal, Subaccount, Timestamp, U256,
        Ulid,
    },
    value::{Value, ValueEnum},
};
use ic_platform::blake3;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use sha2::Digest;

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            32_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn values() -> Vec<Value> {
    vec![
        Value::Unit,
        Value::Null,
        Value::Bool(true),
        Value::Date(Date::try_new(2024, 1, 1).unwrap()),
        Value::Duration(Duration::from_secs(1)),
        Value::Timestamp(Timestamp::from_secs(1)),
        Value::Float32(Float32::try_new(-0.0).unwrap()),
        Value::Float64(Float64::try_new(-0.0).unwrap()),
        Value::Int64(i64::MIN),
        Value::Nat64(u64::MAX),
        Value::Int128(i128::MIN),
        Value::Nat128(u128::MAX),
        Value::Decimal(Decimal::new(-12300, 4)),
        Value::Ulid(Ulid::from_u128(42)),
        Value::U256(U256::MAX),
        Value::Subaccount(Subaccount::from_array([1; 32])),
        Value::Principal(Principal::from_slice(&[0; 29])),
        Value::Account(Account::from_owner_and_subaccount(
            Principal::from_slice(&[0; 29]),
            None,
        )),
        Value::Account(Account::from_owner_and_subaccount(
            Principal::from_slice(&[0; 29]),
            Some(Subaccount::from_array([1; 32])),
        )),
        Value::Text("İΣé\0".repeat(1024)),
        Value::Text(String::new()),
        Value::Blob(vec![255; 4096]),
        Value::Enum(ValueEnum::test_unit(1, 2)),
        Value::Enum(ValueEnum::test_payload(
            1,
            2,
            Value::List(vec![Value::Text("nested".into())]),
        )),
        Value::List(vec![Value::Nat64(1), Value::Text("Σ".repeat(128))]),
        Value::Map(vec![
            (Value::Text("z".into()), Value::Nat64(2)),
            (Value::Text("a".into()), Value::Nat64(1)),
        ]),
    ]
}

fn assert_capacity_and_digest(predicate: &Predicate) {
    let snapshot = predicate.clone();
    let mut expected = Vec::new();
    write_normalized_predicate_sort_key(&mut expected, predicate);
    let expected_digest = *blake3::hash(&expected).as_bytes();
    let root = request(Resource::TemporaryBytes, 32_000_000);
    PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        let capacity = normalized_predicate_key_capacity(predicate, work).unwrap();
        let mut bytes = Vec::with_capacity(capacity);
        let allocated = bytes.capacity();
        write_normalized_predicate_sort_key(&mut bytes, predicate);
        assert!(bytes.len() <= capacity);
        assert_eq!(
            bytes.capacity(),
            allocated,
            "no growth beyond admitted backing"
        );
        assert_eq!(bytes, expected);
        assert_eq!(
            predicate_fingerprint_normalized(predicate, work).unwrap(),
            expected_digest
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(predicate, &snapshot);
}

#[test]
fn normalized_fingerprint_capacity_covers_value_families_and_coercions() {
    for value in values() {
        for coercion in [
            CoercionId::Strict,
            CoercionId::CollectionElement,
            CoercionId::NumericWiden,
            CoercionId::TextCasefold,
        ] {
            for op in [CompareOp::Eq, CompareOp::In, CompareOp::NotIn] {
                let operand = if op == CompareOp::Eq {
                    value.clone()
                } else {
                    Value::List(vec![value.clone()])
                };
                let compare =
                    ComparePredicate::with_coercion("field".repeat(64), op, operand, coercion);
                assert_capacity_and_digest(&Predicate::Compare(compare));
            }
        }
    }
}

#[test]
fn normalized_fingerprint_capacity_covers_predicate_frames_and_depth() {
    let children = vec![
        Predicate::True,
        Predicate::False,
        Predicate::IsNull { field: "a".into() },
        Predicate::IsNotNull { field: "a".into() },
        Predicate::IsMissing { field: "a".into() },
        Predicate::IsEmpty { field: "a".into() },
        Predicate::IsNotEmpty { field: "a".into() },
        Predicate::TextContains {
            field: "a".into(),
            value: Value::Text("z".repeat(512)),
        },
        Predicate::TextContainsCi {
            field: "a".into(),
            value: Value::Text("İΣ".repeat(512)),
        },
        Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "left",
            CompareOp::Eq,
            "right",
            CoercionId::Strict,
        )),
    ];
    assert_capacity_and_digest(&Predicate::And(children.clone()));
    assert_capacity_and_digest(&Predicate::Or(children));
    let mut predicate = Predicate::eq("payload".into(), Value::List(values()));
    for _ in 0..120 {
        predicate = Predicate::Not(Box::new(predicate));
    }
    assert_capacity_and_digest(&predicate);
}

#[test]
fn normalized_fingerprint_exhaustion_is_cumulative_and_never_returns_a_digest() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![Value::Text("İΣ".repeat(64))]),
        CoercionId::TextCasefold,
    ));
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let measured = request(Resource::TemporaryBytes, 32_000_000);
        let expected = PreparationWork::run(&measured.scope(), lane, |work| {
            predicate_fingerprint_normalized(&predicate, work).map_err(QueryError::execute)
        })
        .unwrap();
        for resource in [
            Resource::TemporaryBytes,
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
        ] {
            let exact = measured.observed(resource);
            for limit in [0, exact - 1, 2 * exact] {
                let root = request(resource, limit);
                for attempt in 1..=3 {
                    let result = PreparationWork::run(&root.scope(), lane, |work| {
                        predicate_fingerprint_normalized(&predicate, work)
                            .map_err(QueryError::execute)
                    });
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
            }
        }
    }
}

#[test]
fn raw_fingerprint_capacity_preserves_coerced_membership_and_nested_values() {
    for value in values() {
        for coercion in [
            CoercionId::Strict,
            CoercionId::CollectionElement,
            CoercionId::NumericWiden,
            CoercionId::TextCasefold,
        ] {
            for op in [CompareOp::Eq, CompareOp::In, CompareOp::NotIn] {
                let operand = if op == CompareOp::Eq {
                    value.clone()
                } else {
                    Value::List(vec![value.clone(), Value::Null, value.clone()])
                };
                let predicate = Predicate::Compare(ComparePredicate::with_coercion(
                    "field", op, operand, coercion,
                ));
                let mut expected = Vec::new();
                write_predicate_sort_key(&mut expected, &predicate);
                let root = request(Resource::TemporaryBytes, 32_000_000);
                PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                    let capacity = raw_predicate_key_capacity(&predicate, work).unwrap();
                    let mut encoded = Vec::with_capacity(capacity);
                    let allocated = encoded.capacity();
                    write_predicate_sort_key(&mut encoded, &predicate);
                    assert_eq!(encoded.capacity(), allocated);
                    assert_eq!(encoded, expected);
                    let mut hasher = new_hash_sha256();
                    hash_predicate_structural(&mut hasher, &predicate, work).unwrap();
                    let mut reference = new_hash_sha256();
                    reference.update(&expected);
                    assert_eq!(
                        finalize_hash_sha256(hasher),
                        finalize_hash_sha256(reference)
                    );
                    Ok(())
                })
                .unwrap();
            }
        }
    }
}

#[test]
fn predicate_hash_copy_and_encoding_exhaustion_preserve_input_and_hash_stream() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![Value::Text("İΣ".repeat(64)), Value::Text("a".into())]),
        CoercionId::TextCasefold,
    ));
    let snapshot = predicate.clone();
    let measured = request(Resource::TemporaryBytes, 32_000_000);
    PreparationWork::run(&measured.scope(), Lane::PublicRead, |work| {
        hash_predicate(&mut new_hash_sha256(), &predicate, work).map_err(QueryError::execute)
    })
    .unwrap();
    for resource in [
        Resource::TemporaryBytes,
        Resource::PredicateExpressionSteps,
        Resource::NestedValueSteps,
    ] {
        // Zero rejects the input copy; one below full cost rejects later
        // preflight/output admission without publishing any predicate bytes.
        for limit in [0, measured.observed(resource) - 1] {
            let root = request(resource, limit);
            let mut hasher = new_hash_sha256();
            hasher.update(b"preceding section");
            let before = finalize_hash_sha256(hasher.clone());
            let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                hash_predicate(&mut hasher, &predicate, work).map_err(QueryError::execute)
            })
            .unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
            );
            assert_eq!(finalize_hash_sha256(hasher), before);
            assert_eq!(predicate, snapshot);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn raw_ordering_refusal_preserves_existing_hash_stream() {
    let predicate = Predicate::eq(
        "map".into(),
        Value::Map(vec![
            (Value::Text("k".into()), Value::Text("z".repeat(256))),
            (Value::Text("k".into()), Value::Text("a".repeat(256))),
        ]),
    );
    let membership = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![
            Value::Text("İ".repeat(128)),
            Value::Text("a".into()),
            Value::Text("İ".repeat(128)),
        ]),
        CoercionId::TextCasefold,
    ));
    for (predicate, entries, comparisons) in [(predicate, 2, 3), (membership, 3, 12)] {
        let mut encoded = Vec::new();
        write_predicate_sort_key(&mut encoded, &predicate);
        for (resource, exact) in [
            (Resource::SortEntries, entries),
            (Resource::SortComparisons, comparisons),
        ] {
            for limit in [exact - 1, exact] {
                let root = request(resource, limit);
                let mut hasher = new_hash_sha256();
                hasher.update(b"previous section");
                let before = hasher.clone();
                let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                    hash_predicate_structural(&mut hasher, &predicate, work)
                        .map_err(QueryError::execute)
                });
                if limit == exact {
                    result.unwrap();
                    let mut expected = before;
                    expected.update(&encoded);
                    assert_eq!(finalize_hash_sha256(hasher), finalize_hash_sha256(expected));
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                    assert_eq!(finalize_hash_sha256(hasher), finalize_hash_sha256(before));
                }
            }
        }
    }
}

#[test]
fn raw_membership_sort_and_coerced_payload_allowances_are_exact_and_cumulative() {
    use crate::value::lower_text_construction_allowance;

    for (value, capacity, nodes, bytes) in [
        (Value::Text("İΣ".repeat(64)), 320, 2, 256),
        (Value::Blob(vec![1; 256]), 320, 2, 256),
        (Value::Nat128(u128::MAX), 64, 2, 0),
        (
            Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![1; 256]))]),
            )),
            513,
            8,
            257,
        ),
    ] {
        for coercion in [
            CoercionId::Strict,
            CoercionId::CollectionElement,
            CoercionId::NumericWiden,
            CoercionId::TextCasefold,
        ] {
            let (capacity, compared_bytes, fold_steps) = if coercion == CoercionId::TextCasefold
                && let Value::Text(text) = &value
            {
                let (backing, steps) = lower_text_construction_allowance(text.len());
                (64 + 2 * bytes, backing, steps)
            } else {
                (capacity, bytes, 0)
            };
            let capacity = 129 + 3 * capacity;
            for op in [CompareOp::In, CompareOp::NotIn] {
                let predicate = Predicate::Compare(ComparePredicate::with_coercion(
                    "x",
                    op,
                    Value::List(vec![value.clone(); 3]),
                    coercion,
                ));
                // Three items: six sort comparisons and three two-entry scans.
                // Raw admission walks source payloads; repetitions use the bound
                // after direct coercion. Nested payloads are never casefolded.
                for (resource, amount) in [
                    (Resource::SortEntries, 3),
                    (Resource::SortComparisons, 12),
                    (Resource::NestedValueSteps, 1 + 3 * nodes / 2 + 15 * nodes),
                    (
                        Resource::PredicateExpressionSteps,
                        1 + 3 * bytes + 12 * compared_bytes + 3 * fold_steps + 2 * capacity,
                    ),
                ] {
                    assert_raw_membership_boundary(&predicate, resource, amount, capacity);
                }
            }
        }
    }
}

fn assert_raw_membership_boundary(
    predicate: &Predicate,
    resource: Resource,
    amount: u64,
    capacity: u64,
) {
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        for limit in [amount - 1, amount, 2 * amount] {
            let root = request(resource, limit);
            for attempt in 1..=3 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    raw_predicate_key_capacity(predicate, work).map_err(QueryError::execute)
                });
                assert_eq!(root.observed(Resource::SortTemporaryBytes), 0);
                if attempt * amount <= limit {
                    assert_eq!(result.unwrap() as u64, capacity);
                    assert_eq!(root.observed(resource), attempt * amount);
                } else {
                    let error = result.unwrap_err();
                    let actual = root.observed(resource);
                    assert!(actual > limit && actual <= attempt * amount);
                    for fact in [
                        (DiagnosticFactTag::BudgetResource, resource.raw()),
                        (DiagnosticFactTag::ExecutionLane, lane.raw()),
                        (DiagnosticFactTag::Limit, limit),
                        (DiagnosticFactTag::Actual, actual),
                    ] {
                        assert!(error.diagnostic_facts().contains(&fact));
                    }
                    break;
                }
            }
        }
    }
}

#[test]
fn normalized_membership_and_empty_singleton_raw_lists_skip_ordering_admission() {
    for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
        for items in [
            vec![],
            vec![Value::Text("İ".repeat(128))],
            vec![Value::Text("a".into()), Value::Text("b".into())],
        ] {
            for op in [CompareOp::In, CompareOp::NotIn] {
                let predicate = Predicate::Compare(ComparePredicate::with_coercion(
                    "x",
                    op,
                    Value::List(items.clone()),
                    coercion,
                ));
                for resource in [Resource::SortEntries, Resource::SortComparisons] {
                    let root = request(resource, 0);
                    PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                        normalized_predicate_key_capacity(&predicate, work)
                            .map_err(QueryError::execute)
                    })
                    .unwrap();
                    assert_eq!(root.observed(resource), 0);
                    if items.len() < 2 {
                        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                            raw_predicate_key_capacity(&predicate, work)
                                .map_err(QueryError::execute)
                        })
                        .unwrap();
                        assert_eq!(root.observed(resource), 0);
                    }
                }
            }
        }
    }
}
