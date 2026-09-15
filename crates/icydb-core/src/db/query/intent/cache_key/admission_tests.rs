//! Literal hashing uses current authority without publishing failed memo entries.

use super::ValueCacheKey;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::MissingRowPolicy,
        query::{
            intent::StructuralQuery,
            plan::expr::{Expr, ProjectionField, ProjectionSelection},
            preparation::PreparationWork,
        },
    },
    types::{Decimal, IntBig, NatBig},
    value::{Value, ValueEnum, hash_value, test_hash_budget_error, with_test_hash_override},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn literal_key(
    value: &Value,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<ValueCacheKey, QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        ValueCacheKey::from_value(value, work)
    })
}

#[test]
fn literal_hash_admission_preserves_digests_and_cumulative_limits() {
    let signed = IntBig::from_bigint(-(num_bigint::BigInt::from(1u8) << 223usize));
    let values = [
        Value::Text("text".repeat(1024)),
        Value::Blob(vec![7; 4096]),
        Value::Decimal(Decimal::from(7_u64)),
        Value::IntBig(signed),
        Value::NatBig(NatBig::from(8192_u32)),
        Value::Enum(ValueEnum::test_payload(
            1,
            1,
            Value::List(vec![Value::Null, Value::Nat64(7)]),
        )),
        Value::Map(vec![
            (
                Value::Text("b".into()),
                Value::List(vec![Value::Bool(true)]),
            ),
            (Value::Text("a".into()), Value::Text("payload".into())),
        ]),
    ];
    for value in &values {
        let expected = ValueCacheKey::Canonical(hash_value(value).unwrap());
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let measured = request(Resource::NestedValueSteps, 16_000_000);
            assert_eq!(literal_key(value, &measured, lane).unwrap(), expected);
            if !matches!(value, Value::Map(_)) {
                // Hash streaming does not reserve the encoded extent as a buffer.
                assert_eq!(measured.observed(Resource::TemporaryBytes), 0);
            }
            for resource in [
                Resource::NestedValueSteps,
                Resource::PredicateExpressionSteps,
                Resource::TemporaryBytes,
            ] {
                let exact = measured.observed(resource);
                if exact == 0 {
                    continue;
                }
                for limit in [exact - 1, exact, 2 * exact] {
                    let root = request(resource, limit);
                    for attempt in 1..=3 {
                        let result = literal_key(value, &root, lane);
                        if attempt * exact <= limit {
                            assert_eq!(result.unwrap(), expected);
                        } else {
                            let facts = result.unwrap_err().diagnostic_facts();
                            assert!(
                                facts
                                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                            );
                            assert!(
                                facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
                            );
                            break;
                        }
                    }
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                    assert_eq!(root.observed(Resource::QueryExecutions), 0);
                }
            }
        }
    }
}

#[test]
fn map_scratch_is_admitted_before_hashing_without_changing_canonical_order() {
    let entries = vec![
        (Value::Nat64(1), Value::Text("first".into())),
        (Value::Nat64(2), Value::Text("second".into())),
    ];
    let expected = hash_value(&Value::Map(entries.clone())).unwrap();
    let scratch = 4 * size_of::<&(Value, Value)>() as u64;
    for entries in [entries.clone(), entries.into_iter().rev().collect()] {
        let value = Value::Map(entries);
        let root = request(Resource::TemporaryBytes, scratch);
        assert_eq!(
            literal_key(&value, &root, Lane::PublicRead).unwrap(),
            ValueCacheKey::Canonical(expected)
        );
        assert_eq!(root.observed(Resource::TemporaryBytes), scratch);
        with_test_hash_override(Err(test_hash_budget_error), || {
            let denied = request(Resource::TemporaryBytes, scratch - 1);
            let error = literal_key(&value, &denied, Lane::PublicRead).unwrap_err();
            // A hash call would report the override's Mutation lane instead.
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::ExecutionLane, Lane::PublicRead.raw()))
            );
        });
    }
}

#[test]
fn literal_hash_failure_leaves_no_partial_memo_and_completed_reuse_skips_hashing() {
    let query = StructuralQuery::new(MissingRowPolicy::Ignore).projection_selection(
        ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
            expr: Expr::Literal(Value::List(vec![Value::Text("retained literal".into())])),
            alias: None,
        }]),
    );
    let build = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            query.structural_cache_key_with_normalized_predicate_fingerprint(None, work)
        })
    };
    let denied = request(Resource::NestedValueSteps, 0);
    for _ in 0..2 {
        assert!(build(&denied).unwrap_err().diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::NestedValueSteps.raw(),
        )));
    }
    let admitted = request(Resource::NestedValueSteps, 2);
    // A genuine writer failure also propagates without installing a memo.
    with_test_hash_override(Err(test_hash_budget_error), || {
        let fresh = request(Resource::NestedValueSteps, 2);
        let error = build(&fresh).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::ExecutionLane, Lane::Mutation.raw()))
        );
    });
    let key = build(&admitted).unwrap();
    assert_eq!(admitted.observed(Resource::NestedValueSteps), 2);
    assert_eq!(build(&admitted).unwrap(), key);
    assert_eq!(admitted.observed(Resource::NestedValueSteps), 2);
    assert_eq!(build(&denied).unwrap(), key);
}
