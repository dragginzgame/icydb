//! Scalar backing is reserved once, with query admission before encoding.

use super::{admit_query_index_component, component_capacity, encode_canonical_index_component};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::preparation::PreparationWork,
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
        Timestamp, U256, Ulid,
    },
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::cell::Cell;

#[test]
fn scalar_admission_covers_shared_capacity_and_rejects_before_encoding() {
    let samples = [
        (Value::Unit, 1),
        (Value::Bool(true), 2),
        (Value::Date(Date::try_new(2024, 1, 1).unwrap()), 5),
        (Value::Duration(Duration::from_secs(1)), 9),
        (Value::Timestamp(Timestamp::from_secs(1)), 9),
        (Value::Float32(Float32::try_new(-0.0).unwrap()), 5),
        (Value::Float64(Float64::try_new(-0.0).unwrap()), 9),
        (Value::Int64(i64::MIN), 9),
        (Value::Nat64(u64::MAX), 9),
        (Value::Int128(i128::MIN), 17),
        (Value::Nat128(u128::MAX), 17),
        (Value::Ulid(Ulid::from_u128(1)), 17),
        (Value::U256(U256::MAX), 33),
        (Value::Subaccount(Subaccount::from_array([1; 32])), 33),
        (
            Value::Account(Account::from_owner_and_subaccount(
                Principal::from_slice(&[0; 29]),
                None,
            )),
            63,
        ),
        (Value::Principal(Principal::from_slice(&[0; 29])), 61),
        (Value::Text(String::new()), 3),
        (Value::Text("a\0é".into()), 11),
        // Comparison operands keep their existing domain beyond stored-key caps.
        (Value::Text("x".repeat(4096)), 8195),
        (Value::IntBig(IntBig::from(0)), 2),
        (Value::IntBig(IntBig::from(-256)), 6),
        (Value::NatBig(NatBig::from(0u64)), 3),
        (Value::NatBig(NatBig::from(256u64)), 5),
        (Value::Decimal(Decimal::new(0, 0)), 46),
        (Value::Decimal(Decimal::new(-11, 1)), 46),
    ];
    for (value, bytes) in samples {
        assert_eq!(component_capacity(&value).unwrap(), bytes);
        let expected = encode_canonical_index_component(&value).unwrap();
        assert_eq!(expected.capacity(), bytes);
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
                let root = RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(resource, 2 * bytes as u64 - 1),
                );
                for invocation in 0..2 {
                    let encoded = Cell::new(false);
                    let result = PreparationWork::run(&root.scope(), lane, |work| {
                        admit_query_index_component(&value, work).map_err(QueryError::execute)?;
                        encoded.set(true);
                        encode_canonical_index_component(&value)
                            .map_err(|error| QueryError::execute(error.into()))
                    });
                    if invocation == 0 {
                        assert_eq!(result.unwrap(), expected);
                        assert!(encoded.get());
                        assert_eq!(root.observed(resource), bytes as u64);
                    } else {
                        let error = result.unwrap_err();
                        assert!(!encoded.get());
                        assert!(
                            error
                                .diagnostic_facts()
                                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                    }
                }
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn accepted_enum_output_admission_uses_the_catalog_key_width() {
    let value = Value::Enum(ValueEnum::test_unit(1, 1));
    for limit in [10, 11] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, limit),
        );
        let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            admit_query_index_component(&value, work).map_err(QueryError::execute)
        });
        assert_eq!(result.is_ok(), limit == 11);
        assert_eq!(root.observed(Resource::TemporaryBytes), 11);
        // Budget admission does not replace accepted enum validation or make
        // generic scalar encoding responsible for catalog identities.
        assert!(encode_canonical_index_component(&value).is_err());
    }
}
