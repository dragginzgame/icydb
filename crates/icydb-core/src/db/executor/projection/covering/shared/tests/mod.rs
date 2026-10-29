//! Covering projection ownership, admission and hybrid component boundaries.

use super::*;
use crate::{
    db::{
        index::EncodedValue,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        query::plan::FieldSlot,
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            current_execution_budget_usage, with_execution_budget_for_tests,
        },
    },
    types::{Decimal, EntityTag, Ulid},
    value::ValueTag,
};

use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane, RuntimeBoundaryCode,
};

type ProjectRow =
    fn(&DecodedDataStoreKey, &[CoveringReadField], Value) -> Result<Vec<Value>, InternalError>;

const PROJECTORS: [ProjectRow; 3] = [
    |key, fields, value| project_covering_row_from_decoded_values(key, fields, &[2], &[value]),
    |key, fields, value| {
        project_covering_row_from_owned_decoded_values(key, fields, &[2], vec![value])
    },
    |key, fields, value| project_covering_row_from_single_decoded_value(key, fields, 2, value),
];

fn projection_fields(
    sources: impl IntoIterator<Item = CoveringReadFieldSource>,
) -> Vec<CoveringReadField> {
    sources
        .into_iter()
        .enumerate()
        .map(|(index, source)| CoveringReadField {
            field_slot: FieldSlot::from_test_slot(index, "test"),
            source,
        })
        .collect()
}

fn projection_key() -> DecodedDataStoreKey {
    DecodedDataStoreKey::new(
        EntityTag::new(1),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(42)),
    )
}

#[test]
fn covering_projectors_preserve_output_and_last_use_ownership() {
    let fields = projection_fields([
        CoveringReadFieldSource::IndexComponent { component_index: 2 },
        CoveringReadFieldSource::Constant(Value::Nat64(7)),
        CoveringReadFieldSource::PrimaryKey { component_index: 0 },
        CoveringReadFieldSource::IndexExpressionComponent { component_index: 2 },
    ]);
    for (ownership, project) in PROJECTORS.into_iter().enumerate() {
        let payload = "independently owned covering columns".to_string();
        let value = Value::Text(payload.clone());
        let Value::Text(text) = &value else {
            unreachable!()
        };
        let original_allocation = text.as_ptr();
        let row = project(&projection_key(), &fields, value).unwrap();
        assert_eq!(
            row,
            [
                Value::Text(payload.clone()),
                Value::Nat64(7),
                Value::Nat64(42),
                Value::Text(payload)
            ]
        );
        let (Value::Text(first), Value::Text(last)) = (&row[0], &row[3]) else {
            panic!("expected text columns")
        };
        assert_ne!(first.as_ptr(), last.as_ptr());
        assert_ne!(first.as_ptr(), original_allocation);
        assert_eq!(last.as_ptr() == original_allocation, ownership != 0);
    }
}

#[test]
fn covering_projectors_preserve_invariant_refusals_and_primary_key_output() {
    let key = projection_key();
    let expected = InternalError::query_executor_invariant();
    for source in [
        CoveringReadFieldSource::RowField,
        CoveringReadFieldSource::IndexComponent { component_index: 9 },
    ] {
        let fields = projection_fields([source]);
        for project in PROJECTORS {
            let error = project(&key, &fields, Value::Unit).unwrap_err();
            assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
            assert_eq!(error.diagnostic().detail(), expected.diagnostic().detail());
        }
    }
    let fields = projection_fields([CoveringReadFieldSource::PrimaryKey { component_index: 0 }]);
    for project in PROJECTORS {
        assert_eq!(
            project(&key, &fields, Value::Unit).unwrap(),
            [Value::Nat64(42)]
        );
    }
    // Malformed shape is rejected even when the primary-key fast path needs no components.
    for result in [
        project_covering_row_from_decoded_values(&key, &fields, &[2], &[]),
        project_covering_row_from_owned_decoded_values(&key, &fields, &[2], vec![]),
    ] {
        let error = result.unwrap_err();
        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
        assert_eq!(error.diagnostic().detail(), expected.diagnostic().detail());
    }
}

#[test]
fn covering_projectors_charge_once_before_source_admission() {
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0,
    );
    for project in PROJECTORS {
        for limit in [0, 3] {
            let budget = HardExecutionBudget::uniform_for_tests(
                32_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(
                DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
                limit,
            );
            let result = with_execution_budget_for_tests(
                budget,
                context,
                || {
                    let sources = if limit == 0 {
                        vec![CoveringReadFieldSource::RowField]
                    } else {
                        vec![CoveringReadFieldSource::IndexComponent { component_index: 2 }; 3]
                    };
                    let fields = projection_fields(sources);
                    project(&projection_key(), &fields, Value::Unit)?;
                    assert_eq!(
                        current_execution_budget_usage()?
                            .observed(DiagnosticExecutionBudgetResource::PredicateExpressionSteps),
                        3
                    );
                    Ok(())
                },
                std::convert::identity,
            );
            if limit == 0 {
                assert!(matches!(
                    result.unwrap_err().diagnostic().detail(),
                    Some(DiagnosticDetail::RuntimeBoundary {
                        boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
                    })
                ));
            } else {
                result.unwrap();
            }
        }
    }
}

#[test]
fn hybrid_components_preserve_supported_values_and_component_positions() {
    let values = [
        Value::Bool(true),
        Value::Int64(-42),
        Value::Nat64(42),
        Value::Text("sample".into()),
        Value::Ulid(Ulid::from_u128(7)),
        Value::Unit,
    ];
    let components = values
        .iter()
        .map(|value| EncodedValue::try_new(value).unwrap().into_bytes())
        .collect::<Vec<_>>();
    let indices = [1, 3, 4, 6, 7, 9];
    assert_eq!(
        decode_hybrid_covering_components(&indices, components.into()).unwrap(),
        Some(indices.into_iter().zip(values).collect())
    );
    assert_eq!(
        decode_hybrid_covering_components(&[], Vec::new().into()).unwrap(),
        Some(Vec::new())
    );
}

#[test]
fn hybrid_components_decline_the_whole_projection_after_a_supported_component() {
    let components = [Value::Nat64(3), Value::Decimal(Decimal::new(1250, 2))]
        .iter()
        .map(|value| EncodedValue::try_new(value).unwrap().into_bytes())
        .collect::<Vec<_>>();
    assert!(
        decode_hybrid_covering_components(&[0, 1], components.into())
            .unwrap()
            .is_none()
    );
}

#[test]
fn hybrid_components_preserve_typed_malformed_payload_errors() {
    for (component, expected) in [
        (
            Vec::new(),
            InternalError::bytes_covering_component_payload_empty(),
        ),
        (
            vec![ValueTag::Bool.to_u8()],
            InternalError::bytes_covering_bool_payload_truncated(),
        ),
        (
            vec![ValueTag::Bool.to_u8(), 2],
            InternalError::bytes_covering_bool_payload_invalid_value(),
        ),
        (
            vec![ValueTag::Bool.to_u8(), 1, 0],
            InternalError::bytes_covering_component_payload_invalid_length(),
        ),
    ] {
        let error = decode_hybrid_covering_components(&[0], vec![component].into()).unwrap_err();
        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
        assert_eq!(error.diagnostic().detail(), expected.diagnostic().detail());
    }
}
