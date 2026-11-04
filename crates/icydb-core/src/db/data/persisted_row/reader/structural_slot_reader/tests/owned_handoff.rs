//! Reader handoff retains exact row authority and only already-touched slots.

use super::*;
use crate::{
    db::{
        data::{CanonicalSlotReader, DataRow, DecodedDataStoreKey, ReadDataRow},
        key_taxonomy::PrimaryKeyComponent,
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            current_execution_budget_usage, with_execution_budget_for_tests,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope as Scope,
    DiagnosticExecutionLane as Lane, DiagnosticFactTag,
};
use std::cell::Cell;

fn data_row(contract: &StructuralRowContract, payload: &Value) -> DataRow {
    let values = [Value::Nat64(7), Value::Bool(true), payload.clone()];
    let raw = canonical_row_from_runtime_value_source_with_accepted_contract(contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row();
    (
        DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(7).into()),
        raw,
    )
}

#[test]
fn owned_and_iterator_handoffs_retain_scalar_admission_and_lazy_slots() {
    for (kind, value) in [
        (
            AcceptedFieldKind::Text { max_len: None },
            Value::Text("界İ".repeat(128)),
        ),
        (
            AcceptedFieldKind::Blob { max_len: None },
            Value::Blob(vec![7; 1024]),
        ),
        (AcceptedFieldKind::Nat64, Value::Nat64(u64::MAX)),
        (AcceptedFieldKind::Bool, Value::Null),
    ] {
        let codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
        let contract = payload_contract(kind, codec, true);
        for borrowed in [false, true] {
            let row = data_row(&contract, &value);
            let original_bytes = row.1.as_bytes().to_vec();
            let filter = |reader: &StructuralSlotReader<'_>| {
                assert_eq!(reader.get_scalar(2)?.unwrap().into_value(), value);
                Ok(true)
            };
            let admitted = if borrowed {
                ReadDataRow::filter_borrowed(row.0.clone(), &row.1, &contract, filter)
            } else {
                ReadDataRow::filter(row, &contract, filter)
            }
            .unwrap()
            .unwrap();
            // Moving the owner cannot separate validation from its immutable bytes.
            let mut moved = vec![admitted];
            let admitted = moved.pop().unwrap();
            assert_eq!(admitted.data_row().1.as_bytes(), original_bytes);
            admitted
                .with_reader(&contract, |key, mut reader| {
                    let cached = reader.cached_values.get().unwrap();
                    assert!(cached[2].validated.get().is_some());
                    assert!(cached[2].materialized.get().is_none());
                    assert!(scalar_slot_is_untouched(&cached[0]));
                    assert!(scalar_slot_is_untouched(&cached[1]));
                    reader.validate_primary_key(key)?;
                    assert_eq!(reader.take_required_value(2)?, value);
                    assert!(scalar_slot_is_untouched(
                        &reader.cached_values.get().unwrap()[1]
                    ));
                    Ok(())
                })
                .unwrap();
        }
    }
}

#[test]
fn structural_materialization_moves_without_cloning_and_contract_rebinding_refuses() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: None }));
    let contract = payload_contract(kind, LeafCodec::Structural, false);
    let value = Value::List(vec![Value::Blob(vec![7; 1024])]);
    let pointer = Cell::new(std::ptr::null());
    let admitted = ReadDataRow::filter(data_row(&contract, &value), &contract, |reader| {
        let Value::List(items) = reader.required_cached_value(2)? else {
            panic!("list")
        };
        pointer.set(items.as_ptr());
        Ok(true)
    })
    .unwrap()
    .unwrap();
    admitted
        .with_reader(&contract, |_, mut reader| {
            let Value::List(items) = reader.take_required_value(2)? else {
                panic!("list")
            };
            assert_eq!(items.as_ptr(), pointer.get());
            assert_eq!(Value::List(items), value);
            Ok(())
        })
        .unwrap();

    let admitted = ReadDataRow::filter(data_row(&contract, &value), &contract, |_| Ok(true))
        .unwrap()
        .unwrap();
    let wrong = selective_payload_contract();
    let error = admitted
        .with_reader::<()>(&wrong, |_, _| panic!("different contract cannot evaluate"))
        .unwrap_err();
    assert_eq!(
        error.diagnostic(),
        InternalError::query_executor_invariant().diagnostic()
    );
    assert!(
        ReadDataRow::filter(data_row(&contract, &value), &contract, |_| Ok(false))
            .unwrap()
            .is_none()
    );
}

#[test]
fn handoff_backing_has_exact_and_one_short_typed_refusals() {
    let contract = selective_payload_contract();
    let value = Value::Blob(vec![7; 1024]);
    let run = |limit| {
        let budget = HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, limit);
        with_execution_budget_for_tests(
            budget,
            HardExecutionContext::new(Scope::Execution, Lane::TrustedRead, 0),
            || {
                let row = ReadDataRow::filter(data_row(&contract, &value), &contract, |reader| {
                    reader.get_scalar(2)?;
                    Ok(true)
                })?
                .unwrap();
                row.with_reader(&contract, |_, mut reader| {
                    assert_eq!(reader.take_required_value(2)?, value);
                    Ok(())
                })?;
                Ok::<_, InternalError>(
                    current_execution_budget_usage()?.observed(Resource::TemporaryBytes),
                )
            },
            std::convert::identity,
        )
    };
    let exact = run(u64::MAX).unwrap();
    assert!(exact > 0);
    assert_eq!(run(exact).unwrap(), exact);
    let error = run(exact - 1).unwrap_err();
    for fact in [
        (
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw(),
        ),
        (DiagnosticFactTag::Limit, exact - 1),
        (DiagnosticFactTag::Actual, exact),
        (DiagnosticFactTag::ExecutionLane, Lane::TrustedRead.raw()),
    ] {
        assert!(error.diagnostic_facts().contains(&fact), "{error:?}");
    }
}

#[test]
fn handoff_preserves_untouched_corruption_and_primary_key_refusal_order() {
    use crate::db::data::persisted_row::{
        canonical::encode_canonical_value_for_accepted_field_contract,
        contract::emit_raw_row_from_slot_payloads,
    };
    let contract = payload_contract(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob { max_len: None })),
        LeafCodec::Structural,
        false,
    );
    let values = [
        Value::Nat64(7),
        Value::Bool(true),
        Value::List(vec![Value::Blob(vec![7; 16])]),
    ];
    let mut payloads = values
        .iter()
        .enumerate()
        .map(|(slot, value)| {
            encode_canonical_value_for_accepted_field_contract(
                contract
                    .required_accepted_field_persistence_contract(slot)
                    .unwrap(),
                value,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    payloads[2].pop();
    let raw = emit_raw_row_from_slot_payloads(
        contract.current_layout_version(),
        contract.field_count(),
        &payloads,
    )
    .unwrap()
    .into_raw_row();
    let expected = StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &contract)
        .unwrap()
        .required_cached_value(2)
        .unwrap_err();
    for borrowed in [false, true] {
        let key =
            DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(8).into());
        let expected_key =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &contract)
                .unwrap()
                .validate_primary_key(&key)
                .unwrap_err();
        let filter = |reader: &StructuralSlotReader<'_>| {
            assert_eq!(reader.required_scalar(1)?.into_value(), Value::Bool(true));
            Ok(true)
        };
        let admitted = if borrowed {
            ReadDataRow::filter_borrowed(key, &raw, &contract, filter)
        } else {
            ReadDataRow::filter((key, raw.clone()), &contract, filter)
        }
        .unwrap()
        .unwrap();
        assert!(admitted.retained_backing_bytes() > raw.len() as u64);
        admitted
            .with_reader(&contract, |key, reader| {
                assert!(scalar_slot_is_untouched(
                    &reader.cached_values.get().unwrap()[2]
                ));
                assert_eq!(
                    reader.required_cached_value(2).unwrap_err().diagnostic(),
                    expected.diagnostic()
                );
                let key_error = reader.validate_primary_key(key).unwrap_err();
                assert_eq!(key_error.diagnostic(), expected_key.diagnostic());
                assert_eq!(
                    key_error.diagnostic_facts(),
                    expected_key.diagnostic_facts()
                );
                // A failed selected read cannot fill the unadmitted cache cell.
                assert!(scalar_slot_is_untouched(
                    &reader.cached_values.get().unwrap()[2]
                ));
                Ok(())
            })
            .unwrap();
    }
}
