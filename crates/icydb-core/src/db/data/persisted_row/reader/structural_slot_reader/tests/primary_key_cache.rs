//! Key checks and projections consume the same admitted scalar/materialized value.

use super::*;
use crate::{
    db::{
        data::{
            RawRow, StructuralRowFieldBytes,
            persisted_row::{
                canonical::encode_canonical_value_for_accepted_field_contract,
                codec::{ScalarSlotValueRef, encode_scalar_slot_value},
                contract::emit_raw_row_from_slot_payloads,
                reader::primary_key::validate_primary_key_value_from_field_bytes,
            },
        },
        key_taxonomy::{
            CompositePrimaryKeyValue, CompositePrimaryKeyValueError, PrimaryKeyComponent,
            PrimaryKeyValue,
        },
    },
    error::InternalError,
    types::{Account, Principal, Subaccount, Timestamp, U256, Ulid},
};

fn key_contract(
    kind: AcceptedFieldKind,
    storage: FieldStorageDecode,
    composite: bool,
) -> StructuralRowContract {
    let fields = [
        (AcceptedFieldKind::Bool, FieldStorageDecode::ByKind),
        (kind, storage),
        (AcceptedFieldKind::Nat64, FieldStorageDecode::ByKind),
    ]
    .into_iter()
    .enumerate()
    .map(|(slot, (kind, storage))| {
        let codec = kind.leaf_codec_for_storage(storage);
        PersistedFieldSnapshot::new_initial(
            FieldId::new(u32::try_from(slot + 1).unwrap()),
            format!("field_{slot}"),
            SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
            kind,
            vec![],
            false,
            SchemaInsertDefault::None,
            storage,
            codec,
        )
    })
    .collect::<Vec<_>>();
    let ids = if composite {
        vec![FieldId::new(3), FieldId::new(2)]
    } else {
        vec![FieldId::new(2)]
    };
    let accepted = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::PrimaryCache".into(),
        "PrimaryCache".into(),
        ids,
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ))
    .unwrap();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        descriptor.row_decode_contract(AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        )),
    )
}

fn key_cases() -> Vec<(AcceptedFieldKind, Value)> {
    vec![
        (AcceptedFieldKind::Nat8, Value::Nat64(u64::from(u8::MAX))),
        (AcceptedFieldKind::Nat16, Value::Nat64(u64::from(u16::MAX))),
        (AcceptedFieldKind::Nat32, Value::Nat64(u64::from(u32::MAX))),
        (AcceptedFieldKind::Nat64, Value::Nat64(u64::MAX)),
        (AcceptedFieldKind::Int8, Value::Int64(i64::from(i8::MIN))),
        (AcceptedFieldKind::Int16, Value::Int64(i64::from(i16::MIN))),
        (AcceptedFieldKind::Int32, Value::Int64(i64::from(i32::MIN))),
        (AcceptedFieldKind::Int64, Value::Int64(i64::MIN)),
        (
            AcceptedFieldKind::Principal,
            Value::Principal(Principal::from_slice(&[7; 29])),
        ),
        (
            AcceptedFieldKind::Subaccount,
            Value::Subaccount(Subaccount::from_array([7; 32])),
        ),
        (
            AcceptedFieldKind::Timestamp,
            Value::Timestamp(Timestamp::from_millis(i64::MIN)),
        ),
        (
            AcceptedFieldKind::Ulid,
            Value::Ulid(Ulid::from_u128(u128::MAX)),
        ),
        (AcceptedFieldKind::Unit, Value::Unit),
        (AcceptedFieldKind::U256, Value::U256(U256::MAX)),
        (AcceptedFieldKind::Int128, Value::Int128(i128::MIN)),
        (AcceptedFieldKind::Nat128, Value::Nat128(u128::MAX)),
        (
            AcceptedFieldKind::Account,
            Value::Account(Account::from_owner_and_subaccount(
                Principal::from_slice(&[7; 29]),
                Some(Subaccount::from_array([7; 32])),
            )),
        ),
    ]
}

#[test]
fn key_validation_retains_each_admitted_value_for_projection_and_rereads() {
    for (kind, value) in key_cases() {
        for storage in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
            for composite in [false, true] {
                let component = PrimaryKeyComponent::from_runtime_value(&value).unwrap();
                let expected = if composite {
                    match CompositePrimaryKeyValue::try_from_components(&[
                        PrimaryKeyComponent::Nat64(9),
                        component,
                    ]) {
                        Ok(key) => PrimaryKeyValue::Composite(key),
                        Err(error) => {
                            // The canonical key owner admits Unit only as a scalar.
                            assert!(matches!(
                                error,
                                CompositePrimaryKeyValueError::UnitComponent { index: 1 }
                            ));
                            continue;
                        }
                    }
                } else {
                    PrimaryKeyValue::Scalar(component)
                };
                let contract = key_contract(kind.clone(), storage, composite);
                let values = [Value::Bool(true), value.clone(), Value::Nat64(9)];
                let row = canonical_row_from_runtime_value_source_with_accepted_contract(
                    &contract,
                    |slot| Ok(Cow::Borrowed(&values[slot])),
                )
                .unwrap()
                .into_raw_row();
                let spans =
                    StructuralRowFieldBytes::from_raw_row_with_contract(&row, &contract).unwrap();
                validate_primary_key_value_from_field_bytes(&contract, &spans, &expected).unwrap();
                for warm in [false, true] {
                    let mut reader =
                        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract)
                            .unwrap();
                    if warm {
                        for slot in contract.primary_key_slot_indices() {
                            reader.required_cached_value(*slot).unwrap();
                        }
                    }
                    for _ in 0..2 {
                        reader.validate_primary_key_value(&expected).unwrap();
                    }
                    for &slot in contract.primary_key_slot_indices() {
                        match &reader.cached_values[slot] {
                            CachedSlotValue::Scalar { validated, .. } => {
                                assert!(validated.get().is_some());
                            }
                            CachedSlotValue::Deferred { materialized } => {
                                assert!(materialized.get().is_some());
                            }
                        }
                    }
                    for &slot in contract.primary_key_slot_indices() {
                        assert_eq!(
                            reader.take_direct_projection_value(slot).unwrap(),
                            values[slot]
                        );
                        assert_eq!(reader.required_cached_value(slot).unwrap(), &values[slot]);
                    }
                }
            }
        }
    }
}

#[test]
fn key_mismatches_keep_the_typed_error_and_do_not_poison_the_cache() {
    for composite in [false, true] {
        let contract = key_contract(
            AcceptedFieldKind::Nat64,
            FieldStorageDecode::ByKind,
            composite,
        );
        let values = [Value::Bool(true), Value::Nat64(7), Value::Nat64(9)];
        let row =
            canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .unwrap()
            .into_raw_row();
        let keys = if composite {
            let make = |parts: &[u64]| {
                PrimaryKeyValue::Composite(
                    CompositePrimaryKeyValue::try_from_components(
                        &parts
                            .iter()
                            .copied()
                            .map(PrimaryKeyComponent::Nat64)
                            .collect::<Vec<_>>(),
                    )
                    .unwrap(),
                )
            };
            (
                make(&[9, 7]),
                vec![
                    (make(&[9, 99]), InternalError::persisted_row_key_mismatch()),
                    (make(&[7, 9]), InternalError::persisted_row_key_mismatch()),
                    (
                        make(&[9, 7, 8]),
                        InternalError::persisted_row_decode_corruption(),
                    ),
                ],
            )
        } else {
            (
                PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
                vec![(
                    PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(99)),
                    InternalError::persisted_row_key_mismatch(),
                )],
            )
        };
        for (wrong, expected) in keys.1 {
            let reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
            let error = reader.validate_primary_key_value(&wrong).unwrap_err();
            assert_eq!(error.diagnostic(), expected.diagnostic());
            assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
            reader.validate_primary_key_value(&keys.0).unwrap();
            assert_eq!(reader.required_cached_value(1).unwrap(), &values[1]);
        }
    }
}

#[test]
fn malformed_key_payloads_match_direct_validation_without_cached_errors() {
    for storage in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
        let kind = AcceptedFieldKind::Nat64;
        let codec = kind.leaf_codec_for_storage(storage);
        let contract = key_contract(kind, storage, false);
        let encode = |slot, value: &Value| {
            encode_canonical_value_for_accepted_field_contract(
                contract
                    .required_accepted_field_persistence_contract(slot)
                    .unwrap(),
                value,
            )
            .unwrap()
        };
        let valid = encode(1, &Value::Nat64(7));
        let mut trailing = valid.clone();
        trailing.push(0);
        let mut truncated = valid;
        truncated.pop();
        let null = match codec {
            LeafCodec::Scalar(codec) => {
                encode_scalar_slot_value(ScalarSlotValueRef::Null, codec).unwrap()
            }
            LeafCodec::Structural => {
                crate::db::data::structural_field::encode_canonical_value_storage_bytes(
                    &Value::Null,
                )
                .unwrap()
            }
        };
        for payload in [null, trailing, truncated, vec![0]] {
            let row: RawRow = emit_raw_row_from_slot_payloads(
                contract.current_layout_version(),
                3,
                &[
                    encode(0, &Value::Bool(true)),
                    payload,
                    encode(2, &Value::Nat64(9)),
                ],
            )
            .unwrap()
            .into_raw_row();
            let expected = PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7));
            let spans =
                StructuralRowFieldBytes::from_raw_row_with_contract(&row, &contract).unwrap();
            let direct = validate_primary_key_value_from_field_bytes(&contract, &spans, &expected)
                .unwrap_err();
            let reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
            for _ in 0..2 {
                let error = reader.validate_primary_key_value(&expected).unwrap_err();
                assert_eq!(error.diagnostic(), direct.diagnostic());
                assert_eq!(error.diagnostic_facts(), direct.diagnostic_facts());
            }
        }
    }
}
