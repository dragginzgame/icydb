//! Reads and fixed updates share accepted historical fills and corruption boundaries.

use super::*;
use crate::{
    db::{
        data::{
            CanonicalSlotReader, RawRow, SlotReader,
            persisted_row::{
                canonical::encode_canonical_value_for_accepted_field_contract,
                contract::emit_raw_row_from_slot_payloads,
            },
        },
        schema::{
            PersistedFieldOrigin, RowLayoutVersion, SchemaFieldWritePolicy, SchemaHistoricalFill,
        },
    },
    error::ErrorClass,
};

fn historical_contract(
    kind: AcceptedFieldKind,
    codec: LeafCodec,
    fill: SchemaHistoricalFill,
) -> StructuralRowContract {
    let current = RowLayoutVersion::INITIAL.checked_next().unwrap();
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            vec![],
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_with_write_policy_and_origin(
            FieldId::new(2),
            "added".into(),
            SchemaFieldSlot::new(1),
            kind,
            vec![],
            true,
            current,
            SchemaInsertDefault::None,
            fill,
            SchemaFieldWritePolicy::none(),
            PersistedFieldOrigin::SqlDdl,
            match codec {
                LeafCodec::Scalar(_) => FieldStorageDecode::ByKind,
                LeafCodec::Structural => FieldStorageDecode::CatalogValue,
            },
            codec,
        ),
    ];
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::HistoricalScalar".into(),
        "HistoricalScalar".into(),
        FieldId::new(1),
        SchemaRowLayout::new(
            current,
            RowLayoutVersion::INITIAL,
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        layout.row_decode_contract(AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        )),
    )
}

fn old_row(contract: &StructuralRowContract) -> RawRow {
    let key = encode_canonical_value_for_accepted_field_contract(
        contract
            .required_accepted_field_persistence_contract(0)
            .unwrap(),
        &Value::Nat64(1),
    )
    .unwrap();
    emit_raw_row_from_slot_payloads(RowLayoutVersion::INITIAL, 1, &[key])
        .unwrap()
        .into_raw_row()
}

#[test]
fn historical_scalar_views_and_projection_agree_with_materialization() {
    for (kind, scalar, value) in [
        (
            AcceptedFieldKind::Nat64,
            ScalarCodec::Nat64,
            Value::Nat64(7),
        ),
        (
            AcceptedFieldKind::Int64,
            ScalarCodec::Int64,
            Value::Int64(-7),
        ),
        (
            AcceptedFieldKind::Bool,
            ScalarCodec::Bool,
            Value::Bool(true),
        ),
        (
            AcceptedFieldKind::Text { max_len: None },
            ScalarCodec::Text,
            Value::Text("héllo".into()),
        ),
        (
            AcceptedFieldKind::Blob { max_len: None },
            ScalarCodec::Blob,
            Value::Blob(vec![0, 1, 255]),
        ),
    ] {
        for codec in [LeafCodec::Scalar(scalar), LeafCodec::Structural] {
            let encoding = historical_contract(kind.clone(), codec, SchemaHistoricalFill::Null);
            let payload = encode_canonical_value_for_accepted_field_contract(
                encoding
                    .required_accepted_field_persistence_contract(1)
                    .unwrap(),
                &value,
            )
            .unwrap();
            for fill in [
                SchemaHistoricalFill::Null,
                SchemaHistoricalFill::SlotPayload(payload),
            ] {
                let expected = if matches!(fill, SchemaHistoricalFill::Null) {
                    Value::Null
                } else {
                    value.clone()
                };
                let contract = historical_contract(kind.clone(), codec, fill);
                let row = old_row(&contract);
                let mut reader =
                    StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                        &row, &contract,
                    )
                    .unwrap();
                assert!(reader.get_bytes(1).is_none());
                for _ in 0..2 {
                    let scalar = match codec {
                        LeafCodec::Scalar(_) => reader.required_scalar(1).unwrap(),
                        LeafCodec::Structural => {
                            reader.required_value_storage_scalar(1).unwrap().unwrap()
                        }
                    };
                    assert_eq!(scalar.into_value(), expected);
                    assert_eq!(reader.required_cached_value(1).unwrap(), &expected);
                    assert_eq!(reader.take_direct_projection_value(1).unwrap(), expected);
                }
            }
        }
    }
}

#[test]
fn historical_scalar_reads_reject_forbidden_or_malformed_fills() {
    for codec in [LeafCodec::Scalar(ScalarCodec::Nat64), LeafCodec::Structural] {
        for fill in [
            SchemaHistoricalFill::Reject,
            SchemaHistoricalFill::SlotPayload(vec![255]),
        ] {
            let contract = historical_contract(AcceptedFieldKind::Nat64, codec, fill);
            let row = old_row(&contract);
            let mut reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
            let error = match codec {
                LeafCodec::Scalar(_) => reader.required_scalar(1).unwrap_err(),
                LeafCodec::Structural => reader.required_value_storage_scalar(1).unwrap_err(),
            };
            assert_eq!(error.class(), ErrorClass::Corruption);
            assert_eq!(
                reader.take_direct_projection_value(1).unwrap_err().class(),
                ErrorClass::Corruption
            );
            assert_eq!(
                contract
                    .historical_slot_value(1, contract.current_layout_version())
                    .unwrap_err()
                    .class(),
                ErrorClass::Corruption
            );
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn historical_update_compares_canonical_null_and_text_fills() {
    use crate::db::data::{AcceptedFixedUpdatePatch, FieldSlot};

    let text = Value::Text("frozen".into());
    for codec in [LeafCodec::Scalar(ScalarCodec::Text), LeafCodec::Structural] {
        let kind = AcceptedFieldKind::Text { max_len: Some(6) };
        let encoding = historical_contract(kind.clone(), codec, SchemaHistoricalFill::Null);
        let payload = encode_canonical_value_for_accepted_field_contract(
            encoding
                .required_accepted_field_persistence_contract(1)
                .unwrap(),
            &text,
        )
        .unwrap();
        for (fill, actual) in [
            (SchemaHistoricalFill::Null, Value::Null),
            (SchemaHistoricalFill::SlotPayload(payload), text.clone()),
        ] {
            let contract = historical_contract(kind.clone(), codec, fill);
            let historical = old_row(&contract);
            let current =
                canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                    Ok(Cow::Owned(if slot == 0 {
                        Value::Nat64(1)
                    } else {
                        actual.clone()
                    }))
                })
                .unwrap()
                .into_raw_row();
            for target in [Value::Null, text.clone(), Value::Text("new".into())] {
                let payload = encode_canonical_value_for_accepted_field_contract(
                    contract
                        .required_accepted_field_persistence_contract(1)
                        .unwrap(),
                    &target,
                )
                .unwrap();
                let patch = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
                    FieldSlot::from_validated_index(1),
                    payload,
                )])
                .unwrap();
                for raw in [&historical, &current] {
                    let row = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                        raw, &contract,
                    )
                    .unwrap();
                    assert_eq!(patch.is_satisfied_by(&row).unwrap(), actual == target);
                }
            }
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn historical_update_rejects_forbidden_and_malformed_fills() {
    use crate::db::data::{AcceptedFixedUpdatePatch, FieldSlot};

    for codec in [LeafCodec::Scalar(ScalarCodec::Nat64), LeafCodec::Structural] {
        for fill in [
            SchemaHistoricalFill::Reject,
            SchemaHistoricalFill::SlotPayload(vec![255]),
        ] {
            let contract = historical_contract(AcceptedFieldKind::Nat64, codec, fill);
            let payload = encode_canonical_value_for_accepted_field_contract(
                contract
                    .required_accepted_field_persistence_contract(1)
                    .unwrap(),
                &Value::Nat64(7),
            )
            .unwrap();
            let patch = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
                FieldSlot::from_validated_index(1),
                payload,
            )])
            .unwrap();
            let raw = old_row(&contract);
            let row =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &contract).unwrap();
            assert_eq!(
                patch.is_satisfied_by(&row).unwrap_err().class(),
                ErrorClass::Corruption
            );
        }
    }
}
