mod canonical_materialization;
mod direct_projection;
mod full_row;
mod historical_scalar;
mod owned_handoff;
mod primary_key_cache;

use super::{CachedSlotValue, StructuralSlotReader};
use crate::{
    db::{
        data::{
            StructuralRowContract, canonical_row_from_runtime_value_source_with_accepted_contract,
            persisted_row::types::SlotReader,
        },
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedRowLayoutRuntimeContract,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId,
            FieldStorageDecode, LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            TestEnumDefinition, TestEnumVariant, build_accepted_enum_catalog_for_tests,
            empty_accepted_enum_catalog_for_tests,
        },
    },
    value::{Value, ValueEnum},
};
use std::borrow::Cow;

fn selective_payload_contract() -> StructuralRowContract {
    payload_contract(
        AcceptedFieldKind::Blob {
            max_len: Some(512 * 1_024),
        },
        LeafCodec::Scalar(ScalarCodec::Blob),
        false,
    )
}

fn payload_contract(
    kind: AcceptedFieldKind,
    codec: LeafCodec,
    nullable: bool,
) -> StructuralRowContract {
    let storage_decode = match codec {
        LeafCodec::Scalar(_) => FieldStorageDecode::ByKind,
        LeafCodec::Structural => FieldStorageDecode::CatalogValue,
    };
    payload_contract_with_storage(kind, codec, nullable, storage_decode)
}

fn payload_contract_with_storage(
    kind: AcceptedFieldKind,
    codec: LeafCodec,
    nullable: bool,
    storage_decode: FieldStorageDecode,
) -> StructuralRowContract {
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "matches".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Bool,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Bool),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(3),
            "snapshot".to_string(),
            SchemaFieldSlot::new(2),
            kind,
            Vec::new(),
            nullable,
            SchemaInsertDefault::None,
            storage_decode,
            codec,
        ),
    ];
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::SelectivePayload".to_string(),
        "SelectivePayload".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("large-payload accepted row layout should build");
    let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );

    StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        descriptor.row_decode_contract(value_catalog),
    )
}

fn scalar_slot_is_untouched(value: &CachedSlotValue<'_>) -> bool {
    value.validated.get().is_none() && value.materialized.get().is_none()
}

#[test]
fn selected_constraint_slots_borrow_cached_values_in_full_layout_positions() {
    let contract = selective_payload_contract();
    let values = [
        Value::Nat64(7),
        Value::Bool(true),
        Value::Blob(vec![9; 1024]),
    ];
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row();
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
            .unwrap();

    let selected = reader.decode_selected_slot_values(&[2, 0]).unwrap();
    assert_eq!(selected.len(), 3);
    assert!(selected[1].is_none());
    for slot in [0, 2] {
        let Some(Cow::Borrowed(value)) = &selected[slot] else {
            panic!("borrowed cached value")
        };
        assert!(std::ptr::eq(
            *value,
            reader.required_cached_value(slot).unwrap()
        ));
        assert_eq!(**value, values[slot]);
    }
    assert!(
        reader
            .decode_selected_slot_values(&[])
            .unwrap()
            .iter()
            .all(Option::is_none)
    );
    assert!(reader.decode_selected_slot_values(&[2, 3]).is_err());
    // Constraint evaluation does not consume the reader: migration callers
    // still use the same accepted row for index/relation validation afterward.
    assert_eq!(reader.required_cached_value(2).unwrap(), &values[2]);
}

fn accepted_enum_contract() -> (StructuralRowContract, Value) {
    let enum_catalog = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        "tests::Status",
        vec![TestEnumVariant::unit("Active")],
    )])
    .expect("status enum catalog should build");
    let type_id = enum_catalog
        .type_id("tests::Status")
        .expect("status enum type should exist");
    let variant_id = enum_catalog
        .enum_type(type_id)
        .and_then(|definition| definition.variant_id("Active"))
        .expect("active enum variant should exist");
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "status".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Enum { type_id },
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
    ];
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::EnumProjection".to_string(),
        "EnumProjection".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("enum accepted row layout should build");
    let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
        enum_catalog,
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let contract = StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        descriptor.row_decode_contract(value_catalog),
    );
    let value = Value::Enum(ValueEnum::test_unit(type_id.get(), variant_id.get()));

    (contract, value)
}

#[test]
fn selective_small_field_read_does_not_decode_large_unrelated_payload() {
    let contract = selective_payload_contract();
    let values = [
        Value::Nat64(7),
        Value::Bool(false),
        Value::Blob(vec![0xA5; 300 * 1_024]),
    ];
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .expect("large-payload fixture row should encode")
    .into_raw_row();
    let reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract)
        .expect("large-payload fixture row should open lazily");

    assert!(reader.cached_values.get().is_none());
    assert!(reader.get_bytes(2).is_some());
    assert!(reader.get_bytes(0).is_some());
    assert!(reader.cached_values.get().is_none());
    assert_eq!(
        reader
            .required_cached_value(1)
            .expect("small residual-like metadata slot should decode"),
        &Value::Bool(false),
    );
    assert!(
        scalar_slot_is_untouched(&reader.cached_values()[2]),
        "rejecting on small metadata must not validate or materialize the unrelated snapshot",
    );
}

#[test]
fn projection_and_full_row_use_accepted_enum_wire() {
    let (contract, status) = accepted_enum_contract();
    let values = [Value::Nat64(7), status.clone()];
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .expect("accepted enum fixture row should encode")
    .into_raw_row();
    let mut reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract)
        .expect("accepted enum fixture row should open lazily");

    assert_eq!(
        reader
            .take_direct_projection_value(1)
            .expect("accepted enum direct projection should decode"),
        status,
    );
    let mut output = Vec::new();
    reader.decode_all_values_into(&mut output).unwrap();
    assert_eq!(output, values);
}
