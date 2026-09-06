//! Direct raw-row projection ownership and selective admission.

mod retained_output;

use super::*;
use crate::{
    db::{
        data::{
            DecodedDataStoreKey, StructuralRowContract, StructuralSlotReader,
            canonical_row_from_runtime_value_source_with_accepted_contract,
        },
        key_taxonomy::PrimaryKeyComponent,
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedRowLayoutRuntimeContract,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId,
            FieldStorageDecode, LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            empty_accepted_enum_catalog_for_tests,
        },
    },
    error::ErrorClass,
    types::EntityTag,
};
use std::borrow::Cow;

fn nested_kind(max_len: u32) -> AcceptedFieldKind {
    AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
        value: Box::new(AcceptedFieldKind::Blob {
            max_len: Some(max_len),
        }),
    }))
}

fn nested_payload(items: usize, bytes: usize) -> Value {
    Value::List(
        (0..items)
            .map(|_| {
                Value::Map(vec![(
                    Value::Text("payload".into()),
                    Value::Blob(vec![7; bytes]),
                )])
            })
            .collect(),
    )
}

fn projection_contract(
    kind: AcceptedFieldKind,
    codec: LeafCodec,
    storage: FieldStorageDecode,
) -> StructuralRowContract {
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".into(),
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
            "payload".into(),
            SchemaFieldSlot::new(1),
            kind,
            Vec::new(),
            true,
            SchemaInsertDefault::None,
            storage,
            codec,
        ),
    ];
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::DirectProjection".into(),
        "DirectProjection".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        descriptor.row_decode_contract(catalog),
    )
}

fn projection_row(contract: &StructuralRowContract, payload: &Value) -> DataRow {
    let id = Value::Nat64(7);
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(contract, |slot| {
        Ok(Cow::Borrowed(if slot == 0 { &id } else { payload }))
    })
    .unwrap()
    .into_raw_row();
    (
        DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(7).into()),
        row,
    )
}

#[test]
fn direct_projection_preserves_order_repeated_ownership_empty_and_null() {
    let layout = RowLayout::from_structural_row_contract(projection_contract(
        nested_kind(1024),
        LeafCodec::Structural,
        FieldStorageDecode::CatalogValue,
    ));
    let slots = PreparedDirectProjectionSlots::from_slots(vec![1, 0, 1]);
    for payload in [
        nested_payload(2, 1024),
        Value::List(Vec::new()),
        Value::Null,
    ] {
        let row = projection_row(layout.contract(), &payload);
        let output = project_data_row_from_direct_slots(&layout, &row, &slots).unwrap();
        assert_eq!(output, vec![payload.clone(), Value::Nat64(7), payload]);
        if let (Value::List(first), Value::List(last)) = (&output[0], &output[2])
            && !first.is_empty()
        {
            assert_ne!(first.as_ptr(), last.as_ptr());
        }
    }
}

#[test]
fn direct_projection_preserves_scalar_storage_paths() {
    for storage in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
        let layout = RowLayout::from_structural_row_contract(projection_contract(
            AcceptedFieldKind::Blob {
                max_len: Some(4096),
            },
            LeafCodec::Scalar(ScalarCodec::Blob),
            storage,
        ));
        for payload in [
            Value::Blob(vec![9; 4096]),
            Value::Blob(Vec::new()),
            Value::Null,
        ] {
            let row = projection_row(layout.contract(), &payload);
            for order in [vec![1], vec![1, 1]] {
                let expected = vec![payload.clone(); order.len()];
                let slots = PreparedDirectProjectionSlots::from_slots(order);
                assert_eq!(
                    project_data_row_from_direct_slots(&layout, &row, &slots).unwrap(),
                    expected
                );
            }
        }
    }
}

#[test]
fn direct_projection_validates_selected_slots_and_primary_key_only() {
    let wider = projection_contract(
        nested_kind(16),
        LeafCodec::Structural,
        FieldStorageDecode::CatalogValue,
    );
    let row = projection_row(&wider, &nested_payload(1, 9));
    let layout = RowLayout::from_structural_row_contract(projection_contract(
        nested_kind(8),
        LeafCodec::Structural,
        FieldStorageDecode::CatalogValue,
    ));
    let id_only = PreparedDirectProjectionSlots::from_slots(vec![0]);
    assert_eq!(
        project_data_row_from_direct_slots(&layout, &row, &id_only).unwrap(),
        vec![Value::Nat64(7)]
    );
    for order in [vec![1], vec![0, 1, 1]] {
        let slots = PreparedDirectProjectionSlots::from_slots(order);
        assert_eq!(
            project_data_row_from_direct_slots(&layout, &row, &slots)
                .unwrap_err()
                .class(),
            ErrorClass::Corruption
        );
    }
    let (_, raw) = row;
    let wrong_key =
        DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(8).into());
    assert_eq!(
        project_data_row_from_direct_slots(&layout, &(wrong_key, raw), &id_only)
            .unwrap_err()
            .class(),
        ErrorClass::Corruption
    );
}

#[test]
fn by_kind_materialization_preserves_empty_null_nested_and_repeated_values() {
    let layout = RowLayout::from_structural_row_contract(projection_contract(
        nested_kind(1024),
        LeafCodec::Structural,
        FieldStorageDecode::ByKind,
    ));
    assert!(
        !layout
            .contract()
            .required_accepted_field_decode_contract(1)
            .unwrap()
            .uses_canonical_value_wire()
    );
    let slots = PreparedDirectProjectionSlots::from_slots(vec![1, 0, 1]);
    for payload in [Value::Null, Value::List(vec![]), nested_payload(2, 1024)] {
        let row = projection_row(layout.contract(), &payload);
        assert_eq!(
            project_data_row_from_direct_slots(&layout, &row, &slots).unwrap(),
            vec![payload.clone(), Value::Nat64(7), payload.clone()],
        );
        let mut full = Vec::new();
        layout
            .decode_full_value_row_from_data_key_into(&row.0, &row.1, &mut full)
            .unwrap();
        assert_eq!(full, vec![Value::Nat64(7), payload.clone()]);
        let mut reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row.1, layout.contract())
                .unwrap();
        let taken = reader.take_direct_projection_value(1).unwrap();
        assert_eq!(reader.required_cached_value(1).unwrap(), &taken);
        assert_eq!(taken, payload);
    }
}

#[test]
fn by_kind_lazy_and_eager_materialization_agree_on_corruption() {
    let source = projection_contract(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        LeafCodec::Structural,
        FieldStorageDecode::ByKind,
    );
    let row = projection_row(&source, &Value::List(vec![Value::Nat64(1)]));
    let layout = RowLayout::from_structural_row_contract(projection_contract(
        nested_kind(1024),
        LeafCodec::Structural,
        FieldStorageDecode::ByKind,
    ));
    let id_only = PreparedDirectProjectionSlots::from_slots(vec![0]);
    assert_eq!(
        project_data_row_from_direct_slots(&layout, &row, &id_only).unwrap(),
        vec![Value::Nat64(7)]
    );
    let eager = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
        &row.1,
        layout.contract(),
    )
    .err()
    .expect("invalid unselected field");
    assert_eq!(eager.class(), ErrorClass::Corruption);
    let mut reader =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row.1, layout.contract())
            .unwrap();
    for _ in 0..2 {
        let error = reader.take_direct_projection_value(1).unwrap_err();
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.diagnostic_code(), eager.diagnostic_code());
    }
    let slots = PreparedDirectProjectionSlots::from_slots(vec![1, 1]);
    let error = project_data_row_from_direct_slots(&layout, &row, &slots).unwrap_err();
    assert_eq!(error.diagnostic_code(), eager.diagnostic_code());
}
