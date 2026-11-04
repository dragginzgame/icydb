//! Retained output ownership, selective admission and scalar length overrides.

use super::*;
use crate::db::executor::terminal::{RetainedSlotLayout, RetainedSlotValueMode, RowDecoder};

#[test]
fn retained_sibling_paths_preserve_values_and_error_ownership() {
    let layout = RetainedSlotLayout::compile(2, vec![1]);
    let expressions: Vec<_> = ["city", "name", "city", "missing"]
        .into_iter()
        .map(|name| CompiledExpr::FieldPath {
            root_slot: 1,
            segments: vec!["profile".into(), name.into()].into_boxed_slice(),
            segment_bytes: vec![
                b"profile".to_vec().into_boxed_slice(),
                name.as_bytes().to_vec().into_boxed_slice(),
            ]
            .into_boxed_slice(),
        })
        .collect();
    let profile = Value::Map(vec![
        (Value::Text("name".into()), Value::Text("Ada".into())),
        (Value::Text("city".into()), Value::Null),
        (
            Value::Text("city".into()),
            Value::Text("second match".into()),
        ),
    ]);
    for (root, expected) in [
        (
            Value::Map(vec![(Value::Text("profile".into()), profile)]),
            vec![
                Value::Null,
                Value::Text("Ada".into()),
                Value::Null,
                Value::Null,
            ],
        ),
        (Value::Null, vec![Value::Null; 4]),
        (Value::Map(vec![]), vec![Value::Null; 4]),
        (
            Value::Map(vec![(Value::Text("profile".into()), Value::Null)]),
            vec![Value::Null; 4],
        ),
    ] {
        let row = RetainedSlotRow::from_indexed_values(&layout, vec![Some(root.clone())]);
        let mut shaped = Vec::new();
        project_retained_expressions_into(&expressions, &row, &mut shaped).unwrap();
        assert_eq!(shaped, expected);
        let general: Vec<_> = expressions
            .iter()
            .map(|expr| {
                eval_compiled_expr_with_value_ref_reader(expr, &mut |slot| row.slot_ref(slot))
                    .unwrap()
            })
            .collect();
        assert_eq!(shaped, general);
        assert_eq!(row.slot_ref(1), Some(&root));
    }
    for root in [
        None,
        Some(Value::Nat64(9)),
        Some(Value::Map(vec![(
            Value::Text("profile".into()),
            Value::Nat64(9),
        )])),
    ] {
        let row = RetainedSlotRow::from_indexed_values(&layout, vec![root]);
        let mut shaped = Vec::new();
        let error = project_retained_expressions_into(&expressions, &row, &mut shaped).unwrap_err();
        let general = eval_compiled_expr_with_value_ref_reader(&expressions[0], &mut |slot| {
            row.slot_ref(slot)
        })
        .unwrap_err()
        .into_internal_error();
        assert_eq!(error.class(), general.class());
        assert_eq!(error.origin(), general.origin());
        assert_eq!(error.diagnostic(), general.diagnostic());
        assert_eq!(error.diagnostic_facts(), general.diagnostic_facts());
        assert!(shaped.is_empty());
    }
}

#[test]
fn retained_output_moves_nested_cache_and_preserves_slot_order() {
    let contract = projection_contract(
        nested_kind(1024),
        LeafCodec::Structural,
        FieldStorageDecode::CatalogValue,
    );
    let (key, raw) = projection_row(&contract, &nested_payload(2, 1024));
    let reader =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &contract).unwrap();
    reader.validate_primary_key(&key).unwrap();
    let backing = |value: &Value| {
        let Value::List(items) = value else {
            panic!("list")
        };
        let Value::Map(entries) = &items[0] else {
            panic!("map")
        };
        let (Value::Text(key), Value::Blob(bytes)) = &entries[0] else {
            panic!("entry")
        };
        (
            items.as_ptr(),
            entries.as_ptr(),
            key.as_ptr(),
            bytes.as_ptr(),
        )
    };
    let before = backing(reader.required_cached_value(1).unwrap());
    let slots = RetainedSlotLayout::compile(2, vec![1, 0, 1]);
    let output = RowDecoder::decode_indexed_slot_values_from_reader(reader, &slots).unwrap();
    drop(raw);
    assert_eq!(
        output,
        vec![
            Some(nested_payload(2, 1024)),
            Some(Value::Nat64(7)),
            Some(nested_payload(2, 1024))
        ]
    );
    assert_eq!(backing(output[0].as_ref().unwrap()), before);
    assert_ne!(
        backing(output[0].as_ref().unwrap()),
        backing(output[2].as_ref().unwrap())
    );
}

#[test]
fn retained_output_preserves_null_empty_and_scalar_length_modes() {
    for (kind, codec, populated) in [
        (
            AcceptedFieldKind::Blob {
                max_len: Some(1024),
            },
            ScalarCodec::Blob,
            Value::Blob(vec![7; 1024]),
        ),
        (
            AcceptedFieldKind::Text {
                max_len: Some(1024),
            },
            ScalarCodec::Text,
            Value::Text("é".into()),
        ),
    ] {
        let contract =
            projection_contract(kind, LeafCodec::Scalar(codec), FieldStorageDecode::ByKind);
        let empty = match &populated {
            Value::Blob(_) => Value::Blob(vec![]),
            _ => Value::Text(String::new()),
        };
        for value in [populated, empty, Value::Null] {
            let length = match &value {
                Value::Blob(v) => Value::Nat64(v.len() as u64),
                Value::Text(v) => Value::Nat64(v.len() as u64),
                _ => Value::Null,
            };
            let (key, raw) = projection_row(&contract, &value);
            let reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &contract).unwrap();
            reader.validate_primary_key(&key).unwrap();
            let before = match reader.required_cached_value(1).unwrap() {
                Value::Blob(v) => Some(v.as_ptr()),
                Value::Text(v) => Some(v.as_ptr()),
                _ => None,
            };
            let slots = RetainedSlotLayout::compile_with_value_modes(
                2,
                vec![1, 0, 1],
                vec![
                    RetainedSlotValueMode::Normal,
                    RetainedSlotValueMode::Normal,
                    RetainedSlotValueMode::ScalarOctetLength,
                ],
            );
            let output =
                RowDecoder::decode_indexed_slot_values_from_reader(reader, &slots).unwrap();
            assert_eq!(
                output,
                vec![
                    Some(value.clone()),
                    Some(Value::Nat64(7)),
                    Some(length.clone())
                ]
            );
            let after = match output[0].as_ref().unwrap() {
                Value::Blob(v) => Some(v.as_ptr()),
                Value::Text(v) => Some(v.as_ptr()),
                _ => None,
            };
            assert_eq!(before, after);
            // The data-key entrypoint also owns a reader for mixed modes.
            let layout = RowLayout::from_structural_row_contract(contract.clone());
            assert_eq!(
                RowDecoder::decode_indexed_slot_values_from_data_key(&layout, &key, &raw, &slots)
                    .unwrap(),
                output
            );
            let lengths = RetainedSlotLayout::compile_with_value_modes(
                2,
                vec![1],
                vec![RetainedSlotValueMode::ScalarOctetLength],
            );
            let retained =
                RowDecoder::decode_retained_slots_from_data_key(&layout, &key, &raw, &lengths)
                    .unwrap();
            assert_eq!(retained.slot_ref(1), Some(&length));
        }
    }
    let contract = projection_contract(
        nested_kind(1024),
        LeafCodec::Structural,
        FieldStorageDecode::CatalogValue,
    );
    for value in [Value::Null, Value::List(vec![])] {
        let (_, raw) = projection_row(&contract, &value);
        let reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &contract).unwrap();
        assert_eq!(
            RowDecoder::decode_indexed_slot_values_from_reader(
                reader,
                &RetainedSlotLayout::compile(2, vec![1])
            )
            .unwrap(),
            vec![Some(value)]
        );
    }
}

#[test]
fn retained_output_preserves_selected_admission_and_key_rejection() {
    let wide = projection_contract(
        nested_kind(1024),
        LeafCodec::Structural,
        FieldStorageDecode::CatalogValue,
    );
    let narrow = projection_contract(
        nested_kind(8),
        LeafCodec::Structural,
        FieldStorageDecode::CatalogValue,
    );
    let (_, raw) = projection_row(&wide, &nested_payload(1, 9));
    let slots = RetainedSlotLayout::compile(2, vec![0]);
    let reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &narrow).unwrap();
    assert_eq!(
        RowDecoder::decode_indexed_slot_values_from_reader(reader, &slots).unwrap(),
        vec![Some(Value::Nat64(7))]
    );
    let reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &narrow).unwrap();
    let expected = reader.required_cached_value(1).unwrap_err();
    let error = RowDecoder::decode_indexed_slot_values_from_reader(
        reader,
        &RetainedSlotLayout::compile(2, vec![0, 1]),
    )
    .unwrap_err();
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    // A mixed-mode caller still validates the row key before materialization.
    let contract = projection_contract(
        AcceptedFieldKind::Blob {
            max_len: Some(1024),
        },
        LeafCodec::Scalar(ScalarCodec::Blob),
        FieldStorageDecode::ByKind,
    );
    let (_, raw) = projection_row(&contract, &Value::Blob(vec![1; 16]));
    let layout = RowLayout::from_structural_row_contract(contract);
    let wrong_key =
        DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(8).into());
    let slots = RetainedSlotLayout::compile_with_value_modes(
        2,
        vec![1],
        vec![RetainedSlotValueMode::ScalarOctetLength],
    );
    let error =
        RowDecoder::decode_indexed_slot_values_from_data_key(&layout, &wrong_key, &raw, &slots)
            .unwrap_err();
    assert_eq!(error.class(), ErrorClass::Corruption);
}
