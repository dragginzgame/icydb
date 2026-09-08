//! Owned admitted-value handoff and its normalization/persistence boundaries.

use super::*;
use crate::{
    db::{
        data::{
            StructuralRowContract, decode_runtime_value_from_row_contract,
            encode_canonical_value_for_accepted_field_contract,
        },
        schema::{
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot,
            AcceptedValueAdmissionContract, FieldId, LeafCodec, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaInsertDefault,
            SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
        },
    },
    error::ErrorClass,
    types::{IntBig, NatBig},
};

fn catalog() -> AcceptedValueCatalogHandle {
    AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    )
}

fn nested_kind() -> AcceptedFieldKind {
    AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
        value: Box::new(AcceptedFieldKind::Blob {
            max_len: Some(4096),
        }),
    }))
}

fn nested_input(items: usize, bytes: usize) -> InputValue {
    InputValue::list(
        (0..items)
            .map(|_| {
                InputValue::map(vec![(
                    InputValue::text("payload".into()),
                    InputValue::blob(vec![7; bytes]),
                )])
            })
            .collect(),
    )
}

fn admission(
    catalog: &AcceptedValueCatalogHandle,
    kind: AcceptedFieldKind,
) -> AcceptedValueAdmissionContract<'_> {
    let contract = AcceptedValueContract::from_accepted_field(
        catalog,
        &kind,
        FieldStorageDecode::CatalogValue,
    )
    .unwrap();
    AcceptedValueAdmissionContract::owned(catalog, contract, true)
}

fn row_contract(
    catalog: AcceptedValueCatalogHandle,
    kind: AcceptedFieldKind,
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
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
    ];
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::OwnedHandoff".into(),
        "OwnedHandoff".into(),
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
    StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        descriptor.row_decode_contract(catalog),
    )
}

#[test]
fn owned_handoff_consumption_preserves_nested_heap_backing() {
    let catalog = catalog();
    let admission = admission(&catalog, nested_kind());
    let value = Value::List(vec![Value::Map(vec![(
        Value::Text("payload".into()),
        Value::Blob(vec![7; 128]),
    )])]);
    // Record every heap-owning layer while the source allocations are still live.
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
    let before = backing(&value);
    let admitted = admission
        .admit_canonical(value, &mut ValueAdmissionBudget::standard())
        .unwrap();
    let value = admitted.into_value();
    assert_eq!(backing(&value), before);
}

#[test]
fn owned_handoff_normalization_and_persistence_preserve_nested_and_null_values() {
    let catalog = catalog();
    let admission = admission(&catalog, nested_kind());
    let row = row_contract(catalog.clone(), nested_kind());
    for input in [
        nested_input(3, 128),
        InputValue::list(Vec::new()),
        InputValue::null(),
    ] {
        let value = admission
            .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
            .unwrap();
        let encoded = encode_value(&row, &value);
        assert_eq!(
            decode_runtime_value_from_row_contract(&row, 1, &encoded).unwrap(),
            value
        );
    }
}

#[test]
fn owned_handoff_rejects_invalid_input_and_persisted_values_before_returning() {
    let catalog = catalog();
    let admission = admission(&catalog, nested_kind());
    for (input, expected) in [
        (InputValue::nat64(1), ValueAdmissionError::TypeMismatch),
        (nested_input(1, 4097), ValueAdmissionError::ScalarConstraint),
    ] {
        assert_eq!(
            admission
                .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
                .unwrap_err(),
            expected
        );
    }
    let valid = admission
        .normalize_input_to_runtime(nested_input(1, 128), &mut ValueAdmissionBudget::standard())
        .unwrap();
    let row = row_contract(catalog.clone(), nested_kind());
    let mut truncated = encode_value(&row, &valid);
    truncated.pop();
    let wrong_kind = encode_value(
        &row_contract(catalog.clone(), AcceptedFieldKind::Nat64),
        &Value::Nat64(1),
    );
    let oversized = Value::List(vec![Value::Map(vec![(
        Value::Text("payload".into()),
        Value::Blob(vec![7; 4097]),
    )])]);
    let wider_kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(8) }),
        value: Box::new(AcceptedFieldKind::Blob { max_len: None }),
    }));
    let oversized = encode_value(&row_contract(catalog.clone(), wider_kind), &oversized);
    for encoded in [truncated, wrong_kind, oversized] {
        let error = decode_runtime_value_from_row_contract(&row, 1, &encoded)
            .expect_err("strict persisted admission");
        assert_eq!(error.class(), ErrorClass::Corruption);
    }
}

#[test]
fn big_integer_admission_preserves_size_limits_and_budget_charges() {
    for signed in [-8193_i64, -8192, -65, -64, -1, 0, 1, 63, 64, 8191, 8192] {
        let value = IntBig::from(signed);
        let bytes = u32::try_from(value.to_leb128().len()).unwrap();
        check_big_integer_admission(
            InputValue::int_big(value.clone()),
            Value::IntBig(value),
            bytes,
            |max_bytes| AcceptedFieldKind::IntBig { max_bytes },
        );
    }
    for unsigned in [0_u64, 1, 127, 128, 16383, 16384, u64::MAX] {
        let value = NatBig::from(unsigned);
        let bytes = u32::try_from(value.to_leb128().len()).unwrap();
        check_big_integer_admission(
            InputValue::nat_big(value.clone()),
            Value::NatBig(value),
            bytes,
            |max_bytes| AcceptedFieldKind::NatBig { max_bytes },
        );
    }
}

// The independent encoder defines payload size; both input normalization and
// canonical validation must reject field limits before spending byte budget.
fn check_big_integer_admission(
    input: InputValue,
    value: Value,
    bytes: u32,
    kind: impl Fn(u32) -> AcceptedFieldKind,
) {
    let catalog = catalog();
    for (max_bytes, remaining_bytes, expected) in [
        (bytes, bytes + 5, Ok(())),
        (bytes, bytes + 4, Err(ValueAdmissionError::SizeExceeded)),
        (bytes - 1, 0, Err(ValueAdmissionError::ScalarConstraint)),
        (
            bytes - 1,
            bytes + 5,
            Err(ValueAdmissionError::ScalarConstraint),
        ),
    ] {
        let admission = admission(&catalog, kind(max_bytes));
        let initial = ValueAdmissionBudget {
            remaining_bytes,
            ..ValueAdmissionBudget::standard()
        };
        let mut normalized_budget = initial;
        let normalized =
            admission.normalize_input_to_runtime(input.clone(), &mut normalized_budget);
        let mut canonical_budget = initial;
        let canonical = admission.admit_canonical(value.clone(), &mut canonical_budget);
        assert_eq!(
            normalized.as_ref().map(|_| ()).map_err(|error| *error),
            expected
        );
        assert_eq!(
            canonical.as_ref().map(|_| ()).map_err(|error| *error),
            expected
        );
        assert_eq!(normalized_budget, canonical_budget);
        if expected.is_ok() {
            assert_eq!(normalized.unwrap(), value);
            assert_eq!(canonical.unwrap().into_value(), value);
            assert_eq!(normalized_budget.remaining_bytes, 0);
        } else {
            assert_eq!(normalized_budget, initial);
        }
    }
}

#[test]
fn big_integer_admission_preserves_wide_persisted_values() {
    for digits in [20, 80, 300] {
        let text = "9".repeat(digits);
        let signed: IntBig = format!("-{text}").parse().unwrap();
        let unsigned: NatBig = text.parse().unwrap();
        for (input, value, kind, bytes) in [
            (
                InputValue::int_big(signed.clone()),
                Value::IntBig(signed.clone()),
                AcceptedFieldKind::IntBig { max_bytes: 256 },
                signed.to_leb128().len(),
            ),
            (
                InputValue::nat_big(unsigned.clone()),
                Value::NatBig(unsigned.clone()),
                AcceptedFieldKind::NatBig { max_bytes: 256 },
                unsigned.to_leb128().len(),
            ),
        ] {
            let catalog = catalog();
            let admission = admission(&catalog, kind.clone());
            let mut budget = ValueAdmissionBudget::standard();
            let normalized = admission
                .normalize_input_to_runtime(input, &mut budget)
                .unwrap();
            assert_eq!(normalized, value);
            assert_eq!(
                budget.remaining_bytes,
                MAX_ACCEPTED_VALUE_BYTES - 5 - u32::try_from(bytes).unwrap()
            );
            let row = row_contract(catalog, kind);
            let encoded = encode_value(&row, &normalized);
            assert_eq!(
                decode_runtime_value_from_row_contract(&row, 1, &encoded).unwrap(),
                value
            );
        }
    }
}

fn encode_value(row: &StructuralRowContract, value: &Value) -> Vec<u8> {
    encode_canonical_value_for_accepted_field_contract(
        row.required_accepted_field_persistence_contract(1).unwrap(),
        value,
    )
    .unwrap()
}
