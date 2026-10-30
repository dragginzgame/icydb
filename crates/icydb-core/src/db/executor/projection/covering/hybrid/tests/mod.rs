//! Hybrid row decoding, shaping and last-use ownership contracts.

use super::*;
use crate::{
    db::{
        index::EncodedValue,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        query::plan::FieldSlot,
    },
    types::{Decimal, EntityTag},
};

fn projection_key() -> DecodedDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(1),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(42)),
    )
}

#[test]
fn hybrid_row_projection_preserves_mixed_sources_and_repeated_values() {
    let fields = [
        (
            0,
            CoveringReadFieldSource::IndexComponent { component_index: 2 },
        ),
        (3, CoveringReadFieldSource::RowField),
        (
            0,
            CoveringReadFieldSource::PrimaryKey { component_index: 0 },
        ),
        (
            0,
            CoveringReadFieldSource::IndexExpressionComponent { component_index: 2 },
        ),
        (3, CoveringReadFieldSource::RowField),
        (0, CoveringReadFieldSource::Constant(Value::Bool(true))),
    ]
    .into_iter()
    .map(|(slot, source)| CoveringReadField {
        field_slot: FieldSlot::from_test_slot(slot, "test"),
        source,
    })
    .collect::<Vec<_>>();
    let ownership = HybridProjectionOwnership::compile(&fields);
    let encoded = EncodedValue::try_new(&Value::Text("index".into()))
        .unwrap()
        .into_bytes();
    let row = project_hybrid_covering_row(
        &projection_key(),
        &fields,
        &[2],
        vec![encoded].into(),
        vec![(3, Value::Text("row".into()))],
        &ownership,
    )
    .unwrap();
    assert_eq!(
        row,
        Some(vec![
            Value::Text("index".into()),
            Value::Text("row".into()),
            Value::Nat64(42),
            Value::Text("index".into()),
            Value::Text("row".into()),
            Value::Bool(true),
        ])
    );
}

#[test]
fn hybrid_row_projection_preserves_decode_fallback_and_error_priority() {
    let fields = [CoveringReadField {
        field_slot: FieldSlot::from_test_slot(0, "test"),
        source: CoveringReadFieldSource::IndexComponent {
            component_index: 99,
        },
    }];
    let ownership = HybridProjectionOwnership::compile(&fields);
    let unsupported = EncodedValue::try_new(&Value::Decimal(Decimal::new(1250, 2)))
        .unwrap()
        .into_bytes();
    assert_eq!(
        project_hybrid_covering_row(
            &projection_key(),
            &fields,
            &[0],
            vec![unsupported].into(),
            Vec::new(),
            &ownership,
        )
        .unwrap(),
        None
    );
    let error = project_hybrid_covering_row(
        &projection_key(),
        &fields,
        &[0],
        vec![Vec::new()].into(),
        Vec::new(),
        &ownership,
    )
    .unwrap_err();
    assert_eq!(
        error.diagnostic(),
        InternalError::bytes_covering_component_payload_empty().diagnostic()
    );
}

#[test]
fn final_hybrid_use_moves_large_value_without_retaining_or_cloning_backing() {
    let payload = vec![7_u8; 300 * 1_024];
    let payload_ptr = payload.as_ptr();
    let mut values = vec![(4, Value::Blob(payload))];

    let moved =
        take_or_clone_compact_value(&mut values, 4, true).expect("final use should move the value");
    let Value::Blob(moved) = moved else {
        panic!("fixture should remain a blob");
    };

    assert_eq!(moved.as_ptr(), payload_ptr);
    assert!(values.is_empty());
}

#[test]
fn repeated_hybrid_use_clones_until_the_final_owned_use() {
    let payload = vec![9_u8; 40 * 1_024];
    let payload_ptr = payload.as_ptr();
    let mut values = vec![(2, Value::Blob(payload))];

    let cloned = take_or_clone_compact_value(&mut values, 2, false)
        .expect("non-final use should clone the value");
    let Value::Blob(cloned) = cloned else {
        panic!("fixture should remain a blob");
    };
    let retained_ptr = match &values[0].1 {
        Value::Blob(retained) => retained.as_ptr(),
        _ => panic!("fixture should remain a blob"),
    };

    assert_ne!(cloned.as_ptr(), payload_ptr);
    assert_eq!(retained_ptr, payload_ptr);
}
