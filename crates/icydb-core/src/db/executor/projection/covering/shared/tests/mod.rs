//! Hybrid component support, decline and typed payload boundaries.

use super::*;
use crate::{
    db::index::EncodedValue,
    types::{Decimal, Ulid},
    value::ValueTag,
};

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
