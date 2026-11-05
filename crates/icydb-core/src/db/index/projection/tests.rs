use super::*;

#[test]
fn decode_covering_projection_component_decodes_fast_path_text_payload() {
    let component = [
        ValueTag::Text.to_u8(),
        b't',
        b'e',
        b'x',
        b't',
        COVERING_TEXT_TERMINATOR,
        COVERING_TEXT_TERMINATOR,
    ];

    let decoded = decode_covering_projection_component(component.as_slice())
        .expect("fast-path text payload should decode")
        .expect("text payload should remain supported");

    assert_eq!(decoded, Value::Text(String::from("text")));
}

#[test]
fn covering_capability_agrees_with_component_decode() {
    use crate::{
        db::index::EncodedValue,
        types::{Date, Decimal, Duration, Float32, Float64, Timestamp},
    };
    for (kind, value, supported) in [
        (AcceptedFieldKind::Bool, Value::Bool(true), true),
        (AcceptedFieldKind::Int8, Value::Int64(-128), true),
        (AcceptedFieldKind::Int16, Value::Int64(-32_768), true),
        (
            AcceptedFieldKind::Int32,
            Value::Int64(i64::from(i32::MIN)),
            true,
        ),
        (AcceptedFieldKind::Int64, Value::Int64(i64::MIN), true),
        (AcceptedFieldKind::Nat8, Value::Nat64(255), true),
        (AcceptedFieldKind::Nat16, Value::Nat64(65_535), true),
        (
            AcceptedFieldKind::Nat32,
            Value::Nat64(u64::from(u32::MAX)),
            true,
        ),
        (AcceptedFieldKind::Nat64, Value::Nat64(u64::MAX), true),
        (
            AcceptedFieldKind::Text { max_len: None },
            Value::Text("a\0β".into()),
            true,
        ),
        (
            AcceptedFieldKind::Text { max_len: Some(8) },
            Value::Text("sample".into()),
            true,
        ),
        (
            AcceptedFieldKind::Ulid,
            Value::Ulid(Ulid::from_u128(7)),
            true,
        ),
        (AcceptedFieldKind::Unit, Value::Unit, true),
        (
            AcceptedFieldKind::Relation {
                target_path: "Target".into(),
                target_entity_name: "Target".into(),
                target_entity_tag: crate::types::EntityTag::new(1),
                target_store_path: "Store".into(),
                key_kind: Box::new(AcceptedFieldKind::Nat64),
            },
            Value::Nat64(7),
            true,
        ),
        (
            AcceptedFieldKind::Timestamp,
            Value::Timestamp(Timestamp::from_millis(-1)),
            false,
        ),
        (AcceptedFieldKind::Date, Value::Date(Date::MIN), false),
        (
            AcceptedFieldKind::Duration,
            Value::Duration(Duration::from_millis(7)),
            false,
        ),
        (
            AcceptedFieldKind::Decimal { scale: 2 },
            Value::Decimal(Decimal::new(125, 2)),
            false,
        ),
        (
            AcceptedFieldKind::Float32,
            Value::Float32(Float32::try_new(1.25).unwrap()),
            false,
        ),
        (
            AcceptedFieldKind::Float64,
            Value::Float64(Float64::try_new(1.25).unwrap()),
            false,
        ),
        (AcceptedFieldKind::Int128, Value::Int128(i128::MIN), false),
        (AcceptedFieldKind::Nat128, Value::Nat128(u128::MAX), false),
    ] {
        let encoded = EncodedValue::try_new(&value).unwrap();
        assert_eq!(
            index_component_projection_supported(&kind),
            supported,
            "{kind:?}"
        );
        assert_eq!(
            decode_covering_projection_component(encoded.encoded()).unwrap(),
            supported.then_some(value)
        );
    }
}
