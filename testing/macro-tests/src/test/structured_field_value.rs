#[cfg(test)]
use crate::prelude::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        db::{decode_persisted_custom_slot_payload, encode_persisted_custom_slot_payload},
        deserialize, serialize,
        traits::{FieldProjection, FieldValue},
        value::Value,
    };

    #[record(fields(
        field(ident = "bio", value(item(prim = "Text")), default = "String::new"),
        field(ident = "visits", value(item(prim = "Nat32")), default = 0u32)
    ))]
    pub struct StructuredProfileHarness {}

    #[record(fields(
        field(ident = "city", value(item(prim = "Text")), default = "String::new"),
        field(ident = "zip", value(item(prim = "Nat32")), default = 0u32)
    ))]
    pub struct StructuredAddressHarness {}

    #[record(fields(
        field(ident = "name", value(item(prim = "Text")), default = "String::new"),
        field(
            ident = "address",
            value(item(is = "StructuredAddressHarness")),
            default = "StructuredAddressHarness::default"
        )
    ))]
    pub struct StructuredNestedProfileHarness {}

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(ident = "profile", value(item(is = "StructuredProfileHarness"))),
            field(
                ident = "opt_profile",
                value(opt, item(is = "StructuredProfileHarness"))
            )
        )
    )]
    pub struct StructuredProfileEntityHarness {}

    fn expected_profile_value() -> Value {
        Value::from_map(vec![
            (Value::Text("bio".to_string()), Value::Text(String::new())),
            (Value::Text("visits".to_string()), Value::Uint(0)),
        ])
        .expect("expected profile map should be canonical")
    }

    fn expected_nested_profile_value() -> Value {
        Value::from_map(vec![
            (
                Value::Text("address".to_string()),
                Value::from_map(vec![
                    (Value::Text("city".to_string()), Value::Text(String::new())),
                    (Value::Text("zip".to_string()), Value::Uint(0)),
                ])
                .expect("expected nested address map should be canonical"),
            ),
            (Value::Text("name".to_string()), Value::Text(String::new())),
        ])
        .expect("expected nested profile map should be canonical")
    }

    #[test]
    fn record_default_field_value_preserves_structured_map_shape() {
        let profile = StructuredProfileHarness::default();
        let value = FieldValue::to_value(&profile);

        assert_eq!(value, expected_profile_value());

        let bytes = serialize(&value).expect("serialize structured profile value");
        let decoded: Value = deserialize(&bytes).expect("deserialize structured profile value");

        assert_eq!(decoded, expected_profile_value());
    }

    #[test]
    fn option_record_field_value_distinguishes_some_default_from_none() {
        let some_profile = Some(StructuredProfileHarness::default());
        let none_profile: Option<StructuredProfileHarness> = None;

        assert_eq!(
            FieldValue::to_value(&some_profile),
            expected_profile_value()
        );
        assert_eq!(FieldValue::to_value(&none_profile), Value::Null);
        assert_ne!(FieldValue::to_value(&some_profile), Value::Null);
    }

    #[test]
    fn entity_field_projection_preserves_nested_record_payload() {
        let entity = StructuredProfileEntityHarness {
            id: Ulid::from_parts(700, 1),
            profile: StructuredProfileHarness::default(),
            opt_profile: Some(StructuredProfileHarness::default()),
            ..Default::default()
        };

        assert_eq!(entity.get_value_by_index(1), Some(expected_profile_value()));
        assert_eq!(entity.get_value_by_index(2), Some(expected_profile_value()));

        let none_entity = StructuredProfileEntityHarness {
            id: Ulid::from_parts(701, 1),
            profile: StructuredProfileHarness::default(),
            opt_profile: None,
            ..Default::default()
        };

        assert_eq!(none_entity.get_value_by_index(2), Some(Value::Null));
    }

    #[test]
    fn nested_record_custom_slot_payload_roundtrips_through_storage_helpers() {
        let profile = StructuredNestedProfileHarness::default();
        let payload = encode_persisted_custom_slot_payload(&profile, "profile")
            .expect("encode nested record payload");
        let decoded = decode_persisted_custom_slot_payload::<StructuredNestedProfileHarness>(
            payload.as_slice(),
            "profile",
        )
        .expect("decode nested record payload");

        assert_eq!(
            FieldValue::to_value(&profile),
            expected_nested_profile_value()
        );
        assert_eq!(decoded, profile);
    }
}
