#[cfg(test)]
use crate::prelude::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        db::{
            InternalError, PersistedRow, ScalarSlotValueRef, SlotReader, SlotWriter,
            decode_persisted_custom_slot_payload, decode_persisted_slot_payload,
            encode_persisted_custom_slot_payload,
        },
        deserialize, serialize,
        traits::{EntitySchema, FieldProjection, FieldValue},
        value::Value,
    };
    use std::fmt::Debug;

    ///
    /// CaptureSlotWriter
    ///
    /// CaptureSlotWriter
    ///
    /// CaptureSlotWriter stores generated persisted slot payloads in-memory so
    /// macro tests can inspect the exact encoded field image before any store
    /// boundary rewraps it into a raw row.
    ///

    struct CaptureSlotWriter {
        slots: Vec<Option<Vec<u8>>>,
    }

    impl CaptureSlotWriter {
        fn new(slot_count: usize) -> Self {
            Self {
                slots: vec![None; slot_count],
            }
        }

        fn into_slots(self) -> Vec<Option<Vec<u8>>> {
            self.slots
        }
    }

    impl SlotWriter for CaptureSlotWriter {
        fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError> {
            let cell = self
                .slots
                .get_mut(slot)
                .unwrap_or_else(|| panic!("test writer slot {slot} outside capture bounds"));
            *cell = payload.map(Vec::from);

            Ok(())
        }
    }

    ///
    /// CaptureSlotReader
    ///
    /// CaptureSlotReader
    ///
    /// CaptureSlotReader replays captured slot payloads back through generated
    /// `PersistedRow::materialize_from_slots` code so the tests cover the same
    /// decode lane selection used by persisted structured fields in production.
    ///

    struct CaptureSlotReader {
        model: &'static icydb::model::entity::EntityModel,
        slots: Vec<Option<Vec<u8>>>,
    }

    impl CaptureSlotReader {
        fn new(
            model: &'static icydb::model::entity::EntityModel,
            slots: &[Option<Vec<u8>>],
        ) -> Self {
            Self {
                model,
                slots: slots.to_vec(),
            }
        }
    }

    impl SlotReader for CaptureSlotReader {
        fn model(&self) -> &'static icydb::model::entity::EntityModel {
            self.model
        }

        fn has(&self, slot: usize) -> bool {
            self.slots.get(slot).is_some_and(Option::is_some)
        }

        fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
            self.slots.get(slot).and_then(Option::as_deref)
        }

        fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
            panic!("test reader scalar fast path should not be used for slot {slot}");
        }

        fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError> {
            let Some(bytes) = self.get_bytes(slot) else {
                return Ok(None);
            };

            decode_persisted_slot_payload::<Value>(bytes, "test-slot").map(Some)
        }
    }

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

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(ident = "profile", value(item(is = "StructuredProfileHarness"))),
            field(
                ident = "opt_profile",
                value(opt, item(is = "StructuredProfileHarness"))
            ),
            field(
                ident = "nested_profile",
                value(item(is = "StructuredNestedProfileHarness"))
            ),
            field(
                ident = "profile_history",
                value(many, item(is = "StructuredProfileHarness"))
            )
        )
    )]
    pub struct StructuredPersistenceMatrixEntityHarness {}

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(ident = "nickname", value(item(prim = "Text")), default = "\"guest\""),
            field(ident = "note", value(opt, item(prim = "Text")))
        )
    )]
    pub struct StructuredDefaultedEntityHarness {}

    fn profile_with(bio: &str, visits: u32) -> StructuredProfileHarness {
        StructuredProfileHarness {
            bio: bio.to_string(),
            visits,
        }
    }

    fn address_with(city: &str, zip: u32) -> StructuredAddressHarness {
        StructuredAddressHarness {
            city: city.to_string(),
            zip,
        }
    }

    fn nested_profile_with(name: &str, city: &str, zip: u32) -> StructuredNestedProfileHarness {
        StructuredNestedProfileHarness {
            name: name.to_string(),
            address: address_with(city, zip),
        }
    }

    fn profile_value(profile: &StructuredProfileHarness) -> Value {
        Value::from_map(vec![
            (
                Value::Text("bio".to_string()),
                Value::Text(profile.bio.clone()),
            ),
            (
                Value::Text("visits".to_string()),
                Value::Uint(u64::from(profile.visits)),
            ),
        ])
        .expect("profile map should be canonical")
    }

    fn nested_profile_value(profile: &StructuredNestedProfileHarness) -> Value {
        Value::from_map(vec![
            (
                Value::Text("address".to_string()),
                Value::from_map(vec![
                    (
                        Value::Text("city".to_string()),
                        Value::Text(profile.address.city.clone()),
                    ),
                    (
                        Value::Text("zip".to_string()),
                        Value::Uint(u64::from(profile.address.zip)),
                    ),
                ])
                .expect("nested address map should be canonical"),
            ),
            (
                Value::Text("name".to_string()),
                Value::Text(profile.name.clone()),
            ),
        ])
        .expect("nested profile map should be canonical")
    }

    fn capture_entity_slots<E>(entity: &E) -> Vec<Option<Vec<u8>>>
    where
        E: PersistedRow + EntitySchema,
    {
        let mut writer = CaptureSlotWriter::new(E::MODEL.fields().len());
        entity
            .write_slots(&mut writer)
            .expect("generated persisted row should write slots");

        writer.into_slots()
    }

    fn decode_entity_from_captured_slots<E>(slots: &[Option<Vec<u8>>]) -> E
    where
        E: PersistedRow + EntitySchema,
    {
        let mut reader = CaptureSlotReader::new(E::MODEL, slots);
        E::materialize_from_slots(&mut reader)
            .expect("generated persisted row should materialize from captured slots")
    }

    fn roundtrip_entity_through_captured_slots<E>(entity: &E) -> Vec<Option<Vec<u8>>>
    where
        E: PersistedRow + EntitySchema + PartialEq + Debug,
    {
        let slots = capture_entity_slots(entity);
        let decoded = decode_entity_from_captured_slots::<E>(slots.as_slice());

        assert_eq!(
            decoded, *entity,
            "generated persisted row should round-trip through captured slots",
        );

        slots
    }

    fn required_slot_payload(slots: &[Option<Vec<u8>>], slot: usize) -> &[u8] {
        slots
            .get(slot)
            .and_then(Option::as_deref)
            .unwrap_or_else(|| panic!("expected captured payload for slot {slot}"))
    }

    fn expected_profile_value() -> Value {
        profile_value(&StructuredProfileHarness::default())
    }

    fn expected_nested_profile_value() -> Value {
        nested_profile_value(&StructuredNestedProfileHarness::default())
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

    #[test]
    fn generated_persisted_row_roundtrip_preserves_structured_field_matrix() {
        let entity = StructuredPersistenceMatrixEntityHarness {
            id: Ulid::from_parts(710, 1),
            profile: profile_with("Ada", 7),
            opt_profile: Some(profile_with("Grace", 9)),
            nested_profile: nested_profile_with("Primary", "Paris", 75_001),
            profile_history: vec![profile_with("Ada", 7), profile_with("Grace", 9)],
            ..Default::default()
        };

        let slots = roundtrip_entity_through_captured_slots(&entity);

        assert_eq!(
            decode_persisted_slot_payload::<Value>(required_slot_payload(&slots, 1), "profile")
                .expect("decode required structured payload"),
            profile_value(&entity.profile),
        );
        assert_eq!(
            decode_persisted_slot_payload::<Value>(required_slot_payload(&slots, 2), "profile")
                .expect("decode optional structured payload"),
            profile_value(
                entity
                    .opt_profile
                    .as_ref()
                    .expect("optional profile should exist")
            ),
        );
        assert_eq!(
            decode_persisted_slot_payload::<Value>(
                required_slot_payload(&slots, 3),
                "nested_profile",
            )
            .expect("decode nested structured payload"),
            nested_profile_value(&entity.nested_profile),
        );
        assert_eq!(
            decode_persisted_slot_payload::<Value>(
                required_slot_payload(&slots, 4),
                "profile_history",
            )
            .expect("decode many structured payload"),
            Value::List(entity.profile_history.iter().map(profile_value).collect()),
        );
    }

    #[test]
    fn generated_persisted_row_distinguishes_required_record_from_optional_null() {
        let entity = StructuredPersistenceMatrixEntityHarness {
            id: Ulid::from_parts(711, 1),
            profile: profile_with("Required", 13),
            opt_profile: None,
            nested_profile: nested_profile_with("Nested", "Berlin", 10_115),
            profile_history: vec![profile_with("History", 21)],
            ..Default::default()
        };

        let slots = roundtrip_entity_through_captured_slots(&entity);

        assert_eq!(
            decode_persisted_slot_payload::<Value>(required_slot_payload(&slots, 1), "profile")
                .expect("decode required profile payload"),
            profile_value(&entity.profile),
        );
        assert_ne!(
            decode_persisted_slot_payload::<Value>(required_slot_payload(&slots, 1), "profile")
                .expect("decode required profile payload"),
            Value::Null,
            "required record payload must not collapse to null",
        );
        assert_eq!(
            decode_persisted_slot_payload::<Value>(required_slot_payload(&slots, 2), "profile")
                .expect("decode optional profile payload"),
            Value::Null,
            "optional record payload should preserve explicit null",
        );
    }

    #[test]
    fn generated_persisted_row_projection_preserves_many_record_field_shape() {
        let entity = StructuredPersistenceMatrixEntityHarness {
            id: Ulid::from_parts(712, 1),
            profile: profile_with("Primary", 5),
            opt_profile: Some(profile_with("Optional", 8)),
            nested_profile: nested_profile_with("Nested", "Rome", 10042),
            profile_history: vec![
                profile_with("Timeline-A", 1),
                profile_with("Timeline-B", 2),
                profile_with("Timeline-C", 3),
            ],
            ..Default::default()
        };

        let slots = capture_entity_slots(&entity);
        let mut reader = CaptureSlotReader::new(
            StructuredPersistenceMatrixEntityHarness::MODEL,
            slots.as_slice(),
        );

        assert_eq!(
            StructuredPersistenceMatrixEntityHarness::project_slot(&mut reader, 4)
                .expect("project many structured slot"),
            Some(Value::List(
                entity.profile_history.iter().map(profile_value).collect(),
            )),
        );
    }

    #[test]
    fn generated_persisted_row_materializes_default_and_optional_missing_slots() {
        let entity = StructuredDefaultedEntityHarness {
            id: Ulid::from_parts(713, 1),
            nickname: "custom".to_string(),
            note: Some("memo".to_string()),
            ..Default::default()
        };
        let mut slots = capture_entity_slots(&entity);

        slots[1] = None;
        slots[2] = None;

        let decoded =
            decode_entity_from_captured_slots::<StructuredDefaultedEntityHarness>(slots.as_slice());

        assert_eq!(decoded.id, entity.id);
        assert_eq!(decoded.nickname, "\"guest\"".to_string());
        assert_eq!(decoded.note, None);
    }

    #[test]
    fn generated_persisted_row_sparse_defaults_converge_with_dense_slot_emission() {
        let sparse_source = StructuredDefaultedEntityHarness {
            id: Ulid::from_parts(714, 1),
            nickname: "custom".to_string(),
            note: Some("memo".to_string()),
            ..Default::default()
        };
        let mut sparse_slots = capture_entity_slots(&sparse_source);

        sparse_slots[1] = None;
        sparse_slots[2] = None;

        let decoded = decode_entity_from_captured_slots::<StructuredDefaultedEntityHarness>(
            sparse_slots.as_slice(),
        );
        let expected = StructuredDefaultedEntityHarness {
            id: sparse_source.id,
            nickname: "\"guest\"".to_string(),
            note: None,
            ..Default::default()
        };

        assert_eq!(
            decoded, expected,
            "sparse slot materialization should land on the same logical after-image as dense defaults",
        );
        assert_eq!(
            capture_entity_slots(&decoded),
            capture_entity_slots(&expected),
            "sparse decoded entity should re-emit the same dense slot image as the equivalent full entity",
        );
    }
}
