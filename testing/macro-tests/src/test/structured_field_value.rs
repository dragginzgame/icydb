#[cfg(test)]
use crate::prelude::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        __macro::{
            FieldProjection, PersistedStructuredFieldCodec, RuntimeValueEncode, ScalarSlotValueRef,
            Value, decode_generated_structural_enum_payload_bytes,
            decode_generated_structural_map_payload_bytes,
            decode_generated_structural_text_payload_bytes,
            decode_persisted_structured_slot_payload,
            encode_generated_structural_enum_payload_bytes,
            encode_generated_structural_map_payload_bytes,
            encode_generated_structural_text_payload_bytes,
            encode_persisted_structured_slot_payload, runtime_value_from_value,
            runtime_value_to_value,
        },
        db::{InternalError, PersistedRow, SlotReader, SlotWriter},
        traits::EntitySchema,
    };
    use std::{collections::BTreeMap, fmt::Debug};

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
        fn generated_compatible_field_model(
            &self,
            slot: usize,
        ) -> Result<&icydb::model::field::FieldModel, InternalError> {
            Ok(self
                .model
                .fields()
                .get(slot)
                .expect("structured field capture reader slot must exist"))
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
            panic!("test reader value path should not be used for slot {slot}");
        }
    }

    #[record(fields(
        field(
            ident = "bio",
            value(item(prim = "Text", unbounded)),
            default = "String::new"
        ),
        field(ident = "visits", value(item(prim = "Nat32")), default = 0u32)
    ))]
    pub struct StructuredProfileHarness {}

    #[record(fields(
        field(
            ident = "city",
            value(item(prim = "Text", unbounded)),
            default = "String::new"
        ),
        field(ident = "zip", value(item(prim = "Nat32")), default = 0u32)
    ))]
    pub struct StructuredAddressHarness {}

    #[record(fields(
        field(
            ident = "name",
            value(item(prim = "Text", unbounded)),
            default = "String::new"
        ),
        field(
            ident = "address",
            value(item(is = "StructuredAddressHarness")),
            default = "StructuredAddressHarness::default"
        )
    ))]
    pub struct StructuredNestedProfileHarness {}

    ///
    /// StructuredAddressEnvelopeHarness
    ///
    /// StructuredAddressEnvelopeHarness carries a generated record inside a
    /// generated enum payload so the macro tests can prove enum recursion now
    /// targets the direct persisted structured codec lane.
    ///

    #[enum_(
        variant(unspecified, default),
        variant(ident = "Address", value(item(is = "StructuredAddressHarness")))
    )]
    pub struct StructuredAddressEnvelopeHarness {}

    ///
    /// StructuredRecordWithEnumHarness
    ///
    /// StructuredRecordWithEnumHarness stores a generated enum as one record
    /// field so the tests cover record -> enum -> record nesting through the
    /// direct structured payload path.
    ///

    #[record(fields(
        field(
            ident = "label",
            value(item(prim = "Text", unbounded)),
            default = "String::new"
        ),
        field(
            ident = "address",
            value(item(is = "StructuredAddressEnvelopeHarness"))
        )
    ))]
    pub struct StructuredRecordWithEnumHarness {}

    ///
    /// StructuredProfileEnvelopeHarness
    ///
    /// StructuredProfileEnvelopeHarness carries a generated record payload so
    /// the tests also pin the inverse enum -> record nesting direction.
    ///

    #[enum_(
        variant(unspecified, default),
        variant(ident = "Profile", value(item(is = "StructuredNestedProfileHarness")))
    )]
    pub struct StructuredProfileEnvelopeHarness {}

    ///
    /// StructuredLayerHarness
    ///
    /// Minimal related-entity anchor used to prove relation-backed `Ulid`
    /// fields inside nested records keep the primitive key shape at the
    /// persisted-row boundary.
    ///

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
    )]
    pub struct StructuredLayerHarness {}

    ///
    /// StructuredPartHarness
    ///
    /// Second related-entity anchor paired with `StructuredLayerHarness` so
    /// the nested record can mirror the `SelectedPart` relation shape from the
    /// application boundary.
    ///

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
    )]
    pub struct StructuredPartHarness {}

    ///
    /// StructuredSelectedPartHarness
    ///
    /// This record mirrors one nested relation payload stored inside a parent
    /// entity so macro tests can lock the `Value::Ulid` round-trip contract for
    /// relation-backed record fields.
    ///

    #[record(fields(
        field(
            ident = "layer_id",
            value(item(prim = "Ulid", rel = "StructuredLayerHarness"))
        ),
        field(
            ident = "part_id",
            value(item(prim = "Ulid", rel = "StructuredPartHarness"))
        )
    ))]
    pub struct StructuredSelectedPartHarness {}

    ///
    /// StructuredAssetSelectionHarness
    ///
    /// StructuredAssetSelectionHarness mirrors app records that store a list of
    /// asset ids alongside an optional default id. It specifically protects the
    /// persisted structured codec path for `Option<Ulid>` nested inside a
    /// record, where null probing must accept local value-storage tags.
    ///

    #[record(fields(
        field(ident = "asset_ids", value(many, item(prim = "Ulid"))),
        field(ident = "default_asset_id", value(opt, item(prim = "Ulid")))
    ))]
    pub struct StructuredAssetSelectionHarness {}

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

    ///
    /// StructuredSelectedPartEntityHarness
    ///
    /// Stores a repeated nested-record payload whose fields carry relation
    /// metadata while still persisting as primitive `Ulid` values. This is the
    /// closest framework-level match to `GenerationOutput.selected_parts`.
    ///

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(
                ident = "selected_parts",
                value(many, item(is = "StructuredSelectedPartHarness"))
            )
        )
    )]
    pub struct StructuredSelectedPartEntityHarness {}

    ///
    /// StructuredAssetSelectionEntityHarness
    ///
    /// StructuredAssetSelectionEntityHarness persists the asset-selection record
    /// through generated row slots so tests exercise the same materialization
    /// path as application reads from stable storage.
    ///

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(
                ident = "asset_selection",
                value(item(is = "StructuredAssetSelectionHarness"))
            )
        )
    )]
    pub struct StructuredAssetSelectionEntityHarness {}

    #[entity(
        store = "TestStore",
        pk(field = "id"),
        fields(
            field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
            field(
                ident = "nickname",
                value(item(prim = "Text", unbounded)),
                default = "\"guest\""
            ),
            field(ident = "note", value(opt, item(prim = "Text", unbounded)))
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

    fn selected_part_with(layer_id: Ulid, part_id: Ulid) -> StructuredSelectedPartHarness {
        StructuredSelectedPartHarness { layer_id, part_id }
    }

    fn asset_selection_with(
        asset_ids: Vec<Ulid>,
        default_asset_id: Option<Ulid>,
    ) -> StructuredAssetSelectionHarness {
        StructuredAssetSelectionHarness {
            asset_ids,
            default_asset_id,
        }
    }

    fn record_with_enum(label: &str, city: &str, zip: u32) -> StructuredRecordWithEnumHarness {
        StructuredRecordWithEnumHarness {
            label: label.to_string(),
            address: StructuredAddressEnvelopeHarness::Address(address_with(city, zip)),
        }
    }

    fn profile_envelope_with(name: &str, city: &str, zip: u32) -> StructuredProfileEnvelopeHarness {
        StructuredProfileEnvelopeHarness::Profile(nested_profile_with(name, city, zip))
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

    fn selected_part_value(part: &StructuredSelectedPartHarness) -> Value {
        Value::from_map(vec![
            (
                Value::Text("layer_id".to_string()),
                Value::Ulid(part.layer_id),
            ),
            (
                Value::Text("part_id".to_string()),
                Value::Ulid(part.part_id),
            ),
        ])
        .expect("selected part map should be canonical")
    }

    fn asset_selection_value(selection: &StructuredAssetSelectionHarness) -> Value {
        Value::from_map(vec![
            (
                Value::Text("asset_ids".to_string()),
                Value::List(
                    selection
                        .asset_ids
                        .iter()
                        .copied()
                        .map(Value::Ulid)
                        .collect(),
                ),
            ),
            (
                Value::Text("default_asset_id".to_string()),
                selection.default_asset_id.map_or(Value::Null, Value::Ulid),
            ),
        ])
        .expect("asset selection map should be canonical")
    }

    fn assert_structured_slot_payload_roundtrip_is_canonical<T>(value: &T, field_name: &'static str)
    where
        T: PartialEq + Debug + PersistedStructuredFieldCodec,
    {
        let bytes = encode_persisted_structured_slot_payload(value, field_name)
            .expect("encode structured payload");
        let decoded = decode_persisted_structured_slot_payload::<T>(bytes.as_slice(), field_name)
            .expect("decode structured payload");
        let reencoded = encode_persisted_structured_slot_payload(&decoded, field_name)
            .expect("re-encode payload");

        assert_eq!(
            decoded, *value,
            "typed structured payload should round-trip"
        );
        assert_eq!(
            reencoded, bytes,
            "typed structured payload should re-emit canonical bytes after decode",
        );
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

    fn decode_structured_payload_value<T>(bytes: &[u8], field_name: &'static str) -> Value
    where
        T: PersistedStructuredFieldCodec + RuntimeValueEncode,
    {
        let decoded = decode_persisted_structured_slot_payload::<T>(bytes, field_name)
            .expect("decode structured payload");

        runtime_value_to_value(&decoded)
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
        let value = runtime_value_to_value(&profile);

        assert_eq!(value, expected_profile_value());

        let bytes = encode_persisted_structured_slot_payload(&profile, "profile")
            .expect("encode structured profile value");
        let decoded =
            decode_structured_payload_value::<StructuredProfileHarness>(&bytes, "profile");

        assert_eq!(decoded, expected_profile_value());
    }

    #[test]
    fn option_record_field_value_distinguishes_some_default_from_none() {
        let some_profile = Some(StructuredProfileHarness::default());
        let none_profile: Option<StructuredProfileHarness> = None;

        assert_eq!(
            runtime_value_to_value(&some_profile),
            expected_profile_value()
        );
        assert_eq!(runtime_value_to_value(&none_profile), Value::Null);
        assert_ne!(runtime_value_to_value(&some_profile), Value::Null);
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
    fn nested_record_structured_slot_payload_roundtrips_through_storage_helpers() {
        let profile = StructuredNestedProfileHarness::default();
        let payload = encode_persisted_structured_slot_payload(&profile, "profile")
            .expect("encode nested record payload");
        let decoded = decode_persisted_structured_slot_payload::<StructuredNestedProfileHarness>(
            payload.as_slice(),
            "profile",
        )
        .expect("decode nested record payload");

        assert_eq!(
            runtime_value_to_value(&profile),
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
            decode_structured_payload_value::<StructuredProfileHarness>(
                required_slot_payload(&slots, 1),
                "profile"
            ),
            profile_value(&entity.profile),
        );
        assert_eq!(
            decode_structured_payload_value::<Option<StructuredProfileHarness>>(
                required_slot_payload(&slots, 2),
                "profile"
            ),
            profile_value(
                entity
                    .opt_profile
                    .as_ref()
                    .expect("optional profile should exist")
            ),
        );
        assert_eq!(
            decode_structured_payload_value::<StructuredNestedProfileHarness>(
                required_slot_payload(&slots, 3),
                "nested_profile",
            ),
            nested_profile_value(&entity.nested_profile),
        );
        assert_eq!(
            decode_structured_payload_value::<Vec<StructuredProfileHarness>>(
                required_slot_payload(&slots, 4),
                "profile_history",
            ),
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
            decode_structured_payload_value::<StructuredProfileHarness>(
                required_slot_payload(&slots, 1),
                "profile"
            ),
            profile_value(&entity.profile),
        );
        assert_ne!(
            decode_structured_payload_value::<StructuredProfileHarness>(
                required_slot_payload(&slots, 1),
                "profile"
            ),
            Value::Null,
            "required record payload must not collapse to null",
        );
        assert_eq!(
            decode_structured_payload_value::<Option<StructuredProfileHarness>>(
                required_slot_payload(&slots, 2),
                "profile"
            ),
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

        assert_eq!(
            entity.get_value_by_index(4),
            Some(Value::List(
                entity.profile_history.iter().map(profile_value).collect(),
            )),
        );
    }

    #[test]
    fn relation_backed_ulid_record_many_roundtrips_through_generated_persisted_row() {
        let entity = StructuredSelectedPartEntityHarness {
            id: Ulid::from_parts(720, 1),
            selected_parts: vec![
                selected_part_with(Ulid::from_parts(721, 1), Ulid::from_parts(721, 2)),
                selected_part_with(Ulid::from_parts(722, 1), Ulid::from_parts(722, 2)),
            ],
            ..Default::default()
        };

        let slots = roundtrip_entity_through_captured_slots(&entity);

        assert_eq!(
            decode_structured_payload_value::<Vec<StructuredSelectedPartHarness>>(
                required_slot_payload(&slots, 1),
                "selected_parts",
            ),
            Value::List(
                entity
                    .selected_parts
                    .iter()
                    .map(selected_part_value)
                    .collect(),
            ),
        );
    }

    #[test]
    fn relation_backed_ulid_record_field_value_roundtrips_as_value_ulids() {
        let selected = selected_part_with(Ulid::from_parts(730, 1), Ulid::from_parts(730, 2));
        let value = runtime_value_to_value(&selected);

        assert_eq!(value, selected_part_value(&selected));
        assert_eq!(
            runtime_value_from_value::<StructuredSelectedPartHarness>(&value),
            Some(selected),
        );
    }

    #[test]
    fn record_with_optional_ulid_roundtrips_through_generated_persisted_row() {
        let cases = [
            asset_selection_with(vec![], None),
            asset_selection_with(vec![Ulid::from_parts(740, 1)], None),
            asset_selection_with(
                vec![Ulid::from_parts(741, 1), Ulid::from_parts(741, 2)],
                Some(Ulid::from_parts(741, 2)),
            ),
        ];

        for (case_index, asset_selection) in cases.into_iter().enumerate() {
            let entity = StructuredAssetSelectionEntityHarness {
                id: Ulid::from_parts(740, u128::try_from(case_index + 1).expect("case id fits")),
                asset_selection,
                ..Default::default()
            };
            let slots = roundtrip_entity_through_captured_slots(&entity);

            assert_eq!(
                decode_structured_payload_value::<StructuredAssetSelectionHarness>(
                    required_slot_payload(&slots, 1),
                    "asset_selection",
                ),
                asset_selection_value(&entity.asset_selection),
            );
        }
    }

    #[test]
    fn record_containing_generated_enum_roundtrips_through_direct_custom_payloads() {
        let value = record_with_enum("primary", "Paris", 75_001);

        assert_structured_slot_payload_roundtrip_is_canonical(&value, "record_with_enum");
    }

    #[test]
    fn enum_payload_containing_generated_record_roundtrips_through_direct_custom_payloads() {
        let value = profile_envelope_with("Ada", "Berlin", 10_115);

        assert_structured_slot_payload_roundtrip_is_canonical(&value, "profile_envelope");
    }

    #[test]
    fn map_of_generated_structured_wrappers_roundtrips_through_direct_custom_payloads() {
        let mut value = BTreeMap::new();
        value.insert(
            "home".to_string(),
            nested_profile_with("Ada", "Paris", 75_001),
        );
        value.insert(
            "work".to_string(),
            nested_profile_with("Grace", "Berlin", 10_115),
        );

        assert_structured_slot_payload_roundtrip_is_canonical(&value, "profile_map");
    }

    #[test]
    fn malformed_nested_generated_payload_fails_closed() {
        let value = record_with_enum("broken", "Paris", 75_001);
        let bytes =
            encode_persisted_structured_slot_payload(&value, "record_with_enum").expect("encode");
        let entries =
            decode_generated_structural_map_payload_bytes(bytes.as_slice()).expect("decode outer");
        let mut corrupted_entries = Vec::with_capacity(entries.len());

        for (entry_key, entry_value) in entries {
            let entry_name =
                decode_generated_structural_text_payload_bytes(entry_key).expect("decode key");

            if entry_name == "address" {
                let (variant, path, payload) =
                    decode_generated_structural_enum_payload_bytes(entry_value)
                        .expect("decode nested enum");
                let payload = payload.expect("nested enum should carry payload");
                let nested_entries =
                    decode_generated_structural_map_payload_bytes(payload).expect("decode record");
                let mut corrupted_nested_entries = Vec::with_capacity(nested_entries.len());

                for (nested_key, nested_value) in nested_entries {
                    let nested_name = decode_generated_structural_text_payload_bytes(nested_key)
                        .expect("decode nested key");

                    if nested_name == "zip" {
                        corrupted_nested_entries.push((
                            encode_generated_structural_text_payload_bytes("zip"),
                            vec![0xFF, 0xFF],
                        ));
                    } else {
                        corrupted_nested_entries.push((nested_key.to_vec(), nested_value.to_vec()));
                    }
                }

                let corrupted_nested_refs = corrupted_nested_entries
                    .iter()
                    .map(|(key_bytes, value_bytes)| (key_bytes.as_slice(), value_bytes.as_slice()))
                    .collect::<Vec<_>>();
                let corrupted_payload =
                    encode_generated_structural_map_payload_bytes(&corrupted_nested_refs);
                let corrupted_enum = encode_generated_structural_enum_payload_bytes(
                    variant.as_str(),
                    path.as_deref(),
                    Some(corrupted_payload.as_slice()),
                );

                corrupted_entries.push((entry_key.to_vec(), corrupted_enum));
            } else {
                corrupted_entries.push((entry_key.to_vec(), entry_value.to_vec()));
            }
        }

        let corrupted_refs = corrupted_entries
            .iter()
            .map(|(key_bytes, value_bytes)| (key_bytes.as_slice(), value_bytes.as_slice()))
            .collect::<Vec<_>>();
        let corrupted_bytes = encode_generated_structural_map_payload_bytes(&corrupted_refs);
        let decode = decode_persisted_structured_slot_payload::<StructuredRecordWithEnumHarness>(
            corrupted_bytes.as_slice(),
            "record_with_enum",
        );

        assert!(
            decode.is_err(),
            "nested payload corruption must fail closed"
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
