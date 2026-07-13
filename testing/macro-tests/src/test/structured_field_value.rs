#[cfg(test)]
use crate::prelude::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::__macro::{
        PersistedStructuralValueCodec, RuntimeValueEncode, Value,
        decode_persisted_structured_slot_payload, encode_persisted_structured_slot_payload,
        runtime_value_from_value, runtime_value_to_value,
    };
    use std::{collections::BTreeMap, fmt::Debug};

    #[record(
        fields(
            field(
                ident = "bio",
                value(item(prim = "Text", unbounded)),
                default = "String::new"
            ),
            field(ident = "visits", value(item(prim = "Nat32")), default = 0u32)
        ),
        traits(add(Default))
    )]
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
        field(ident = "address", value(item(is = "StructuredAddressHarness")))
    ))]
    pub struct StructuredNestedProfileHarness {}

    ///
    /// StructuredLayerHarness
    ///
    /// Minimal related-entity anchor used to prove relation-backed `Ulid`
    /// fields inside nested records keep the primitive key shape at the
    /// persisted-row boundary.
    ///

    #[entity(
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        fields(field(
            ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ))
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
        version = 1,
        pk(fields = ["id"]),
        fields(field(
            ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ))
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
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
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
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
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
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
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
    /// path as application reads.
    ///

    #[entity(
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(
                ident = "asset_selection",
                value(item(is = "StructuredAssetSelectionHarness"))
            )
        )
    )]
    pub struct StructuredAssetSelectionEntityHarness {}

    #[entity(
        store = "TestStore",
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(
                ident = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(
                ident = "nickname",
                value(item(prim = "Text", unbounded)),
                default = "\"guest\""
            ),
            field(ident = "note", value(opt, item(prim = "Text", unbounded)))
        )
    )]
    pub struct StructuredDefaultedEntityHarness {}

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

    fn profile_value(profile: &StructuredProfileHarness) -> Value {
        Value::from_map(vec![
            (
                Value::Text("bio".to_string()),
                Value::Text(profile.bio.clone()),
            ),
            (
                Value::Text("visits".to_string()),
                Value::Nat64(u64::from(profile.visits)),
            ),
        ])
        .expect("profile map should be canonical")
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

    fn assert_structured_slot_payload_roundtrip_is_canonical<T>(value: &T, field_name: &'static str)
    where
        T: PartialEq + Debug + PersistedStructuralValueCodec,
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

    fn decode_structured_payload_value<T>(bytes: &[u8], field_name: &'static str) -> Value
    where
        T: PersistedStructuralValueCodec + RuntimeValueEncode,
    {
        let decoded = decode_persisted_structured_slot_payload::<T>(bytes, field_name)
            .expect("decode structured payload");

        runtime_value_to_value(&decoded)
    }

    fn expected_profile_value() -> Value {
        profile_value(&StructuredProfileHarness::default())
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
    fn nested_record_structured_slot_payload_roundtrips_through_storage_helpers() {
        let profile = nested_profile_with("", "", 0);
        let payload = encode_persisted_structured_slot_payload(&profile, "profile")
            .expect("encode nested record payload");
        let decoded = decode_persisted_structured_slot_payload::<StructuredNestedProfileHarness>(
            payload.as_slice(),
            "profile",
        )
        .expect("decode nested record payload");

        assert_eq!(decoded, profile);
    }

    #[test]
    fn relation_backed_ulid_record_field_value_roundtrips_as_value_ulids() {
        let selected = selected_part_with(test_ulid(730, 1), test_ulid(730, 2));
        let value = runtime_value_to_value(&selected);

        assert_eq!(value, selected_part_value(&selected));
        assert_eq!(
            runtime_value_from_value::<StructuredSelectedPartHarness>(&value),
            Some(selected),
        );
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
}
