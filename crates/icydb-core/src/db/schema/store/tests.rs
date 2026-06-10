use super::{
    RawSchemaKey, RawSchemaSnapshot, SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION,
    SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_VERSION,
    SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_VERSION, SchemaStore, SchemaStoreBackend,
    SchemaStoreVisit,
};
use crate::{
    db::{
        direction::Direction,
        schema::{
            AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, SchemaFieldDefault,
            SchemaFieldSlot, SchemaRowLayout, SchemaVersion, accepted_schema_cache_fingerprint,
            encode_persisted_schema_snapshot, persisted_schema_snapshot_decode_count_for_tests,
            reset_persisted_schema_snapshot_decode_count_for_tests,
        },
    },
    model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    testing::test_memory,
    traits::Storable,
    types::EntityTag,
};
use std::borrow::Cow;
use std::convert::Infallible;

#[test]
fn raw_schema_key_round_trips_entity_and_version() {
    let key = RawSchemaKey::from_entity_version(EntityTag::new(0x0102_0304_0506_0708), {
        SchemaVersion::initial()
    });
    let encoded = key.to_bytes().into_owned();
    let decoded = RawSchemaKey::from_bytes(Cow::Owned(encoded));

    assert_eq!(decoded.entity_tag(), EntityTag::new(0x0102_0304_0506_0708));
    assert_eq!(decoded.version(), SchemaVersion::initial().get());
}

#[test]
fn raw_schema_snapshot_round_trips_payload_bytes() {
    let snapshot = RawSchemaSnapshot::from_bytes(vec![1, 2, 3, 5, 8]);
    let encoded = snapshot.to_bytes().into_owned();
    let decoded = <RawSchemaSnapshot as Storable>::from_bytes(Cow::Owned(encoded));

    assert_eq!(decoded.as_bytes(), &[1, 2, 3, 5, 8]);
    assert_eq!(decoded.into_bytes(), vec![1, 2, 3, 5, 8]);
}

#[test]
fn raw_schema_snapshot_round_trips_identity_header_for_typed_snapshot() {
    let snapshot = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Header");
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot.clone())
        .expect("typed schema snapshot should be accepted");
    let expected_fingerprint = accepted_schema_cache_fingerprint(&accepted)
        .expect("accepted schema fingerprint should derive");
    let raw = RawSchemaSnapshot::from_persisted_snapshot(&snapshot)
        .expect("typed schema snapshot should encode");
    let encoded = raw.to_bytes().into_owned();
    let decoded = <RawSchemaSnapshot as Storable>::from_bytes(Cow::Owned(encoded));
    let owned_encoded = <RawSchemaSnapshot as Storable>::into_bytes(raw.clone());
    let owned_decoded = <RawSchemaSnapshot as Storable>::from_bytes(Cow::Owned(owned_encoded));

    assert_eq!(decoded.as_bytes(), raw.as_bytes());
    assert_eq!(
        decoded
            .accepted_schema_fingerprint()
            .expect("identity header should decode"),
        expected_fingerprint
    );
    assert_eq!(
        owned_decoded
            .accepted_schema_fingerprint()
            .expect("owned identity header should decode"),
        expected_fingerprint
    );
}

#[test]
fn schema_store_persists_raw_snapshots_by_entity_version_key() {
    let mut store = SchemaStore::init(test_memory(251));
    let key = RawSchemaKey::from_entity_version(EntityTag::new(17), SchemaVersion::initial());

    assert!(store.is_empty());
    assert!(!store.contains_raw_snapshot(&key));

    store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(vec![9, 4, 6]));

    assert_eq!(store.len(), 1);
    assert!(store.contains_raw_snapshot(&key));
    assert_eq!(
        store
            .get_raw_snapshot(&key)
            .expect("schema snapshot should be present")
            .as_bytes(),
        &[9, 4, 6],
    );

    store.clear();
    assert!(store.is_empty());
}

#[test]
fn schema_store_loads_latest_snapshot_for_entity() {
    let mut store = SchemaStore::init(test_memory(252));
    let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
    let newer = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Newer");
    let other_entity = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "Other");

    store
        .insert_persisted_snapshot(EntityTag::new(41), &initial)
        .expect("initial schema snapshot should encode");
    store
        .insert_persisted_snapshot(EntityTag::new(42), &other_entity)
        .expect("other entity schema snapshot should encode");
    store
        .insert_persisted_snapshot(EntityTag::new(41), &newer)
        .expect("newer schema snapshot should encode");

    let latest = store
        .latest_persisted_snapshot(EntityTag::new(41))
        .expect("latest schema snapshot should decode")
        .expect("schema snapshot should exist");

    assert_eq!(latest.version(), SchemaVersion::new(2));
    assert_eq!(latest.entity_name(), "Newer");
}

#[test]
fn schema_store_entity_footprint_counts_raw_snapshots_without_decoding() {
    let mut store = SchemaStore::init(test_memory(242));
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(71), SchemaVersion::initial()),
        RawSchemaSnapshot::from_bytes(vec![1, 2, 3]),
    );
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(72), SchemaVersion::new(3)),
        RawSchemaSnapshot::from_bytes(vec![5, 8]),
    );
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(71), SchemaVersion::new(2)),
        RawSchemaSnapshot::from_bytes(vec![13, 21, 34, 55]),
    );

    let footprint = store.entity_footprint(EntityTag::new(71));

    assert_eq!(footprint.snapshots(), 2);
    assert_eq!(footprint.encoded_bytes(), 7);
    assert_eq!(footprint.latest_snapshot_bytes(), 4);
}

#[test]
fn schema_store_visit_raw_snapshots_preserves_key_order() {
    let mut store = SchemaStore::init(test_memory(235));
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(3), SchemaVersion::new(2)),
        RawSchemaSnapshot::from_bytes(vec![32]),
    );
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(1), SchemaVersion::new(3)),
        RawSchemaSnapshot::from_bytes(vec![13]),
    );
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(1), SchemaVersion::new(1)),
        RawSchemaSnapshot::from_bytes(vec![11]),
    );

    let mut visited = Vec::new();
    let _: Result<(), Infallible> = store.visit_raw_snapshots(|key, snapshot| {
        visited.push((
            key.entity_tag().value(),
            key.version(),
            snapshot.as_bytes()[0],
        ));
        Ok(SchemaStoreVisit::Continue)
    });

    assert_eq!(visited, vec![(1, 1, 11), (1, 3, 13), (3, 2, 32)]);
}

#[test]
fn schema_store_visit_raw_snapshots_can_stop_without_error() {
    let mut store = SchemaStore::init(test_memory(234));
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(2), SchemaVersion::new(1)),
        RawSchemaSnapshot::from_bytes(vec![21]),
    );
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(2), SchemaVersion::new(2)),
        RawSchemaSnapshot::from_bytes(vec![22]),
    );

    let mut visited = Vec::new();
    let _: Result<(), Infallible> = store.visit_raw_snapshots(|key, _| {
        visited.push(key.version());
        Ok(SchemaStoreVisit::Stop)
    });

    assert_eq!(visited, vec![1]);
}

#[test]
fn heap_schema_store_preserves_order_latest_snapshot_and_early_stop() {
    let mut store = SchemaStore::init_heap();
    let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
    let newer = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Newer");
    let other_entity = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "Other");

    store
        .insert_persisted_snapshot(EntityTag::new(41), &initial)
        .expect("initial heap schema snapshot should encode");
    store
        .insert_persisted_snapshot(EntityTag::new(42), &other_entity)
        .expect("other heap schema snapshot should encode");
    store
        .insert_persisted_snapshot(EntityTag::new(41), &newer)
        .expect("newer heap schema snapshot should encode");

    let latest = store
        .latest_persisted_snapshot(EntityTag::new(41))
        .expect("latest heap schema snapshot should decode")
        .expect("heap schema snapshot should exist");
    assert_eq!(latest.version(), SchemaVersion::new(2));
    assert_eq!(latest.entity_name(), "Newer");

    let mut visited = Vec::new();
    let _: Result<(), Infallible> = store.visit_raw_snapshots(|key, snapshot| {
        visited.push((
            key.entity_tag().value(),
            key.version(),
            snapshot.as_bytes().len(),
        ));
        Ok(if visited.len() == 2 {
            SchemaStoreVisit::Stop
        } else {
            SchemaStoreVisit::Continue
        })
    });
    assert_eq!(
        visited
            .iter()
            .map(|(entity, version, _)| (*entity, *version))
            .collect::<Vec<_>>(),
        vec![(41, 1), (41, 2)]
    );
}

#[test]
fn journaled_schema_store_streams_overlay_latest_snapshot_and_early_stop() {
    let mut store = SchemaStore::init_journaled(test_memory(233));
    let canonical_initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
    let canonical_replaced = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Canonical");
    let live_replacement = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Live");
    let live_newer = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "LiveNewer");
    let other_entity = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Other");

    store
        .fold_persisted_snapshot(EntityTag::new(61), &canonical_initial)
        .expect("initial canonical schema snapshot should encode");
    store
        .fold_persisted_snapshot(EntityTag::new(61), &canonical_replaced)
        .expect("canonical schema snapshot should encode");
    store
        .fold_persisted_snapshot(EntityTag::new(62), &other_entity)
        .expect("other canonical schema snapshot should encode");
    store
        .insert_persisted_snapshot(EntityTag::new(61), &live_replacement)
        .expect("live replacement schema snapshot should encode");
    store
        .insert_persisted_snapshot(EntityTag::new(61), &live_newer)
        .expect("live newer schema snapshot should encode");

    let latest = store
        .latest_persisted_snapshot(EntityTag::new(61))
        .expect("latest journaled schema snapshot should decode")
        .expect("journaled schema snapshot should exist");
    assert_eq!(latest.version(), SchemaVersion::new(3));
    assert_eq!(latest.entity_name(), "LiveNewer");
    assert_eq!(store.len(), 4);

    let mut visited = Vec::new();
    let _: Result<(), Infallible> = store.visit_raw_snapshots(|key, snapshot| {
        let decoded = snapshot
            .decode_persisted_snapshot()
            .expect("visited schema snapshot should decode");
        visited.push((
            key.entity_tag().value(),
            key.version(),
            decoded.entity_name().to_string(),
        ));
        Ok(if visited.len() == 3 {
            SchemaStoreVisit::Stop
        } else {
            SchemaStoreVisit::Continue
        })
    });
    assert_eq!(
        visited,
        vec![
            (61, 1, "Initial".to_string()),
            (61, 2, "Live".to_string()),
            (61, 3, "LiveNewer".to_string()),
        ],
    );

    store.clear();
    assert!(store.is_empty());
    assert!(
        store
            .latest_persisted_snapshot(EntityTag::new(61))
            .expect("cleared journaled latest snapshot lookup should decode")
            .is_none(),
    );
}

#[test]
fn journaled_schema_store_latest_snapshot_reads_each_overlay_source() {
    let entity = EntityTag::new(71);

    let mut canonical_only = SchemaStore::init_journaled(test_memory(231));
    let canonical = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "CanonicalOnly");
    canonical_only
        .fold_persisted_snapshot(entity, &canonical)
        .expect("canonical-only schema snapshot should encode");
    assert_latest_schema(
        &canonical_only,
        entity,
        SchemaVersion::initial(),
        "CanonicalOnly",
    );

    let mut live_only = SchemaStore::init_journaled(test_memory(230));
    let live = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "LiveOnly");
    live_only
        .insert_persisted_snapshot(entity, &live)
        .expect("live-only schema snapshot should encode");
    assert_latest_schema(&live_only, entity, SchemaVersion::new(2), "LiveOnly");

    let mut live_override = SchemaStore::init_journaled(test_memory(229));
    let canonical_duplicate =
        persisted_schema_snapshot_for_test(SchemaVersion::new(3), "CanonicalDuplicate");
    let live_duplicate = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "LiveDuplicate");
    live_override
        .fold_persisted_snapshot(entity, &canonical_duplicate)
        .expect("canonical duplicate schema snapshot should encode");
    live_override
        .insert_persisted_snapshot(entity, &live_duplicate)
        .expect("live duplicate schema snapshot should encode");
    assert_latest_schema(
        &live_override,
        entity,
        SchemaVersion::new(3),
        "LiveDuplicate",
    );
}

#[test]
fn journaled_schema_store_descending_range_orders_live_between_canonical_versions() {
    let mut store = SchemaStore::init_journaled(test_memory(228));
    let entity = EntityTag::new(72);
    let lower_entity = EntityTag::new(71);
    let higher_entity = EntityTag::new(73);
    let canonical_initial =
        persisted_schema_snapshot_for_test(SchemaVersion::initial(), "CanonicalV1");
    let canonical_duplicate =
        persisted_schema_snapshot_for_test(SchemaVersion::new(2), "CanonicalV2");
    let live_duplicate = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "LiveV2");
    let canonical_latest = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "CanonicalV3");
    let unrelated_lower =
        persisted_schema_snapshot_for_test(SchemaVersion::new(9), "UnrelatedLower");

    store
        .fold_persisted_snapshot(entity, &canonical_initial)
        .expect("canonical v1 schema snapshot should encode");
    store
        .fold_persisted_snapshot(entity, &canonical_duplicate)
        .expect("canonical v2 schema snapshot should encode");
    store
        .fold_persisted_snapshot(entity, &canonical_latest)
        .expect("canonical v3 schema snapshot should encode");
    store
        .fold_persisted_snapshot(lower_entity, &unrelated_lower)
        .expect("lower unrelated schema snapshot should encode");
    store
        .insert_persisted_snapshot(entity, &live_duplicate)
        .expect("live v2 schema snapshot should encode");
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(higher_entity, SchemaVersion::new(1)),
        RawSchemaSnapshot::from_bytes(vec![0xff]),
    );

    let visited = visit_journaled_schema_range(&store, entity, Direction::Desc, usize::MAX);
    assert_eq!(
        visited,
        vec![
            (3, "CanonicalV3".to_string()),
            (2, "LiveV2".to_string()),
            (1, "CanonicalV1".to_string()),
        ],
    );

    let early_stop = visit_journaled_schema_range(&store, entity, Direction::Desc, 1);
    assert_eq!(early_stop, vec![(3, "CanonicalV3".to_string())]);
}

#[test]
fn journaled_schema_store_latest_snapshot_skips_tombstoned_latest_version() {
    let entity = EntityTag::new(74);

    let mut canonical_latest_tombstoned = SchemaStore::init_journaled(test_memory(227));
    let canonical_initial =
        persisted_schema_snapshot_for_test(SchemaVersion::initial(), "CanonicalV1");
    let canonical_latest = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "CanonicalV2");
    canonical_latest_tombstoned
        .fold_persisted_snapshot(entity, &canonical_initial)
        .expect("canonical v1 schema snapshot should encode");
    canonical_latest_tombstoned
        .fold_persisted_snapshot(entity, &canonical_latest)
        .expect("canonical v2 schema snapshot should encode");
    tombstone_journaled_raw_snapshot(
        &mut canonical_latest_tombstoned,
        entity,
        SchemaVersion::new(2),
    );

    assert_latest_schema(
        &canonical_latest_tombstoned,
        entity,
        SchemaVersion::initial(),
        "CanonicalV1",
    );
    assert!(
        canonical_latest_tombstoned
            .get_persisted_snapshot(entity, SchemaVersion::new(2))
            .expect("tombstoned canonical snapshot lookup should not decode")
            .is_none(),
    );

    let mut live_latest_tombstoned = SchemaStore::init_journaled(test_memory(226));
    let live_latest = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "LiveV2");
    live_latest_tombstoned
        .fold_persisted_snapshot(entity, &canonical_initial)
        .expect("canonical v1 schema snapshot should encode");
    live_latest_tombstoned
        .insert_persisted_snapshot(entity, &live_latest)
        .expect("live v2 schema snapshot should encode");
    tombstone_journaled_raw_snapshot(&mut live_latest_tombstoned, entity, SchemaVersion::new(2));

    assert_latest_schema(
        &live_latest_tombstoned,
        entity,
        SchemaVersion::initial(),
        "CanonicalV1",
    );
    assert!(
        live_latest_tombstoned
            .get_persisted_snapshot(entity, SchemaVersion::new(2))
            .expect("tombstoned live snapshot lookup should not decode")
            .is_none(),
    );
}

#[test]
fn schema_store_catalog_metadata_is_absent_without_accepted_snapshots() {
    let store = SchemaStore::init(test_memory(241));

    assert_eq!(
        store
            .catalog_metadata()
            .expect("empty schema catalog metadata should derive"),
        None
    );
}

#[test]
fn schema_store_latest_catalog_identity_uses_version_neutral_header_without_decoding() {
    let mut store = SchemaStore::init(test_memory(239));
    let entity = EntityTag::new(80);
    let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Versioned");
    let newer = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Versioned");
    let expected_fingerprint = accepted_schema_cache_fingerprint(
        &AcceptedSchemaSnapshot::try_new(initial.clone())
            .expect("initial schema snapshot should be accepted"),
    )
    .expect("accepted schema fingerprint should derive");

    store
        .insert_persisted_snapshot(entity, &initial)
        .expect("initial schema snapshot should encode");
    store
        .insert_persisted_snapshot(entity, &newer)
        .expect("newer schema snapshot should encode");

    reset_persisted_schema_snapshot_decode_count_for_tests();
    let selection = store
        .latest_catalog_identity(entity, "entities::Versioned", "schema_store_test")
        .expect("latest catalog identity should derive from header")
        .expect("latest catalog identity should exist");

    assert_eq!(persisted_schema_snapshot_decode_count_for_tests(), 0);
    assert_eq!(
        selection.identity().accepted_schema_version(),
        SchemaVersion::new(2)
    );
    assert_eq!(
        selection.identity().accepted_schema_fingerprint(),
        expected_fingerprint,
        "accepted catalog identity fingerprint must exclude schema_version",
    );
}

#[test]
fn schema_store_catalog_metadata_uses_latest_persisted_snapshots() {
    let mut store = SchemaStore::init(test_memory(240));
    let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
    let newer = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Newer");
    let other = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "Other");

    store
        .insert_persisted_snapshot(EntityTag::new(81), &initial)
        .expect("initial schema snapshot should encode");
    let initial_metadata = store
        .catalog_metadata()
        .expect("initial schema catalog metadata should derive")
        .expect("initial schema catalog metadata should be present");

    store
        .insert_persisted_snapshot(EntityTag::new(81), &newer)
        .expect("newer schema snapshot should encode");
    store
        .insert_persisted_snapshot(EntityTag::new(82), &other)
        .expect("other schema snapshot should encode");
    let updated_metadata = store
        .catalog_metadata()
        .expect("updated schema catalog metadata should derive")
        .expect("updated schema catalog metadata should be present");

    assert_eq!(initial_metadata.schema_version(), SchemaVersion::initial());
    assert_eq!(
        initial_metadata.schema_fingerprint_method_version(),
        SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION
    );
    assert_eq!(initial_metadata.entity_count(), 1);
    assert_eq!(updated_metadata.schema_version(), SchemaVersion::new(3));
    assert_eq!(
        updated_metadata.schema_fingerprint_method_version(),
        SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION
    );
    assert_eq!(updated_metadata.entity_count(), 2);
    assert_ne!(
        initial_metadata.schema_fingerprint(),
        updated_metadata.schema_fingerprint(),
        "catalog fingerprint must change when latest accepted schema catalog changes"
    );
}

#[test]
fn schema_store_catalog_metadata_is_independent_of_insertion_order() {
    let first = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "First");
    let second = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "Second");

    let mut left = SchemaStore::init(test_memory(239));
    left.insert_persisted_snapshot(EntityTag::new(91), &first)
        .expect("first schema snapshot should encode");
    left.insert_persisted_snapshot(EntityTag::new(92), &second)
        .expect("second schema snapshot should encode");

    let mut right = SchemaStore::init(test_memory(238));
    right
        .insert_persisted_snapshot(EntityTag::new(92), &second)
        .expect("second schema snapshot should encode");
    right
        .insert_persisted_snapshot(EntityTag::new(91), &first)
        .expect("first schema snapshot should encode");

    let left_metadata = left
        .catalog_metadata()
        .expect("left schema catalog metadata should derive");
    let right_metadata = right
        .catalog_metadata()
        .expect("right schema catalog metadata should derive");

    assert_eq!(left_metadata, right_metadata);
}

#[test]
fn schema_store_allocation_metadata_uses_role_specific_fingerprints() {
    let without_index =
        persisted_schema_snapshot_for_test(SchemaVersion::initial(), "RoleSpecific");
    let with_index = persisted_schema_snapshot_with_index_for_test(
        SchemaVersion::initial(),
        "RoleSpecific",
        "payload_idx",
    );

    let mut base = SchemaStore::init(test_memory(237));
    base.insert_persisted_snapshot(EntityTag::new(93), &without_index)
        .expect("base schema snapshot should encode");
    let base_metadata = base
        .allocation_metadata()
        .expect("base allocation metadata should derive")
        .expect("base allocation metadata should be present");

    let mut indexed = SchemaStore::init(test_memory(236));
    indexed
        .insert_persisted_snapshot(EntityTag::new(93), &with_index)
        .expect("indexed schema snapshot should encode");
    let indexed_metadata = indexed
        .allocation_metadata()
        .expect("indexed allocation metadata should derive")
        .expect("indexed allocation metadata should be present");

    assert_eq!(
        base_metadata.data().schema_fingerprint(),
        indexed_metadata.data().schema_fingerprint(),
        "data allocation metadata should ignore accepted index catalog changes"
    );
    assert_eq!(
        indexed_metadata.data().schema_fingerprint_method_version(),
        SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_VERSION
    );
    assert_eq!(
        indexed_metadata.index().schema_fingerprint_method_version(),
        SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_VERSION
    );
    assert_eq!(
        indexed_metadata
            .schema()
            .schema_fingerprint_method_version(),
        SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION
    );
    assert_ne!(
        base_metadata.index().schema_fingerprint(),
        indexed_metadata.index().schema_fingerprint(),
        "index allocation metadata should change when accepted index catalog changes"
    );
    assert_ne!(
        base_metadata.schema().schema_fingerprint(),
        indexed_metadata.schema().schema_fingerprint(),
        "schema allocation metadata should change when full accepted catalog changes"
    );
    assert_ne!(
        indexed_metadata.data().schema_fingerprint(),
        indexed_metadata.index().schema_fingerprint(),
        "data and index allocation metadata should have distinct role fingerprints"
    );
    assert_ne!(
        indexed_metadata.index().schema_fingerprint(),
        indexed_metadata.schema().schema_fingerprint(),
        "index and schema allocation metadata should have distinct role fingerprints"
    );
}

#[test]
fn schema_store_rejects_mismatched_snapshot_and_layout_versions() {
    let mut store = SchemaStore::init(test_memory(253));
    let invalid = persisted_schema_snapshot_with_layout_version_for_test(
        SchemaVersion::new(2),
        SchemaVersion::initial(),
        "Invalid",
    );

    let err = store
        .insert_persisted_snapshot(EntityTag::new(43), &invalid)
        .expect_err("schema store should reject mismatched snapshot/layout versions");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "schema store should preserve the version mismatch diagnostic"
    );
}

#[test]
fn schema_store_rejects_typed_snapshot_with_zero_schema_version() {
    let mut store = SchemaStore::init(test_memory(254));
    let invalid = persisted_schema_snapshot_for_test(SchemaVersion::new(0), "ZeroSchemaVersion");

    let err = store
        .insert_persisted_snapshot(EntityTag::new(44), &invalid)
        .expect_err("schema store should reject non-positive schema versions");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "schema store should hard-cut non-positive persisted schema versions"
    );
}

#[test]
fn schema_store_rejects_typed_snapshot_with_divergent_field_slots() {
    let mut store = SchemaStore::init(test_memory(232));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "InvalidSlots");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.first_primary_key_field_id(),
        SchemaRowLayout::new(
            base.version(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(3)),
            ],
        ),
        base.fields().to_vec(),
    );

    let err = store
        .insert_persisted_snapshot(EntityTag::new(44), &invalid)
        .expect_err("schema store should reject divergent field/layout slots");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "schema store should report the duplicated slot divergence"
    );
}

#[test]
fn schema_store_rejects_typed_snapshot_with_duplicate_row_layout_slot() {
    let mut store = SchemaStore::init(test_memory(246));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateLayoutSlot");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.first_primary_key_field_id(),
        SchemaRowLayout::new(
            base.version(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(0)),
            ],
        ),
        base.fields().to_vec(),
    );

    let err = store
        .insert_persisted_snapshot(EntityTag::new(49), &invalid)
        .expect_err("schema store should reject duplicate row-layout slots");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "schema store should report the row-layout slot ambiguity"
    );
}

#[test]
fn schema_store_rejects_typed_snapshot_with_missing_primary_key_field() {
    let mut store = SchemaStore::init(test_memory(248));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "MissingPk");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        FieldId::new(99),
        base.row_layout().clone(),
        base.fields().to_vec(),
    );

    let err = store
        .insert_persisted_snapshot(EntityTag::new(47), &invalid)
        .expect_err("schema store should reject snapshots without the primary-key field");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "schema store should report the missing primary-key field"
    );
}

#[test]
fn schema_store_does_not_fallback_when_latest_snapshot_is_corrupt() {
    let mut store = SchemaStore::init(test_memory(249));
    let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
    let corrupt_key = RawSchemaKey::from_entity_version(EntityTag::new(45), SchemaVersion::new(3));

    store
        .insert_persisted_snapshot(EntityTag::new(45), &initial)
        .expect("initial schema snapshot should encode");
    store.insert_raw_snapshot(corrupt_key, RawSchemaSnapshot::from_bytes(vec![0xff, 0x00]));

    let err = store
        .latest_persisted_snapshot(EntityTag::new(45))
        .expect_err("latest corrupt schema snapshot must fail closed");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "latest-version lookup should report the corrupt newest snapshot"
    );
}

#[test]
fn schema_store_rejects_raw_snapshot_with_divergent_field_slots() {
    let mut store = SchemaStore::init(test_memory(250));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "RawInvalidSlots");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.first_primary_key_field_id(),
        SchemaRowLayout::new(
            base.version(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(3)),
            ],
        ),
        base.fields().to_vec(),
    );
    let raw = encode_persisted_schema_snapshot(&invalid)
        .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
    let key = RawSchemaKey::from_entity_version(EntityTag::new(46), invalid.version());

    store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(raw));

    let err = store
        .latest_persisted_snapshot(EntityTag::new(46))
        .expect_err("raw decode should reject divergent field/layout slots");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "schema codec should report the raw decoded slot divergence"
    );
}

#[test]
fn schema_store_rejects_raw_snapshot_with_missing_primary_key_field() {
    let mut store = SchemaStore::init(test_memory(247));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "RawMissingPk");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        FieldId::new(99),
        base.row_layout().clone(),
        base.fields().to_vec(),
    );
    let raw = encode_persisted_schema_snapshot(&invalid)
        .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
    let key = RawSchemaKey::from_entity_version(EntityTag::new(48), invalid.version());

    store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(raw));

    let err = store
        .latest_persisted_snapshot(EntityTag::new(48))
        .expect_err("raw decode should reject snapshots without the primary-key field");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "schema codec should report the raw decoded missing primary-key field"
    );
}

#[test]
fn schema_store_rejects_raw_snapshot_with_duplicate_field_name() {
    let mut store = SchemaStore::init(test_memory(245));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateFieldName");
    let mut fields = base.fields().to_vec();
    let duplicate = PersistedFieldSnapshot::new(
        fields[1].id(),
        fields[0].name().to_string(),
        fields[1].slot(),
        fields[1].kind().clone(),
        fields[1].nested_leaves().to_vec(),
        fields[1].nullable(),
        fields[1].default().clone(),
        fields[1].storage_decode(),
        fields[1].leaf_codec(),
    );
    fields[1] = duplicate;
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.first_primary_key_field_id(),
        base.row_layout().clone(),
        fields,
    );
    let raw = encode_persisted_schema_snapshot(&invalid)
        .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
    let key = RawSchemaKey::from_entity_version(EntityTag::new(50), invalid.version());

    store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(raw));

    let err = store
        .latest_persisted_snapshot(EntityTag::new(50))
        .expect_err("raw decode should reject duplicate field names");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "schema codec should report the raw decoded field-name ambiguity"
    );
}

#[test]
fn schema_store_rejects_typed_snapshot_with_empty_nested_leaf_path() {
    let mut store = SchemaStore::init(test_memory(244));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "EmptyNestedLeaf");
    let mut fields = base.fields().to_vec();
    let invalid_field = PersistedFieldSnapshot::new(
        fields[1].id(),
        fields[1].name().to_string(),
        fields[1].slot(),
        fields[1].kind().clone(),
        vec![PersistedNestedLeafSnapshot::new(
            Vec::new(),
            PersistedFieldKind::Blob { max_len: None },
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Blob),
        )],
        fields[1].nullable(),
        fields[1].default().clone(),
        fields[1].storage_decode(),
        fields[1].leaf_codec(),
    );
    fields[1] = invalid_field;
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.first_primary_key_field_id(),
        base.row_layout().clone(),
        fields,
    );

    let err = store
        .insert_persisted_snapshot(EntityTag::new(51), &invalid)
        .expect_err("schema store should reject empty nested leaf paths");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "schema store should report the empty nested leaf path"
    );
}

#[test]
fn schema_store_rejects_raw_snapshot_with_duplicate_nested_leaf_path() {
    let mut store = SchemaStore::init(test_memory(243));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateNestedLeaf");
    let mut fields = base.fields().to_vec();
    let duplicate_leaves = vec![
        PersistedNestedLeafSnapshot::new(
            vec!["bytes".to_string()],
            PersistedFieldKind::Blob { max_len: None },
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Blob),
        ),
        PersistedNestedLeafSnapshot::new(
            vec!["bytes".to_string()],
            PersistedFieldKind::Text { max_len: None },
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        ),
    ];
    let invalid_field = PersistedFieldSnapshot::new(
        fields[1].id(),
        fields[1].name().to_string(),
        fields[1].slot(),
        fields[1].kind().clone(),
        duplicate_leaves,
        fields[1].nullable(),
        fields[1].default().clone(),
        fields[1].storage_decode(),
        fields[1].leaf_codec(),
    );
    fields[1] = invalid_field;
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.first_primary_key_field_id(),
        base.row_layout().clone(),
        fields,
    );
    let raw = encode_persisted_schema_snapshot(&invalid)
        .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
    let key = RawSchemaKey::from_entity_version(EntityTag::new(52), invalid.version());

    store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(raw));

    let err = store
        .latest_persisted_snapshot(EntityTag::new(52))
        .expect_err("raw decode should reject duplicate nested leaf paths");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "schema codec should report the raw decoded nested path ambiguity"
    );
}

#[test]
fn raw_schema_snapshot_encodes_and_decodes_typed_snapshot() {
    let snapshot = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Encoded");

    let raw = RawSchemaSnapshot::from_persisted_snapshot(&snapshot)
        .expect("schema snapshot should encode");
    let decoded = raw
        .decode_persisted_snapshot()
        .expect("schema snapshot should decode");

    assert_eq!(decoded, snapshot);
}

// Build one typed schema snapshot used by schema-store tests. The exact
// field contracts are intentionally rich enough to cover nested metadata,
// scalar codecs, and structural fallback payloads through the raw store.
fn assert_latest_schema(
    store: &SchemaStore,
    entity: EntityTag,
    version: SchemaVersion,
    entity_name: &str,
) {
    let latest = store
        .latest_persisted_snapshot(entity)
        .expect("latest schema snapshot should decode")
        .expect("latest schema snapshot should exist");

    assert_eq!(latest.version(), version);
    assert_eq!(latest.entity_name(), entity_name);
}

fn tombstone_journaled_raw_snapshot(
    store: &mut SchemaStore,
    entity: EntityTag,
    version: SchemaVersion,
) {
    let key = RawSchemaKey::from_entity_version(entity, version);
    let SchemaStoreBackend::Journaled { tombstones, .. } = &mut store.backend else {
        panic!("schema tombstone test helper requires a journaled store");
    };

    tombstones.insert(key);
}

fn visit_journaled_schema_range(
    store: &SchemaStore,
    entity: EntityTag,
    direction: Direction,
    stop_after: usize,
) -> Vec<(u32, String)> {
    let SchemaStoreBackend::Journaled {
        canonical,
        live,
        tombstones,
    } = &store.backend
    else {
        panic!("schema range test helper requires a journaled store");
    };

    let mut visited = Vec::new();
    let _: Result<(), Infallible> = SchemaStore::visit_journaled_raw_snapshot_range(
        canonical,
        live,
        tombstones,
        RawSchemaKey::entity_range_bounds(entity),
        direction,
        |key, snapshot| {
            let decoded = snapshot
                .decode_persisted_snapshot()
                .expect("visited schema snapshot should decode");
            visited.push((key.version(), decoded.entity_name().to_string()));
            Ok(if visited.len() >= stop_after {
                SchemaStoreVisit::Stop
            } else {
                SchemaStoreVisit::Continue
            })
        },
    );

    visited
}

fn persisted_schema_snapshot_for_test(
    version: SchemaVersion,
    entity_name: &str,
) -> PersistedSchemaSnapshot {
    persisted_schema_snapshot_with_layout_version_for_test(version, version, entity_name)
}

fn persisted_schema_snapshot_with_index_for_test(
    version: SchemaVersion,
    entity_name: &str,
    index_name: &str,
) -> PersistedSchemaSnapshot {
    let base = persisted_schema_snapshot_for_test(version, entity_name);

    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        base.row_layout().clone(),
        base.fields().to_vec(),
        vec![PersistedIndexSnapshot::new(
            0,
            index_name.to_string(),
            "RoleSpecificStore".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(1),
                SchemaFieldSlot::new(0),
                vec!["id".to_string()],
                PersistedFieldKind::Ulid,
                false,
            )]),
            None,
        )],
    )
}

// Build one typed schema snapshot with independently selectable snapshot
// and row-layout versions. Production snapshots should keep these aligned;
// tests can deliberately break that invariant at the store boundary.
fn persisted_schema_snapshot_with_layout_version_for_test(
    version: SchemaVersion,
    layout_version: SchemaVersion,
    entity_name: &str,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        version,
        format!("entities::{entity_name}"),
        entity_name.to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            layout_version,
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "payload".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Map {
                    key: Box::new(PersistedFieldKind::Text { max_len: None }),
                    value: Box::new(PersistedFieldKind::List(Box::new(
                        PersistedFieldKind::Nat64,
                    ))),
                },
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["bytes".to_string()],
                    PersistedFieldKind::Blob { max_len: None },
                    false,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Blob),
                )],
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::StructuralFallback,
            ),
        ],
    )
}
