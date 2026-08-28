use super::{
    ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_BOOL, ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_LIST,
    ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_SET, AcceptedStoreCatalogScope, IdentityStateStorageView,
    RawSchemaKey, RawSchemaSnapshot, SCHEMA_STORE_FINGERPRINT_METHOD_VERSION, SchemaStore,
    SchemaStoreBackend, SchemaStoreVisit, accepted_schema_bundle_cache_miss_count_for_tests,
    hash_accepted_field_kind, reset_accepted_schema_bundle_cache_miss_count_for_tests,
};
use crate::db::schema::identity_state::{
    IdentityAdvanceId, IdentityRangeAdvance, IdentityState, IdentityStateLifecycle,
    IdentityStateOwner, MAX_IDENTITY_STATE_RECORDS_PER_DATABASE, decode_identity_state,
    encode_identity_state,
};
use crate::{
    db::schema::{FieldStorageDecode, LeafCodec, ScalarCodec},
    db::{
        codec::{finalize_hash_sha256, new_hash_sha256},
        direction::Direction,
        integrity::DatabaseIncarnationId,
        journal::JournalSequence,
        positioned_overlay::{JournalOverlayPosition, PositionedOverlayRetirement},
        registry::{StoreAllocationIdentities, StoreAllocationIdentity},
        schema::{
            AcceptedCheckExprV1, AcceptedCompositeCatalog, AcceptedFieldKind,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
            CandidateSchemaRevision, ConstraintActivationState, ConstraintOrigin,
            ConstraintValidationJob, FieldId, FieldInsertGeneration, PersistedFieldSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
            SchemaFieldWritePolicy, SchemaIndexId, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, accepted_schema_cache_fingerprint, accepted_schema_candidate_for_tests,
            cardinality_generation::{
                CardinalityBuildCursor, CardinalityBuildPhase, CardinalityBuildTotals,
                CardinalityCountDigest, CardinalityCountRecord, CardinalityCountSlot,
                CardinalityGenerationHeader, CardinalityGenerationId, CardinalityGenerationState,
                CardinalitySourceIdentity,
            },
            composite_catalog::CompositeTypeId,
            empty_accepted_schema_candidate_for_tests,
            encode_unchecked_persisted_schema_snapshot_for_tests,
            enum_catalog::{AcceptedSchemaFingerprint, AcceptedSchemaRevisionBundle},
        },
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
    types::EntityTag,
};

use ic_stable_structures::{Storable, storable::Bound};
use std::borrow::Cow;
use std::convert::Infallible;

fn overlay_position(sequence: u64) -> JournalOverlayPosition {
    JournalOverlayPosition::new(
        StoreAllocationIdentity::new(232, "test::schema"),
        JournalSequence::new(sequence),
    )
}

fn accepted_field_kind_fingerprint(kind: &AcceptedFieldKind) -> [u8; 32] {
    let mut hasher = new_hash_sha256();
    hash_accepted_field_kind(&mut hasher, kind);
    finalize_hash_sha256(hasher)
}

const fn test_database_incarnation() -> DatabaseIncarnationId {
    DatabaseIncarnationId::for_tests(0x61)
}

fn cardinality_source_identity() -> CardinalitySourceIdentity {
    CardinalitySourceIdentity::derive(
        test_database_incarnation(),
        StoreAllocationIdentities::new_journaled(
            StoreAllocationIdentity::new(180, "test.cardinality.data.v1"),
            StoreAllocationIdentity::new(181, "test.cardinality.index.v1"),
            StoreAllocationIdentity::new(182, "test.cardinality.schema.v1"),
            StoreAllocationIdentity::new(183, "test.cardinality.journal.v1"),
        ),
        None,
        [],
        crate::db::journal::FoldWatermark::initial(),
    )
    .expect("complete current source identity should derive")
}

#[test]
fn accepted_field_kind_fingerprint_discriminates_maps_from_composites() {
    let map = AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Bool))),
        value: Box::new(AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Bool))),
    };
    let colliding_payload = u32::from_be_bytes([
        ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_LIST,
        ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_BOOL,
        ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_SET,
        ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_BOOL,
    ]);
    let composite = AcceptedFieldKind::Composite {
        type_id: CompositeTypeId::new(colliding_payload)
            .expect("the collision-proof test ID should be non-zero"),
    };

    assert_ne!(
        accepted_field_kind_fingerprint(&map),
        accepted_field_kind_fingerprint(&composite),
        "semantic field-kind variants must have distinct fingerprint domains",
    );
}

#[test]
fn schema_store_catalog_scope_is_local_and_stable_for_its_lifetime() {
    let first_store = SchemaStore::init_heap();
    let second_store = SchemaStore::init_heap();

    let first = first_store
        .accepted_catalog_scope
        .get_or_init(AcceptedStoreCatalogScope::new)
        .clone();
    let first_again = first_store
        .accepted_catalog_scope
        .get_or_init(AcceptedStoreCatalogScope::new)
        .clone();
    let second = second_store
        .accepted_catalog_scope
        .get_or_init(AcceptedStoreCatalogScope::new)
        .clone();

    assert_eq!(first, first_again);
    assert_ne!(first, second);
}

#[test]
fn schema_store_matches_only_its_current_root_authority() {
    let mut store = SchemaStore::init_heap();
    let initial = empty_accepted_schema_candidate_for_tests(
        "test::AuthoritySchemaStore",
        AcceptedSchemaRevision::INITIAL,
    );
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("initial accepted schema root should bootstrap");
    let store_scope = store
        .accepted_catalog_scope
        .get_or_init(AcceptedStoreCatalogScope::new)
        .clone();
    let root = initial.root();
    let current = AcceptedValueCatalogHandle::new(
        initial.bundle().enum_catalog().clone(),
        initial.bundle().composite_catalog().clone(),
        store_scope.clone(),
        root.revision(),
        root.fingerprint(),
    );
    assert!(
        store
            .current_accepted_persisted_snapshot(EntityTag::new(7))
            .expect("accepted bundle should populate the root cache")
            .is_none(),
    );
    assert!(store.accepted_bundle_cache.borrow().is_some());
    assert!(
        store
            .current_accepted_schema_authority_matches(current.authority())
            .expect("current authority comparison should reuse the cached root"),
    );

    let foreign_store = AcceptedValueCatalogHandle::new(
        initial.bundle().enum_catalog().clone(),
        AcceptedCompositeCatalog::empty(),
        AcceptedStoreCatalogScope::new(),
        root.revision(),
        root.fingerprint(),
    );
    assert!(
        !store
            .current_accepted_schema_authority_matches(foreign_store.authority())
            .expect("foreign authority comparison should reject the store scope"),
    );

    let mut wrong_fingerprint_bytes = root.fingerprint().as_bytes();
    wrong_fingerprint_bytes[0] ^= 1;
    let wrong_fingerprint = AcceptedValueCatalogHandle::new(
        initial.bundle().enum_catalog().clone(),
        AcceptedCompositeCatalog::empty(),
        store_scope,
        root.revision(),
        AcceptedSchemaFingerprint::new(wrong_fingerprint_bytes),
    );
    assert!(
        !store
            .current_accepted_schema_authority_matches(wrong_fingerprint.authority())
            .expect("fingerprint authority comparison should reject the mismatch"),
    );

    let second = empty_accepted_schema_candidate_for_tests(
        "test::AuthoritySchemaStore",
        AcceptedSchemaRevision::new(2),
    );
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &second,
        )
        .expect("second accepted schema root should publish");
    assert!(store.accepted_bundle_cache.borrow().is_none());
    assert!(
        !store
            .current_accepted_schema_authority_matches(current.authority())
            .expect("stale authority comparison should read the new root"),
    );
}

#[test]
fn schema_store_requires_exact_job_closure_for_validating_activation() {
    let entity = EntityTag::new(0xCAFE);
    let empty = empty_accepted_schema_candidate_for_tests(
        "test::ValidationJobStore",
        AcceptedSchemaRevision::INITIAL,
    );
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::Validated".to_string(),
        "Validated".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
    );
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check_activation(
            "pending_check".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            2,
        )
        .expect("activation should reserve identity");
    let snapshot = snapshot.with_constraint_catalog(catalog);
    let initial_bundle = AcceptedSchemaRevisionBundle::new(
        AcceptedSchemaRevision::INITIAL,
        "test::ValidationJobStore",
        empty.bundle().enum_catalog().clone(),
        empty.bundle().composite_catalog().clone(),
        std::collections::BTreeMap::from([(entity, snapshot.clone())]),
    )
    .expect("activation bundle should build");
    let initial =
        CandidateSchemaRevision::new(initial_bundle).expect("activation candidate should encode");
    let mut store = SchemaStore::init_heap();
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("Enforcing activation should publish without a job");

    let validating_catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_validation_started(snapshot.constraint_activations()[0].id())
        .expect("activation should enter validation");
    let validating_snapshot = snapshot.with_constraint_catalog(validating_catalog);
    let activation = &validating_snapshot.constraint_activations()[0];
    assert_eq!(activation.state(), ConstraintActivationState::Validating);
    let job = ConstraintValidationJob::start(
        entity,
        validating_snapshot.entity_path().to_string(),
        activation,
        None,
    )
    .expect("validating activation should create a job");
    let validating_bundle = AcceptedSchemaRevisionBundle::new(
        AcceptedSchemaRevision::new(2),
        "test::ValidationJobStore",
        initial.bundle().enum_catalog().clone(),
        initial.bundle().composite_catalog().clone(),
        std::collections::BTreeMap::from([(entity, validating_snapshot)]),
    )
    .expect("validating bundle should build");
    assert!(
        store
            .validate_constraint_validation_job_closure(&validating_bundle)
            .is_err(),
        "Validating activation without its exact job must fail closed",
    );
    store
        .validate_constraint_validation_job_closure_with_change(
            &validating_bundle,
            Some(&job),
            None,
        )
        .expect("candidate plus exact job should close");
}

#[test]
fn raw_schema_key_round_trips_entity_and_version() {
    let key = RawSchemaKey::from_entity_version(EntityTag::new(0x0102_0304_0506_0708), {
        SchemaVersion::initial()
    });
    let encoded = key.to_bytes().into_owned();
    let decoded = RawSchemaKey::from_bytes(Cow::Owned(encoded));

    assert_eq!(
        key.to_bytes().as_ref(),
        &[
            0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00,
            0x00, 0x01,
        ],
        "schema-store entity key namespace and big-endian layout are persisted format",
    );
    assert_eq!(decoded.entity_tag(), EntityTag::new(0x0102_0304_0506_0708));
    assert_eq!(decoded.version(), SchemaVersion::initial().get());
}

#[test]
fn raw_schema_control_record_round_trips_opaque_bytes() {
    let snapshot = RawSchemaSnapshot::from_encoded_control_record(b"ICYDBAEB\x01\x02\x03".to_vec());
    let encoded = snapshot.to_bytes().into_owned();
    let decoded = <RawSchemaSnapshot as Storable>::from_bytes(Cow::Owned(encoded));

    assert_eq!(decoded.as_bytes(), b"ICYDBAEB\x01\x02\x03");
    assert_eq!(decoded.into_bytes(), b"ICYDBAEB\x01\x02\x03");
}

#[test]
fn cardinality_generation_records_use_reserved_schema_namespaces_and_survive_reopen() {
    let memory = test_memory(253);
    let source = cardinality_source_identity();
    let header = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        CardinalityGenerationState::Building,
        CardinalityCountSlot::B,
        source,
    );
    let cursor = CardinalityBuildCursor::new(
        CardinalityGenerationId::INITIAL,
        CardinalityCountSlot::B,
        source,
        CardinalityBuildPhase::Rows,
        None,
        CardinalityBuildTotals::default(),
    )
    .expect("initial row cursor should be valid");
    let digest = CardinalityCountDigest::for_entity(EntityTag::new(42));
    let count = CardinalityCountRecord::new(CardinalityGenerationId::INITIAL, digest, u64::MAX)
        .expect("maximum nonzero count should encode");

    let header_key = RawSchemaKey::from_cardinality_generation_header();
    let cursor_key = RawSchemaKey::from_cardinality_build_cursor();
    let inactive_count_key = RawSchemaKey::from_cardinality_count(CardinalityCountSlot::A, digest);
    let building_count_key = RawSchemaKey::from_cardinality_count(CardinalityCountSlot::B, digest);
    assert_eq!(
        header_key.to_bytes().as_ref(),
        &[5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        cursor_key.to_bytes().as_ref(),
        &[5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
    );
    assert_eq!(inactive_count_key.to_bytes().as_ref()[0], 6);
    assert_eq!(building_count_key.to_bytes().as_ref()[0], 7);
    assert_eq!(
        &building_count_key.to_bytes().as_ref()[1..],
        &digest.as_bytes()[..15]
    );

    let mut store = SchemaStore::init_journaled(memory.clone());
    store
        .insert_canonical_raw_value(header_key, header.encode())
        .expect("generation header should persist");
    store
        .insert_canonical_raw_value(
            cursor_key,
            cursor.encode().expect("bounded build cursor should encode"),
        )
        .expect("build cursor should persist");
    store
        .insert_canonical_raw_value(building_count_key, count.encode().to_vec())
        .expect("count record should persist");
    drop(store);

    let reopened = SchemaStore::init_journaled(memory);
    let reopened_header = reopened
        .get_canonical_raw_value(&header_key)
        .expect("header read should succeed")
        .expect("header should exist");
    let reopened_cursor = reopened
        .get_canonical_raw_value(&cursor_key)
        .expect("cursor read should succeed")
        .expect("cursor should exist");
    let reopened_count = reopened
        .get_canonical_raw_value(&building_count_key)
        .expect("count read should succeed")
        .expect("count should exist");
    assert_eq!(
        CardinalityGenerationHeader::decode(reopened_header.as_bytes())
            .expect("reopened header should decode"),
        header,
    );
    assert_eq!(
        CardinalityBuildCursor::decode(reopened_cursor.as_bytes())
            .expect("reopened cursor should decode"),
        cursor,
    );
    assert_eq!(
        CardinalityCountRecord::decode(reopened_count.as_bytes())
            .expect("reopened count should decode"),
        count,
    );
}

#[test]
fn cardinality_header_cache_revalidates_changed_canonical_bytes() {
    let mut store = SchemaStore::init_journaled(test_memory(242));
    let header = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        CardinalityGenerationState::Ready,
        CardinalityCountSlot::A,
        cardinality_source_identity(),
    );
    store
        .write_cardinality_generation_header(header)
        .expect("valid header should persist");
    assert_eq!(
        store
            .cardinality_generation_header()
            .expect("valid header should populate the decode cache"),
        Some(header),
    );

    let mut malformed = header.encode();
    malformed[0] ^= 0xff;
    store
        .insert_canonical_raw_value(
            RawSchemaKey::from_cardinality_generation_header(),
            malformed,
        )
        .expect("malformed raw header should seed the corruption fixture");
    assert!(
        store.cardinality_generation_header().is_err(),
        "changed canonical bytes must be decoded rather than served from cache",
    );
}

#[test]
fn cardinality_count_page_preflight_rejects_overflow_and_uncoalesced_input() {
    let mut store = SchemaStore::init_journaled(test_memory(251));
    let digest = CardinalityCountDigest::for_entity(EntityTag::new(42));
    let count = CardinalityCountRecord::new(CardinalityGenerationId::INITIAL, digest, u64::MAX)
        .expect("maximum count should construct");
    store
        .insert_canonical_raw_value(
            RawSchemaKey::from_cardinality_count(CardinalityCountSlot::B, digest),
            count.encode().to_vec(),
        )
        .expect("maximum count should seed");
    assert!(
        store
            .prepare_cardinality_count_increments(
                CardinalityCountSlot::B,
                CardinalityGenerationId::INITIAL,
                &[(digest, 1)],
            )
            .is_err(),
        "exact count overflow must fail during read-only preflight",
    );
    let second_digest = CardinalityCountDigest::for_entity(EntityTag::new(43));
    assert!(
        store
            .prepare_cardinality_count_increments(
                CardinalityCountSlot::B,
                CardinalityGenerationId::INITIAL,
                &[(second_digest, 1), (second_digest, 1)],
            )
            .is_err(),
        "page callers must coalesce repeated logical count keys",
    );
}

#[test]
fn ready_cardinality_maintenance_preflights_checked_deltas_and_watermark_publication() {
    let source = cardinality_source_identity();
    let allocations = StoreAllocationIdentities::new_journaled(
        StoreAllocationIdentity::new(180, "test.cardinality.data.v1"),
        StoreAllocationIdentity::new(181, "test.cardinality.index.v1"),
        StoreAllocationIdentity::new(182, "test.cardinality.schema.v1"),
        StoreAllocationIdentity::new(183, "test.cardinality.journal.v1"),
    );
    let next_source = CardinalitySourceIdentity::derive(
        test_database_incarnation(),
        allocations,
        None,
        [],
        crate::db::journal::FoldWatermark::new(JournalSequence::new(1), 1),
    )
    .expect("next fold source should derive");
    let final_source = CardinalitySourceIdentity::derive(
        test_database_incarnation(),
        allocations,
        None,
        [],
        crate::db::journal::FoldWatermark::new(JournalSequence::new(2), 2),
    )
    .expect("final fold source should derive");
    let header = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        CardinalityGenerationState::Ready,
        CardinalityCountSlot::A,
        source,
    );
    let digest = CardinalityCountDigest::for_entity(EntityTag::new(42));
    let count = CardinalityCountRecord::new(CardinalityGenerationId::INITIAL, digest, 3)
        .expect("initial count should encode");
    let mut store = SchemaStore::init_journaled(test_memory(250));
    store
        .write_cardinality_generation_header(header)
        .expect("Ready header should persist");
    store
        .insert_canonical_raw_value(
            RawSchemaKey::from_cardinality_count(CardinalityCountSlot::A, digest),
            count.encode().to_vec(),
        )
        .expect("initial count should persist");

    let prepared = store
        .prepare_cardinality_maintenance(header, source, next_source, &[(digest, 2)])
        .expect("increment and next watermark should preflight");
    store
        .apply_prepared_cardinality_maintenance(prepared)
        .expect("preflighted positive maintenance should apply");
    let next_header = store
        .cardinality_generation_header()
        .expect("next header should decode")
        .expect("next header should exist");
    assert_eq!(
        store
            .cardinality_count(
                CardinalityCountSlot::A,
                CardinalityGenerationId::INITIAL,
                digest
            )
            .expect("incremented count should decode"),
        Some(5),
    );
    assert!(next_header.validate_source(next_source).is_ok());

    let prepared = store
        .prepare_cardinality_maintenance(next_header, next_source, final_source, &[(digest, -5)])
        .expect("exact removal should preflight");
    store
        .apply_prepared_cardinality_maintenance(prepared)
        .expect("preflighted zero maintenance should apply");
    assert_eq!(
        store
            .cardinality_count(
                CardinalityCountSlot::A,
                CardinalityGenerationId::INITIAL,
                digest
            )
            .expect("removed zero count should decode"),
        None,
    );
    assert!(
        store
            .prepare_cardinality_maintenance(
                store
                    .cardinality_generation_header()
                    .expect("final header should decode")
                    .expect("final header should exist"),
                final_source,
                final_source,
                &[(digest, -1)],
            )
            .is_err(),
        "count underflow must fail before stable mutation",
    );
}

#[test]
fn cardinality_slot_clear_is_bounded_and_installs_the_cursor_only_when_empty() {
    let memory = test_memory(252);
    let source = cardinality_source_identity();
    let header = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        CardinalityGenerationState::Building,
        CardinalityCountSlot::A,
        source,
    );
    let cursor = CardinalityBuildCursor::new(
        CardinalityGenerationId::INITIAL,
        CardinalityCountSlot::A,
        source,
        CardinalityBuildPhase::Rows,
        None,
        CardinalityBuildTotals::default(),
    )
    .expect("initial row cursor should be valid");
    let mut store = SchemaStore::init_journaled(memory.clone());
    store
        .write_cardinality_generation_header(header)
        .expect("Building header should persist");
    for entity in 1..=4_097 {
        let digest = CardinalityCountDigest::for_entity(EntityTag::new(entity));
        let record = CardinalityCountRecord::new(CardinalityGenerationId::INITIAL, digest, 1)
            .expect("nonzero count should construct");
        store
            .insert_canonical_raw_value(
                RawSchemaKey::from_cardinality_count(CardinalityCountSlot::A, digest),
                record.encode().to_vec(),
            )
            .expect("stale candidate count should seed");
    }

    assert!(
        store
            .clear_cardinality_count_slot_page(header, &cursor, 4_096)
            .expect("first clear page should succeed")
    );
    assert!(
        store
            .cardinality_build_cursor()
            .expect("cursor lookup should succeed")
            .is_none(),
        "a partially cleared slot must not start scanning",
    );
    assert!(
        !store
            .cardinality_count_slot_is_empty(CardinalityCountSlot::A)
            .expect("slot probe should succeed")
    );
    assert!(
        !store
            .clear_cardinality_count_slot_page(header, &cursor, 4_096)
            .expect("final clear page should succeed")
    );
    assert!(
        store
            .cardinality_count_slot_is_empty(CardinalityCountSlot::A)
            .expect("slot should now be empty")
    );
    assert_eq!(
        store
            .cardinality_build_cursor()
            .expect("cursor should decode")
            .expect("cursor should be installed"),
        cursor,
    );
    drop(store);

    let reopened = SchemaStore::init_journaled(memory);
    assert!(
        reopened
            .cardinality_count_slot_is_empty(CardinalityCountSlot::A)
            .expect("reopened slot should remain empty")
    );
    assert_eq!(
        reopened
            .cardinality_build_cursor()
            .expect("reopened cursor should decode")
            .expect("reopened cursor should exist"),
        cursor,
    );
}

#[test]
fn raw_schema_snapshot_storable_bound_does_not_amplify_stable_btree_nodes() {
    assert_eq!(RawSchemaSnapshot::BOUND, Bound::Unbounded);
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
    let mut store = SchemaStore::init_journaled(test_memory(251));
    let key = RawSchemaKey::from_entity_version(EntityTag::new(17), SchemaVersion::initial());

    assert!(store.is_empty());
    assert!(!store.contains_raw_snapshot(&key));

    store.insert_raw_snapshot(
        key,
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(vec![9, 4, 6]),
    );

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
    let mut store = SchemaStore::init_journaled(test_memory(252));
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
        .latest_staged_persisted_snapshot(EntityTag::new(41))
        .expect("latest schema snapshot should decode")
        .expect("schema snapshot should exist");

    assert_eq!(latest.version(), SchemaVersion::new(2));
    assert_eq!(latest.entity_name(), "Newer");
}

#[test]
fn schema_store_visit_raw_snapshots_preserves_key_order() {
    let mut store = SchemaStore::init_journaled(test_memory(235));
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(3), SchemaVersion::new(2)),
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(vec![32]),
    );
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(1), SchemaVersion::new(3)),
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(vec![13]),
    );
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(1), SchemaVersion::new(1)),
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(vec![11]),
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
    let mut store = SchemaStore::init_journaled(test_memory(234));
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(2), SchemaVersion::new(1)),
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(vec![21]),
    );
    store.insert_raw_snapshot(
        RawSchemaKey::from_entity_version(EntityTag::new(2), SchemaVersion::new(2)),
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(vec![22]),
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
        .latest_staged_persisted_snapshot(EntityTag::new(41))
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
        .latest_staged_persisted_snapshot(EntityTag::new(61))
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
            .latest_staged_persisted_snapshot(EntityTag::new(61))
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
fn journaled_schema_candidate_replay_and_fold_are_idempotent() {
    let memory = test_memory(229);
    let mut store = SchemaStore::init_journaled(memory.clone());
    let initial = empty_accepted_schema_candidate_for_tests(
        "test::JournaledSchemaStore",
        AcceptedSchemaRevision::INITIAL,
    );
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("initial accepted schema root should bootstrap");
    assert_eq!(store.canonical_len_for_tests(), 2);

    let second = empty_accepted_schema_candidate_for_tests(
        "test::JournaledSchemaStore",
        AcceptedSchemaRevision::new(2),
    );
    store
        .apply_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &second,
        )
        .expect("journal replay should publish the live root");
    store
        .apply_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &second,
        )
        .expect("replaying an already-current candidate should be idempotent");
    assert_eq!(
        store
            .current_accepted_schema_bundle()
            .expect("live accepted bundle should decode")
            .expect("live accepted bundle should exist")
            .revision(),
        AcceptedSchemaRevision::new(2),
    );
    assert_eq!(store.canonical_len_for_tests(), 2);

    let mut reopened = SchemaStore::init_journaled(memory);
    assert_eq!(
        reopened
            .current_accepted_schema_bundle()
            .expect("canonical accepted bundle should decode")
            .expect("canonical accepted bundle should exist")
            .revision(),
        AcceptedSchemaRevision::INITIAL,
    );
    reopened
        .apply_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &second,
        )
        .expect("tail replay should restore the live candidate");
    reopened
        .fold_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &second,
        )
        .expect("committed candidate should fold into the canonical BTree");
    reopened
        .fold_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &second,
        )
        .expect("repeated candidate fold should be idempotent");
    reopened
        .reset_journaled_live_projection()
        .expect("live projection should reset");
    assert_eq!(
        reopened
            .current_accepted_schema_bundle()
            .expect("folded accepted bundle should decode")
            .expect("folded accepted bundle should exist")
            .revision(),
        AcceptedSchemaRevision::new(2),
    );
    assert_eq!(reopened.canonical_len_for_tests(), 2);
}

#[test]
fn identity_publication_creates_explicit_zero_state_and_missing_state_is_corruption() {
    let entity_tag = EntityTag::new(101);
    let field_id = FieldId::new(1);
    let initial = identity_candidate_for_test(
        "test::IdentityStateStore",
        AcceptedSchemaRevision::INITIAL,
        entity_tag,
        field_id,
        AcceptedFieldKind::Nat64,
    );
    let mut store = SchemaStore::init_heap();
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("initial identity declaration should publish with zero state");

    let key = RawSchemaKey::from_identity_state(entity_tag, field_id);
    let state = decode_identity_state(
        store
            .get_raw_snapshot(&key)
            .expect("published identity must have explicit state")
            .as_bytes(),
    )
    .expect("published identity state should decode");
    assert_eq!(state.lifecycle(), IdentityStateLifecycle::Active);
    assert_eq!(state.materialized_high_water(), 0);
    assert_eq!(state.last_applied_advance(), None);
    assert_eq!(state.accepted_kind(), &AcceptedFieldKind::Nat64);
    assert_eq!(
        state.owner().database_incarnation_id(),
        test_database_incarnation(),
    );
    assert!(
        store
            .preflight_accepted_schema_candidate(
                test_database_incarnation(),
                AcceptedSchemaRevision::NONE,
                &initial,
            )
            .expect("exact publication replay should preflight"),
    );
    let cursor = store
        .identity_statement_cursor(
            test_database_incarnation(),
            entity_tag,
            field_id,
            &AcceptedFieldKind::Nat64,
        )
        .expect("active accepted state should open a statement cursor");
    assert_eq!(cursor.owner(), state.owner());
    assert_eq!(cursor.expected_high_water(), 0);
    for (incarnation, kind) in [
        (
            DatabaseIncarnationId::for_tests(0xFF),
            AcceptedFieldKind::Nat64,
        ),
        (test_database_incarnation(), AcceptedFieldKind::Nat32),
    ] {
        let error = store
            .identity_statement_cursor(incarnation, entity_tag, field_id, &kind)
            .expect_err("a mismatched owner or kind must reject");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Identity);
    }

    let SchemaStoreBackend::Heap(map) = &mut store.backend else {
        panic!("identity missing-state fixture requires a heap store");
    };
    map.remove(&key);
    let error = store
        .identity_statement_cursor(
            test_database_incarnation(),
            entity_tag,
            field_id,
            &AcceptedFieldKind::Nat64,
        )
        .expect_err("a missing active state must not synthesize zero");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
    let error = store
        .current_accepted_schema_bundle()
        .expect_err("accepted identity without explicit state must be corruption");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Identity);

    let mut malformed = encode_identity_state(&state).expect("identity state should encode");
    malformed[9] ^= 0xFF;
    store.insert_durable_raw_value(key, malformed);
    let error = store
        .current_accepted_schema_bundle()
        .expect_err("malformed identity state must be corruption");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
}

#[test]
fn two_identity_cursors_reject_the_stale_range_and_require_exact_advance_identity() {
    let entity_tag = EntityTag::new(111);
    let field_id = FieldId::new(1);
    let initial = identity_candidate_for_test(
        "test::IdentityRangeStore",
        AcceptedSchemaRevision::INITIAL,
        entity_tag,
        field_id,
        AcceptedFieldKind::Nat64,
    );
    let mut store = SchemaStore::init_heap();
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("initial identity declaration should publish");
    let mut first_cursor = store
        .identity_statement_cursor(
            test_database_incarnation(),
            entity_tag,
            field_id,
            &AcceptedFieldKind::Nat64,
        )
        .expect("the first statement cursor should open at zero");
    let mut stale_cursor = store
        .identity_statement_cursor(
            test_database_incarnation(),
            entity_tag,
            field_id,
            &AcceptedFieldKind::Nat64,
        )
        .expect("the competing statement cursor should observe the same zero state");
    for ordinal in 0..2 {
        first_cursor
            .allocate(0, ordinal)
            .expect("the first cursor should allocate a bounded value");
        stale_cursor
            .allocate(0, ordinal)
            .expect("the stale cursor should tentatively allocate the same value");
    }
    let range = first_cursor
        .into_range_advance()
        .expect("the first cursor should close")
        .expect("the first cursor should carry one range");
    let stale_range = stale_cursor
        .into_range_advance()
        .expect("the stale cursor should close")
        .expect("the stale cursor should carry one range");
    assert_eq!(stale_range, range);
    let advance =
        IdentityAdvanceId::try_new([0x31; 16], [0x41; 16], 7, 2).expect("advance should build");

    store
        .preflight_identity_range_advance(range)
        .expect("current expected high-water should admit");
    store
        .apply_identity_range_advance(range, advance)
        .expect("first range apply should advance state");
    store
        .apply_identity_range_advance(range, advance)
        .expect("exact advance replay should be idempotent");
    store
        .verify_identity_range_advance(range, advance)
        .expect("exact materialized range should verify");

    let stale = store
        .preflight_identity_range_advance(stale_range)
        .expect_err("stale expected high-water must reject before marker publication");
    assert_eq!(stale.class(), ErrorClass::Conflict);
    assert_eq!(stale.origin(), ErrorOrigin::Identity);

    let other_advance =
        IdentityAdvanceId::try_new([0x32; 16], [0x42; 16], 7, 2).expect("advance should build");
    let mismatch = store
        .apply_identity_range_advance(range, other_advance)
        .expect_err("numeric equality without exact advance identity must be corruption");
    assert_eq!(mismatch.class(), ErrorClass::Corruption);
    assert_eq!(mismatch.origin(), ErrorOrigin::Identity);
}

#[test]
fn journaled_identity_range_replay_and_fold_share_exact_advance_identity() {
    let entity_tag = EntityTag::new(112);
    let field_id = FieldId::new(1);
    let initial = identity_candidate_for_test(
        "test::JournaledIdentityRangeStore",
        AcceptedSchemaRevision::INITIAL,
        entity_tag,
        field_id,
        AcceptedFieldKind::Nat128,
    );
    let mut store = SchemaStore::init_journaled(test_memory(182));
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("journaled identity declaration should publish");
    let owner = IdentityStateOwner::try_new(test_database_incarnation(), entity_tag, field_id)
        .expect("identity owner should build");
    let range =
        IdentityRangeAdvance::try_new(owner, 0, 3, 3).expect("contiguous range should build");
    let advance =
        IdentityAdvanceId::try_new([0x51; 16], [0x61; 16], 9, 3).expect("advance should build");

    store
        .apply_identity_range_advance(range, advance)
        .expect("range replay should update the live projection");
    store
        .fold_identity_range_advance(range, advance)
        .expect("range fold should update canonical state");
    store
        .fold_identity_range_advance(range, advance)
        .expect("exact canonical fold replay should be idempotent");
    store
        .reset_journaled_live_projection()
        .expect("live projection should reset to canonical state");
    store
        .verify_identity_range_advance(range, advance)
        .expect("folded exact identity should survive projection rebuild");
}

#[test]
fn identity_removal_retires_state_and_reuse_is_rejected() {
    let entity_tag = EntityTag::new(102);
    let field_id = FieldId::new(1);
    let store_path = "test::IdentityRetirementStore";
    let initial = identity_candidate_for_test(
        store_path,
        AcceptedSchemaRevision::INITIAL,
        entity_tag,
        field_id,
        AcceptedFieldKind::Nat32,
    );
    let removed =
        empty_accepted_schema_candidate_for_tests(store_path, AcceptedSchemaRevision::new(2));
    let reused = identity_candidate_for_test(
        store_path,
        AcceptedSchemaRevision::new(3),
        entity_tag,
        field_id,
        AcceptedFieldKind::Nat32,
    );
    let mut store = SchemaStore::init_heap();
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("identity declaration should publish");
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &removed,
        )
        .expect("identity removal should retire its state");

    let key = RawSchemaKey::from_identity_state(entity_tag, field_id);
    let retired = decode_identity_state(
        store
            .get_raw_snapshot(&key)
            .expect("retired identity state must be retained")
            .as_bytes(),
    )
    .expect("retired state should decode");
    assert_eq!(retired.lifecycle(), IdentityStateLifecycle::Retired);
    assert_eq!(retired.materialized_high_water(), 0);
    assert_eq!(retired.accepted_kind(), &AcceptedFieldKind::Nat32);
    assert_eq!(
        store
            .identity_state_inventory_for_integrity(test_database_incarnation())
            .expect("integrity inventory should retain retired state"),
        vec![retired],
    );
    let incarnation_error = store
        .identity_state_inventory_for_integrity(DatabaseIncarnationId::for_tests(0x62))
        .expect_err("integrity inventory must reject another database incarnation");
    assert_eq!(incarnation_error.class(), ErrorClass::Corruption);
    assert_eq!(incarnation_error.origin(), ErrorOrigin::Identity);

    let error = store
        .preflight_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::new(2),
            &reused,
        )
        .expect_err("retired accepted owner must never be reused");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
    assert_eq!(
        store
            .current_accepted_schema_bundle()
            .expect("current accepted bundle should remain readable")
            .expect("removed revision should remain current")
            .revision(),
        AcceptedSchemaRevision::new(2),
    );
}

#[test]
fn journaled_identity_retirement_replays_and_folds_with_schema_publication() {
    let entity_tag = EntityTag::new(103);
    let field_id = FieldId::new(1);
    let store_path = "test::JournaledIdentityStateStore";
    let initial = identity_candidate_for_test(
        store_path,
        AcceptedSchemaRevision::INITIAL,
        entity_tag,
        field_id,
        AcceptedFieldKind::Nat16,
    );
    let removed =
        empty_accepted_schema_candidate_for_tests(store_path, AcceptedSchemaRevision::new(2));
    let memory = test_memory(181);
    let mut store = SchemaStore::init_journaled(memory.clone());
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("initial identity should bootstrap canonical zero state");
    store
        .apply_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &removed,
        )
        .expect("schema replay should retire the live identity state");

    let live = store
        .identity_state_inventory(IdentityStateStorageView::Effective)
        .expect("live identity inventory should decode");
    let canonical = store
        .identity_state_inventory(IdentityStateStorageView::Canonical)
        .expect("canonical identity inventory should decode");
    assert_eq!(
        live[&(entity_tag, field_id)].lifecycle(),
        IdentityStateLifecycle::Retired,
    );
    assert_eq!(
        canonical[&(entity_tag, field_id)].lifecycle(),
        IdentityStateLifecycle::Active,
    );

    let mut reopened = SchemaStore::init_journaled(memory);
    reopened
        .apply_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &removed,
        )
        .expect("journal replay should reconstruct retired live state");
    reopened
        .fold_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &removed,
        )
        .expect("journal fold should retain retired canonical state");
    reopened
        .reset_journaled_live_projection()
        .expect("live projection should reset to folded authority");
    let folded = reopened
        .identity_state_inventory(IdentityStateStorageView::Effective)
        .expect("folded identity inventory should decode");
    assert_eq!(
        folded[&(entity_tag, field_id)].lifecycle(),
        IdentityStateLifecycle::Retired,
    );
}

#[test]
fn identity_state_inventory_enforces_the_database_lifetime_cap() {
    let incarnation = test_database_incarnation();
    let mut store = SchemaStore::init_heap();
    for ordinal in 0..MAX_IDENTITY_STATE_RECORDS_PER_DATABASE {
        let entity_value =
            u64::try_from(ordinal).expect("identity-state ordinal should fit u64") + 1;
        let owner =
            IdentityStateOwner::try_new(incarnation, EntityTag::new(entity_value), FieldId::new(1))
                .expect("bounded identity owner should admit");
        let state = IdentityState::new_active(owner, AcceptedFieldKind::Nat64)
            .and_then(|state| state.retire())
            .expect("retired identity fixture should admit");
        store.insert_durable_raw_value(
            RawSchemaKey::from_identity_state(owner.entity_tag(), owner.field_id()),
            encode_identity_state(&state).expect("retired identity state should encode"),
        );
    }
    assert_eq!(
        store
            .identity_state_inventory(IdentityStateStorageView::Effective)
            .expect("the exact inventory limit should remain readable")
            .len(),
        MAX_IDENTITY_STATE_RECORDS_PER_DATABASE,
    );
    let candidate = identity_candidate_for_test(
        "test::IdentityCapacityStore",
        AcceptedSchemaRevision::INITIAL,
        EntityTag::new(
            u64::try_from(MAX_IDENTITY_STATE_RECORDS_PER_DATABASE)
                .expect("identity-state limit should fit u64")
                + 1,
        ),
        FieldId::new(1),
        AcceptedFieldKind::Nat64,
    );

    let error = store
        .preflight_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .expect_err("one owner beyond the lifetime cap must reject");
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
}

#[test]
fn accepted_schema_bundle_cache_is_keyed_by_selected_root() {
    let mut store = SchemaStore::init_heap();
    let initial = empty_accepted_schema_candidate_for_tests(
        "test::CachedSchemaStore",
        AcceptedSchemaRevision::INITIAL,
    );
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &initial,
        )
        .expect("initial accepted schema root should bootstrap");
    reset_accepted_schema_bundle_cache_miss_count_for_tests();

    for _ in 0..2 {
        assert!(
            store
                .current_accepted_persisted_snapshot(EntityTag::new(7))
                .expect("accepted bundle should decode")
                .is_none(),
            "empty accepted bundle should not contain the test entity",
        );
        let (selection, bundle) = store
            .current_accepted_schema_authority_ref()
            .expect("accepted authority should decode")
            .expect("accepted authority should exist");
        assert_eq!(selection.root().revision(), bundle.revision());
    }
    assert_eq!(accepted_schema_bundle_cache_miss_count_for_tests(), 1);

    let second = empty_accepted_schema_candidate_for_tests(
        "test::CachedSchemaStore",
        AcceptedSchemaRevision::new(2),
    );
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &second,
        )
        .expect("second accepted schema root should publish");
    assert!(
        store
            .current_accepted_persisted_snapshot(EntityTag::new(7))
            .expect("new accepted bundle should decode")
            .is_none(),
        "new empty accepted bundle should not contain the test entity",
    );
    let (selection, bundle) = store
        .current_accepted_schema_authority_ref()
        .expect("new accepted authority should decode")
        .expect("new accepted authority should exist");
    assert_eq!(selection.root().revision(), bundle.revision());
    assert_eq!(accepted_schema_bundle_cache_miss_count_for_tests(), 2);
}

#[test]
fn accepted_catalog_selection_reuses_verified_entity_bytes() {
    let entity = EntityTag::new(7);
    let empty = empty_accepted_schema_candidate_for_tests(
        "test::CachedCatalogSelectionStore",
        AcceptedSchemaRevision::INITIAL,
    );
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::CachedEntity".to_string(),
        "CachedEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
    );
    let bundle = AcceptedSchemaRevisionBundle::new(
        AcceptedSchemaRevision::INITIAL,
        "test::CachedCatalogSelectionStore",
        empty.bundle().enum_catalog().clone(),
        empty.bundle().composite_catalog().clone(),
        std::collections::BTreeMap::from([(entity, snapshot)]),
    )
    .expect("accepted bundle should build");
    let candidate = CandidateSchemaRevision::new(bundle).expect("accepted candidate should encode");
    let mut store = SchemaStore::init_heap();
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .expect("accepted candidate should publish");

    let first = store
        .current_accepted_catalog_selection(
            entity,
            "entities::CachedEntity",
            "test::CachedCatalogSelectionStore",
        )
        .expect("first selection should resolve")
        .expect("accepted entity should exist");
    let second = store
        .current_accepted_catalog_selection(
            entity,
            "entities::CachedEntity",
            "test::CachedCatalogSelectionStore",
        )
        .expect("second selection should resolve")
        .expect("accepted entity should exist");

    assert_eq!(first.identity(), second.identity());
    assert!(std::rc::Rc::ptr_eq(
        &first.raw_snapshot,
        &second.raw_snapshot,
    ));
    assert_eq!(
        first.value_catalog_handle().authority(),
        second.value_catalog_handle().authority(),
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
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(vec![0xff]),
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
    let store = SchemaStore::init_journaled(test_memory(241));

    assert_eq!(
        store
            .catalog_metadata()
            .expect("empty schema catalog metadata should derive"),
        None
    );
}

#[test]
fn schema_store_catalog_metadata_uses_latest_persisted_snapshots() {
    let mut store = SchemaStore::init_journaled(test_memory(240));
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
        SCHEMA_STORE_FINGERPRINT_METHOD_VERSION
    );
    assert_eq!(initial_metadata.entity_count(), 1);
    assert_eq!(updated_metadata.schema_version(), SchemaVersion::new(3));
    assert_eq!(
        updated_metadata.schema_fingerprint_method_version(),
        SCHEMA_STORE_FINGERPRINT_METHOD_VERSION
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

    let mut left = SchemaStore::init_journaled(test_memory(239));
    left.insert_persisted_snapshot(EntityTag::new(91), &first)
        .expect("first schema snapshot should encode");
    left.insert_persisted_snapshot(EntityTag::new(92), &second)
        .expect("second schema snapshot should encode");

    let mut right = SchemaStore::init_journaled(test_memory(238));
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

    let mut base = SchemaStore::init_journaled(test_memory(237));
    base.insert_persisted_snapshot(EntityTag::new(93), &without_index)
        .expect("base schema snapshot should encode");
    let base_metadata = base
        .allocation_metadata()
        .expect("base allocation metadata should derive")
        .expect("base allocation metadata should be present");

    let mut indexed = SchemaStore::init_journaled(test_memory(236));
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
        SCHEMA_STORE_FINGERPRINT_METHOD_VERSION
    );
    assert_eq!(
        indexed_metadata.index().schema_fingerprint_method_version(),
        SCHEMA_STORE_FINGERPRINT_METHOD_VERSION
    );
    assert_eq!(
        indexed_metadata
            .schema()
            .schema_fingerprint_method_version(),
        SCHEMA_STORE_FINGERPRINT_METHOD_VERSION
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
fn schema_store_rejects_typed_snapshot_with_zero_schema_version() {
    let mut store = SchemaStore::init_journaled(test_memory(254));
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
    let mut store = SchemaStore::init_journaled(test_memory(232));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "InvalidSlots");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(3)),
        ]),
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
    let mut store = SchemaStore::init_journaled(test_memory(246));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateLayoutSlot");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(0)),
        ]),
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
    let mut store = SchemaStore::init_journaled(test_memory(248));
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
    let mut store = SchemaStore::init_journaled(test_memory(249));
    let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
    let corrupt_key = RawSchemaKey::from_entity_version(EntityTag::new(45), SchemaVersion::new(3));

    store
        .insert_persisted_snapshot(EntityTag::new(45), &initial)
        .expect("initial schema snapshot should encode");
    store.insert_raw_snapshot(
        corrupt_key,
        RawSchemaSnapshot::from_encoded_control_record(vec![0xff, 0x00]),
    );

    let err = store
        .latest_staged_persisted_snapshot(EntityTag::new(45))
        .expect_err("latest corrupt schema snapshot must fail closed");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "latest-version lookup should report the corrupt newest snapshot"
    );
}

#[test]
fn schema_store_rejects_raw_snapshot_with_divergent_field_slots() {
    let mut store = SchemaStore::init_journaled(test_memory(250));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "RawInvalidSlots");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(3)),
        ]),
        base.fields().to_vec(),
    );
    let raw = encode_unchecked_persisted_schema_snapshot_for_tests(&invalid)
        .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
    let key = RawSchemaKey::from_entity_version(EntityTag::new(46), invalid.version());

    store.insert_raw_snapshot(
        key,
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(raw),
    );

    let err = store
        .latest_staged_persisted_snapshot(EntityTag::new(46))
        .expect_err("raw decode should reject divergent field/layout slots");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "schema codec should report the raw decoded slot divergence"
    );
}

#[test]
fn schema_store_rejects_raw_snapshot_with_missing_primary_key_field() {
    let mut store = SchemaStore::init_journaled(test_memory(247));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "RawMissingPk");
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        FieldId::new(99),
        base.row_layout().clone(),
        base.fields().to_vec(),
    );
    let raw = encode_unchecked_persisted_schema_snapshot_for_tests(&invalid)
        .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
    let key = RawSchemaKey::from_entity_version(EntityTag::new(48), invalid.version());

    store.insert_raw_snapshot(
        key,
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(raw),
    );

    let err = store
        .latest_staged_persisted_snapshot(EntityTag::new(48))
        .expect_err("raw decode should reject snapshots without the primary-key field");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "schema codec should report the raw decoded missing primary-key field"
    );
}

#[test]
fn schema_store_rejects_raw_snapshot_with_duplicate_field_name() {
    let mut store = SchemaStore::init_journaled(test_memory(245));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateFieldName");
    let mut fields = base.fields().to_vec();
    let duplicate = PersistedFieldSnapshot::new_initial(
        fields[1].id(),
        fields[0].name().to_string(),
        fields[1].slot(),
        fields[1].kind().clone(),
        fields[1].nested_leaves().to_vec(),
        fields[1].nullable(),
        fields[1].insert_default().clone(),
        fields[1].storage_decode(),
        fields[1].leaf_codec(),
    );
    fields[1] = duplicate;
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        base.row_layout().clone(),
        fields,
    );
    let raw = encode_unchecked_persisted_schema_snapshot_for_tests(&invalid)
        .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
    let key = RawSchemaKey::from_entity_version(EntityTag::new(50), invalid.version());

    store.insert_raw_snapshot(
        key,
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(raw),
    );

    let err = store
        .latest_staged_persisted_snapshot(EntityTag::new(50))
        .expect_err("raw decode should reject duplicate field names");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "schema codec should report the raw decoded field-name ambiguity"
    );
}

#[test]
fn schema_store_rejects_typed_snapshot_with_empty_nested_leaf_path() {
    let mut store = SchemaStore::init_journaled(test_memory(244));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "EmptyNestedLeaf");
    let mut fields = base.fields().to_vec();
    let invalid_field = PersistedFieldSnapshot::new_initial(
        fields[1].id(),
        fields[1].name().to_string(),
        fields[1].slot(),
        fields[1].kind().clone(),
        vec![PersistedNestedLeafSnapshot::new(
            Vec::new(),
            AcceptedFieldKind::Blob { max_len: None },
            false,
        )],
        fields[1].nullable(),
        fields[1].insert_default().clone(),
        fields[1].storage_decode(),
        fields[1].leaf_codec(),
    );
    fields[1] = invalid_field;
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
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
    let mut store = SchemaStore::init_journaled(test_memory(243));
    let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateNestedLeaf");
    let mut fields = base.fields().to_vec();
    let duplicate_leaves = vec![
        PersistedNestedLeafSnapshot::new(
            vec!["bytes".to_string()],
            AcceptedFieldKind::Blob { max_len: None },
            false,
        ),
        PersistedNestedLeafSnapshot::new(
            vec!["bytes".to_string()],
            AcceptedFieldKind::Text { max_len: None },
            false,
        ),
    ];
    let invalid_field = PersistedFieldSnapshot::new_initial(
        fields[1].id(),
        fields[1].name().to_string(),
        fields[1].slot(),
        fields[1].kind().clone(),
        duplicate_leaves,
        fields[1].nullable(),
        fields[1].insert_default().clone(),
        fields[1].storage_decode(),
        fields[1].leaf_codec(),
    );
    fields[1] = invalid_field;
    let invalid = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        base.row_layout().clone(),
        fields,
    );
    let raw = encode_unchecked_persisted_schema_snapshot_for_tests(&invalid)
        .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
    let key = RawSchemaKey::from_entity_version(EntityTag::new(52), invalid.version());

    store.insert_raw_snapshot(
        key,
        RawSchemaSnapshot::from_unchecked_persisted_snapshot_payload(raw),
    );

    let err = store
        .latest_staged_persisted_snapshot(EntityTag::new(52))
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

#[test]
fn positioned_schema_overlay_preserves_reinsert_until_exact_retirement() {
    let key = RawSchemaKey::from_entity_version(EntityTag::new(229), SchemaVersion::initial());
    let snapshot = |value| RawSchemaSnapshot::from_encoded_control_record(vec![value]);
    let mut store = SchemaStore::init_journaled(test_memory(232));
    let SchemaStoreBackend::Journaled { canonical, .. } = &mut store.backend else {
        panic!("positioned schema test requires a journaled store");
    };
    canonical.insert(key, snapshot(11));

    store
        .publish_positioned_journal_entry(key, None, overlay_position(1))
        .expect("positioned tombstone should publish");
    store
        .publish_positioned_journal_entry(key, Some(snapshot(33)), overlay_position(2))
        .expect("later schema value should supersede the tombstone");
    let SchemaStoreBackend::Journaled { canonical, .. } = &mut store.backend else {
        panic!("positioned schema test requires a journaled store");
    };
    canonical.remove(&key);
    assert_eq!(
        store
            .retire_positioned_journal_effect(key, overlay_position(1))
            .expect("older retirement should preserve the later value"),
        PositionedOverlayRetirement::Superseded,
    );
    assert_eq!(
        store
            .get_raw_snapshot_for_backend(&key)
            .expect("later schema value must remain visible")
            .as_bytes(),
        [33],
    );

    let SchemaStoreBackend::Journaled { canonical, .. } = &mut store.backend else {
        panic!("positioned schema test requires a journaled store");
    };
    canonical.insert(key, snapshot(33));
    assert_eq!(
        store
            .retire_positioned_journal_effect(key, overlay_position(2))
            .expect("latest schema value should retire exactly"),
        PositionedOverlayRetirement::Exact,
    );
    assert_eq!(
        store
            .get_raw_snapshot_for_backend(&key)
            .expect("canonical schema value should remain visible")
            .as_bytes(),
        [33],
    );
}

#[test]
fn positioned_schema_retirement_ignores_canonical_only_keys_but_requires_overlay_provenance() {
    let position = overlay_position(1);
    let positioned_key =
        RawSchemaKey::from_entity_version(EntityTag::new(230), SchemaVersion::initial());
    let canonical_only_key = RawSchemaKey::from_cardinality_count(
        CardinalityCountSlot::A,
        CardinalityCountDigest::for_entity(EntityTag::new(230)),
    );
    let positioned_without_overlay_key =
        RawSchemaKey::from_accepted_root_slot(0).expect("root slot should encode");
    let snapshot = |value| RawSchemaSnapshot::from_encoded_control_record(vec![value]);
    let mut store = SchemaStore::init_journaled(test_memory(231));

    let SchemaStoreBackend::Journaled {
        canonical,
        positions,
        ..
    } = &mut store.backend
    else {
        panic!("positioned schema test requires a journaled store");
    };
    canonical.insert(canonical_only_key, snapshot(11));
    positions.publish_preflighted(positioned_without_overlay_key, position);
    store
        .publish_positioned_journal_entry(positioned_key, Some(snapshot(22)), position)
        .expect("positioned schema value should publish");

    let prepared = store
        .prepare_positioned_key_retirements(
            [
                positioned_key,
                canonical_only_key,
                positioned_without_overlay_key,
            ],
            position,
        )
        .expect("canonical-only derived metadata should not require overlay provenance");
    assert_eq!(prepared.entries.len(), 2);
    store.apply_prepared_journal_batch_retirement(prepared);

    let SchemaStoreBackend::Journaled {
        canonical,
        positions,
        live,
        ..
    } = &mut store.backend
    else {
        panic!("positioned schema test requires a journaled store");
    };
    assert!(canonical.get(&canonical_only_key).is_some());
    assert!(!positions.is_positioned(&positioned_key));
    assert!(!positions.is_positioned(&positioned_without_overlay_key));

    let missing_position_key =
        RawSchemaKey::from_entity_version(EntityTag::new(231), SchemaVersion::initial());
    live.insert(missing_position_key, snapshot(33));
    let Err(error) = store.prepare_positioned_key_retirements([missing_position_key], position)
    else {
        panic!("a live schema overlay without provenance must remain an invariant failure");
    };
    assert_eq!(error.class(), ErrorClass::InvariantViolation);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

// Build one typed schema snapshot used by schema-store tests. The exact
// field contracts are intentionally rich enough to cover nested metadata,
// scalar codecs, and structural payloads through the raw store.
fn assert_latest_schema(
    store: &SchemaStore,
    entity: EntityTag,
    version: SchemaVersion,
    entity_name: &str,
) {
    let latest = store
        .latest_staged_persisted_snapshot(entity)
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
        ..
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

fn identity_candidate_for_test(
    store_path: &str,
    revision: AcceptedSchemaRevision,
    entity_tag: EntityTag,
    field_id: FieldId,
    kind: AcceptedFieldKind,
) -> CandidateSchemaRevision {
    let leaf_codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        format!("IdentityStateEntity{}", entity_tag.value()),
        format!("IdentityStateEntity{}", entity_tag.value()),
        field_id,
        SchemaRowLayout::initial(vec![(field_id, SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial_with_write_policy(
            field_id,
            "id".to_string(),
            SchemaFieldSlot::new(0),
            kind,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::from_model_policies(
                Some(FieldInsertGeneration::Identity),
                None,
            ),
            FieldStorageDecode::ByKind,
            leaf_codec,
        )],
    );
    accepted_schema_candidate_for_tests(
        store_path,
        revision,
        std::collections::BTreeMap::from([(entity_tag, snapshot)]),
    )
}

fn persisted_schema_snapshot_for_test(
    version: SchemaVersion,
    entity_name: &str,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        version,
        format!("entities::{entity_name}"),
        entity_name.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        persisted_schema_fields_for_test(),
    )
}

fn persisted_schema_snapshot_with_index_for_test(
    version: SchemaVersion,
    entity_name: &str,
    index_name: &str,
) -> PersistedSchemaSnapshot {
    let base = persisted_schema_snapshot_for_test(version, entity_name);

    PersistedSchemaSnapshot::new_with_indexes(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        base.row_layout().clone(),
        base.fields().to_vec(),
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            1,
            index_name.to_string(),
            "RoleSpecificStore".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(1),
                SchemaFieldSlot::new(0),
                vec!["id".to_string()],
                AcceptedFieldKind::Ulid,
                false,
            )]),
            None,
        )],
    )
}

fn persisted_schema_fields_for_test() -> Vec<PersistedFieldSnapshot> {
    vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "payload".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Text { max_len: None }),
                value: Box::new(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
            },
            vec![PersistedNestedLeafSnapshot::new(
                vec!["bytes".to_string()],
                AcceptedFieldKind::Blob { max_len: None },
                false,
            )],
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        ),
    ]
}
