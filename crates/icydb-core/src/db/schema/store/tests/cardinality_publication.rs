//! Catalog cleanup leaves cardinality lifecycle and paged reclamation to their owner.

use super::*;
use crate::db::journal::{JournalBatch, JournalRecord};

const STORE: &str = "test::CardinalityPublication";

fn candidate(revision: u64) -> CandidateSchemaRevision {
    empty_accepted_schema_candidate_for_tests(STORE, AcceptedSchemaRevision::new(revision))
}

fn seed_cardinality(
    store: &mut SchemaStore,
    count: u64,
    state: CardinalityGenerationState,
) -> Vec<(RawSchemaKey, Vec<u8>)> {
    let source = cardinality_source_identity();
    let header = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        state,
        CardinalityCountSlot::A,
        source,
    );
    let cursor = CardinalityBuildCursor::new(
        header.generation(),
        header.slot(),
        source,
        CardinalityBuildPhase::Rows,
        None,
        CardinalityBuildTotals::default(),
    )
    .unwrap();
    let mut entries = vec![(
        RawSchemaKey::from_cardinality_generation_header(),
        header.encode(),
    )];
    if state == CardinalityGenerationState::Building {
        entries.push((
            RawSchemaKey::from_cardinality_build_cursor(),
            cursor.encode().unwrap(),
        ));
    }
    for entity in 1..=count {
        let digest = CardinalityCountDigest::for_entity(EntityTag::new(entity));
        let record = CardinalityCountRecord::new(header.generation(), digest, 1).unwrap();
        for slot in [CardinalityCountSlot::A, CardinalityCountSlot::B] {
            entries.push((
                RawSchemaKey::from_cardinality_count(slot, digest),
                record.encode().to_vec(),
            ));
        }
    }
    for (key, bytes) in &entries {
        store.insert_durable_raw_value(*key, bytes.clone());
    }
    entries
}

fn assert_cardinality_preserved(store: &SchemaStore, entries: &[(RawSchemaKey, Vec<u8>)]) {
    for (key, bytes) in entries {
        assert_eq!(store.get_raw_value(key).unwrap().as_bytes(), bytes);
    }
}

#[test]
fn control_projection_keeps_cursor_decode_out_of_lifecycle_reads() {
    let heap = SchemaStore::init_heap();
    assert_eq!(
        heap.cardinality_generation_control().unwrap_err().class(),
        ErrorClass::InvariantViolation
    );
    assert_eq!(
        heap.cardinality_generation_lifecycle_control()
            .unwrap_err()
            .class(),
        ErrorClass::InvariantViolation
    );
    let mut store =
        SchemaStore::init_journaled(test_memory(229)).expect("fixture storage should initialize");
    assert_eq!(
        store.cardinality_generation_lifecycle_control().unwrap(),
        (None, false)
    );
    assert_eq!(
        store.cardinality_generation_control().unwrap(),
        (None, None)
    );
    seed_cardinality(&mut store, 0, CardinalityGenerationState::Building);
    let (header, cursor) = store.cardinality_generation_control().unwrap();
    assert!(cursor.is_some());
    assert_eq!(
        store.cardinality_generation_lifecycle_control().unwrap(),
        (header, true)
    );

    store.insert_durable_raw_value(RawSchemaKey::from_cardinality_build_cursor(), vec![0]);
    assert_eq!(
        store.cardinality_generation_lifecycle_control().unwrap(),
        (header, true)
    );
    assert_eq!(
        store.cardinality_generation_control().unwrap_err().class(),
        ErrorClass::Corruption
    );

    store.insert_durable_raw_value(RawSchemaKey::from_cardinality_generation_header(), vec![0]);
    assert_eq!(
        store
            .cardinality_generation_lifecycle_control()
            .unwrap_err()
            .class(),
        ErrorClass::Corruption
    );
}

fn batch(candidate: &CandidateSchemaRevision) -> JournalBatch {
    JournalBatch::new(
        [1; 16],
        [2; 16],
        JournalSequence::new(1),
        vec![
            JournalRecord::accepted_schema_publish(
                STORE,
                AcceptedSchemaRevision::INITIAL,
                candidate.encoded_bundle().to_vec(),
                candidate.encoded_root().to_vec(),
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

#[test]
fn direct_catalog_publication_preserves_cardinality_records_and_replay() {
    for journaled in [false, true] {
        let mut store = if journaled {
            SchemaStore::init_journaled(test_memory(229))
                .expect("fixture storage should initialize")
        } else {
            SchemaStore::init_heap()
        };
        store
            .publish_accepted_schema_candidate(
                test_database_incarnation(),
                AcceptedSchemaRevision::NONE,
                &candidate(1),
            )
            .unwrap();
        let entries = seed_cardinality(&mut store, 3, CardinalityGenerationState::Building);
        for _ in 0..2 {
            store
                .publish_accepted_schema_candidate(
                    test_database_incarnation(),
                    AcceptedSchemaRevision::INITIAL,
                    &candidate(2),
                )
                .unwrap();
            assert_cardinality_preserved(&store, &entries);
        }
        let retained = match &store.backend {
            SchemaStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap(),
            SchemaStoreBackend::Journaled { canonical, .. } => canonical.len(),
        };
        assert_eq!(retained, u64::try_from(entries.len()).unwrap() + 2);
    }
}

#[test]
fn journaled_catalog_effects_do_not_grow_with_cardinality_records() {
    for (count, state) in [
        (0, CardinalityGenerationState::Building),
        (4_097, CardinalityGenerationState::Building),
        (4_097, CardinalityGenerationState::Ready),
    ] {
        let memory = crate::testing::test_memory_owner(229);
        let mut store =
            SchemaStore::init_journaled(memory.open()).expect("fixture storage should initialize");
        store
            .publish_accepted_schema_candidate(
                test_database_incarnation(),
                AcceptedSchemaRevision::NONE,
                &candidate(1),
            )
            .unwrap();
        let entries = seed_cardinality(&mut store, count, state);
        let next = candidate(2);
        let batch = batch(&next);
        let publication = store
            .prepare_positioned_journal_batch_publication(
                test_database_incarnation(),
                &batch,
                overlay_position(1),
            )
            .unwrap();
        assert_eq!(
            publication.keys.len(),
            4,
            "only the old and new bundle/root pairs participate"
        );
        store
            .apply_journaled_accepted_schema_candidate(
                test_database_incarnation(),
                AcceptedSchemaRevision::INITIAL,
                &next,
            )
            .unwrap();
        store.publish_prepared_journal_batch_positions(publication);
        store
            .apply_journaled_accepted_schema_candidate(
                test_database_incarnation(),
                AcceptedSchemaRevision::INITIAL,
                &next,
            )
            .unwrap();
        assert_cardinality_preserved(&store, &entries);
        let retirement = store
            .prepare_positioned_journal_batch_retirement(
                test_database_incarnation(),
                &batch,
                overlay_position(1),
            )
            .unwrap();
        assert_eq!(retirement.entries.len(), 4);
        for _ in 0..2 {
            let prepared = store
                .prepare_fold_journaled_accepted_schema_candidate(
                    test_database_incarnation(),
                    AcceptedSchemaRevision::INITIAL,
                    next.clone(),
                )
                .unwrap();
            store.apply_prepared_accepted_schema_fold(prepared).unwrap();
        }
        store.apply_prepared_journal_batch_retirement(retirement);
        let SchemaStoreBackend::Journaled {
            live,
            tombstones,
            positions,
            ..
        } = &store.backend
        else {
            unreachable!();
        };
        assert!(live.is_empty());
        assert!(tombstones.is_empty());
        assert_eq!(positions.len(), 0);
        for (key, _) in &entries {
            assert!(!positions.is_positioned(key));
        }
        drop(store);
        let reopened =
            SchemaStore::init_journaled(memory.open()).expect("fixture storage should initialize");
        assert_cardinality_preserved(&reopened, &entries);
        assert_eq!(
            reopened.canonical_len_for_tests(),
            u64::try_from(entries.len()).unwrap() + 2
        );
    }
}

#[test]
fn cardinality_reclamation_between_publication_and_fold_leaves_no_overlay() {
    let mut store =
        SchemaStore::init_journaled(test_memory(229)).expect("fixture storage should initialize");
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &candidate(1),
        )
        .unwrap();
    let entries = seed_cardinality(&mut store, 4_097, CardinalityGenerationState::Building);
    let next = candidate(2);
    let batch = batch(&next);
    let publication = store
        .prepare_positioned_journal_batch_publication(
            test_database_incarnation(),
            &batch,
            overlay_position(1),
        )
        .unwrap();
    store
        .apply_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &next,
        )
        .unwrap();
    store.publish_prepared_journal_batch_positions(publication);
    let header = store.cardinality_generation_header().unwrap().unwrap();
    let changed_source = CardinalitySourceIdentity::derive(
        test_database_incarnation(),
        StoreAllocationIdentities::new_journaled(
            StoreAllocationIdentity::new(180, "test.cardinality.data.v1"),
            StoreAllocationIdentity::new(181, "test.cardinality.index.v1"),
            StoreAllocationIdentity::new(182, "test.cardinality.schema.v1"),
            StoreAllocationIdentity::new(183, "test.cardinality.journal.v1"),
        ),
        None,
        [],
        crate::db::journal::FoldWatermark::new(JournalSequence::new(1), 1),
    )
    .unwrap();
    let building = store
        .restart_cardinality_generation(header, changed_source)
        .unwrap();
    let cursor = CardinalityBuildCursor::new(
        building.generation(),
        building.slot(),
        changed_source,
        CardinalityBuildPhase::Rows,
        None,
        CardinalityBuildTotals::default(),
    )
    .unwrap();
    assert!(
        store
            .clear_cardinality_count_slot_page(building, &cursor, 4_096)
            .unwrap()
    );
    assert!(
        !store
            .clear_cardinality_count_slot_page(building, &cursor, 4_096)
            .unwrap()
    );
    let retirement = store
        .prepare_positioned_journal_batch_retirement(
            test_database_incarnation(),
            &batch,
            overlay_position(1),
        )
        .unwrap();
    let prepared = store
        .prepare_fold_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            next,
        )
        .unwrap();
    store.apply_prepared_accepted_schema_fold(prepared).unwrap();
    store.apply_prepared_journal_batch_retirement(retirement);
    assert_eq!(
        store.cardinality_generation_header().unwrap(),
        Some(building)
    );
    assert_eq!(store.cardinality_build_cursor().unwrap(), Some(cursor));
    let SchemaStoreBackend::Journaled {
        live,
        tombstones,
        positions,
        ..
    } = &store.backend
    else {
        unreachable!();
    };
    assert!(live.is_empty());
    assert!(tombstones.is_empty());
    assert_eq!(positions.len(), 0);
    for (key, _) in entries {
        assert!(!positions.is_positioned(&key));
    }
}

#[test]
fn building_fold_watermark_and_cursor_survive_reopen_together() {
    use crate::db::journal::FoldWatermark;
    let memory = crate::testing::test_memory_owner(229);
    let source = cardinality_source_identity();
    let next_source = CardinalitySourceIdentity::derive(
        test_database_incarnation(),
        StoreAllocationIdentities::new_journaled(
            StoreAllocationIdentity::new(180, "test.cardinality.data.v1"),
            StoreAllocationIdentity::new(181, "test.cardinality.index.v1"),
            StoreAllocationIdentity::new(182, "test.cardinality.schema.v1"),
            StoreAllocationIdentity::new(183, "test.cardinality.journal.v1"),
        ),
        None,
        [],
        FoldWatermark::new(JournalSequence::new(1), 1),
    )
    .unwrap();
    let header = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        CardinalityGenerationState::Building,
        CardinalityCountSlot::A,
        source,
    );
    let totals = CardinalityBuildTotals::new(7, 70, 2, 1);
    let cursor = CardinalityBuildCursor::new(
        header.generation(),
        header.slot(),
        source,
        CardinalityBuildPhase::Indexes,
        None,
        totals,
    )
    .unwrap();
    let digest = CardinalityCountDigest::for_entity(EntityTag::new(42));
    let mut store =
        SchemaStore::init_journaled(memory.open()).expect("fixture storage should initialize");
    store.write_cardinality_generation_header(header).unwrap();
    store
        .insert_canonical_raw_value(
            RawSchemaKey::from_cardinality_build_cursor(),
            cursor.encode().unwrap(),
        )
        .unwrap();
    let count_key = RawSchemaKey::from_cardinality_count(header.slot(), digest);
    store
        .insert_canonical_raw_value(
            count_key,
            CardinalityCountRecord::new(header.generation(), digest, 2)
                .unwrap()
                .encode()
                .to_vec(),
        )
        .unwrap();
    // Underflow leaves every prepared control unchanged and does not authorize
    // publishing a new watermark with unusable count evidence.
    assert!(
        store
            .prepare_cardinality_maintenance(header, source, next_source, &[(digest, -3)])
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.cardinality_generation_control().unwrap(),
        (Some(header), Some(cursor))
    );
    let prepared = store
        .prepare_cardinality_maintenance(header, source, next_source, &[(digest, 1)])
        .unwrap()
        .unwrap();
    store
        .apply_prepared_cardinality_maintenance(prepared)
        .unwrap();
    drop(store);
    let reopened =
        SchemaStore::init_journaled(memory.open()).expect("fixture storage should initialize");
    let (after, cursor) = reopened.cardinality_generation_control().unwrap();
    let after = after.unwrap();
    let cursor = cursor.unwrap();
    assert_eq!(after.state(), CardinalityGenerationState::Building);
    assert_eq!(after.generation(), header.generation());
    assert_eq!(after.slot(), header.slot());
    assert_eq!(after.source(), next_source);
    cursor.validate_header(after).unwrap();
    assert_eq!(cursor.phase(), CardinalityBuildPhase::Indexes);
    assert_eq!(cursor.checkpoint(), None);
    assert_eq!(cursor.totals(), totals);
    assert_eq!(
        reopened
            .cardinality_count(after.slot(), after.generation(), digest)
            .unwrap(),
        Some(3)
    );
}
