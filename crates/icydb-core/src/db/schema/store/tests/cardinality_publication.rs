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
        assert_eq!(store.get_raw_snapshot(key).unwrap().as_bytes(), bytes);
    }
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
        let memory = test_memory(229);
        let mut store = SchemaStore::init_journaled(memory.clone());
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
        let reopened = SchemaStore::init_journaled(memory);
        assert_cardinality_preserved(&reopened, &entries);
        assert_eq!(
            reopened.canonical_len_for_tests(),
            u64::try_from(entries.len()).unwrap() + 2
        );
    }
}

#[test]
fn cardinality_reclamation_between_publication_and_fold_leaves_no_overlay() {
    let mut store = SchemaStore::init_journaled(test_memory(229));
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
