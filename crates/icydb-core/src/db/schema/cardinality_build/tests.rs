use super::*;
use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey, RawRow},
        index::{IndexEntryValue, RawIndexStoreKey},
        journal::JournalSequence,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        registry::StoreAllocationIdentity,
        schema::{
            AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests,
            enum_catalog::AcceptedSchemaFingerprint,
        },
    },
    testing::test_memory,
};
use ic_stable_structures::Memory;

fn allocations() -> StoreAllocationIdentities {
    StoreAllocationIdentities::new_journaled(
        StoreAllocationIdentity::new(180, "test.cardinality.build.data.v1"),
        StoreAllocationIdentity::new(181, "test.cardinality.build.index.v1"),
        StoreAllocationIdentity::new(182, "test.cardinality.build.schema.v1"),
        StoreAllocationIdentity::new(183, "test.cardinality.build.journal.v1"),
    )
}

fn authority(
    entities: impl IntoIterator<Item = EntityTag>,
    indexes: impl IntoIterator<Item = (IndexId, usize)>,
    watermark_sequence: u64,
) -> CardinalityBuildAuthority {
    let accepted_entities = entities.into_iter().collect::<BTreeSet<_>>();
    let accepted_indexes = indexes.into_iter().collect::<BTreeMap<_, _>>();
    let source = CardinalitySourceIdentity::derive(
        DatabaseIncarnationId::for_tests(0x70),
        allocations(),
        Some(
            CardinalityAcceptedRootIdentity::new(
                AcceptedSchemaRevision::INITIAL,
                AcceptedSchemaFingerprint::new([0x71; 32]),
            )
            .expect("test accepted root should be present"),
        ),
        accepted_indexes.keys().copied(),
        FoldWatermark::new(JournalSequence::new(watermark_sequence), 3),
    )
    .expect("complete current build source should derive");
    CardinalityBuildAuthority::from_parts(source, accepted_entities, accepted_indexes)
}

fn row_key(entity: EntityTag, value: u64) -> RawDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        entity,
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
    )
    .to_raw()
    .expect("bounded test row key should encode")
}

fn index_key(
    index_id: IndexId,
    kind: IndexKeyKind,
    components: &[Vec<u8>],
    value: u64,
) -> RawIndexStoreKey {
    IndexKey::new_from_components_with_primary_key_value(
        &index_id,
        kind,
        components,
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
    )
    .expect("bounded test index key should construct")
    .to_raw()
    .expect("bounded test index key should encode")
}

fn initialized_stores() -> (DataStore, IndexStore, SchemaStore) {
    (
        DataStore::init_journaled(test_memory(180)),
        IndexStore::init_journaled(test_memory(181)),
        SchemaStore::init_journaled(test_memory(182)),
    )
}

fn drive_generation_to_ready(
    data: &DataStore,
    index: &IndexStore,
    schema: &mut SchemaStore,
    authority: &CardinalityBuildAuthority,
) {
    for _ in 0..8 {
        match drive_cardinality_generation_page(data, index, schema, |_| Ok(authority.clone()))
            .expect("bounded generation page should advance")
        {
            CardinalityGenerationPageOutcome::PublishedReady => return,
            CardinalityGenerationPageOutcome::WorkRemaining => {}
            CardinalityGenerationPageOutcome::Quiescent => {
                panic!("the requested changed source should not already be Ready");
            }
        }
    }
    panic!("maximum-shape generation should publish within eight bounded pages");
}

#[test]
fn page_budget_stops_before_every_frozen_dimension_and_checks_arithmetic() {
    let mut entries = BuildPageBudget::default();
    for _ in 0..MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE {
        assert!(entries.admit(0, 0).expect("entry charge should fit"));
    }
    assert!(!entries.admit(0, 0).expect("entry ceiling should stop"));

    let mut bytes = BuildPageBudget::default();
    assert!(
        bytes
            .admit(MAX_CARDINALITY_BUILD_SOURCE_BYTES_PER_PAGE, 0)
            .expect("byte charge should fit")
    );
    assert!(!bytes.admit(1, 0).expect("byte ceiling should stop"));

    let mut prefixes = BuildPageBudget::default();
    assert!(
        prefixes
            .admit(0, MAX_CARDINALITY_BUILD_PREFIX_UPDATES_PER_PAGE)
            .expect("prefix charge should fit")
    );
    assert!(!prefixes.admit(0, 1).expect("prefix ceiling should stop"));

    let mut overflow = BuildPageBudget {
        source_entries: u64::MAX,
        ..BuildPageBudget::default()
    };
    assert!(overflow.admit(0, 0).is_err());
}

#[test]
fn populated_rows_resume_exclusively_across_reopen_and_preserve_exact_totals() {
    let entity = EntityTag::new(7);
    let data_memory = test_memory(190);
    let index_memory = test_memory(191);
    let schema_memory = test_memory(192);
    let mut data = DataStore::init_journaled(data_memory.clone());
    for value in 0..=MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE {
        data.fold_recovered_journal_put(
            row_key(entity, value),
            RawRow::try_new(vec![
                u8::try_from(value % 251).expect("test byte should fit"),
            ])
            .expect("bounded row should construct"),
        )
        .expect("canonical row should seed");
    }
    let index = IndexStore::init_journaled(index_memory.clone());
    let mut schema = SchemaStore::init_journaled(schema_memory.clone());
    let authority = authority([entity], [], 0);

    assert!(matches!(
        advance_cardinality_build_page(&data, &index, &mut schema, &authority)
            .expect("isolated slot should initialize"),
        CardinalityBuildPageOutcome::Clearing {
            has_more: false,
            ..
        }
    ));
    let first_page = advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("first row page should build");
    let CardinalityBuildPageOutcome::Advanced { phase, totals, .. } = first_page else {
        panic!("the first bounded row page should remain in Rows");
    };
    assert_eq!(phase, CardinalityBuildPhase::Rows);
    assert_eq!(
        totals.source_entries(),
        MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE
    );
    assert_eq!(totals.distinct_count_keys(), 1);
    drop(data);
    drop(index);
    drop(schema);

    let data = DataStore::init_journaled(data_memory);
    let index = IndexStore::init_journaled(index_memory);
    let mut schema = SchemaStore::init_journaled(schema_memory);
    let final_row_page = advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("exclusive row checkpoint should resume after reopen");
    let CardinalityBuildPageOutcome::Advanced { phase, totals, .. } = final_row_page else {
        panic!("the final row page should transition to Indexes");
    };
    assert_eq!(phase, CardinalityBuildPhase::Indexes);
    assert_eq!(
        totals.source_entries(),
        MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE + 1
    );
    assert_eq!(totals.prefix_updates(), 0);
    assert!(totals.source_bytes() > totals.source_entries());
    assert_eq!(totals.distinct_count_keys(), 1);

    let complete = advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("empty accepted index domain should complete");
    assert!(matches!(
        complete,
        CardinalityBuildPageOutcome::CandidateComplete { .. }
    ));
    let header = schema
        .cardinality_generation_header()
        .expect("header should decode")
        .expect("header should exist");
    let digest = CardinalityLogicalCountKey::Entity(entity)
        .digest()
        .expect("entity key should hash");
    assert_eq!(
        schema
            .cardinality_count(header.slot(), header.generation(), digest)
            .expect("entity count should decode"),
        Some(MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE + 1)
    );
}

#[test]
fn distinct_entity_counts_stay_within_the_frozen_physical_growth_bound() {
    let data_memory = test_memory(196);
    let index_memory = test_memory(197);
    let schema_memory = test_memory(198);
    let schema_measurement = schema_memory.clone();
    let mut data = DataStore::init_journaled(data_memory);
    let mut entities = BTreeSet::new();
    for value in 1..=MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE {
        let entity = EntityTag::new(value);
        entities.insert(entity);
        data.fold_recovered_journal_put(
            row_key(entity, value),
            RawRow::try_new(vec![1]).expect("bounded row should construct"),
        )
        .expect("canonical distinct-entity row should seed");
    }
    let index = IndexStore::init_journaled(index_memory);
    let mut schema = SchemaStore::init_journaled(schema_memory);
    let baseline_bytes = schema_measurement.size() * 65_536;
    let authority = authority(entities, [], 0);

    advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("isolated slot should initialize");
    let rows = advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("maximum distinct row page should build");
    let CardinalityBuildPageOutcome::Advanced { totals, .. } = rows else {
        panic!("the complete row page should transition to Indexes");
    };
    assert_eq!(
        totals.distinct_count_keys(),
        MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE
    );
    assert!(matches!(
        advance_cardinality_build_page(&data, &index, &mut schema, &authority)
            .expect("empty index domain should complete"),
        CardinalityBuildPageOutcome::CandidateComplete { .. }
    ));

    let physical_growth = schema_measurement
        .size()
        .checked_mul(65_536)
        .and_then(|bytes| bytes.checked_sub(baseline_bytes))
        .expect("physical schema-memory growth should remain representable");
    let physical_growth_limit = 256_u64
        .checked_mul(MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE)
        .and_then(|bytes| bytes.checked_add(262_144))
        .expect("frozen physical growth bound should remain representable");
    assert!(
        physical_growth <= physical_growth_limit,
        "isolated candidate grew by {physical_growth} bytes above a {physical_growth_limit}-byte bound",
    );
    eprintln!(
        "cardinality candidate growth for {MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE} distinct keys: {physical_growth} bytes",
    );
}

#[test]
fn repeated_maximum_shape_reuses_both_count_slots_at_fixed_physical_high_water() {
    let data_memory = test_memory(202);
    let index_memory = test_memory(203);
    let schema_memory = test_memory(204);
    let schema_measurement = schema_memory.clone();
    let mut data = DataStore::init_journaled(data_memory);
    let mut entities = BTreeSet::new();
    for value in 1..=MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE {
        let entity = EntityTag::new(value);
        entities.insert(entity);
        data.fold_recovered_journal_put(
            row_key(entity, value),
            RawRow::try_new(vec![1]).expect("bounded row should construct"),
        )
        .expect("canonical maximum-shape row should seed");
    }
    let index = IndexStore::init_journaled(index_memory);
    let mut schema = SchemaStore::init_journaled(schema_memory);

    drive_generation_to_ready(
        &data,
        &index,
        &mut schema,
        &authority(entities.iter().copied(), [], 0),
    );
    let first_slot_bytes = schema_measurement.size() * 65_536;
    drive_generation_to_ready(
        &data,
        &index,
        &mut schema,
        &authority(entities.iter().copied(), [], 1),
    );
    let both_slots_bytes = schema_measurement.size() * 65_536;
    assert!(both_slots_bytes >= first_slot_bytes);

    drive_generation_to_ready(&data, &index, &mut schema, &authority(entities, [], 2));
    let reused_slot_bytes = schema_measurement.size() * 65_536;
    assert_eq!(
        reused_slot_bytes, both_slots_bytes,
        "refilling a previously maximum-shaped slot must reuse its physical high-water",
    );
    eprintln!(
        "cardinality two-slot high-water: first_slot={first_slot_bytes} both_slots={both_slots_bytes} reused_slot={reused_slot_bytes}",
    );
}

#[test]
fn accepted_present_indexes_count_every_prefix_and_exclude_other_domains() {
    let entity = EntityTag::new(8);
    let accepted = IndexId::new_with_generation(entity, 1, 4);
    let candidate = IndexId::new_with_generation(entity, 1, 5);
    let (data, mut index, mut schema) = initialized_stores();
    let authority = authority([entity], [(accepted, 2)], 0);
    for (raw, value) in [
        (
            index_key(accepted, IndexKeyKind::User, &[vec![1], vec![2]], 1),
            IndexEntryValue::presence_only(),
        ),
        (
            index_key(accepted, IndexKeyKind::User, &[vec![1], vec![3]], 2),
            IndexEntryValue::presence_only(),
        ),
        (
            index_key(accepted, IndexKeyKind::User, &[vec![9], vec![9]], 3),
            IndexEntryValue::from_persisted_bytes(vec![1]),
        ),
        (
            index_key(accepted, IndexKeyKind::System, &[vec![4], vec![5]], 4),
            IndexEntryValue::presence_only(),
        ),
        (
            index_key(candidate, IndexKeyKind::User, &[vec![6], vec![7]], 5),
            IndexEntryValue::presence_only(),
        ),
    ] {
        index
            .fold_recovered_journal_entry(raw, Some(value))
            .expect("canonical index entry should seed");
    }

    assert!(matches!(
        advance_cardinality_build_page(&data, &index, &mut schema, &authority)
            .expect("isolated slot should initialize"),
        CardinalityBuildPageOutcome::Clearing { .. }
    ));
    assert!(matches!(
        advance_cardinality_build_page(&data, &index, &mut schema, &authority)
            .expect("empty row domain should advance"),
        CardinalityBuildPageOutcome::Advanced {
            phase: CardinalityBuildPhase::Indexes,
            ..
        }
    ));
    let complete = advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("accepted index page should complete");
    let CardinalityBuildPageOutcome::CandidateComplete { totals, .. } = complete else {
        panic!("the bounded accepted index page should complete");
    };
    assert_eq!(totals.source_entries(), 5);
    assert_eq!(totals.prefix_updates(), 4);
    assert_eq!(totals.distinct_count_keys(), 3);

    let header = schema
        .cardinality_generation_header()
        .expect("header should decode")
        .expect("header should exist");
    for (components, expected) in [
        (vec![vec![1]], 2),
        (vec![vec![1], vec![2]], 1),
        (vec![vec![1], vec![3]], 1),
    ] {
        let digest = CardinalityCountDigest::for_user_index_prefix(accepted, &components)
            .expect("accepted prefix should hash");
        assert_eq!(
            schema
                .cardinality_count(header.slot(), header.generation(), digest)
                .expect("prefix count should decode"),
            Some(expected)
        );
    }
    let excluded = CardinalityCountDigest::for_user_index_prefix(candidate, &[vec![6]])
        .expect("candidate prefix should hash");
    assert_eq!(
        schema
            .cardinality_count(header.slot(), header.generation(), excluded)
            .expect("excluded prefix lookup should remain valid"),
        None
    );
}

#[test]
fn maximum_fanout_index_pages_resume_exclusively_at_the_prefix_gate() {
    let entity = EntityTag::new(10);
    let accepted = IndexId::new_with_generation(entity, 1, 7);
    let data_memory = test_memory(193);
    let index_memory = test_memory(194);
    let schema_memory = test_memory(195);
    let schema_measurement = schema_memory.clone();
    let data = DataStore::init_journaled(data_memory.clone());
    let mut index = IndexStore::init_journaled(index_memory.clone());
    for value in 0..=MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE {
        let raw = index_key(
            accepted,
            IndexKeyKind::User,
            &[vec![1], vec![2], vec![3], vec![4]],
            value,
        );
        index
            .fold_recovered_journal_entry(raw, Some(IndexEntryValue::presence_only()))
            .expect("canonical maximum-fanout entry should seed");
    }
    let mut schema = SchemaStore::init_journaled(schema_memory.clone());
    let baseline_bytes = schema_measurement.size() * 65_536;
    let authority = authority([entity], [(accepted, MAX_INDEX_FIELDS)], 0);

    advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("isolated slot should initialize");
    advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("empty row phase should advance");
    let first_index_page = advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("first index page should build");
    let CardinalityBuildPageOutcome::Advanced { phase, totals, .. } = first_index_page else {
        panic!("the first maximum-fanout page should retain an Indexes checkpoint");
    };
    assert_eq!(phase, CardinalityBuildPhase::Indexes);
    assert_eq!(
        totals.source_entries(),
        MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE
    );
    assert_eq!(
        totals.prefix_updates(),
        MAX_CARDINALITY_BUILD_PREFIX_UPDATES_PER_PAGE
    );
    drop(data);
    drop(index);
    drop(schema);

    let data = DataStore::init_journaled(data_memory);
    let index = IndexStore::init_journaled(index_memory);
    let mut schema = SchemaStore::init_journaled(schema_memory);
    let complete = advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("exclusive index checkpoint should resume after reopen");
    let CardinalityBuildPageOutcome::CandidateComplete { totals, .. } = complete else {
        panic!("the final maximum-fanout index page should complete");
    };
    let maximum_index_fields = u64::try_from(MAX_INDEX_FIELDS).expect("index-field cap should fit");
    assert_eq!(
        totals.source_entries(),
        MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE + 1
    );
    assert_eq!(
        totals.prefix_updates(),
        MAX_CARDINALITY_BUILD_PREFIX_UPDATES_PER_PAGE + maximum_index_fields
    );
    assert_eq!(totals.distinct_count_keys(), maximum_index_fields);
    let header = schema
        .cardinality_generation_header()
        .expect("header should decode")
        .expect("header should exist");
    let digest = CardinalityCountDigest::for_user_index_prefix(accepted, &[vec![1]])
        .expect("accepted prefix should hash");
    assert_eq!(
        schema
            .cardinality_count(header.slot(), header.generation(), digest)
            .expect("prefix count should decode"),
        Some(MAX_CARDINALITY_BUILD_SOURCE_ENTRIES_PER_PAGE + 1)
    );
    let physical_growth = schema_measurement
        .size()
        .checked_mul(65_536)
        .and_then(|bytes| bytes.checked_sub(baseline_bytes))
        .expect("physical schema-memory growth should remain representable");
    let physical_growth_limit = 256_u64
        .checked_mul(maximum_index_fields)
        .and_then(|bytes| bytes.checked_add(262_144))
        .expect("frozen physical growth bound should remain representable");
    assert!(
        physical_growth <= physical_growth_limit,
        "isolated candidate grew by {physical_growth} bytes above a {physical_growth_limit}-byte bound",
    );
    eprintln!("cardinality candidate physical growth: {physical_growth} bytes");
}

#[test]
fn malformed_row_and_index_keys_fail_without_advancing_the_durable_cursor() {
    let entity = EntityTag::new(9);
    let accepted = IndexId::new_with_generation(entity, 1, 1);
    let (mut data, mut index, mut schema) = initialized_stores();
    let authority = authority([entity], [(accepted, 1)], 0);
    data.fold_recovered_journal_put(
        RawDataStoreKey::from_persisted_bytes(Vec::new()),
        RawRow::try_new(vec![1]).expect("bounded raw row should construct"),
    )
    .expect("malformed raw key should seed corruption fixture");
    advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("isolated slot should initialize");
    assert!(advance_cardinality_build_page(&data, &index, &mut schema, &authority).is_err());
    let cursor = schema
        .cardinality_build_cursor()
        .expect("cursor should decode")
        .expect("cursor should remain");
    assert_eq!(cursor.phase(), CardinalityBuildPhase::Rows);
    assert_eq!(cursor.checkpoint(), None);
    assert_eq!(cursor.totals(), CardinalityBuildTotals::default());

    let (data, _, mut schema) = initialized_stores();
    index
        .fold_recovered_journal_entry(
            RawIndexStoreKey::from_persisted_bytes(vec![0]),
            Some(IndexEntryValue::presence_only()),
        )
        .expect("malformed raw index key should seed corruption fixture");
    advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("isolated slot should initialize");
    advance_cardinality_build_page(&data, &index, &mut schema, &authority)
        .expect("empty row phase should advance");
    assert!(advance_cardinality_build_page(&data, &index, &mut schema, &authority).is_err());
    let cursor = schema
        .cardinality_build_cursor()
        .expect("cursor should decode")
        .expect("cursor should remain");
    assert_eq!(cursor.phase(), CardinalityBuildPhase::Indexes);
    assert_eq!(cursor.checkpoint(), None);
    assert_eq!(cursor.totals(), CardinalityBuildTotals::default());
}

#[test]
fn source_change_is_reported_without_reusing_the_candidate() {
    let (data, index, mut schema) = initialized_stores();
    let first = authority([], [], 3);
    let changed = authority([], [], 4);
    advance_cardinality_build_page(&data, &index, &mut schema, &first)
        .expect("first source should initialize");
    let cursor = schema
        .cardinality_build_cursor()
        .expect("cursor should decode")
        .expect("cursor should exist");
    assert_eq!(
        advance_cardinality_build_page(&data, &index, &mut schema, &changed)
            .expect("source mismatch should be typed"),
        CardinalityBuildPageOutcome::SourceChanged(CardinalitySourceMismatch::FoldWatermark)
    );
    assert_eq!(
        schema
            .cardinality_build_cursor()
            .expect("cursor should decode")
            .expect("cursor should remain"),
        cursor
    );
}

#[test]
fn canonical_accepted_root_derives_the_build_source_without_generated_models() {
    let mut schema = SchemaStore::init_journaled(test_memory(200));
    let candidate = empty_accepted_schema_candidate_for_tests(
        "test::CardinalityBuildAuthority",
        AcceptedSchemaRevision::INITIAL,
    );
    schema
        .fold_journaled_accepted_schema_candidate(
            DatabaseIncarnationId::for_tests(0x70),
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .expect("canonical accepted root should seed");
    let authority = CardinalityBuildAuthority::derive(
        &schema,
        DatabaseIncarnationId::for_tests(0x70),
        allocations(),
        FoldWatermark::initial(),
    )
    .expect("canonical accepted source should derive");
    assert!(authority.accepted_entities.is_empty());
    assert!(authority.accepted_indexes.is_empty());
}

#[test]
fn empty_canonical_domains_publish_ready_zero_without_a_scan_cursor() {
    let (data, index, mut schema) = initialized_stores();
    let authority = authority([], [], 0);

    assert_eq!(
        drive_cardinality_generation_page(&data, &index, &mut schema, |_| {
            Ok(authority.clone())
        })
        .expect("empty exact source should publish"),
        CardinalityGenerationPageOutcome::PublishedReady,
    );
    let header = schema
        .cardinality_generation_header()
        .expect("ready header should decode")
        .expect("ready header should exist");
    assert_eq!(header.state(), CardinalityGenerationState::Ready);
    assert_eq!(header.generation(), CardinalityGenerationId::INITIAL);
    assert_eq!(header.slot(), CardinalityCountSlot::A);
    assert_eq!(
        schema
            .cardinality_build_cursor()
            .expect("empty publication should have a valid cursor state"),
        None,
    );
    assert_eq!(
        drive_cardinality_generation_page(&data, &index, &mut schema, |_| {
            Ok(authority.clone())
        })
        .expect("current Ready evidence should remain quiescent"),
        CardinalityGenerationPageOutcome::Quiescent,
    );
}

#[test]
fn complete_candidate_publishes_once_and_removes_its_build_cursor() {
    let entity = EntityTag::new(13);
    let (mut data, index, mut schema) = initialized_stores();
    data.fold_recovered_journal_put(
        row_key(entity, 1),
        RawRow::try_new(vec![1]).expect("bounded row should construct"),
    )
    .expect("canonical row should seed");
    let authority = authority([entity], [], 0);

    for expected in [
        CardinalityGenerationPageOutcome::WorkRemaining,
        CardinalityGenerationPageOutcome::WorkRemaining,
        CardinalityGenerationPageOutcome::PublishedReady,
    ] {
        assert_eq!(
            drive_cardinality_generation_page(&data, &index, &mut schema, |_| {
                Ok(authority.clone())
            })
            .expect("bounded generation page should advance"),
            expected,
        );
    }
    let header = schema
        .cardinality_generation_header()
        .expect("published header should decode")
        .expect("published header should exist");
    assert_eq!(header.state(), CardinalityGenerationState::Ready);
    assert_eq!(
        schema
            .cardinality_build_cursor()
            .expect("publication cursor state should decode"),
        None,
    );
    let digest = CardinalityLogicalCountKey::Entity(entity)
        .digest()
        .expect("entity count should hash");
    assert_eq!(
        schema
            .cardinality_count(header.slot(), header.generation(), digest)
            .expect("published count should decode"),
        Some(1),
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one lifecycle scenario proves row, index, Ready, and two-slot drift closure"
)]
fn source_drift_discards_rows_indexes_and_ready_evidence_into_alternate_slots() {
    let entity = EntityTag::new(14);
    let accepted = IndexId::new_with_generation(entity, 1, 1);
    let (mut data, mut index, mut schema) = initialized_stores();
    data.fold_recovered_journal_put(
        row_key(entity, 1),
        RawRow::try_new(vec![1]).expect("bounded row should construct"),
    )
    .expect("canonical row should seed");
    index
        .fold_recovered_journal_entry(
            index_key(accepted, IndexKeyKind::User, &[vec![1]], 1),
            Some(IndexEntryValue::presence_only()),
        )
        .expect("canonical accepted index should seed");
    let first = authority([entity], [(accepted, 1)], 0);
    let second = authority([entity], [(accepted, 1)], 1);
    let third = authority([entity], [(accepted, 1)], 2);

    assert_eq!(
        drive_cardinality_generation_page(&data, &index, &mut schema, |_| Ok(first.clone()))
            .expect("initial clearing should complete"),
        CardinalityGenerationPageOutcome::WorkRemaining,
    );
    let mut calls = 0_u8;
    assert_eq!(
        drive_cardinality_generation_page(&data, &index, &mut schema, |_| {
            calls += 1;
            Ok(if calls == 1 {
                first.clone()
            } else {
                second.clone()
            })
        })
        .expect("post-row source drift should restart"),
        CardinalityGenerationPageOutcome::WorkRemaining,
    );
    let after_rows = schema
        .cardinality_generation_header()
        .expect("restarted header should decode")
        .expect("restarted header should exist");
    assert_eq!(after_rows.generation().get(), 2);
    assert_eq!(after_rows.slot(), CardinalityCountSlot::B);
    assert_eq!(
        schema
            .cardinality_build_cursor()
            .expect("restart cursor state should decode"),
        None,
    );

    for _ in 0..2 {
        assert_eq!(
            drive_cardinality_generation_page(&data, &index, &mut schema, |_| {
                Ok(second.clone())
            })
            .expect("second source should reach its index phase"),
            CardinalityGenerationPageOutcome::WorkRemaining,
        );
    }
    let mut calls = 0_u8;
    assert_eq!(
        drive_cardinality_generation_page(&data, &index, &mut schema, |_| {
            calls += 1;
            Ok(if calls == 1 {
                second.clone()
            } else {
                third.clone()
            })
        })
        .expect("post-index source drift should restart"),
        CardinalityGenerationPageOutcome::WorkRemaining,
    );
    let after_indexes = schema
        .cardinality_generation_header()
        .expect("second restarted header should decode")
        .expect("second restarted header should exist");
    assert_eq!(after_indexes.generation().get(), 3);
    assert_eq!(after_indexes.slot(), CardinalityCountSlot::A);
    assert_eq!(after_indexes.state(), CardinalityGenerationState::Building);

    for expected in [
        CardinalityGenerationPageOutcome::WorkRemaining,
        CardinalityGenerationPageOutcome::WorkRemaining,
        CardinalityGenerationPageOutcome::PublishedReady,
    ] {
        assert_eq!(
            drive_cardinality_generation_page(&data, &index, &mut schema, |_| {
                Ok(third.clone())
            })
            .expect("third source should publish"),
            expected,
        );
    }
    let changed_index = IndexId::new_with_generation(entity, 1, 2);
    let fourth = authority([entity], [(changed_index, 1)], 2);
    assert_eq!(
        drive_cardinality_generation_page(&data, &index, &mut schema, |_| Ok(fourth.clone()))
            .expect("accepted-index drift should invalidate Ready evidence"),
        CardinalityGenerationPageOutcome::WorkRemaining,
    );
    let after_ready = schema
        .cardinality_generation_header()
        .expect("Ready invalidation header should decode")
        .expect("Ready invalidation header should exist");
    assert_eq!(after_ready.generation().get(), 4);
    assert_eq!(after_ready.slot(), CardinalityCountSlot::B);
    assert_eq!(after_ready.state(), CardinalityGenerationState::Building);
    assert_eq!(after_ready.validate_source(fourth.source()), Ok(()));

    for expected in [
        CardinalityGenerationPageOutcome::WorkRemaining,
        CardinalityGenerationPageOutcome::WorkRemaining,
        CardinalityGenerationPageOutcome::PublishedReady,
    ] {
        assert_eq!(
            drive_cardinality_generation_page(&data, &index, &mut schema, |_| {
                Ok(fourth.clone())
            })
            .expect("changed accepted-index identity should rebuild"),
            expected,
        );
    }
    let rebuilt = schema
        .cardinality_generation_header()
        .expect("rebuilt header should decode")
        .expect("rebuilt header should exist");
    let retired = CardinalityCountDigest::for_user_index_prefix(accepted, &[vec![1]])
        .expect("retired prefix should hash");
    assert_eq!(
        schema
            .cardinality_count(rebuilt.slot(), rebuilt.generation(), retired)
            .expect("retired prefix lookup should remain well formed"),
        None,
        "the reused slot must not leak a count from the predecessor index identity",
    );
}

#[test]
fn stale_complete_candidate_cannot_publish_after_a_generation_restart() {
    let entity = EntityTag::new(15);
    let (mut data, index, mut schema) = initialized_stores();
    data.fold_recovered_journal_put(
        row_key(entity, 1),
        RawRow::try_new(vec![1]).expect("bounded row should construct"),
    )
    .expect("canonical row should seed");
    let first = authority([entity], [], 0);
    let changed = authority([entity], [], 1);
    advance_cardinality_build_page(&data, &index, &mut schema, &first)
        .expect("candidate should initialize");
    advance_cardinality_build_page(&data, &index, &mut schema, &first)
        .expect("row phase should complete");
    let complete = advance_cardinality_build_page(&data, &index, &mut schema, &first)
        .expect("index phase should complete");
    let CardinalityBuildPageOutcome::CandidateComplete { candidate, .. } = complete else {
        panic!("candidate should be complete");
    };
    let old_header = candidate.header();
    schema
        .restart_cardinality_generation(old_header, changed.source())
        .expect("changed source should restart");
    assert!(
        schema
            .publish_ready_cardinality_generation(&candidate, first.source())
            .is_err(),
    );
    let current = schema
        .cardinality_generation_header()
        .expect("current header should decode")
        .expect("current header should exist");
    assert_eq!(current.generation().get(), 2);
    assert_eq!(current.state(), CardinalityGenerationState::Building);
}
