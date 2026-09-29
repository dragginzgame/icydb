//! Moving a retained field preserves its allocation state in every schema view.

use super::*;
use crate::db::schema::{
    IdentityStatementCursor, accepted_schema_candidate_with_field_bindings_for_tests,
};
use icydb_schema::FieldSourceKey;
use std::collections::BTreeMap;

const ENTITY: EntityTag = EntityTag::new(114);
const STORE: &str = "test::IdentityRemap";

fn candidate(removed: bool, source: &str) -> CandidateSchemaRevision {
    let id = FieldId::new(if removed { 1 } else { 2 });
    let slot = SchemaFieldSlot::new(u16::from(!removed));
    let mut fields = Vec::new();
    if !removed {
        fields.push(PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "archived".into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Bool,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Bool),
        ));
    }
    fields.push(PersistedFieldSnapshot::new_initial_with_write_policy(
        id,
        "id".into(),
        slot,
        AcceptedFieldKind::Nat8,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        SchemaFieldWritePolicy::from_model_policies(Some(FieldInsertGeneration::Identity), None),
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat8),
    ));
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::new(if removed { 2 } else { 1 }),
        "SequenceRow".into(),
        "SequenceRow".into(),
        id,
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    accepted_schema_candidate_with_field_bindings_for_tests(
        STORE,
        AcceptedSchemaRevision::new(if removed { 2 } else { 1 }),
        BTreeMap::from([(ENTITY, snapshot)]),
        BTreeMap::from([((ENTITY, FieldSourceKey::try_new(source).unwrap()), id)]),
    )
}

fn publish_initial(store: &mut SchemaStore) {
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::NONE,
            &candidate(false, "stable-id-source"),
        )
        .unwrap();
}

fn exhausted_range() -> (IdentityRangeAdvance, IdentityAdvanceId) {
    let owner =
        IdentityStateOwner::try_new(test_database_incarnation(), ENTITY, FieldId::new(2)).unwrap();
    (
        IdentityRangeAdvance::try_new(owner, 0, 255, 255).unwrap(),
        IdentityAdvanceId::try_new([1; 16], [2; 16], 1, 0).unwrap(),
    )
}

fn assert_exhausted(store: &SchemaStore, advance: IdentityAdvanceId) {
    let states = store
        .identity_state_inventory_for_integrity(test_database_incarnation())
        .unwrap();
    assert_eq!(states.len(), 1);
    let state = &states[0];
    assert_eq!(state.owner().field_id(), FieldId::new(1));
    assert_eq!(state.materialized_high_water(), 255);
    assert_eq!(state.last_applied_advance(), Some(advance));
    assert_eq!(state.lifecycle(), IdentityStateLifecycle::Active);
    assert!(
        IdentityStatementCursor::from_active_state(state)
            .unwrap()
            .allocate(0, 0)
            .is_err()
    );
}

#[test]
fn identity_field_remap_preserves_exhaustion_in_heap_publication() {
    let mut store = SchemaStore::init_heap();
    publish_initial(&mut store);
    let (range, advance) = exhausted_range();
    store.apply_identity_range_advance(range, advance).unwrap();
    store
        .publish_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &candidate(true, "stable-id-source"),
        )
        .unwrap();
    assert_exhausted(&store, advance);
}

#[test]
fn identity_field_remap_replays_before_canonical_fold() {
    let memory = test_memory(227);
    let mut store = SchemaStore::init_journaled(memory.clone());
    publish_initial(&mut store);
    let (range, advance) = exhausted_range();
    store.fold_identity_range_advance(range, advance).unwrap();
    let after = candidate(true, "stable-id-source");
    store
        .apply_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &after,
        )
        .unwrap();
    assert_exhausted(&store, advance);
    let old_key = RawSchemaKey::from_identity_state(ENTITY, FieldId::new(2));
    assert!(store.get_canonical_raw_value(&old_key).unwrap().is_some());
    let mut reopened = SchemaStore::init_journaled(memory);
    reopened
        .apply_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &after,
        )
        .unwrap();
    assert_exhausted(&reopened, advance);
    let prepared = reopened
        .prepare_fold_journaled_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            after,
        )
        .unwrap();
    assert!(
        reopened
            .get_canonical_raw_value(&old_key)
            .unwrap()
            .is_some()
    );
    reopened
        .apply_prepared_accepted_schema_fold(prepared)
        .unwrap();
    reopened.reset_journaled_live_projection().unwrap();
    assert!(
        reopened
            .get_canonical_raw_value(&old_key)
            .unwrap()
            .is_none()
    );
    assert_exhausted(&reopened, advance);
}

#[test]
fn identity_field_remap_rejects_unproven_lineage_without_effects() {
    let mut store = SchemaStore::init_heap();
    publish_initial(&mut store);
    let before = store
        .identity_state_inventory_for_integrity(test_database_incarnation())
        .unwrap();
    let error = store
        .preflight_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &candidate(true, "different-source"),
        )
        .unwrap_err();
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(
        store
            .identity_state_inventory_for_integrity(test_database_incarnation())
            .unwrap(),
        before
    );
}

#[test]
fn identity_field_remap_rejects_occupied_destination_without_effects() {
    let mut store = SchemaStore::init_heap();
    publish_initial(&mut store);
    let retired = IdentityState::new_active(
        IdentityStateOwner::try_new(test_database_incarnation(), ENTITY, FieldId::new(1)).unwrap(),
        AcceptedFieldKind::Nat8,
    )
    .unwrap()
    .retire()
    .unwrap();
    store.insert_durable_raw_value(
        RawSchemaKey::from_identity_state(ENTITY, FieldId::new(1)),
        encode_identity_state(&retired).unwrap(),
    );
    let before = store
        .identity_state_inventory_for_integrity(test_database_incarnation())
        .unwrap();
    let error = store
        .preflight_accepted_schema_candidate(
            test_database_incarnation(),
            AcceptedSchemaRevision::INITIAL,
            &candidate(true, "stable-id-source"),
        )
        .unwrap_err();
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(
        store
            .identity_state_inventory_for_integrity(test_database_incarnation())
            .unwrap(),
        before
    );
}
