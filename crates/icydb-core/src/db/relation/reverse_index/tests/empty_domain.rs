//! Relation-removal proofs observe only the accepted physical reverse domain.

use super::*;
use crate::{
    db::{
        index::{IndexKey, IndexKeyKind, IndexState, IndexStore, RawIndexStoreKey},
        relation::reverse_index::prove_empty_reverse_relation_domain,
    },
    error::InternalError,
    testing::test_memory,
};

fn snapshot(relation: &PersistedRelationEdgeSnapshot) -> PersistedSchemaSnapshot {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "Source".into(),
        "Source".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        ["id", "target_id"]
            .into_iter()
            .zip(0_u16..)
            .map(|(name, slot)| {
                PersistedFieldSnapshot::new_initial(
                    FieldId::new(u32::from(slot) + 1),
                    name.into(),
                    SchemaFieldSlot::new(slot),
                    AcceptedFieldKind::Nat64,
                    Vec::new(),
                    false,
                    SchemaInsertDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Nat64),
                )
            })
            .collect(),
    );
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        std::slice::from_ref(relation),
    )
    .unwrap();
    let snapshot = snapshot
        .with_relations(vec![relation.clone()])
        .with_constraint_catalog(constraints);
    AcceptedSchemaSnapshot::try_new(snapshot.clone()).unwrap();
    snapshot
}

fn key(entity: u64, generation: u64, relation: u32, kind: IndexKeyKind) -> RawIndexStoreKey {
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new_with_generation(EntityTag::new(entity), u16::MAX, generation),
        kind,
        &[relation.to_be_bytes().as_slice(), &[42]],
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
    )
    .unwrap()
    .to_raw()
    .unwrap()
}

#[test]
fn empty_reverse_domain_ignores_other_entities_generations_relations_and_kinds() {
    for (entity, generation, relation_id) in [(1, 0, 1), (u64::MAX, u64::MAX, u32::MAX)] {
        let relation = PersistedRelationEdgeSnapshot::new_direct(
            RelationId::new(relation_id).unwrap(),
            "target".into(),
            "Target".into(),
            vec![FieldId::new(2)],
        )
        .clone_with_physical_generation(generation);
        let snapshot = snapshot(&relation);
        for backend in 0..4 {
            let mut store = if backend == 0 {
                IndexStore::init_heap()
            } else {
                IndexStore::init_journaled(test_memory(98))
            };
            let neighbors = [
                key(entity ^ 1, generation, relation_id, IndexKeyKind::System),
                key(entity, generation ^ 1, relation_id, IndexKeyKind::System),
                key(entity, generation, relation_id ^ 2, IndexKeyKind::System),
                key(entity, generation, relation_id, IndexKeyKind::User),
            ];
            for neighbor in neighbors {
                if backend >= 2 {
                    store
                        .apply_canonical_entry(neighbor, Some(IndexEntryValue::presence()))
                        .unwrap();
                } else {
                    store.insert(neighbor, IndexEntryValue::presence());
                }
            }
            let matching = key(entity, generation, relation_id, IndexKeyKind::System);
            if backend == 3 {
                store
                    .apply_canonical_entry(matching.clone(), Some(IndexEntryValue::presence()))
                    .unwrap();
                store.remove(&matching);
            }
            let before = IndexStore::current_entry_read_count();
            prove_empty_reverse_relation_domain(
                &store,
                EntityTag::new(entity),
                &snapshot,
                &relation,
            )
            .unwrap();
            assert_eq!(IndexStore::current_entry_read_count() - before, 0);

            // An effective matching entry, whether heap, live or canonical,
            // prevents removal; unrelated entries cannot supply that witness.
            if backend == 2 {
                store
                    .apply_canonical_entry(matching.clone(), Some(IndexEntryValue::presence()))
                    .unwrap();
            } else {
                store.insert(matching.clone(), IndexEntryValue::presence());
            }
            let error = prove_empty_reverse_relation_domain(
                &store,
                EntityTag::new(entity),
                &snapshot,
                &relation,
            )
            .unwrap_err();
            assert_eq!(
                error.diagnostic_code(),
                InternalError::store_unsupported().diagnostic_code()
            );
            store.remove(&matching);
            // Preserve conservative refusal of a matching relation prefix
            // with an unexpected, but codec-admitted, component count.
            let relation_bytes = relation_id.to_be_bytes();
            let components = [relation_bytes.as_slice(), &[42], &[43], &[44]];
            for count in [1, 3, 4] {
                let stray = IndexKey::new_from_components_with_primary_key_value(
                    &IndexId::new_with_generation(EntityTag::new(entity), u16::MAX, generation),
                    IndexKeyKind::System,
                    &components[..count],
                    &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
                )
                .unwrap()
                .to_raw()
                .unwrap();
                store.insert(stray.clone(), IndexEntryValue::presence());
                let error = prove_empty_reverse_relation_domain(
                    &store,
                    EntityTag::new(entity),
                    &snapshot,
                    &relation,
                )
                .unwrap_err();
                assert_eq!(
                    error.diagnostic_code(),
                    InternalError::store_unsupported().diagnostic_code()
                );
                store.remove(&stray);
            }
        }
    }
}

#[test]
fn reverse_domain_proof_preserves_readiness_authority_and_corruption_refusals() {
    let relation = PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(1).unwrap(),
        "target".into(),
        "Target".into(),
        vec![FieldId::new(2)],
    );
    let snapshot = snapshot(&relation);
    let entity = EntityTag::new(1);
    let mut store = IndexStore::init_heap();
    store.set_access_state(IndexState::Building, 2);
    let error =
        prove_empty_reverse_relation_domain(&store, entity, &snapshot, &relation).unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::store_unsupported().diagnostic_code()
    );
    store.set_access_state(IndexState::Ready, 3);
    let other = relation.clone_with_physical_generation(1);
    let error = prove_empty_reverse_relation_domain(&store, entity, &snapshot, &other).unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::store_corruption().diagnostic_code()
    );

    // A malformed frame inside the selected prefix still fails closed.
    let mut bytes = key(1, 0, 1, IndexKeyKind::System).into_bytes();
    bytes.push(0);
    let malformed = RawIndexStoreKey::from_persisted_bytes(bytes);
    assert!(IndexKey::try_from_raw(&malformed).is_err());
    store.insert(malformed, IndexEntryValue::presence());
    let error =
        prove_empty_reverse_relation_domain(&store, entity, &snapshot, &relation).unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::store_corruption().diagnostic_code()
    );
}
