//! Entity-scoped physical index observation and emptiness proofs.

use super::*;
use crate::{db::key_taxonomy::PrimaryKeyComponent, testing::test_memory};

#[test]
fn complete_domain_observation_charges_only_the_selected_entity() {
    for journaled in [false, true] {
        let mut store = if journaled {
            IndexStore::init_journaled(test_memory(98))
        } else {
            IndexStore::init_heap()
        };
        for neighbor in [
            key(8, 0, 0, 1, IndexKeyKind::User),
            key(7, 0, 0, 1, IndexKeyKind::System),
        ] {
            store.insert(neighbor, IndexEntryValue::presence());
        }
        let mut budget = StagedUserIndexDomainBudget::standard();
        budget.usage.projection_work_units = MAX_SCHEMA_PROJECTION_WORK_UNITS;
        assert!(
            observe_current_user_index_domain(&store, EntityTag::new(7), &mut budget)
                .map_err(StagedUserIndexDomainError::into_internal_error)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            budget.usage.projection_work_units,
            MAX_SCHEMA_PROJECTION_WORK_UNITS
        );
        assert_eq!(budget.usage.deletion_keys, 0);
        for (ordinal, generation) in [(0, 0), (u16::MAX, u64::MAX)] {
            let own = key(7, ordinal, generation, 1, IndexKeyKind::User);
            if journaled {
                store
                    .apply_canonical_entry(own, Some(IndexEntryValue::presence()))
                    .unwrap();
            } else {
                store.insert(own, IndexEntryValue::presence());
            }
        }
        let mut budget = StagedUserIndexDomainBudget::standard();
        let observed = observe_current_user_index_domain(&store, EntityTag::new(7), &mut budget)
            .map_err(StagedUserIndexDomainError::into_internal_error)
            .unwrap();
        assert_eq!(observed.len(), 2);
        assert_eq!(budget.usage.projection_work_units, 2);
        assert_eq!(budget.usage.deletion_keys, 2);
        budget.usage.projection_work_units = MAX_SCHEMA_PROJECTION_WORK_UNITS - 1;
        assert!(matches!(
            observe_current_user_index_domain(&store, EntityTag::new(7), &mut budget),
            Err(StagedUserIndexDomainError::BudgetExceeded(
                SchemaTransitionBudgetResource::ProjectionWorkUnits
            ))
        ));
    }
}

fn key(
    entity: u64,
    ordinal: u16,
    generation: u64,
    count: usize,
    kind: IndexKeyKind,
) -> RawIndexStoreKey {
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new_with_generation(EntityTag::new(entity), ordinal, generation),
        kind,
        &vec![vec![42]; count],
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
    )
    .unwrap()
    .to_raw()
    .unwrap()
}

#[test]
fn empty_user_domain_ignores_neighbors_and_observes_all_ordinals_and_generations() {
    for entity in [0, u64::MAX] {
        for backend in 0..4 {
            let mut store = if backend == 0 {
                IndexStore::init_heap()
            } else {
                IndexStore::init_journaled(test_memory(98))
            };
            let neighbors = [
                key(entity ^ 1, 0, 0, 1, IndexKeyKind::User),
                key(entity, 0, 0, 1, IndexKeyKind::System),
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
            for ordinal in [0, u16::MAX] {
                for generation in [0, u64::MAX] {
                    for count in 0..=crate::MAX_INDEX_FIELDS {
                        let matching = key(entity, ordinal, generation, count, IndexKeyKind::User);
                        if backend == 3 {
                            store
                                .apply_canonical_entry(
                                    matching.clone(),
                                    Some(IndexEntryValue::presence()),
                                )
                                .unwrap();
                            store.remove(&matching);
                        }
                        let before = IndexStore::current_entry_read_count();
                        assert!(
                            prove_empty_user_index_domain(&store, EntityTag::new(entity)).is_ok()
                        );
                        assert_eq!(IndexStore::current_entry_read_count() - before, 0);
                        if backend == 2 {
                            store
                                .apply_canonical_entry(
                                    matching.clone(),
                                    Some(IndexEntryValue::presence()),
                                )
                                .unwrap();
                        } else {
                            store.insert(matching.clone(), IndexEntryValue::presence());
                        }
                        let before = IndexStore::current_entry_read_count();
                        assert!(matches!(
                            prove_empty_user_index_domain(&store, EntityTag::new(entity)),
                            Err(StagedUserIndexDomainError::CurrentDomainMismatch)
                        ));
                        if backend != 0 {
                            assert_eq!(IndexStore::current_entry_read_count() - before, 1);
                        }
                        store.remove(&matching);
                    }
                }
            }
        }
    }
}

#[test]
fn user_domain_proof_preserves_readiness_and_local_corruption_refusals() {
    let mut store = IndexStore::init_heap();
    let entity = EntityTag::new(1);
    store.set_access_state(IndexState::Building, 2);
    assert!(matches!(
        prove_empty_user_index_domain(&store, entity),
        Err(StagedUserIndexDomainError::IndexStoreNotReady)
    ));
    store.set_access_state(IndexState::Ready, 3);
    let mut bytes = key(1, 1, 1, 1, IndexKeyKind::User).into_bytes();
    bytes.push(0);
    let malformed = RawIndexStoreKey::from_persisted_bytes(bytes);
    assert!(IndexKey::try_from_raw(&malformed).is_err());
    store.insert(malformed, IndexEntryValue::presence());
    assert!(matches!(
        prove_empty_user_index_domain(&store, entity),
        Err(StagedUserIndexDomainError::PhysicalKeyDecode)
    ));
}
