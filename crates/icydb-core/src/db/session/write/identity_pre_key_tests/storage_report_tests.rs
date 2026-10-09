//! Storage report totals retain the visible store and corruption contracts.

use super::{
    ENTITY_NAME, JOURNALED_STORE_PATH, STORE_PATH, THIRD_ENTITY_NAME,
    drive_journaled_recovery_to_completion, dynamic_payload_patch, initialize,
    initialize_journaled_multi_entity, insert_exact_key_fixture,
};
use crate::{
    db::{
        DbSession, DynamicMutation,
        data::{RawDataStoreKey, RawRow, canonical_row_from_stored_raw_row},
        diagnostics::EntitySnapshot,
        index::{IndexEntryValue, IndexKey, IndexStore, IndexStoreVisit, RawIndexStoreKey},
    },
    traits::CanisterKind,
    value::InputValue,
};
use std::convert::Infallible;

fn assert_report<C: CanisterKind>(
    session: &DbSession<C>,
    valid_rows: u64,
    corrupted_keys: u64,
    corrupted_entries: u64,
) {
    // Independent existing store APIs supply the physical totals, including
    // malformed entries. Observe report traversal only after these baselines.
    let expected = session.db.with_store_registry(|registry| {
        let mut stores = registry
            .iter()
            .map(|(path, store)| {
                let data = store.with_data(|data| (data.len(), data.memory_bytes()));
                let index = store.with_index(|index| {
                    let mut namespaces = [0u64; 2];
                    let _: Result<(), Infallible> = index.visit_entries(|key, _| {
                        if let Ok(key) = IndexKey::try_from_raw(key) {
                            namespaces[usize::from(key.uses_system_namespace())] += 1;
                        }
                        Ok(IndexStoreVisit::Continue)
                    });
                    (index.len(), index.memory_bytes(), namespaces)
                });
                (path, data, index)
            })
            .collect::<Vec<_>>();
        stores.sort_by_key(|entry| entry.0);
        stores
    });
    let mut physical_snapshot = None;
    for aliases in [
        None,
        Some([(ENTITY_NAME, "first"), (THIRD_ENTITY_NAME, "third")]),
    ] {
        let before = IndexStore::current_entry_read_count();
        let report = match aliases {
            None => session.storage_report_default(),
            Some(aliases) => session.storage_report(&aliases),
        }
        .expect("storage report should count visible entries without mutation");
        let visited = IndexStore::current_entry_read_count() - before;
        assert_eq!(visited, expected.iter().map(|entry| entry.2.0).sum::<u64>());
        assert_eq!(report.storage_data().len(), expected.len());
        for (position, (path, data, index)) in expected.iter().enumerate() {
            let data_snapshot = &report.storage_data()[position];
            let index_snapshot = &report.storage_index()[position];
            assert_eq!(data_snapshot.path(), *path);
            assert_eq!(index_snapshot.path(), *path);
            assert_eq!(
                (data_snapshot.entries(), data_snapshot.memory_bytes()),
                *data
            );
            assert_eq!(
                (index_snapshot.entries(), index_snapshot.memory_bytes()),
                (index.0, index.1)
            );
            assert_eq!(
                [
                    index_snapshot.user_entries(),
                    index_snapshot.system_entries()
                ],
                index.2
            );
            assert_eq!(report.schema_storage()[position].path(), *path);
        }
        assert_eq!(report.corrupted_keys(), corrupted_keys);
        assert_eq!(report.corrupted_entries(), corrupted_entries);
        let physical = candid::encode_one((
            report.storage_data(),
            report.storage_index(),
            report.schema_storage(),
            report.memory_allocations(),
        ))
        .unwrap();
        if let Some(previous) = physical_snapshot.as_ref() {
            assert_eq!(previous, &physical);
        }
        physical_snapshot = Some(physical);
        assert_eq!(
            report
                .entity_storage()
                .iter()
                .map(EntitySnapshot::entries)
                .sum::<u64>(),
            valid_rows
        );
        if aliases.is_some() && valid_rows > 0 {
            assert!(
                report
                    .entity_storage()
                    .iter()
                    .any(|entry| entry.path() == "first")
            );
        }
    }
}

fn corrupt_entries<C: CanisterKind>(session: &DbSession<C>, path: &str) {
    let store = session
        .db
        .store_handle(path)
        .expect("fixture store should resolve");
    store.with_data_mut(|data| {
        data.insert(
            RawDataStoreKey::from_persisted_bytes(vec![1]),
            canonical_row_from_stored_raw_row(RawRow::try_new(vec![2, 3]).unwrap()),
        );
    });
    store.with_index_mut(|index| {
        let mut valid_key = None;
        let _: Result<(), Infallible> = index.visit_entries(|key, _| {
            if !IndexKey::try_from_raw(key).unwrap().uses_system_namespace() {
                valid_key = Some(key.clone());
                return Ok(IndexStoreVisit::Stop);
            }
            Ok(IndexStoreVisit::Continue)
        });
        index.insert(
            valid_key.expect("fixture should have a user index"),
            IndexEntryValue::from_persisted_bytes(vec![9]),
        );
        index.insert(
            RawIndexStoreKey::from_persisted_bytes(vec![1]),
            IndexEntryValue::presence(),
        );
    });
}

#[test]
fn heap_storage_reports_preserve_empty_populated_and_corrupt_totals() {
    let session = initialize();
    assert_report(&session, 0, 0, 0);
    insert_exact_key_fixture(&session, 10);
    insert_exact_key_fixture(&session, 20);
    assert_report(&session, 2, 0, 0);
    corrupt_entries(&session, STORE_PATH);
    assert_report(&session, 2, 1, 2);
}

#[test]
fn journaled_storage_reports_merge_base_and_live_changes_once() {
    let session = initialize_journaled_multi_entity();
    assert_report(&session, 0, 0, 0);
    let retained = insert_exact_key_fixture(&session, 10);
    let deleted = insert_exact_key_fixture(&session, 20);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: THIRD_ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(30),
        })
        .unwrap();
    drive_journaled_recovery_to_completion(&session);
    assert_report(&session, 3, 0, 0);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(retained),
            patch: dynamic_payload_patch(40),
        })
        .unwrap();
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(deleted),
        })
        .unwrap();
    insert_exact_key_fixture(&session, 50);
    assert_report(&session, 3, 0, 0);
    corrupt_entries(&session, JOURNALED_STORE_PATH);
    assert_report(&session, 3, 1, 2);
}
