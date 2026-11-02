//! Live-source relation edges survive recovery of their journaled target.

use super::*;
use crate::{
    db::{
        commit::{RecoveryProgress, continue_recovery, forget_recovered_domain_for_tests},
        executor::{MutationCommitInterruption, interrupt_next_mutation_commit_for_tests},
        journal::JournalTailStore,
        registry::StoreAllocationIdentity,
    },
    testing::test_memory,
};

thread_local! {
    static TAIL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(test_memory(144)).expect("fixture storage should initialize"));
    static MIXED_STORES: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            STORE_PATH, &DATA_STORE, &INDEX_STORE, &SCHEMA_STORE, &TAIL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(141, "mixed.recovery.data.v1"),
                StoreAllocationIdentity::new(142, "mixed.recovery.index.v1"),
                StoreAllocationIdentity::new(143, "mixed.recovery.schema.v1"),
                StoreAllocationIdentity::new(144, "mixed.recovery.journal.v1"),
            ), StoreRuntimeStorageCapabilities::journaled(),
        ).unwrap();
        registry.register_store(
            CROSS_STORE_PATH, &CROSS_DATA_STORE, &CROSS_INDEX_STORE, &CROSS_SCHEMA_STORE,
            StoreAllocationIdentities::absent(), StoreRuntimeStorageCapabilities::heap(),
        ).unwrap();
        registry
    };
}

fn initialize_mixed() -> DbSession<TestCanister> {
    DATA_STORE.with_borrow_mut(|store| {
        *store =
            DataStore::init_journaled(test_memory(141)).expect("fixture storage should initialize");
    });
    INDEX_STORE.with_borrow_mut(|store| {
        *store = IndexStore::init_journaled(test_memory(142))
            .expect("fixture storage should initialize");
    });
    SCHEMA_STORE.with_borrow_mut(|store| {
        *store = SchemaStore::init_journaled(test_memory(143))
            .expect("fixture storage should initialize");
    });
    CROSS_DATA_STORE.with_borrow_mut(|store| *store = DataStore::init_heap());
    CROSS_INDEX_STORE.with_borrow_mut(|store| *store = IndexStore::init_heap());
    CROSS_SCHEMA_STORE.with_borrow_mut(|store| *store = SchemaStore::init_heap());
    let session = DbSession::new(
        &MIXED_STORES,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    crate::db::commit::continue_recovery_with_failure_authority(&session.db).unwrap_or_else(
        |failure| {
            panic!(
                "initial recovery: {:?}, {:?}, {:?}",
                failure.authority(),
                failure.error().origin(),
                failure.error()
            );
        },
    );
    for (path, tag, snapshot, bindings) in [
        (
            STORE_PATH,
            ENTITY_TAG,
            relation_snapshot(),
            vec![ID_SOURCE, PARENT_SOURCE, CODE_SOURCE],
        ),
        (
            CROSS_STORE_PATH,
            OTHER_ENTITY_TAG,
            other_snapshot(),
            vec![OTHER_ID_SOURCE, OTHER_VALUE_SOURCE, OTHER_NODE_SOURCE],
        ),
    ] {
        let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
            path,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([(tag, snapshot)]),
            bindings
                .into_iter()
                .enumerate()
                .map(|(index, source)| {
                    (
                        (tag, source_key(source)),
                        FieldId::new(u32::try_from(index + 1).unwrap()),
                    )
                })
                .collect(),
        );
        crate::db::commit::publish_accepted_schema_candidate(
            path,
            session.db.store_handle(path).unwrap(),
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .unwrap();
    }
    session
}

fn recover(session: &DbSession<TestCanister>) {
    forget_recovered_domain_for_tests(&session.db).unwrap();
    for _ in 0..30 {
        if continue_recovery(&session.db).unwrap() == RecoveryProgress::Complete {
            return;
        }
    }
    panic!("bounded fixture recovery must complete");
}

#[test]
fn heap_source_restricts_target_after_online_marker_recovery() {
    let session = initialize_mixed();
    session
        .execute_trusted_dynamic_mutation_batch(vec![insert(1, None)])
        .unwrap();
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Insert {
            entity: OTHER_ENTITY_NAME.into(),
            patch: other_patch_with_node(Some(20), 200, Some(1)),
        }])
        .unwrap();
    let before = session
        .execute_trusted_dynamic_mutation_batch(vec![delete(1)])
        .unwrap_err();
    assert_relation_violation(&before);
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowsPublished);
    session
        .execute_trusted_dynamic_mutation_batch(vec![insert(2, None)])
        .unwrap_err();
    recover(&session);
    let after = session
        .execute_trusted_dynamic_mutation_batch(vec![delete(1)])
        .unwrap_err();
    assert_eq!(after.class(), before.class());
    assert_eq!(after.origin(), before.origin());
    assert_relation_violation(&after);
    // The same reset must still reconstruct ordinary durable-source edges.
    session
        .execute_trusted_dynamic_mutation_batch(vec![insert(3, Some(2))])
        .unwrap();
    recover(&session);
    assert_relation_violation(
        &session
            .execute_trusted_dynamic_mutation_batch(vec![delete(2)])
            .unwrap_err(),
    );
    session
        .execute_trusted_dynamic_mutation_batch(vec![delete(3)])
        .unwrap();
    // A live reference can move normally; recovery must neither lose its new
    // target nor resurrect its removed old-target entry.
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Update {
            entity: OTHER_ENTITY_NAME.into(),
            key: InputValue::nat64(20),
            patch: other_patch_with_node(None, 200, Some(2)),
        }])
        .unwrap();
    recover(&session);
    session
        .execute_trusted_dynamic_mutation_batch(vec![delete(1)])
        .unwrap();
    assert_relation_violation(
        &session
            .execute_trusted_dynamic_mutation_batch(vec![delete(2)])
            .unwrap_err(),
    );
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Delete {
            entity: OTHER_ENTITY_NAME.into(),
            key: InputValue::nat64(20),
        }])
        .unwrap();
    session
        .execute_trusted_dynamic_mutation_batch(vec![delete(2)])
        .unwrap();
}
