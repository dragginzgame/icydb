//! Session authority must not depend on the previously selected commit domain.

use super::*;
use crate::{
    db::{DynamicQuery, commit, database_format, integrity::DatabaseIncarnationId},
    error::InternalError,
};

fn select_foreign_incarnation() -> [u8; 16] {
    let allocation = commit::CommitMemoryAllocation {
        memory_id: 44,
        stable_key: "icydb.typed_adapter_tests.foreign.commit.v1",
    };
    commit::configure_commit_memory_id(allocation.memory_id, allocation.stable_key).unwrap();
    let memory = commit::commit_memory_handle(allocation).unwrap();
    database_format::initialize_current_database_control_for_tests(&memory);
    let incarnation = DatabaseIncarnationId::for_tests(0x72);
    let replacement =
        commit::prepare_commit_control_replacement(memory, incarnation, [0x55; 32], 0, &[])
            .unwrap();
    commit::apply_prepared_commit_control_replacement(replacement);
    assert_eq!(commit::database_incarnation_id().unwrap(), incarnation);
    incarnation.to_bytes()
}

#[test]
fn retained_sessions_select_their_domain_before_validating_bindings() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .unwrap();
    let retained = DbSession::<TestCanister>::new(
        session.db.store,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    for reader in [&session, &retained, &session] {
        let foreign = select_foreign_incarnation();
        assert_ne!(binding.database_incarnation, foreign);
        assert!(reader.typed_entity_binding_is_current(&binding).unwrap());
        assert_eq!(
            commit::database_incarnation_id().unwrap().to_bytes(),
            binding.database_incarnation
        );
        let mut foreign_binding = binding.clone();
        foreign_binding.database_incarnation = select_foreign_incarnation();
        assert!(
            !reader
                .typed_entity_binding_is_current(&foreign_binding)
                .unwrap()
        );
    }
}

#[test]
fn mixed_batch_rejects_late_foreign_incarnation_without_writes() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .unwrap();
    let mut foreign = binding.clone();
    foreign.database_incarnation = select_foreign_incarnation();
    let result = session
        .execute_trusted_typed_mutation_batch(vec![
            (binding.clone(), typed_insert(&binding, 1, 10)),
            (foreign.clone(), typed_insert(&foreign, 2, 20)),
        ])
        .unwrap();
    assert!(result.is_none());
    let rows = session
        .execute_trusted_live_page(&DynamicQuery::new("Entity"), None)
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[test]
fn binding_validation_preserves_startup_admission() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .unwrap();
    commit::forget_recovered_domain_for_tests(&session.db).unwrap();
    select_foreign_incarnation();
    let error = session
        .typed_entity_binding_is_current(&binding)
        .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::recovery_pending().diagnostic_code()
    );
}
