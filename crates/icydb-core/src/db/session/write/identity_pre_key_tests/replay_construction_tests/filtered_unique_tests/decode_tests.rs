//! Decode refusal cannot release filtered unique ownership or bypass conflicts.

use super::*;
use crate::db::{commit::observe_commit_control, tests::support::request_with_limit};
use ic_storage::adapter::Storable;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

type StoredEntries = Vec<(Vec<u8>, Vec<u8>)>;

fn retained_entries() -> (StoredEntries, StoredEntries) {
    let rows = JOURNALED_DATA_STORE.with_borrow(|store| {
        let mut entries = Vec::new();
        store
            .visit_entries(|key, value| {
                entries.push((
                    key.to_bytes().unwrap().into_owned(),
                    value.to_bytes().unwrap().into_owned(),
                ));
                Ok::<_, InternalError>(crate::db::data::StoreVisit::Continue)
            })
            .unwrap();
        entries
    });
    let indexes = JOURNALED_INDEX_STORE.with_borrow(|store| {
        let mut entries = Vec::new();
        store
            .visit_entries(|key, value| {
                entries.push((
                    key.to_bytes().unwrap().into_owned(),
                    value.to_bytes().unwrap().into_owned(),
                ));
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
            .unwrap();
        entries
    });
    (rows, indexes)
}

fn assert_budget_then_unique_conflict(expression: bool) {
    let session = seeded_filtered(expression);
    let before = retained_entries();
    let control = observe_commit_control().unwrap();
    for attempt in [
        set_active(2, true),
        DynamicMutation::Insert {
            entity: ENTITY_NAME.into(),
            patch: filtered_payload_patch("Shared", true),
        },
    ] {
        let root = request_with_limit(Resource::DecodedBytes, 0);
        let bounded = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
        let error = bounded
            .execute_trusted_dynamic_mutation_batch(vec![attempt.clone()])
            .unwrap_err();
        assert!(root.observed(Resource::DecodedBytes) > 0);
        for fact in [
            (
                DiagnosticFactTag::BudgetResource,
                Resource::DecodedBytes.raw(),
            ),
            (DiagnosticFactTag::Limit, 0),
            (
                DiagnosticFactTag::Actual,
                root.observed(Resource::DecodedBytes),
            ),
            (DiagnosticFactTag::ExecutionLane, Lane::Mutation.raw()),
        ] {
            assert!(error.diagnostic_facts().contains(&fact), "{error:?}");
        }
        assert_eq!(retained_entries(), before);
        assert_eq!(observe_commit_control().unwrap(), control);
        assert_owner(&session, 1);
        // The fresh operation must reach the real uniqueness owner, rather than
        // inheriting the exhausted request or accepting the conflicting image.
        let fresh = DbSession::<JournaledTestCanister>::new(
            &JOURNALED_STORE_REGISTRY,
            &crate::db::RequestExecutionRoot::__new_runtime_root(),
        );
        let error = fresh
            .execute_trusted_dynamic_mutation_batch(vec![attempt])
            .unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
        );
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::ConstraintKind,
            DiagnosticConstraintKind::Unique.raw()
        )));
        assert_eq!(retained_entries(), before);
        assert_eq!(observe_commit_control().unwrap(), control);
        assert_owner(&session, 1);
    }
    // Releasing the current owner and admitting its replacement remains legal.
    session
        .execute_trusted_dynamic_mutation_batch(vec![set_active(2, true), set_active(1, false)])
        .unwrap();
    assert_owner(&session, 2);
    forget_recovered_domain_for_tests(&session.db).unwrap();
    drive_journaled_recovery_to_completion(&session);
    assert_owner(&session, 2);
    let inserted = session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![filtered_payload_patch("Different", true)],
        )
        .unwrap();
    assert_eq!(
        inserted.rows,
        vec![vec![
            OutputValue::nat64(3),
            OutputValue::text("Different".into()),
            OutputValue::boolean(true)
        ]]
    );
}

#[test]
fn field_path_unique_conflicts_survive_decode_refusal_and_retry() {
    assert_budget_then_unique_conflict(false);
}

#[test]
fn expression_unique_conflicts_survive_decode_refusal_and_retry() {
    assert_budget_then_unique_conflict(true);
}
