//! Decode admission must preserve populated stores across mixed-batch refusal.

use super::*;

// Capture raw retained entry bytes, independent of query/materialization paths.
type StoredEntries = Vec<(Vec<u8>, Vec<u8>)>;

fn retained_entries() -> (StoredEntries, StoredEntries) {
    let rows = DATA_STORE.with_borrow(|store| {
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
    let indexes = INDEX_STORE.with_borrow(|store| {
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

fn populated() -> DbSession<TestCanister> {
    let session = initialize_with_snapshot(partial_index_snapshot(
        "payload != 10 AND payload != 20 AND payload != 30",
    ));
    let inserted = session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![dynamic_payload_patch(40), dynamic_payload_patch(50)],
        )
        .unwrap();
    assert_eq!(
        inserted.rows,
        vec![expected_dynamic_row(1, 40), expected_dynamic_row(2, 50)]
    );
    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 2);
    session
}

fn mixed_batch() -> Vec<DynamicMutation> {
    vec![
        DynamicMutation::Delete {
            entity: ENTITY_NAME.into(),
            key: InputValue::nat64(1),
        },
        DynamicMutation::Update {
            entity: ENTITY_NAME.into(),
            key: InputValue::nat64(2),
            patch: dynamic_payload_patch(70),
        },
        DynamicMutation::Insert {
            entity: ENTITY_NAME.into(),
            patch: dynamic_payload_patch(60),
        },
    ]
}

// Reference counters establish complete-operation boundary values, not cost savings.
fn complete_decode_usage() -> u64 {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                populated();
                let root = request_with_limit(Resource::DecodedBytes, u64::MAX);
                let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
                session
                    .execute_trusted_dynamic_mutation_batch(mixed_batch())
                    .unwrap();
                root.observed(Resource::DecodedBytes)
            })
            .join()
            .unwrap()
    })
}

#[test]
fn populated_mixed_batch_decode_refusal_preserves_bytes_and_identity() {
    let required = complete_decode_usage();
    assert!(required > 0);
    for limit in [0, required - 1] {
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let initialized = populated();
                    let before = retained_entries();
                    let control = observe_commit_control().unwrap();
                    let fingerprint = initialized
                        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                        .unwrap()
                        .fingerprint();
                    let root = request_with_limit(Resource::DecodedBytes, limit);
                    let bounded = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
                    let error = bounded
                        .execute_trusted_dynamic_mutation_batch(mixed_batch())
                        .unwrap_err();
                    for fact in [
                        (
                            DiagnosticFactTag::BudgetResource,
                            Resource::DecodedBytes.raw(),
                        ),
                        (DiagnosticFactTag::Limit, limit),
                        (
                            DiagnosticFactTag::Actual,
                            root.observed(Resource::DecodedBytes),
                        ),
                        (
                            DiagnosticFactTag::ExecutionLane,
                            DiagnosticExecutionLane::Mutation.raw(),
                        ),
                    ] {
                        assert!(error.diagnostic_facts().contains(&fact), "{error:?}");
                    }
                    assert!(root.observed(Resource::DecodedBytes) > limit);
                    assert_eq!(retained_entries(), before);
                    assert_eq!(observe_commit_control().unwrap(), control);
                    assert!(
                        crate::db::commit::retained_commit_marker_measurement_for_tests()
                            .unwrap()
                            .is_none()
                    );
                    assert_eq!(
                        initialized
                            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                            .unwrap()
                            .fingerprint(),
                        fingerprint
                    );
                    SCHEMA_STORE.with_borrow(|store| {
                        let cursor = store
                            .identity_statement_cursor(
                                database_incarnation_id().unwrap(),
                                ENTITY_TAG,
                                FieldId::new(1),
                                &AcceptedFieldKind::Nat64,
                            )
                            .unwrap();
                        assert_eq!(cursor.expected_high_water(), 2);
                        assert!(!cursor.has_allocations());
                    });
                    let retry = DbSession::<TestCanister>::new(
                        &STORE_REGISTRY,
                        &crate::db::RequestExecutionRoot::__new_runtime_root(),
                    );
                    retry
                        .execute_trusted_dynamic_mutation_batch(mixed_batch())
                        .unwrap();
                    assert_dynamic_payload(&retry, 2, 70);
                    assert_dynamic_payload(&retry, 3, 60);
                    assert_eq!(DATA_STORE.with_borrow(DataStore::len), 2);
                    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 2);
                    let next = retry
                        .execute_trusted_dynamic_insert_batch(
                            ENTITY_NAME,
                            vec![dynamic_payload_patch(80)],
                        )
                        .unwrap();
                    assert_eq!(next.rows, vec![expected_dynamic_row(4, 80)]);
                })
                .join()
                .unwrap();
        });
    }
}

#[test]
fn populated_mixed_batch_accepts_the_exact_decode_allowance() {
    let required = complete_decode_usage();
    populated();
    let root = request_with_limit(Resource::DecodedBytes, required);
    let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
    session
        .execute_trusted_dynamic_mutation_batch(mixed_batch())
        .unwrap();
    assert_eq!(root.observed(Resource::DecodedBytes), required);
    assert_eq!(DATA_STORE.with_borrow(DataStore::len), 2);
    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 2);
    assert!(
        crate::db::commit::retained_commit_marker_measurement_for_tests()
            .unwrap()
            .is_none()
    );
    // Read with a fresh request: the exact operation allowance is already spent.
    let reader = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    assert_dynamic_payload(&reader, 2, 70);
    assert_dynamic_payload(&reader, 3, 60);
}

fn followup_batch() -> Vec<DynamicMutation> {
    vec![
        DynamicMutation::Update {
            entity: ENTITY_NAME.into(),
            key: InputValue::nat64(2),
            patch: dynamic_payload_patch(90),
        },
        DynamicMutation::Insert {
            entity: ENTITY_NAME.into(),
            patch: dynamic_payload_patch(100),
        },
    ]
}

// Both operations share one request; successful context reuse may skip work,
// but a new operation cannot reset counters already consumed by the first.
fn sequential_decode_usage() -> (u64, u64) {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                populated();
                let root = request_with_limit(Resource::DecodedBytes, u64::MAX);
                let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
                session
                    .execute_trusted_dynamic_mutation_batch(mixed_batch())
                    .unwrap();
                let first = root.observed(Resource::DecodedBytes);
                session
                    .execute_trusted_dynamic_mutation_batch(followup_batch())
                    .unwrap();
                (first, root.observed(Resource::DecodedBytes))
            })
            .join()
            .unwrap()
    })
}

#[test]
fn sequential_decode_refusal_preserves_prior_commits_and_request_usage() {
    let (first, total) = sequential_decode_usage();
    assert!(first > 0 && total > first);
    for (limit, admitted) in [(first, false), (total - 1, false), (total, true)] {
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    populated();
                    let root = request_with_limit(Resource::DecodedBytes, limit);
                    let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
                    session
                        .execute_trusted_dynamic_mutation_batch(mixed_batch())
                        .unwrap();
                    assert_eq!(root.observed(Resource::DecodedBytes), first);
                    let committed = retained_entries();
                    let control = observe_commit_control().unwrap();
                    let result = session.execute_trusted_dynamic_mutation_batch(followup_batch());
                    if admitted {
                        assert!(result.is_ok());
                        assert_eq!(root.observed(Resource::DecodedBytes), total);
                    } else {
                        let error = result.unwrap_err();
                        assert!(root.observed(Resource::DecodedBytes) > limit);
                        for fact in [
                            (
                                DiagnosticFactTag::BudgetResource,
                                Resource::DecodedBytes.raw(),
                            ),
                            (DiagnosticFactTag::Limit, limit),
                            (
                                DiagnosticFactTag::Actual,
                                root.observed(Resource::DecodedBytes),
                            ),
                            (
                                DiagnosticFactTag::ExecutionLane,
                                DiagnosticExecutionLane::Mutation.raw(),
                            ),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact), "{error:?}");
                        }
                        assert_eq!(retained_entries(), committed);
                        assert_eq!(observe_commit_control().unwrap(), control);
                        assert!(
                            crate::db::commit::retained_commit_marker_measurement_for_tests()
                                .unwrap()
                                .is_none()
                        );
                        SCHEMA_STORE.with_borrow(|store| {
                            let cursor = store
                                .identity_statement_cursor(
                                    database_incarnation_id().unwrap(),
                                    ENTITY_TAG,
                                    FieldId::new(1),
                                    &AcceptedFieldKind::Nat64,
                                )
                                .unwrap();
                            assert_eq!(cursor.expected_high_water(), 3);
                            assert!(!cursor.has_allocations());
                        });
                    }
                    let fresh = DbSession::<TestCanister>::new(
                        &STORE_REGISTRY,
                        &crate::db::RequestExecutionRoot::__new_runtime_root(),
                    );
                    if !admitted {
                        fresh
                            .execute_trusted_dynamic_mutation_batch(followup_batch())
                            .unwrap();
                    }
                    assert_dynamic_payload(&fresh, 2, 90);
                    assert_dynamic_payload(&fresh, 3, 60);
                    assert_dynamic_payload(&fresh, 4, 100);
                    assert_eq!(DATA_STORE.with_borrow(DataStore::len), 3);
                    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 3);
                    let next = fresh
                        .execute_trusted_dynamic_insert_batch(
                            ENTITY_NAME,
                            vec![dynamic_payload_patch(80)],
                        )
                        .unwrap();
                    assert_eq!(next.rows, vec![expected_dynamic_row(5, 80)]);
                })
                .join()
                .unwrap();
        });
    }
}
