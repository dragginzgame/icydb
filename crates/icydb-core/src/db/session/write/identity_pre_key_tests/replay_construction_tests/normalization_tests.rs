//! Shared predicate normalization must preserve index effects through recovery.

use super::*;
use crate::db::{
    commit::{CommitPrepareContextCache, CommitPrepareMode, observe_commit_control},
    sql_shared::MAX_SQL_EXPR_DEPTH,
    tests::support::request_with_limit,
};
use ic_storage::adapter::Storable;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope,
    DiagnosticExecutionLane, DiagnosticFactTag,
};

const NORMALIZATION_RESOURCES: [Resource; 7] = [
    Resource::PredicateExpressionSteps,
    Resource::DecodedBytes,
    Resource::NestedValueSteps,
    Resource::TemporaryBytes,
    Resource::SortEntries,
    Resource::SortComparisons,
    Resource::SortTemporaryBytes,
];

fn partial_index_snapshot(sql: &str) -> PersistedSchemaSnapshot {
    let base = identity_snapshot(STORE_PATH, false);
    let index = &base.indexes()[0];
    PersistedSchemaSnapshot::new_with_indexes(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        base.row_layout().clone(),
        base.fields().to_vec(),
        vec![PersistedIndexSnapshot::new(
            index.schema_id(),
            index.ordinal(),
            index.name().to_string(),
            index.store().to_string(),
            index.unique(),
            index.key().clone(),
            Some(crate::db::schema::AcceptedIndexPredicate::bind_test_sql(
                sql,
                base.fields(),
            )),
        )],
    )
}

#[test]
fn commit_normalization_requires_request_capacity_only_for_new_writes() {
    let initialized = initialize_with_snapshot(partial_index_snapshot(
        "payload != 10 AND payload != 20 AND payload != 30",
    ));
    let fingerprint = initialized
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap()
        .fingerprint();
    // Calibrate complete request preparation, including accepted projection,
    // then exercise its one-short boundary and each exhausted resource.
    let root = request_with_limit(Resource::PredicateExpressionSteps, u64::MAX);
    let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
    session
        .db
        .accepted_runtime_entity_for_path(ENTITY_SOURCE)
        .unwrap()
        .prepare_commit_context(&session.db, fingerprint, CommitPrepareMode::NormalWrite)
        .unwrap();
    let steps = root.observed(Resource::PredicateExpressionSteps);
    for (resource, limit) in std::iter::once((Resource::PredicateExpressionSteps, steps - 1))
        .chain(NORMALIZATION_RESOURCES.map(|resource| (resource, 0)))
    {
        let root = request_with_limit(resource, limit);
        let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
        let entity = session
            .db
            .accepted_runtime_entity_for_path(ENTITY_SOURCE)
            .unwrap();
        // Retained preparation must work both before and after ordinary
        // admission exhausts this same request; it must not reset its counters.
        let assert_retained_preparation = || {
            let before = NORMALIZATION_RESOURCES.map(|resource| root.observed(resource));
            for mode in [
                CommitPrepareMode::RecoveryReplay,
                CommitPrepareMode::DerivedRebuild,
            ] {
                entity
                    .prepare_commit_context(&session.db, fingerprint, mode)
                    .unwrap();
                assert_eq!(
                    NORMALIZATION_RESOURCES.map(|resource| root.observed(resource)),
                    before
                );
            }
        };
        assert_retained_preparation();
        let error = entity
            .prepare_commit_context(&session.db, fingerprint, CommitPrepareMode::NormalWrite)
            .err()
            .expect("new writes must admit normalization");
        let actual = root.observed(resource);
        assert!(actual > limit);
        if limit == steps - 1 {
            assert_eq!(actual, steps);
        }
        for fact in [
            (DiagnosticFactTag::BudgetResource, resource.raw()),
            (DiagnosticFactTag::Limit, limit),
            (DiagnosticFactTag::Actual, actual),
        ] {
            assert!(error.diagnostic_facts().contains(&fact), "{error:?}");
        }
        assert_retained_preparation();
    }
    assert_eq!(DATA_STORE.with_borrow(DataStore::len), 0);
    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 0);
    assert_eq!(
        initialized
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .unwrap()
            .fingerprint(),
        fingerprint,
    );
}

#[test]
fn commit_predicate_work_accumulates_across_fresh_contexts_but_not_cache_hits() {
    let allowance = commit_predicate_steps();
    let initialized = initialize_with_snapshot(partial_index_snapshot(
        "payload != 10 AND payload != 20 AND payload != 30",
    ));
    let fingerprint = initialized
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap()
        .fingerprint();
    let root = request_with_limit(Resource::PredicateExpressionSteps, allowance);
    let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
    let entity = session
        .db
        .accepted_runtime_entity_for_path(ENTITY_SOURCE)
        .unwrap();
    let mut contexts = CommitPrepareContextCache::new(CommitPrepareMode::NormalWrite);
    contexts
        .get_or_prepare(ENTITY_SOURCE, fingerprint, |mode| {
            entity.prepare_commit_context(&session.db, fingerprint, mode)
        })
        .unwrap();
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), allowance);
    contexts
        .get_or_prepare(ENTITY_SOURCE, fingerprint, |_| {
            panic!("a successful operation-local context must be reused")
        })
        .unwrap();
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), allowance);
    // A fresh context performs real normalization again against the same root.
    let Err(error) =
        entity.prepare_commit_context(&session.db, fingerprint, CommitPrepareMode::NormalWrite)
    else {
        panic!("a new context cannot reset the request allowance");
    };
    let actual = root.observed(Resource::PredicateExpressionSteps);
    assert!(actual > allowance);
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::Actual, actual))
    );
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::Limit, allowance))
    );
}

#[test]
fn dynamic_insert_refuses_partial_index_work_without_publication_or_identity_gaps() {
    let limit = insert_predicate_steps(false) + 2;
    assert_eq!(
        assert_insert_refusal(Resource::PredicateExpressionSteps, limit),
        limit + 1
    );
}

fn assert_insert_refusal(resource: Resource, limit: u64) -> u64 {
    let initialized = initialize_with_snapshot(partial_index_snapshot(
        "payload != 10 AND payload != 20 AND payload != 30",
    ));
    let fingerprint = initialized
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap()
        .fingerprint();
    let control = observe_commit_control().unwrap();
    let root = request_with_limit(resource, limit);
    let bounded = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
    let error = bounded
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(40)])
        .expect_err("partial-index construction must refuse before publication");
    let actual = root.observed(resource);
    assert!(actual > limit);
    for fact in [
        (DiagnosticFactTag::BudgetResource, resource.raw()),
        (DiagnosticFactTag::Limit, limit),
        (DiagnosticFactTag::Actual, actual),
        (
            DiagnosticFactTag::ExecutionBudgetScope,
            DiagnosticExecutionBudgetScope::Request.raw(),
        ),
        (
            DiagnosticFactTag::ExecutionLane,
            DiagnosticExecutionLane::Mutation.raw(),
        ),
    ] {
        assert!(error.diagnostic_facts().contains(&fact), "{error:?}");
    }
    assert_eq!(observe_commit_control().unwrap(), control);
    assert!(
        crate::db::commit::retained_commit_marker_measurement_for_tests()
            .unwrap()
            .is_none()
    );
    assert_eq!(DATA_STORE.with_borrow(DataStore::len), 0);
    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 0);
    assert_eq!(
        initialized
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .unwrap()
            .fingerprint(),
        fingerprint,
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
        assert_eq!(cursor.expected_high_water(), 0);
        assert!(!cursor.has_allocations());
    });
    let retry = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    let inserted = retry
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(40)])
        .unwrap();
    assert_eq!(
        inserted.rows,
        vec![vec![OutputValue::nat64(1), OutputValue::nat64(40)]]
    );
    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 1);
    actual
}

#[test]
fn dynamic_insert_accepts_the_exact_partial_index_work_allowance() {
    assert_insert_at_exact_limit(
        Resource::PredicateExpressionSteps,
        insert_predicate_steps(true),
    );
}

#[test]
fn dynamic_insert_decode_refusal_is_atomic_and_retries_without_identity_gaps() {
    let bytes = insert_construction_usage(true, Resource::DecodedBytes);
    assert!(bytes > insert_construction_usage(false, Resource::DecodedBytes));
    // Each case needs fresh durable thread-local state, including commit control.
    for limit in [0, bytes - 1] {
        std::thread::scope(|scope| {
            let actual = scope
                .spawn(move || assert_insert_refusal(Resource::DecodedBytes, limit))
                .join()
                .unwrap();
            if limit != 0 {
                assert_eq!(actual, bytes);
            }
        });
    }
    assert_insert_at_exact_limit(Resource::DecodedBytes, bytes);
}

fn assert_insert_at_exact_limit(resource: Resource, limit: u64) {
    initialize_with_snapshot(partial_index_snapshot(
        "payload != 10 AND payload != 20 AND payload != 30",
    ));
    let root = request_with_limit(resource, limit);
    let bounded = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
    let inserted = bounded
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(40)])
        .unwrap();
    assert_eq!(
        inserted.rows,
        vec![vec![OutputValue::nat64(1), OutputValue::nat64(40)]]
    );
    assert_eq!(root.observed(resource), limit);
    assert_eq!(DATA_STORE.with_borrow(DataStore::len), 1);
    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len).unwrap(), 1);
    assert!(
        crate::db::commit::retained_commit_marker_measurement_for_tests()
            .unwrap()
            .is_none()
    );
}

// Measure the complete admitted construction, including canonical sort/key work.
// These counters qualify exact refusal boundaries, not performance. A fresh
// thread gives each reference its own durable fixture state.
fn insert_predicate_steps(filtered: bool) -> u64 {
    insert_construction_usage(filtered, Resource::PredicateExpressionSteps)
}

fn insert_construction_usage(filtered: bool, resource: Resource) -> u64 {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                if filtered {
                    initialize_with_snapshot(partial_index_snapshot(
                        "payload != 10 AND payload != 20 AND payload != 30",
                    ));
                } else {
                    initialize();
                }
                let root = request_with_limit(resource, u64::MAX);
                let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
                let inserted = session
                    .execute_trusted_dynamic_insert_batch(
                        ENTITY_NAME,
                        vec![dynamic_payload_patch(40)],
                    )
                    .unwrap();
                assert_eq!(
                    inserted.rows,
                    vec![vec![OutputValue::nat64(1), OutputValue::nat64(40)]]
                );
                root.observed(resource)
            })
            .join()
            .unwrap()
    })
}

// Count one fresh normal preparation; cache hits must not repeat any of it.
fn commit_predicate_steps() -> u64 {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let initialized = initialize_with_snapshot(partial_index_snapshot(
                    "payload != 10 AND payload != 20 AND payload != 30",
                ));
                let fingerprint = initialized
                    .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                    .unwrap()
                    .fingerprint();
                let root = request_with_limit(Resource::PredicateExpressionSteps, u64::MAX);
                let session = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
                session
                    .db
                    .accepted_runtime_entity_for_path(ENTITY_SOURCE)
                    .unwrap()
                    .prepare_commit_context(
                        &session.db,
                        fingerprint,
                        CommitPrepareMode::NormalWrite,
                    )
                    .unwrap();
                let amount = root.observed(Resource::PredicateExpressionSteps);
                assert!(amount > 3, "canonical encoding must join pair admission");
                amount
            })
            .join()
            .unwrap()
    })
}

#[test]
fn normalized_partial_indexes_replay_the_same_effects_as_uninterrupted_writes() {
    let cases = [
        (vec!["payload >= 10"; MAX_SQL_EXPR_DEPTH].join(" AND "), 3),
        // Frontend equality/membership literals bind to the accepted Nat64
        // field; ordered operands retain explicit numeric widening.
        (
            (10..10 + MAX_SQL_EXPR_DEPTH)
                .map(|value| format!("payload = {value}"))
                .collect::<Vec<_>>()
                .join(" OR "),
            2,
        ),
        ("payload = 10 OR payload >= 200".to_string(), 2),
        (
            format!("{}payload < 10", "NOT ".repeat(MAX_SQL_EXPR_DEPTH - 1)),
            3,
        ),
        ("payload >= 10 AND payload < 10".to_string(), 0),
        ("payload >= 10 AND payload <= 10".to_string(), 1),
        (
            "payload IS NOT NULL AND payload >= 10 AND payload <= 10".to_string(),
            1,
        ),
    ];
    for (sql, expected_count) in cases {
        let expected = replayed_index_entries(&sql, None, Resource::PredicateExpressionSteps);
        assert_eq!(expected.len(), expected_count);
        for interruption in [
            MutationCommitInterruption::MarkerPersisted,
            MutationCommitInterruption::RowPrefixPublished,
            MutationCommitInterruption::RowsPublished,
            MutationCommitInterruption::StateMaterialized,
        ] {
            for resource in NORMALIZATION_RESOURCES {
                assert_eq!(
                    replayed_index_entries(&sql, Some(interruption), resource),
                    expected,
                    "replay must preserve full index effects with {resource:?} exhausted"
                );
            }
        }
    }
}

// Each run needs independent thread-local durable identity/checkpoint state.
fn replayed_index_entries(
    sql: &str,
    interruption: Option<MutationCommitInterruption>,
    resource: Resource,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let session = initialize_with_snapshot(partial_index_snapshot(sql));
                let fingerprint = session
                    .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                    .unwrap()
                    .fingerprint();
                if let Some(interruption) = interruption {
                    interrupt_next_mutation_commit_for_tests(interruption);
                }
                let result = session.execute_trusted_dynamic_insert_batch(
                    ENTITY_NAME,
                    [10, 20, 200]
                        .into_iter()
                        .map(dynamic_payload_patch)
                        .collect(),
                );
                if interruption.is_some() {
                    let error = result.unwrap_err();
                    assert_eq!(error.class(), ErrorClass::InvariantViolation, "{error:?}");
                    // Restart from the persisted schema and marker under a fresh
                    // request that cannot admit any work in the selected resource.
                    let root = request_with_limit(resource, 0);
                    let recovery = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
                    SCHEMA_STORE.with_borrow_mut(|store| *store = SchemaStore::init_heap());
                    forget_recovered_domain_for_tests(&recovery.db).unwrap();
                    let mut complete = false;
                    for _ in 0..8 {
                        complete = recovery.db.drive_startup_recovery_page().unwrap();
                        if complete {
                            break;
                        }
                    }
                    assert!(complete, "admitted partial-index writes must recover");
                    assert!(recovery.db.drive_startup_recovery_page().unwrap());
                    for resource in NORMALIZATION_RESOURCES {
                        assert_eq!(root.observed(resource), 0);
                    }
                    assert!(
                        crate::db::commit::retained_commit_marker_measurement_for_tests()
                            .unwrap()
                            .is_none()
                    );
                } else {
                    assert_eq!(result.unwrap().rows.len(), 3);
                }
                assert_eq!(
                    session
                        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                        .unwrap()
                        .fingerprint(),
                    fingerprint
                );
                for (id, payload) in [(1, 10), (2, 20), (3, 200)] {
                    assert_dynamic_payload(&session, id, payload);
                }
                INDEX_STORE.with_borrow(|store| {
                    let mut entries = Vec::new();
                    store
                        .visit_entries(|key, value| {
                            entries.push((
                                key.to_bytes().expect("fixture encoding").into_owned(),
                                value.to_bytes().expect("fixture encoding").into_owned(),
                            ));
                            Ok::<_, InternalError>(IndexStoreVisit::Continue)
                        })
                        .unwrap();
                    entries
                })
            })
            .join()
            .unwrap()
    })
}

#[test]
fn typed_filtered_index_reads_match_primary_scan_and_guard_admission() {
    use crate::db::{FieldRef, query::explain::ExplainAccessPath};

    let session = initialize_with_snapshot(partial_index_snapshot("payload = 10"));
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            [10, 20, 200]
                .into_iter()
                .map(dynamic_payload_patch)
                .collect(),
        )
        .unwrap();
    let binding = session
        .issue_typed_entity_binding(&TYPED_DESCRIPTOR)
        .unwrap();
    let scan = DynamicQuery::new(ENTITY_NAME).select(["payload"]);
    let plan = session
        .explain_query_for_typed_binding(&binding, &scan)
        .unwrap()
        .unwrap();
    assert!(matches!(plan.access(), ExplainAccessPath::FullScan));
    // The maintained test page quantum is two rows; compare complete results
    // across continuation boundaries rather than assuming one page is exhaustive.
    let collect = |request: &DynamicQuery| {
        let mut rows = Vec::new();
        let mut continuation = None;
        for _ in 0..4 {
            let page = session
                .execute_trusted_live_page(request, continuation.as_deref())
                .unwrap();
            rows.extend(page.rows);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        assert!(continuation.is_none());
        rows
    };
    let rows = collect(&scan);
    assert_eq!(rows.len(), 3);
    for payload in [10_u64, 20, 200] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("payload").eq(payload))
            .select(["payload"]);
        let plan = session
            .explain_query_for_typed_binding(&binding, &query)
            .unwrap()
            .unwrap();
        if payload == 10 {
            assert!(matches!(
                plan.access(),
                ExplainAccessPath::IndexPrefix { .. }
            ));
        } else {
            assert!(matches!(plan.access(), ExplainAccessPath::FullScan));
        }
        let expected = rows
            .iter()
            .filter(|row| row[0] == OutputValue::nat64(payload))
            .cloned()
            .collect::<Vec<_>>();
        for _ in 0..2 {
            assert_eq!(collect(&query), expected);
        }
    }
}

mod mutation_decode_tests;
