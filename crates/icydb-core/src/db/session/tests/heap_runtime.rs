use super::*;
use crate::{db::StoreSnapshotStorageMode, metrics::sink::MutationCommitClass};

fn mutation_commit_classes_for_entity(
    events: &[MetricsEvent],
    entity_path: &'static str,
) -> Vec<MutationCommitClass> {
    events
        .iter()
        .filter_map(|event| match event {
            MetricsEvent::MutationCommitPlan {
                entity_path: path,
                class,
            } if *path == entity_path => Some(*class),
            _ => None,
        })
        .collect()
}

fn capture_mutation_commit_classes<R>(
    entity_path: &'static str,
    run: impl FnOnce() -> R,
) -> (R, Vec<MutationCommitClass>) {
    let sink = SessionMetricsCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let classes = mutation_commit_classes_for_entity(&sink.into_events(), entity_path);

    (output, classes)
}

fn public_projection_rows<E>(session: &DbSession<SessionSqlCanister>, sql: &str) -> Vec<Vec<Value>>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let result = session
        .execute_sql_query::<E>(sql)
        .unwrap_or_else(|err| panic!("public SQL query should succeed: {sql}: {err}"));

    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("public SQL query should emit projection rows: {sql}");
    };

    rows.into_iter()
        .map(|row| row.into_iter().map(runtime_output).collect())
        .collect()
}

fn public_explain_text<E>(session: &DbSession<SessionSqlCanister>, sql: &str) -> String
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let result = session
        .execute_sql_query::<E>(sql)
        .unwrap_or_else(|err| panic!("public EXPLAIN query should succeed: {sql}: {err}"));

    let SqlStatementResult::Explain(explain) = result else {
        panic!("public EXPLAIN query should emit explain text: {sql}");
    };

    explain
}

fn seed_heap_session_entities(session: &DbSession<SessionSqlCanister>) {
    for (id, name, age) in [(1, "Atlas", 20), (2, "Beryl", 30), (3, "Cato", 40)] {
        session
            .insert(HeapSessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("heap typed insert should succeed while live");
    }
}

fn heap_snapshot_counts(
    session: &DbSession<SessionSqlCanister>,
) -> (u64, u64, u64, Option<u32>, Option<String>) {
    let report = session
        .storage_report_default()
        .expect("heap storage report should build");
    let data = report
        .storage_data()
        .iter()
        .find(|snapshot| snapshot.path() == HeapSessionSqlStore::PATH)
        .expect("heap data snapshot should be present");
    let index = report
        .storage_index()
        .iter()
        .find(|snapshot| snapshot.path() == HeapSessionSqlStore::PATH)
        .expect("heap index snapshot should be present");
    let schema = report
        .schema_storage()
        .iter()
        .find(|snapshot| snapshot.path() == HeapSessionSqlStore::PATH)
        .expect("heap schema snapshot should be present");

    assert_eq!(data.storage(), StoreSnapshotStorageMode::Heap);
    assert_eq!(data.memory_id(), None);
    assert_eq!(index.storage(), StoreSnapshotStorageMode::Heap);
    assert_eq!(index.memory_id(), None);
    assert_eq!(schema.storage(), StoreSnapshotStorageMode::Heap);
    assert_eq!(schema.memory_id(), None);

    (
        data.entries(),
        index.entries(),
        schema.entity_count(),
        schema.schema_version(),
        schema.schema_fingerprint().map(ToOwned::to_owned),
    )
}

#[test]
fn heap_backed_session_write_read_and_index_query_round_trip_while_live() {
    reset_heap_session_sql_store();
    let session = heap_sql_session();
    seed_heap_session_entities(&session);

    let loaded = session
        .load::<HeapSessionSqlEntity>()
        .order_term(crate::db::asc("id"))
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("heap fluent load should read live rows")
        .entities();
    assert_eq!(
        loaded
            .iter()
            .map(|entity| (entity.id, entity.name.as_str(), entity.age))
            .collect::<Vec<_>>(),
        vec![(1, "Atlas", 20), (2, "Beryl", 30), (3, "Cato", 40)],
    );

    let rows = public_projection_rows::<HeapSessionSqlEntity>(
        &session,
        "SELECT name, age FROM HeapSessionSqlEntity \
         WHERE name >= 'B' AND name < 'D' \
         ORDER BY name ASC",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Beryl".to_string()), Value::Nat64(30)],
            vec![Value::Text("Cato".to_string()), Value::Nat64(40)],
        ],
    );

    let explain = public_explain_text::<HeapSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT name \
         FROM HeapSessionSqlEntity \
         WHERE name >= 'B' AND name < 'D' \
         ORDER BY name ASC",
    );
    assert!(
        explain.contains("access_strategy=IndexRange(name)"),
        "heap indexed query should keep the secondary-index route: {explain}",
    );
    assert!(
        !explain.contains("access=FullScan"),
        "heap indexed query should not collapse to a full scan: {explain}",
    );

    let (data_entries, index_entries, schema_entities, schema_version, schema_fingerprint) =
        heap_snapshot_counts(&session);
    assert_eq!(data_entries, 3);
    assert_eq!(index_entries, 3);
    assert_eq!(schema_entities, 1);
    assert!(schema_version.is_some());
    assert!(schema_fingerprint.is_some());
}

#[test]
fn heap_backed_session_reinit_loses_rows_and_indexes_but_reconciles_live_schema() {
    reset_heap_session_sql_store();
    let session = heap_sql_session();
    seed_heap_session_entities(&session);
    assert_eq!(heap_snapshot_counts(&session).0, 3);

    reinitialize_heap_session_sql_store();
    let session = heap_sql_session();

    let loaded = session
        .load::<HeapSessionSqlEntity>()
        .order_term(crate::db::asc("id"))
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("heap fluent load should read after reinit")
        .entities();
    assert_eq!(
        loaded,
        Vec::<HeapSessionSqlEntity>::new(),
        "heap rows must not be recovered from stable commit state after store reinit",
    );

    let rows = public_projection_rows::<HeapSessionSqlEntity>(
        &session,
        "SELECT name, age FROM HeapSessionSqlEntity \
         WHERE name >= 'A' \
         ORDER BY name ASC",
    );
    assert_eq!(
        rows,
        Vec::<Vec<Value>>::new(),
        "heap SQL query should observe an empty live store after reinit",
    );

    let (data_entries, index_entries, schema_entities, schema_version, schema_fingerprint) =
        heap_snapshot_counts(&session);
    assert_eq!(data_entries, 0);
    assert_eq!(index_entries, 0);
    assert_eq!(
        schema_entities, 1,
        "heap schema metadata is rebuilt for live validation/diagnostics, not recovered as rows",
    );
    assert!(schema_version.is_some());
    assert!(schema_fingerprint.is_some());
}

#[test]
fn stable_source_strong_relation_to_heap_target_rejects_at_runtime_boundary() {
    reset_mixed_heap_relation_stores();
    let session = mixed_heap_relation_sql_session();
    seed_heap_session_entities(&session);

    let err = session
        .insert(StableSessionSqlSourceToHeapTargetEntity {
            id: 10,
            target_id: 1,
        })
        .expect_err("stable source strong relation to heap target should fail closed");
    assert_eq!(err.class(), ErrorClass::Unsupported);

    let persisted = session
        .load::<StableSessionSqlSourceToHeapTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("post-rejection stable-source load should succeed")
        .entities();
    assert_eq!(
        persisted,
        Vec::<StableSessionSqlSourceToHeapTargetEntity>::new(),
        "stable-source relation rejection must not persist the row",
    );
}

#[test]
fn stable_source_weak_relation_to_heap_target_remains_non_enforcing() {
    reset_mixed_heap_relation_stores();
    let session = mixed_heap_relation_sql_session();

    session
        .insert(StableSessionSqlWeakSourceToHeapTargetEntity {
            id: 11,
            target_id: 9_999,
        })
        .expect("weak stable-source relation to heap target should not take strong policy");

    let persisted = session
        .load::<StableSessionSqlWeakSourceToHeapTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("weak stable-source relation load should succeed")
        .entities();
    assert_eq!(
        persisted,
        vec![StableSessionSqlWeakSourceToHeapTargetEntity {
            id: 11,
            target_id: 9_999,
        }],
        "non-strong relation behavior should stay independent of target durability",
    );
}

#[test]
fn heap_source_strong_relation_to_heap_target_keeps_live_validation_semantics() {
    reset_heap_session_sql_store();
    let session = heap_sql_session();
    seed_heap_session_entities(&session);

    session
        .insert(HeapSessionSqlSourceToHeapTargetEntity {
            id: 20,
            target_id: 1,
        })
        .expect("heap source relation to heap target should validate while live");

    let persisted = session
        .load::<HeapSessionSqlSourceToHeapTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("heap-source relation load should succeed")
        .entities();
    assert_eq!(
        persisted,
        vec![HeapSessionSqlSourceToHeapTargetEntity {
            id: 20,
            target_id: 1,
        }],
    );
}

#[test]
fn public_writes_emit_durable_live_and_mixed_commit_classifications() {
    reset_session_sql_store();
    let session = sql_session();
    let (stable_result, stable_classes) =
        capture_mutation_commit_classes(SessionSqlWriteEntity::PATH, || {
            session.insert(SessionSqlWriteEntity {
                id: 170_200,
                name: "stable".to_string(),
                age: 41,
            })
        });
    stable_result.expect("stable public insert should succeed");
    assert_eq!(stable_classes, vec![MutationCommitClass::DurableOnly]);

    reset_heap_session_sql_store();
    let session = heap_sql_session();
    let (heap_result, heap_classes) =
        capture_mutation_commit_classes(HeapSessionSqlEntity::PATH, || {
            session.insert(HeapSessionSqlEntity {
                id: 170_201,
                name: "heap".to_string(),
                age: 42,
            })
        });
    heap_result.expect("heap public insert should succeed");
    assert_eq!(heap_classes, vec![MutationCommitClass::LiveOnly]);

    reset_mixed_heap_relation_stores();
    let session = mixed_heap_relation_sql_session();
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 170_202,
            parent: None,
        })
        .expect("stable relation target should seed");
    let (mixed_result, mixed_classes) =
        capture_mutation_commit_classes(HeapSessionSqlSourceToStableTargetEntity::PATH, || {
            session.insert(HeapSessionSqlSourceToStableTargetEntity {
                id: 170_203,
                target_id: 170_202,
            })
        });
    mixed_result.expect("heap source to stable target should validate while live");
    assert_eq!(
        mixed_classes,
        vec![MutationCommitClass::MixedDurableAndLive]
    );
}

#[test]
fn failed_heap_source_to_stable_target_write_leaves_no_heap_side_effects() {
    reset_mixed_heap_relation_stores();
    let session = mixed_heap_relation_sql_session();

    let (result, classes) =
        capture_mutation_commit_classes(HeapSessionSqlSourceToStableTargetEntity::PATH, || {
            session.insert(HeapSessionSqlSourceToStableTargetEntity {
                id: 170_204,
                target_id: 170_205,
            })
        });
    result.expect_err("missing stable target should reject before heap write apply");
    assert_eq!(
        classes,
        Vec::<MutationCommitClass>::new(),
        "failed preflight must not advertise a commit classification"
    );

    let persisted = session
        .load::<HeapSessionSqlSourceToStableTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("heap source load after failed write should succeed")
        .entities();
    assert_eq!(
        persisted,
        Vec::<HeapSessionSqlSourceToStableTargetEntity>::new(),
        "failed mixed write must not leave heap source side effects"
    );
}
