use super::*;
use crate::{
    db::{
        StoreSnapshotStorageMode,
        commit::{CommitMarker, begin_commit},
    },
    metrics::sink::MutationCommitClass,
};

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

#[cfg(feature = "sql-explain")]
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

fn mixed_relation_durable_index_entries() -> u64 {
    MIXED_HEAP_RELATION_DB
        .with_store_registry(|registry| {
            registry
                .try_get_store(SessionSqlStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("mixed relation durable store should be registered")
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

    #[cfg(feature = "sql-explain")]
    {
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
    }

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
fn durable_source_strong_relation_to_heap_target_rejects_at_runtime_boundary() {
    reset_mixed_heap_relation_stores();
    let session = mixed_heap_relation_sql_session();
    seed_heap_session_entities(&session);

    let (result, classes) =
        capture_mutation_commit_classes(DurableSessionSqlSourceToHeapTargetEntity::PATH, || {
            session.insert(DurableSessionSqlSourceToHeapTargetEntity {
                id: 10,
                target_id: 1,
            })
        });
    let err = result.expect_err("durable source strong relation to heap target should fail closed");
    assert_eq!(err.class(), ErrorClass::Unsupported);
    assert_eq!(
        classes,
        Vec::<MutationCommitClass>::new(),
        "failed relation policy must not emit a commit classification",
    );

    let persisted = session
        .load::<DurableSessionSqlSourceToHeapTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("post-rejection durable-source load should succeed")
        .entities();
    assert_eq!(
        persisted,
        Vec::<DurableSessionSqlSourceToHeapTargetEntity>::new(),
        "durable-source relation rejection must not persist the row",
    );
}

#[test]
fn durable_source_weak_relation_to_heap_target_remains_non_enforcing() {
    reset_mixed_heap_relation_stores();
    let session = mixed_heap_relation_sql_session();

    let (result, classes) = capture_mutation_commit_classes(
        DurableSessionSqlWeakSourceToHeapTargetEntity::PATH,
        || {
            session.insert(DurableSessionSqlWeakSourceToHeapTargetEntity {
                id: 11,
                target_id: 9_999,
            })
        },
    );
    result.expect("weak durable-source relation to heap target should not take strong policy");
    assert_eq!(
        classes,
        vec![MutationCommitClass::DurableOnly],
        "a non-enforcing heap target reference must not make the durable source write live-only or mixed",
    );

    let persisted = session
        .load::<DurableSessionSqlWeakSourceToHeapTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("weak durable-source relation load should succeed")
        .entities();
    assert_eq!(
        persisted,
        vec![DurableSessionSqlWeakSourceToHeapTargetEntity {
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

    let (result, classes) =
        capture_mutation_commit_classes(HeapSessionSqlSourceToHeapTargetEntity::PATH, || {
            session.insert(HeapSessionSqlSourceToHeapTargetEntity {
                id: 20,
                target_id: 1,
            })
        });
    result.expect("heap source relation to heap target should validate while live");
    assert_eq!(
        classes,
        vec![MutationCommitClass::LiveOnly],
        "heap source plus heap relation maintenance should stay live-only",
    );

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
    let (durable_result, durable_classes) =
        capture_mutation_commit_classes(SessionSqlWriteEntity::PATH, || {
            session.insert(SessionSqlWriteEntity {
                id: 170_200,
                name: "durable".to_string(),
                age: 41,
            })
        });
    durable_result.expect("durable public insert should succeed");
    assert_eq!(durable_classes, vec![MutationCommitClass::DurableOnly]);

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
        .expect("durable relation target should seed");
    let (mixed_result, mixed_classes) =
        capture_mutation_commit_classes(HeapSessionSqlSourceToDurableTargetEntity::PATH, || {
            session.insert(HeapSessionSqlSourceToDurableTargetEntity {
                id: 170_203,
                target_id: 170_202,
            })
        });
    mixed_result.expect("heap source to durable target should validate while live");
    assert_eq!(
        mixed_classes,
        vec![MutationCommitClass::MixedDurableAndLive]
    );
}

#[test]
fn failed_heap_source_to_durable_target_write_leaves_no_heap_side_effects() {
    reset_mixed_heap_relation_stores();
    let session = mixed_heap_relation_sql_session();

    let (result, classes) =
        capture_mutation_commit_classes(HeapSessionSqlSourceToDurableTargetEntity::PATH, || {
            session.insert(HeapSessionSqlSourceToDurableTargetEntity {
                id: 170_204,
                target_id: 170_205,
            })
        });
    result.expect_err("missing durable target should reject before heap write apply");
    assert_eq!(
        classes,
        Vec::<MutationCommitClass>::new(),
        "failed preflight must not advertise a commit classification"
    );

    let persisted = session
        .load::<HeapSessionSqlSourceToDurableTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("heap source load after failed write should succeed")
        .entities();
    assert_eq!(
        persisted,
        Vec::<HeapSessionSqlSourceToDurableTargetEntity>::new(),
        "failed mixed write must not leave heap source side effects"
    );
}

#[test]
fn mixed_heap_source_reinit_recovery_purges_durable_reverse_index_state() {
    reset_mixed_heap_relation_stores();
    let session = mixed_heap_relation_sql_session();
    let target_id = 171_300;
    let source_id = 171_301;

    session
        .insert(SessionSqlSelfRelationEntity {
            id: target_id,
            parent: None,
        })
        .expect("durable target seed should succeed");
    let durable_index_baseline = mixed_relation_durable_index_entries();
    let (result, classes) =
        capture_mutation_commit_classes(HeapSessionSqlSourceToDurableTargetEntity::PATH, || {
            session.insert(HeapSessionSqlSourceToDurableTargetEntity {
                id: source_id,
                target_id,
            })
        });
    result.expect("heap source to durable target should validate while live");
    assert_eq!(
        classes,
        vec![MutationCommitClass::MixedDurableAndLive],
        "successful heap-source to durable-target write should remain classified as mixed",
    );
    assert!(
        mixed_relation_durable_index_entries() > durable_index_baseline,
        "live mixed write should maintain durable reverse-index state while the heap source exists",
    );

    reinitialize_heap_session_sql_store();
    let marker = CommitMarker::new(Vec::new()).expect("empty recovery marker should build");
    begin_commit(marker).expect("empty recovery marker should force startup rebuild");
    ensure_recovered(&MIXED_HEAP_RELATION_DB)
        .expect("mixed recovery should purge volatile reverse-index state");
    let session = mixed_heap_relation_sql_session();

    let heap_sources = session
        .load::<HeapSessionSqlSourceToDurableTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("heap source load after reinit should succeed")
        .entities();
    assert_eq!(
        heap_sources,
        Vec::<HeapSessionSqlSourceToDurableTargetEntity>::new(),
        "heap source rows must not be recovered after heap store reinit",
    );
    let delete_sql = format!("DELETE FROM SessionSqlSelfRelationEntity WHERE id = {target_id}");
    let delete = session
        .execute_sql_update::<SessionSqlSelfRelationEntity>(delete_sql.as_str())
        .expect("durable target should be deletable after volatile relation state is purged");
    let SqlStatementResult::Count { row_count } = delete else {
        panic!("DELETE without RETURNING should emit a count payload");
    };
    assert_eq!(row_count, 1);
}
