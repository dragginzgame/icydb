use super::*;
use crate::metrics::sink::MutationCommitClass;

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

fn seed_journaled_session_entities(session: &DbSession<SessionSqlCanister>) {
    for (id, name, age) in [(1, "Atlas", 20), (2, "Beryl", 30), (3, "Cato", 40)] {
        session
            .insert(JournaledSessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("journaled typed insert should succeed while live");
    }
}

fn first_journaled_session_batch() -> JournalBatch {
    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|store| {
        let mut captured = None;
        store
            .visit_batches_after(JournalSequence::new(0), |batch| {
                captured = Some(batch.clone());
                Ok(JournalTailVisit::Stop)
            })
            .expect("journal tail should be readable");

        captured.expect("journal tail should contain at least one committed batch")
    })
}

#[test]
fn journaled_session_write_read_and_index_query_round_trip_while_live() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    seed_journaled_session_entities(&session);

    let loaded = session
        .load::<JournaledSessionSqlEntity>()
        .order_term(crate::db::asc("id"))
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("journaled fluent load should read live rows")
        .entities();
    assert_eq!(
        loaded
            .iter()
            .map(|entity| (entity.id, entity.name.as_str(), entity.age))
            .collect::<Vec<_>>(),
        vec![(1, "Atlas", 20), (2, "Beryl", 30), (3, "Cato", 40)],
    );

    let rows = public_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT name, age FROM JournaledSessionSqlEntity \
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

    let explain = public_explain_text::<JournaledSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT name \
         FROM JournaledSessionSqlEntity \
         WHERE name >= 'B' AND name < 'D' \
         ORDER BY name ASC",
    );
    assert!(
        explain.contains("access_strategy=IndexRange(name)"),
        "journaled indexed query should keep the secondary-index route: {explain}",
    );
    assert!(
        !explain.contains("access=FullScan"),
        "journaled indexed query should not collapse to a full scan: {explain}",
    );
}

#[test]
fn journaled_session_writes_append_journal_and_leave_canonical_btrees_untouched() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    seed_journaled_session_entities(&session);

    JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 3);
        assert_eq!(
            store.canonical_len_for_tests(),
            0,
            "normal journaled writes must not fold into canonical data BTree",
        );
    });
    JOURNALED_SESSION_SQL_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 3);
        assert_eq!(
            store.canonical_len_for_tests(),
            0,
            "normal journaled writes must not fold into canonical index BTree",
        );
    });
    JOURNALED_SESSION_SQL_SCHEMA_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 1);
        assert_eq!(
            store.canonical_len_for_tests(),
            0,
            "live schema reconciliation must not fold into canonical schema BTree",
        );
    });
    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|store| {
        assert_eq!(
            store.len(),
            3,
            "each committed row mutation should append one marker-bound journal batch",
        );
    });
}

#[test]
fn journaled_session_recovery_folds_committed_tail_into_canonical_btrees() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    seed_journaled_session_entities(&session);

    reinitialize_journaled_session_sql_store();
    let recovered_session = journaled_sql_session();

    let loaded = recovered_session
        .load::<JournaledSessionSqlEntity>()
        .order_term(crate::db::asc("id"))
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("journaled recovery should restore live rows from the tail")
        .entities();
    assert_eq!(
        loaded
            .iter()
            .map(|entity| (entity.id, entity.name.as_str(), entity.age))
            .collect::<Vec<_>>(),
        vec![(1, "Atlas", 20), (2, "Beryl", 30), (3, "Cato", 40)],
    );

    let rows = public_projection_rows::<JournaledSessionSqlEntity>(
        &recovered_session,
        "SELECT name, age FROM JournaledSessionSqlEntity \
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

    JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 3);
        assert_eq!(
            store.canonical_len_for_tests(),
            3,
            "recovery fold must apply committed row batches to canonical data",
        );
    });
    JOURNALED_SESSION_SQL_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 3);
        assert_eq!(
            store.canonical_len_for_tests(),
            3,
            "recovery fold must materialize derived indexes into canonical index",
        );
    });
    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|store| {
        let watermark = store
            .fold_watermark()
            .expect("journal fold watermark should be readable");
        assert_eq!(store.len(), 0);
        assert_eq!(watermark.highest_folded_journal_sequence().get(), 3);
        assert_eq!(watermark.fold_epoch(), 1);
    });
}

#[test]
fn journaled_session_recovery_repairs_missing_marker_bound_journal_tail_batch() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Atlas".to_string(),
            age: 20,
        })
        .expect("journaled typed insert should succeed while live");

    let batch = first_journaled_session_batch();
    JOURNALED_SESSION_SQL_DATA_STORE
        .with_borrow_mut(|store| *store = DataStore::init_journaled(test_memory(180)));
    JOURNALED_SESSION_SQL_INDEX_STORE
        .with_borrow_mut(|store| *store = IndexStore::init_journaled(test_memory(181)));
    JOURNALED_SESSION_SQL_SCHEMA_STORE
        .with_borrow_mut(|store| *store = SchemaStore::init_journaled(test_memory(182)));
    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);

    let marker = crate::db::commit::CommitMarker::from_parts(
        batch.commit_marker_id(),
        Vec::new(),
        vec![batch],
    )
    .expect("marker-bound journal recovery fixture should build");
    crate::db::commit::begin_commit(marker)
        .expect("marker-bound journal recovery fixture should persist marker");
    ensure_recovered(&JOURNALED_SESSION_SQL_DB)
        .expect("journaled recovery should repair marker-bound journal publication");

    let recovered_session = journaled_sql_session();
    let loaded = recovered_session
        .load::<JournaledSessionSqlEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("journaled recovery should replay repaired journal batch")
        .entities();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].name, "Atlas");
    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|store| {
        let watermark = store
            .fold_watermark()
            .expect("journal fold watermark should be readable");
        assert_eq!(
            store.len(),
            0,
            "recovery should publish then fold the embedded marker-bound batch",
        );
        assert_eq!(watermark.highest_folded_journal_sequence().get(), 1);
    });
    JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(|store| {
        assert_eq!(
            store.canonical_len_for_tests(),
            1,
            "repaired marker-bound batch should fold into canonical data",
        );
    });
}

#[test]
fn stable_source_strong_relation_to_journaled_target_uses_durable_capabilities() {
    reset_mixed_journaled_relation_stores();
    let session = mixed_journaled_relation_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Atlas".to_string(),
            age: 20,
        })
        .expect("journaled relation target should seed while live");

    let (result, classes) = capture_mutation_commit_classes(
        StableSessionSqlSourceToJournaledTargetEntity::PATH,
        || {
            session.insert(StableSessionSqlSourceToJournaledTargetEntity {
                id: 10,
                target_id: 1,
            })
        },
    );
    result.expect("stable source strong relation to journaled target should validate as durable");
    assert_eq!(
        classes,
        vec![MutationCommitClass::DurableOnly],
        "journaled durable targets must not make stable-source relation writes live-only or mixed",
    );

    let persisted = session
        .load::<StableSessionSqlSourceToJournaledTargetEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("stable-source journaled-target relation load should succeed")
        .entities();
    assert_eq!(
        persisted,
        vec![StableSessionSqlSourceToJournaledTargetEntity {
            id: 10,
            target_id: 1,
        }],
    );
}
