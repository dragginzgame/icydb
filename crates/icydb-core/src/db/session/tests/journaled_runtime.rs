use super::*;
use crate::{db::data::StoreVisit, metrics::sink::MutationCommitClass};

#[derive(Debug, Eq, PartialEq)]
struct JournaledSessionStorageBytes {
    data: Vec<(Vec<u8>, Vec<u8>)>,
    indexes: Vec<(Vec<u8>, Vec<u8>)>,
}

fn journaled_session_storage_bytes() -> JournaledSessionStorageBytes {
    let mut data = JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(|store| {
        let mut rows = Vec::new();
        let _: Result<(), std::convert::Infallible> = store.visit_entries(|key, row| {
            rows.push((key.as_bytes().to_vec(), row.as_bytes().to_vec()));
            Ok(StoreVisit::Continue)
        });
        rows
    });
    let mut indexes = JOURNALED_SESSION_SQL_INDEX_STORE.with_borrow(|store| {
        let mut entries = Vec::new();
        let _: Result<(), std::convert::Infallible> = store.visit_entries(|key, value| {
            entries.push((key.as_bytes().to_vec(), value.as_bytes().to_vec()));
            Ok(IndexStoreVisit::Continue)
        });
        entries
    });
    data.sort();
    indexes.sort();

    JournaledSessionStorageBytes { data, indexes }
}

fn journaled_session_schema_snapshot() -> PersistedSchemaSnapshot {
    JOURNALED_SESSION_SQL_SCHEMA_STORE.with_borrow(|store| {
        store
            .current_accepted_persisted_snapshot(JournaledSessionSqlEntity::ENTITY_TAG)
            .expect("journaled temporal schema should remain readable")
            .expect("journaled temporal schema should be published")
    })
}

fn journaled_session_raw_row(id: u64) -> crate::db::data::RawRow {
    let key = DecodedDataStoreKey::try_new::<JournaledSessionSqlEntity>(id)
        .expect("journaled temporal row key should build")
        .to_raw()
        .expect("journaled temporal row key should encode");

    JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(|store| {
        store
            .get(&key)
            .expect("journaled temporal row should exist")
    })
}

fn journaled_session_row_layout_version(id: u64) -> u32 {
    crate::db::codec::decode_row_payload_bytes(journaled_session_raw_row(id).as_bytes())
        .expect("journaled temporal row envelope should decode")
        .layout_version()
        .get()
}

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
    let (output, events) = capture_session_metrics(run);
    let classes = mutation_commit_classes_for_entity(&events, entity_path);

    (output, classes)
}

fn public_projection_rows<E>(session: &DbSession<SessionSqlCanister>, sql: &str) -> Vec<Vec<Value>>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let result = session
        .execute_trusted_sql_query::<E>(sql)
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
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let result = session
        .execute_trusted_sql_query::<E>(sql)
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
                if captured.is_none() {
                    captured = Some(batch.clone());
                }
                Ok(())
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
        .trusted_read_unchecked()
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

    #[cfg(feature = "sql-explain")]
    {
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
}

#[test]
fn journaled_session_writes_append_journal_after_schema_bootstrap_is_canonical() {
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
            3,
            "catalog bootstrap should persist the entity snapshot, immutable bundle, and current root",
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
        .trusted_read_unchecked()
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

fn seed_journaled_temporal_default_eras(session: &DbSession<SessionSqlCanister>) {
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Early".to_string(),
            age: 31,
        })
        .expect("layout-one row should persist before additive DDL");
    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "ALTER TABLE JournaledSessionSqlEntity ADD COLUMN score nat64 NOT NULL DEFAULT 7",
                1,
            ),
        )
        .expect("score addition should freeze layout-one historical fill");
    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "ALTER TABLE JournaledSessionSqlEntity ALTER COLUMN score SET DEFAULT 8",
                2,
            ),
        )
        .expect("score default change should affect only future writes");
    session
        .insert(JournaledSessionSqlEntity {
            id: 2,
            name: "Middle".to_string(),
            age: 32,
        })
        .expect("layout-two row should use the changed score default");
    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "ALTER TABLE JournaledSessionSqlEntity ADD COLUMN nickname text DEFAULT 'historical'",
                3,
            ),
        )
        .expect("nickname addition should freeze prior-row historical fill");
    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "ALTER TABLE JournaledSessionSqlEntity ALTER COLUMN score SET DEFAULT 9",
                4,
            ),
        )
        .expect("second score default change should stay future-only");
    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "ALTER TABLE JournaledSessionSqlEntity ALTER COLUMN nickname SET DEFAULT 'current'",
                5,
            ),
        )
        .expect("nickname default change should stay future-only");
    session
        .insert(JournaledSessionSqlEntity {
            id: 3,
            name: "Current".to_string(),
            age: 33,
        })
        .expect("layout-three row should use both current defaults");
}

fn expected_promoted_journaled_temporal_rows() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Nat64(1),
            Value::Nat64(41),
            Value::Nat64(7),
            Value::Text("historical".to_string()),
        ],
        vec![
            Value::Nat64(2),
            Value::Nat64(42),
            Value::Nat64(8),
            Value::Text("historical".to_string()),
        ],
        vec![
            Value::Nat64(3),
            Value::Nat64(43),
            Value::Nat64(9),
            Value::Text("current".to_string()),
        ],
    ]
}

#[test]
fn journaled_temporal_defaults_remain_frozen_through_three_layouts_and_recovery() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    seed_journaled_temporal_default_eras(&session);

    assert_eq!(journaled_session_row_layout_version(1), 1);
    assert_eq!(journaled_session_row_layout_version(2), 2);
    assert_eq!(journaled_session_row_layout_version(3), 3);
    assert_eq!(
        public_projection_rows::<JournaledSessionSqlEntity>(
            &session,
            "SELECT id, score, nickname FROM JournaledSessionSqlEntity ORDER BY id ASC",
        ),
        vec![
            vec![
                Value::Nat64(1),
                Value::Nat64(7),
                Value::Text("historical".to_string()),
            ],
            vec![
                Value::Nat64(2),
                Value::Nat64(8),
                Value::Text("historical".to_string()),
            ],
            vec![
                Value::Nat64(3),
                Value::Nat64(9),
                Value::Text("current".to_string()),
            ],
        ],
    );

    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "ALTER TABLE JournaledSessionSqlEntity ALTER COLUMN score DROP DEFAULT",
                6,
            ),
        )
        .expect("dropping score default should affect only future omission");
    let omission_error = session
        .insert(JournaledSessionSqlEntity {
            id: 4,
            name: "Rejected".to_string(),
            age: 34,
        })
        .expect_err("future omission should reject after required score loses its default");
    assert_eq!(
        omission_error.diagnostic().detail(),
        Some(&DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationRequiredFieldMissing,
        }),
    );

    for (id, age) in [(1, 41), (2, 42), (3, 43)] {
        execute_exact_sql_update_for_tests::<JournaledSessionSqlEntity>(
            &session,
            &format!("UPDATE JournaledSessionSqlEntity SET age = {age} WHERE id = {id}"),
        )
        .unwrap_or_else(|error| {
            panic!(
                "unrelated updates should preserve frozen values and promote each row era: {:?}",
                error.diagnostic(),
            )
        });
        assert_eq!(journaled_session_row_layout_version(id), 3);
    }

    let expected_rows = expected_promoted_journaled_temporal_rows();
    let before_rows = public_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT id, age, score, nickname FROM JournaledSessionSqlEntity ORDER BY id ASC",
    );
    let before_storage = journaled_session_storage_bytes();
    let before_schema = journaled_session_schema_snapshot();
    assert_eq!(before_rows, expected_rows);

    reinitialize_journaled_session_sql_store();
    let recovered_session = journaled_sql_session();

    assert_eq!(
        public_projection_rows::<JournaledSessionSqlEntity>(
            &recovered_session,
            "SELECT id, age, score, nickname FROM JournaledSessionSqlEntity ORDER BY id ASC",
        ),
        expected_rows,
    );
    assert_eq!(journaled_session_storage_bytes(), before_storage);
    assert_eq!(journaled_session_schema_snapshot(), before_schema);
    for id in 1..=3 {
        assert_eq!(journaled_session_row_layout_version(id), 3);
    }
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
    JOURNALED_SESSION_SQL_SCHEMA_STORE.with_borrow_mut(|store| {
        *store =
            SchemaStore::init_journaled(JOURNALED_SESSION_SQL_SCHEMA_MEMORY.with(Clone::clone));
    });
    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);

    let marker = crate::db::commit::CommitMarker::from_parts(batch.commit_marker_id(), vec![batch])
        .expect("marker-bound journal recovery fixture should build");
    crate::db::commit::begin_commit(marker)
        .expect("marker-bound journal recovery fixture should persist marker");
    ensure_recovered(&JOURNALED_SESSION_SQL_DB)
        .expect("journaled recovery should repair marker-bound journal publication");

    let recovered_session = journaled_sql_session();
    let loaded = recovered_session
        .load::<JournaledSessionSqlEntity>()
        .trusted_read_unchecked()
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
fn journaled_session_recovery_reuses_matching_marker_bound_journal_tail_batch() {
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
    JOURNALED_SESSION_SQL_SCHEMA_STORE.with_borrow_mut(|store| {
        *store =
            SchemaStore::init_journaled(JOURNALED_SESSION_SQL_SCHEMA_MEMORY.with(Clone::clone));
    });

    let marker = crate::db::commit::CommitMarker::from_parts(batch.commit_marker_id(), vec![batch])
        .expect("marker-bound journal recovery fixture should build");
    crate::db::commit::begin_commit(marker)
        .expect("marker-bound journal recovery fixture should persist marker");
    ensure_recovered(&JOURNALED_SESSION_SQL_DB)
        .expect("journaled recovery should treat an existing matching journal batch as idempotent");

    let recovered_session = journaled_sql_session();
    let loaded = recovered_session
        .load::<JournaledSessionSqlEntity>()
        .trusted_read_unchecked()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("journaled recovery should replay the idempotent journal batch once")
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
            "recovery should fold the already-persisted matching batch exactly once",
        );
        assert_eq!(watermark.highest_folded_journal_sequence().get(), 1);
    });
    JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(|store| {
        assert_eq!(
            store.canonical_len_for_tests(),
            1,
            "idempotent marker-bound recovery should not duplicate canonical rows",
        );
    });
    JOURNALED_SESSION_SQL_INDEX_STORE.with_borrow(|store| {
        assert_eq!(
            store.canonical_len_for_tests(),
            1,
            "idempotent marker-bound recovery should not duplicate derived index rows",
        );
    });
}

#[test]
fn journaled_session_recovery_rejects_mismatched_marker_bound_journal_tail_batch() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Atlas".to_string(),
            age: 20,
        })
        .expect("journaled typed insert should succeed while live");

    let existing = first_journaled_session_batch();
    let conflicting = JournalBatch::new(
        [0xE1; 16],
        [0xE2; 16],
        existing.journal_sequence(),
        existing.records().to_vec(),
    )
    .expect("conflicting same-sequence journal batch should build");
    let marker = crate::db::commit::CommitMarker::from_parts(
        conflicting.commit_marker_id(),
        vec![conflicting],
    )
    .expect("conflicting marker-bound journal fixture should build");
    crate::db::commit::begin_commit(marker)
        .expect("conflicting marker-bound journal fixture should persist marker");

    let err = ensure_recovered(&JOURNALED_SESSION_SQL_DB)
        .expect_err("recovery should reject marker payload that conflicts with journal tail bytes");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        crate::db::commit::commit_marker_present().expect("commit marker check should succeed"),
        "failed journal publication must keep the marker persisted for retry",
    );
    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|store| {
        let watermark = store
            .fold_watermark()
            .expect("journal fold watermark should be readable");
        assert_eq!(
            watermark.highest_folded_journal_sequence().get(),
            0,
            "conflicting marker-bound batch must fail before fold watermark advances",
        );
        assert_eq!(
            store.len(),
            1,
            "conflicting marker-bound recovery must not mutate the existing journal tail",
        );
    });

    crate::db::commit::clear_commit_marker_for_tests()
        .expect("failed marker cleanup should succeed");
}

#[test]
fn durable_source_relation_to_journaled_target_uses_durable_capabilities() {
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
        DurableSessionSqlSourceToJournaledTargetEntity::PATH,
        || {
            session.insert(DurableSessionSqlSourceToJournaledTargetEntity {
                id: 10,
                target_id: 1,
            })
        },
    );
    result.expect("durable source relation to journaled target should validate as durable");
    assert_eq!(
        classes,
        vec![MutationCommitClass::DurableOnly],
        "journaled durable targets must not make durable-source relation writes live-only or mixed",
    );

    let persisted = session
        .load::<DurableSessionSqlSourceToJournaledTargetEntity>()
        .trusted_read_unchecked()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("durable-source journaled-target relation load should succeed")
        .entities();
    assert_eq!(
        persisted,
        vec![DurableSessionSqlSourceToJournaledTargetEntity {
            id: 10,
            target_id: 1,
        }],
    );
}
