use super::*;

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
