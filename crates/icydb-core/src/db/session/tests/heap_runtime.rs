use super::*;
use crate::db::StoreSnapshotStorageMode;

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
    assert_eq!(data.storage(), StoreSnapshotStorageMode::Heap);
    assert_eq!(data.memory_id(), None);
    assert_eq!(data.entries(), 3);
    assert_eq!(index.storage(), StoreSnapshotStorageMode::Heap);
    assert_eq!(index.memory_id(), None);
    assert_eq!(index.entries(), 3);
}
