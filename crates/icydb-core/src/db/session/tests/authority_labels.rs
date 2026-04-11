use super::*;

#[test]
fn execute_sql_dispatch_explain_execution_secondary_non_covering_age_projection_stays_off_removed_authority_labels()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_220_u128, "alice", 10_u64),
        (9_221, "bob", 20),
        (9_222, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL non-covering explain fixture insert should succeed");
    }

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT age FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2",
    )
    .expect("non-covering secondary-order age projection EXPLAIN EXECUTION should execute");

    assert!(
        explain.contains("cov_read_route=Text(\"materialized\")")
            && !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "single-component non-covering secondary-order explain should stay off the removed authority-label surface: {explain}",
    );
    assert!(
        !explain.contains("witness_validated") && !explain.contains("storage_existence_witness"),
        "single-component non-covering secondary-order explain must not surface legacy authority labels: {explain}",
    );
}

#[test]
fn store_backed_execution_descriptor_json_secondary_non_covering_age_projection_stays_off_removed_authority_labels()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_223_u128, "alice", 10_u64),
        (9_224, "bob", 20),
        (9_225, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL non-covering descriptor fixture insert should succeed");
    }

    let descriptor_json = store_backed_execution_descriptor_json_for_sql::<IndexedSessionSqlEntity>(
        &session,
        "SELECT age FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2",
    );

    assert!(
        !descriptor_json.contains("\"authority_decision\"")
            && !descriptor_json.contains("\"authority_reason\"")
            && !descriptor_json.contains("\"index_state\""),
        "store-backed execution descriptor json should keep the single-component non-covering route off the removed authority-label surface: {descriptor_json}",
    );
}

#[test]
fn execute_sql_dispatch_explain_execution_secondary_covering_stays_off_removed_authority_labels() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_226_u128, "alice", 10_u64),
        (9_227, "bob", 20),
        (9_228, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL covering explain surface fixture insert should succeed");
    }

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("secondary covering EXPLAIN EXECUTION should execute");

    assert!(
        explain.contains("CoveringRead")
            && explain.contains("existing_row_mode=Text(\"planner_proven\")")
            && !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "store-backed secondary covering explain should stay off the removed authority-label surface: {explain}",
    );
}

#[test]
fn store_backed_execution_descriptor_json_secondary_covering_stays_off_removed_authority_labels() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_229_u128, "alice", 10_u64),
        (9_230, "bob", 20),
        (9_231, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL covering descriptor surface fixture insert should succeed");
    }

    let descriptor_json = store_backed_execution_descriptor_json_for_sql::<IndexedSessionSqlEntity>(
        &session,
        "SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    );

    assert!(
        descriptor_json.contains("\"existing_row_mode\":\"Text(\\\"planner_proven\\\")\"")
            && !descriptor_json.contains("\"authority_decision\"")
            && !descriptor_json.contains("\"authority_reason\"")
            && !descriptor_json.contains("\"index_state\""),
        "store-backed secondary covering descriptor json should stay off the removed authority-label surface: {descriptor_json}",
    );
}

#[test]
fn execute_sql_dispatch_explain_execution_secondary_covering_order_field_building_index_becomes_planner_invisible()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_460_u128, "alice", 10_u64),
        (9_461, "bob", 20),
        (9_462, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL building-state explain fixture insert should succeed");
    }
    mark_indexed_session_sql_index_building();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("building-index secondary covering EXPLAIN EXECUTION should execute");

    assert!(
        explain.contains("FullScan")
            && explain.contains("OrderByMaterializedSort")
            && !explain.contains("CoveringRead")
            && !explain.contains("existing_row_mode")
            && !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "building indexes must disappear from planner visibility and explain as a materialized full-scan fallback: {explain}",
    );
    assert!(
        !explain.contains("witness_validated") && !explain.contains("storage_existence_witness"),
        "building indexes must not leave legacy secondary covering labels behind once they are planner-invisible: {explain}",
    );

    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("building-index secondary covering query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Ulid(Ulid::from_u128(9_460)),
                Value::Text("alice".to_string()),
            ],
            vec![
                Value::Ulid(Ulid::from_u128(9_461)),
                Value::Text("bob".to_string()),
            ],
        ],
        "planner-invisibility fallback should preserve the same ordered query output",
    );
}
