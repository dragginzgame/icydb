//! SQL grouped counts share maintained prefix metadata and grouped semantics.

use super::{initialize, initialize_with_composite_payload_index, insert_exact_key_fixture};
use crate::{
    db::{SqlStatementResult, data::DataStore, index::IndexStore},
    value::OutputValue,
};

#[test]
fn grouped_count_metadata_reads_no_rows_or_index_entries() {
    assert_grouped_count_reads_no_rows(initialize());
}

#[test]
fn grouped_count_metadata_reads_composite_prefix_without_rows() {
    assert_grouped_count_reads_no_rows(initialize_with_composite_payload_index());
}

fn assert_grouped_count_reads_no_rows(session: super::DbSession<super::TestCanister>) {
    for payload in [20, 10, 10] {
        insert_exact_key_fixture(&session, payload);
    }
    for _ in 0..2 {
        let data_reads = DataStore::current_get_call_count();
        let index_reads = IndexStore::current_entry_read_count();
        let SqlStatementResult::Grouped {
            columns,
            rows,
            next_cursor,
            ..
        } = session
            .execute_trusted_sql_query(
                "SELECT payload, COUNT(*) AS token_count FROM IdentityRow \
                     WHERE payload IN (99, 20, 10, 10) GROUP BY payload",
            )
            .expect("indexed grouped count should execute")
        else {
            panic!("grouped output expected");
        };
        assert_eq!(columns, ["payload", "token_count"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].group_key(), [OutputValue::nat64(10)]);
        assert_eq!(rows[0].aggregate_values(), [OutputValue::nat64(2)]);
        assert_eq!(rows[1].group_key(), [OutputValue::nat64(20)]);
        assert_eq!(rows[1].aggregate_values(), [OutputValue::nat64(1)]);
        assert!(next_cursor.is_none());
        assert_eq!(DataStore::current_get_call_count(), data_reads);
        assert_eq!(IndexStore::current_entry_read_count(), index_reads);
    }
}

#[test]
fn grouped_count_metadata_preserves_having_windows_and_empty_groups() {
    let session = initialize();
    for payload in [10, 10, 20, 30] {
        insert_exact_key_fixture(&session, payload);
    }
    for (suffix, expected) in [
        ("HAVING COUNT(*) > 1", vec![(10, 2)]),
        ("ORDER BY payload DESC LIMIT 1", vec![(30, 1)]),
        ("ORDER BY payload ASC LIMIT 1 OFFSET 1", vec![(20, 1)]),
    ] {
        let sql = format!(
            "SELECT payload, COUNT(*) FROM IdentityRow WHERE payload IN (10, 20, 30, 99) GROUP BY payload {suffix}"
        );
        let data_reads = DataStore::current_get_call_count();
        let SqlStatementResult::Grouped { rows, .. } = session
            .execute_trusted_sql_query(&sql)
            .expect("grouped window should execute")
        else {
            panic!("grouped output expected");
        };
        assert_eq!(rows.len(), expected.len());
        for (row, (key, count)) in rows.iter().zip(expected) {
            assert_eq!(row.group_key(), [OutputValue::nat64(key)]);
            assert_eq!(row.aggregate_values(), [OutputValue::nat64(count)]);
        }
        assert_eq!(DataStore::current_get_call_count(), data_reads);
    }
    let SqlStatementResult::Grouped { rows, .. } = session
        .execute_trusted_sql_query(
            "SELECT payload, COUNT(*) FROM IdentityRow WHERE payload = 99 GROUP BY payload",
        )
        .expect("empty count should execute")
    else {
        panic!("grouped output expected");
    };
    assert!(rows.is_empty());
}

#[test]
fn grouped_count_metadata_does_not_ignore_residual_predicates() {
    let session = initialize();
    for payload in [10, 10, 20] {
        insert_exact_key_fixture(&session, payload);
    }
    let SqlStatementResult::Grouped { rows, .. } = session.execute_trusted_sql_query(
        "SELECT payload, COUNT(*) FROM IdentityRow WHERE payload IN (10, 20) AND id > 1 GROUP BY payload",
    ).expect("residual-filtered count should execute") else { panic!("grouped output expected"); };
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|row| row.aggregate_values() == [OutputValue::nat64(1)])
    );
}

#[test]
fn grouped_count_metadata_respects_resource_limits() {
    use super::{assert_sql_query_exhausts, assert_sql_query_fits_resource_limit};
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let session = initialize();
    for payload in [10, 10, 20] {
        insert_exact_key_fixture(&session, payload);
    }
    let sql =
        "SELECT payload, COUNT(*) FROM IdentityRow WHERE payload IN (10, 20, 99) GROUP BY payload";
    assert_sql_query_fits_resource_limit(&session, sql, Resource::RowsVisited, 0);
    assert_sql_query_exhausts(&session, sql, Resource::KeyIndexEntriesVisited);
    assert_sql_query_exhausts(&session, sql, Resource::GroupDistinctEntries);
    assert_sql_query_exhausts(&session, sql, Resource::ResultBytes);
}

#[test]
fn grouped_count_metadata_tracks_journaled_mutations_on_warm_queries() {
    use super::{
        ENTITY_NAME, drive_journaled_cardinality_to_ready, dynamic_payload_patch,
        initialize_journaled_with_root_and_payload_uniqueness, mark_journaled_cardinality_building,
    };
    use crate::{db::DynamicMutation, value::InputValue};

    let (session, _root) = initialize_journaled_with_root_and_payload_uniqueness(false);
    let first = insert_exact_key_fixture(&session, 10);
    let second = insert_exact_key_fixture(&session, 20);
    for _ in 0..2 {
        session
            .db
            .drive_startup_recovery_page()
            .expect("fold initial writes");
    }
    drive_journaled_cardinality_to_ready(&session);
    let sql =
        "SELECT payload, COUNT(*) FROM IdentityRow WHERE payload IN (10, 20, 99) GROUP BY payload";
    session
        .execute_trusted_sql_query(sql)
        .expect("warm grouped query");
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(second),
            patch: dynamic_payload_patch(10),
        })
        .expect("move token between groups");
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(first),
        })
        .expect("delete token");
    for _ in 0..2 {
        insert_exact_key_fixture(&session, 10);
    }
    let data_reads = DataStore::current_get_call_count();
    let index_reads = IndexStore::current_entry_read_count();
    let SqlStatementResult::Grouped { rows, .. } = session
        .execute_trusted_sql_query(sql)
        .expect("warm count after mutations")
    else {
        panic!("grouped output expected");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].group_key(), [OutputValue::nat64(10)]);
    assert_eq!(rows[0].aggregate_values(), [OutputValue::nat64(3)]);
    assert_eq!(DataStore::current_get_call_count(), data_reads);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads);
    mark_journaled_cardinality_building();
    let SqlStatementResult::Grouped {
        rows: scanned_rows, ..
    } = session
        .execute_trusted_sql_query(sql)
        .expect("unavailable metadata retains grouped execution")
    else {
        panic!("grouped output expected");
    };
    assert_eq!(scanned_rows, rows);
    assert!(DataStore::current_get_call_count() > data_reads);
}

#[test]
fn grouped_count_metadata_accepts_non_null_membership_on_nullable_field() {
    use super::{
        ENTITY_NAME, STORE_PATH, dynamic_payload_patch, identity_snapshot_with_nullable_payload,
        initialize_with_snapshot,
    };
    use crate::db::DynamicStructuralPatch;

    let session = initialize_with_snapshot(identity_snapshot_with_nullable_payload(STORE_PATH));
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![
                dynamic_payload_patch(10),
                dynamic_payload_patch(10),
                DynamicStructuralPatch::new(Vec::new()),
            ],
        )
        .expect("nullable fixture");
    let data_reads = DataStore::current_get_call_count();
    let SqlStatementResult::Grouped { rows, .. } = session
        .execute_trusted_sql_query(
            "SELECT payload, COUNT(*) FROM IdentityRow WHERE payload IN (10, 99) GROUP BY payload",
        )
        .expect("non-null grouped count")
    else {
        panic!("grouped output expected");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].aggregate_values(), [OutputValue::nat64(2)]);
    assert_eq!(DataStore::current_get_call_count(), data_reads);
}
