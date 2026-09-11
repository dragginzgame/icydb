//! SQL-free consuming API, semantic rejection and generated decode isolation.

use super::*;

fn reason(error: TypedOperationError) -> QueryReadAdmissionCode {
    let TypedOperationError::Database(error) = error else {
        panic!("expected a database admission failure");
    };
    let Some(DiagnosticDetail::QueryReadAdmission { reason }) =
        error.diagnostic().detail().copied()
    else {
        panic!("expected a typed admission reason");
    };
    reason
}

#[test]
fn typed_explain_is_detached_and_never_decodes_rows() {
    let id = insert_one_native_row("explain-present-row");
    EXACT_ROW_DECODES.set(0);
    let plan = icydb::db::with_request_execution(|| {
        let database = db().unwrap();
        database
            .query::<ObservedExactAdapter>()
            .unwrap()
            .filter(FieldRef::new("id").eq(id))
            .explain()
            .unwrap()
    });
    assert_eq!(EXACT_ROW_DECODES.get(), 0);
    assert_eq!(
        plan.access_decision().selected.kind,
        icydb::db::query::ExplainAccessDecisionKind::ByKey
    );
    assert!(plan.render_text_canonical().is_ok());
    assert!(plan.render_json_canonical().is_ok());
}

#[test]
fn typed_explain_accepts_logical_scan_sort_and_grouped_queries_without_execution_permission() {
    crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
    icydb::db::with_request_execution(|| {
        let database = db().unwrap();
        let scan = database
            .query::<OneSimpleEntity01>()
            .unwrap()
            .order_by(asc("name"))
            .explain()
            .unwrap();
        assert_eq!(
            scan.access_decision().selected.kind,
            icydb::db::query::ExplainAccessDecisionKind::FullScan
        );
        assert!(
            database
                .query::<OneSimpleEntity01>()
                .unwrap()
                .order_by(asc("name"))
                .execute_live_page(None)
                .is_err()
        );
        let grouped = database
            .query::<OneSimpleEntity01>()
            .unwrap()
            .group_by("name")
            .aggregate(count())
            .grouped_limits(16, 4096)
            .explain()
            .unwrap();
        assert!(
            grouped
                .render_json_canonical()
                .unwrap()
                .contains("\"aggregates\":[")
        );
        assert_eq!(
            reason(
                database
                    .query::<OneSimpleEntity01>()
                    .unwrap()
                    .group_by("name")
                    .aggregate(count())
                    .explain()
                    .unwrap_err()
            ),
            QueryReadAdmissionCode::GroupedQueryRequiresLimits
        );
        assert!(
            database
                .query::<OneSimpleEntity01>()
                .unwrap()
                .grouped_limits(16, 4096)
                .explain()
                .is_err()
        );
        assert_eq!(
            reason(
                database
                    .query::<OneSimpleEntity01>()
                    .unwrap()
                    .group_by("name")
                    .aggregate(count())
                    .grouped_limits(0, 4096)
                    .explain()
                    .unwrap_err()
            ),
            QueryReadAdmissionCode::GroupedQueryRequiresLimits
        );
        assert!(
            database
                .query::<OneSimpleEntity01>()
                .unwrap()
                .filter(FieldRef::new("missing_field").eq("payload"))
                .explain()
                .is_err()
        );
    });
}

#[test]
fn typed_explain_rejects_every_supplied_cursor_before_conversion() {
    crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
    icydb::db::with_request_execution(|| {
        let database = db().unwrap();
        for cursor in ["", "not-a-cursor"] {
            for grouped in [false, true] {
                let mut query = database
                    .query::<OneSimpleEntity01>()
                    .unwrap()
                    .filter(FieldRef::new("missing_field").eq("unconverted"));
                if grouped {
                    query = query.group_by("name").aggregate(count());
                }
                assert_eq!(
                    reason(query.cursor(cursor).explain().unwrap_err()),
                    QueryReadAdmissionCode::ExplainDoesNotAcceptCursor
                );
            }
        }
    });
}

#[test]
fn typed_explain_rejects_oversized_owned_input_and_incompatible_values() {
    crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
    icydb::db::with_request_execution(|| {
        let database = db().unwrap();
        let mut filter = FieldRef::new("name").eq("value");
        for _ in 0..256 {
            filter = icydb::db::query::FilterExpr::not(filter);
        }
        assert_eq!(
            reason(
                database
                    .query::<OneSimpleEntity01>()
                    .unwrap()
                    .filter(filter)
                    .explain()
                    .unwrap_err()
            ),
            QueryReadAdmissionCode::InputDepthExceeded,
        );
        assert!(
            database
                .query::<OneSimpleEntity01>()
                .unwrap()
                .filter(FieldRef::new("id").eq("not-an-ulid"))
                .explain()
                .is_err()
        );
        assert!(
            database
                .query::<OneSimpleEntity01>()
                .unwrap()
                .explain()
                .is_ok()
        );
    });
}
