//! Compact CASE uses the maintained accepted-schema preparation/execution path.

use super::*;

#[test]
fn compact_nested_case_executes_current_bindings_on_warm_preparation() {
    let session = initialize();
    seed_singleton(&session);
    let mut condition = "label = ?".to_string();
    for level in 0..6 {
        condition = format!(
            "CASE WHEN {condition} THEN amount >= U256 '{level}' ELSE amount < U256 '{level}' END"
        );
    }
    let sql = format!("SELECT label FROM Singleton WHERE {condition} ORDER BY id ASC LIMIT 1");
    let dispatch = sql_statement_dispatch(&sql).expect("nested CASE parses");
    for label in ["singleton", "missing", "singleton"] {
        let expected = sql_rows(&session, &sql.replace('?', &format!("'{label}'")));
        let (result, entity) = new_request_session()
            .execute_trusted_sql_query_with_entity_name(
                &dispatch,
                &[InputValue::text(label.into())],
            )
            .expect("compact CASE bound execution");
        assert_eq!(entity, ENTITY_NAME);
        let SqlStatementResult::Projection { rows: actual, .. } = result else {
            panic!("expected rows");
        };
        assert_eq!(actual, expected);
        // Stored amount is 2; the final three branches invert the initial match.
        assert_eq!(actual.len(), usize::from(label == "missing"));
    }
}
