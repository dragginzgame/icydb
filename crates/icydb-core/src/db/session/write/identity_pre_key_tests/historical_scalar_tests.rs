//! Historical scalar defaults remain readable before any row is rewritten.

use super::{DbSession, JournaledTestCanister, initialize_journaled, insert_exact_key_fixture};
use crate::db::SqlStatementResult;
use crate::value::OutputValue;

fn sql_projection_rows(
    session: &DbSession<JournaledTestCanister>,
    sql: &str,
) -> Vec<Vec<OutputValue>> {
    let SqlStatementResult::Projection { rows, .. } =
        session.execute_trusted_sql_query(sql).unwrap()
    else {
        panic!("projection expected");
    };
    rows
}

#[test]
fn historical_scalar_defaults_support_sql_filters_before_rewrite() {
    let session = initialize_journaled();
    for payload in [10, 20] {
        insert_exact_key_fixture(&session, payload);
    }
    session
        .execute_admin_sql_ddl("ALTER TABLE IdentityRow ADD COLUMN score nat64 NOT NULL DEFAULT 7 EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2")
        .unwrap();
    session
        .execute_admin_sql_ddl("ALTER TABLE IdentityRow ADD COLUMN optional_score nat64 EXPECT SCHEMA VERSION 2 SET SCHEMA VERSION 3")
        .unwrap();
    session.execute_admin_sql_ddl(
        "ALTER TABLE IdentityRow ALTER COLUMN score SET DEFAULT 9 EXPECT SCHEMA VERSION 3 SET SCHEMA VERSION 4",
    ).unwrap();
    assert_eq!(
        sql_projection_rows(&session, "SELECT COUNT(*) FROM IdentityRow WHERE score = 7",),
        vec![vec![OutputValue::nat64(2)]]
    );
    for condition in [
        "score = 7",
        "score < 8",
        "score = score",
        "optional_score IS NULL",
    ] {
        assert_eq!(
            sql_projection_rows(
                &session,
                &format!("SELECT payload FROM IdentityRow WHERE {condition} ORDER BY payload ASC")
            ),
            vec![vec![OutputValue::nat64(10)], vec![OutputValue::nat64(20)]]
        );
    }
    assert!(
        sql_projection_rows(&session, "SELECT payload FROM IdentityRow WHERE score = 9",)
            .is_empty()
    );
    session
        .execute_trusted_sql_exact_update(
            "UPDATE IdentityRow SET payload = 11 WHERE score = 7 AND payload = 10",
            2,
        )
        .unwrap();
    session
        .execute_trusted_sql_mutation("DELETE FROM IdentityRow WHERE score = 7 AND payload = 20")
        .unwrap();
    assert_eq!(
        sql_projection_rows(&session, "SELECT payload, score FROM IdentityRow",),
        vec![vec![OutputValue::nat64(11), OutputValue::nat64(7)]]
    );
}

#[test]
fn historical_scalar_text_supports_byte_length_before_rewrite() {
    let session = initialize_journaled();
    insert_exact_key_fixture(&session, 10);
    session
        .execute_admin_sql_ddl(
            "ALTER TABLE IdentityRow ADD COLUMN note text NOT NULL DEFAULT 'abc' EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
        )
        .unwrap();
    assert_eq!(
        sql_projection_rows(&session, "SELECT OCTET_LENGTH(note) FROM IdentityRow",),
        vec![vec![OutputValue::nat64(3)]]
    );
    assert_eq!(
        sql_projection_rows(
            &session,
            "SELECT payload FROM IdentityRow WHERE note = 'abc'",
        ),
        vec![vec![OutputValue::nat64(10)]]
    );
}
