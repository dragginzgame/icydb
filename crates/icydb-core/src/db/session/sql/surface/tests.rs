use super::*;
use icydb_diagnostic_code::{DiagnosticCode, DiagnosticDetail};

#[test]
fn frozen_operational_sql_families_require_introspection() {
    for sql in [
        "DESCRIBE Example",
        "SHOW CONSTRAINTS FROM Example",
        "SHOW INDEXES FROM Example",
        "SHOW COLUMNS Example",
        "SHOW RELATIONS FROM Example",
        "SHOW ENTITIES",
        "SHOW STORES",
        "SHOW MEMORY",
    ] {
        let dispatch = sql_statement_dispatch(sql)
            .unwrap_or_else(|error| panic!("{sql} should classify: {error}"));
        assert!(dispatch.requires_introspection(), "{sql}");
    }

    let select_sql = "SELECT id FROM Example";
    let select = sql_statement_dispatch(select_sql).expect("ordinary SELECT should classify");
    assert!(!select.requires_introspection());
    assert_eq!(select.sql(), select_sql);
}

#[cfg(feature = "sql")]
#[test]
fn explain_requires_introspection() {
    let dispatch =
        sql_statement_dispatch("EXPLAIN SELECT id FROM Example").expect("EXPLAIN should classify");

    assert!(dispatch.requires_introspection());
}

#[test]
fn shell_routes_only_available_endpoints_and_preserves_mutation_refusals() {
    for (sql, expected) in [
        ("SELECT * FROM Character", SqlStatementShellSurface::Query),
        ("SHOW ENTITIES", SqlStatementShellSurface::Query),
        (
            "CREATE INDEX name_idx ON Character (name)",
            SqlStatementShellSurface::Ddl,
        ),
        (
            "UPDATE Character SET name = 'Ada' WHERE id = 1",
            SqlStatementShellSurface::Update,
        ),
    ] {
        assert_eq!(sql_statement_shell_surface(sql).unwrap(), expected);
    }
    for (sql, mismatch) in [
        (
            "INSERT INTO Character (id) VALUES (1)",
            SqlSurfaceMismatchCode::QueryRejectsInsert,
        ),
        (
            " \t insert into Character (id) SELECT id FROM Other;",
            SqlSurfaceMismatchCode::QueryRejectsInsert,
        ),
        (
            "DELETE FROM Character",
            SqlSurfaceMismatchCode::QueryRejectsDelete,
        ),
        (
            " \n delete from Character WHERE id = 1;",
            SqlSurfaceMismatchCode::QueryRejectsDelete,
        ),
        (
            "DELETE FROM Character RETURNING name",
            SqlSurfaceMismatchCode::QueryRejectsDelete,
        ),
        (
            "INSERT INTO Character (id, name) VALUES (1, 'Ada') RETURNING name",
            SqlSurfaceMismatchCode::QueryRejectsInsert,
        ),
    ] {
        let error = sql_statement_shell_surface(sql).unwrap_err();
        assert_eq!(
            error.diagnostic_code(),
            DiagnosticCode::QuerySqlSurfaceMismatch
        );
        assert_eq!(
            error.diagnostic().detail(),
            Some(&DiagnosticDetail::SqlSurfaceMismatch { mismatch })
        );
        // Generated query ingress still owns the same typed refusal; trusted
        // application mutation dispatch remains independently available.
        assert_eq!(
            sql_statement_surface(sql).unwrap(),
            SqlStatementSurface::Query
        );
        assert!(sql_statement_dispatch(sql).is_ok());
    }
}
