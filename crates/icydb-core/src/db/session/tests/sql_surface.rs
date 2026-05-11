use super::*;
use crate::{
    db::{
        FieldRef, MutationMode, asc,
        codec::{decode_row_payload_bytes, serialize_row_payload},
        data::{RawRow, encode_runtime_value_into_slot},
        executor::EntityAuthority,
        response::Row,
        schema::{
            AcceptedSchemaSnapshot, accepted_schema_cache_fingerprint,
            compiled_schema_proposal_for_model,
        },
        session::{query::QueryPlanVisibility, sql::SqlCompiledCommandCacheKey},
        sql::lowering::{SqlCommand, compile_sql_command},
    },
    error::ErrorClass,
};
use std::collections::HashSet;

// Assert that one representative SQL surface stays fail-closed for a matrix of
// statement lanes that belong to some other surface.
fn assert_sql_surface_rejects_statement_lanes<T, F>(cases: &[(&str, &str)], mut execute: F)
where
    F: FnMut(&str) -> Result<T, QueryError>,
{
    for (sql, context) in cases {
        assert_unsupported_sql_surface_result(execute(sql), context);
    }
}

fn session_sql_entity_initial_accepted_schema_cache_fingerprint() -> [u8; 16] {
    let proposal = compiled_schema_proposal_for_model(SessionSqlEntity::MODEL);
    let accepted = AcceptedSchemaSnapshot::try_new(proposal.initial_persisted_schema_snapshot())
        .expect("session SQL test schema snapshot should be accepted");

    accepted_schema_cache_fingerprint(&accepted)
        .expect("session SQL test schema cache fingerprint should derive")
}

// Assert that one representative SQL surface keeps its own user-facing lane
// rejection message for each disallowed statement family.
fn assert_sql_surface_rejects_statement_lanes_with_message<T, F>(
    cases: &[(&str, &str)],
    mut execute: F,
    surface: &str,
) where
    F: FnMut(&str) -> Result<T, QueryError>,
{
    for (sql, expected) in cases {
        let Err(err) = execute(sql) else {
            panic!("{surface} should reject the non-owned lane: {sql}");
        };
        assert!(
            err.to_string().contains(expected),
            "{surface} should preserve a surface-local lane boundary message: {sql}",
        );
    }
}

// Encode one old physical row for `SessionNullableSqlEntity` as it existed
// before the nullable `nickname` field was appended. This lets the SQL surface
// test prove startup transition acceptance and old-row decode together.
fn old_nullable_sql_raw_row_for_test(id: Ulid, name: &str) -> RawRow {
    let id_payload =
        encode_runtime_value_into_slot(SessionNullableSqlEntity::MODEL, 0, &Value::Ulid(id))
            .expect("old nullable SQL id payload should encode");
    let name_payload = encode_runtime_value_into_slot(
        SessionNullableSqlEntity::MODEL,
        1,
        &Value::Text(name.to_string()),
    )
    .expect("old nullable SQL name payload should encode");
    let slot_payload =
        encode_sql_surface_slot_payload_for_test(&[id_payload.as_slice(), name_payload.as_slice()]);

    RawRow::try_new(serialize_row_payload(slot_payload).expect("old nullable row should serialize"))
        .expect("old nullable row should be valid raw row bytes")
}

// Build the slot-framed row payload used by one owner-local old-row fixture.
// Production row writers still own canonical new-row encoding.
fn encode_sql_surface_slot_payload_for_test(slots: &[&[u8]]) -> Vec<u8> {
    let field_count =
        u16::try_from(slots.len()).expect("SQL surface slot fixture count should fit in u16");
    let mut row_payload = Vec::new();
    let mut payload_bytes = Vec::new();

    row_payload.extend_from_slice(&field_count.to_be_bytes());
    for bytes in slots {
        let start = u32::try_from(payload_bytes.len())
            .expect("SQL surface slot fixture start should fit in u32");
        let len =
            u32::try_from(bytes.len()).expect("SQL surface slot fixture length should fit in u32");
        row_payload.extend_from_slice(&start.to_be_bytes());
        row_payload.extend_from_slice(&len.to_be_bytes());
        payload_bytes.extend_from_slice(bytes);
    }
    row_payload.extend_from_slice(payload_bytes.as_slice());

    row_payload
}

// Install the accepted schema snapshot that represents `SessionNullableSqlEntity`
// before `nickname` was added. The generated proposal remains current, so
// reconciliation must perform the append-only nullable transition.
fn install_nullable_sql_old_accepted_schema_prefix() {
    let proposal =
        compiled_schema_proposal_for_model(<SessionNullableSqlEntity as EntitySchema>::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_prefix = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_id(),
        SchemaRowLayout::new(
            expected.row_layout().version(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        expected.fields()[..2].to_vec(),
    );

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(SessionNullableSqlEntity::ENTITY_TAG, &stored_prefix)
            .expect("old nullable SQL schema prefix should persist");
    });
}

// Seed one old two-slot row directly into the data store so the SQL surface can
// exercise accepted-schema row decode without first writing a current-layout row.
fn insert_old_nullable_sql_row_for_test(id: Ulid, name: &str) {
    let key = DataKey::try_new::<SessionNullableSqlEntity>(id)
        .expect("old nullable SQL data key should build")
        .to_raw()
        .expect("old nullable SQL data key should encode");
    let row = old_nullable_sql_raw_row_for_test(id, name);

    SESSION_SQL_DATA_STORE.with_borrow_mut(|store| {
        let _ = store.insert_raw_for_test(key, row);
    });
}

// Load the raw nullable SQL fixture row so tests can distinguish accepted
// short-row reads from current-layout writeback.
fn nullable_sql_raw_row_for_test(id: Ulid) -> RawRow {
    let key = DataKey::try_new::<SessionNullableSqlEntity>(id)
        .expect("nullable SQL data key should build")
        .to_raw()
        .expect("nullable SQL data key should encode");

    SESSION_SQL_DATA_STORE.with_borrow(|store| {
        store
            .get(&key)
            .expect("nullable SQL row should exist in data store")
    })
}

// Read the dense slot-count header from one raw persisted row. This keeps
// write-layout tests precise without routing through typed decode, which would
// hide whether the physical row was rewritten to the current accepted layout.
fn raw_row_slot_count_for_test(raw_row: &RawRow) -> u16 {
    let payload = decode_row_payload_bytes(raw_row.as_bytes())
        .expect("nullable SQL raw row envelope should decode");
    let count_bytes = payload
        .get(..2)
        .expect("nullable SQL raw row should include slot-count header");

    u16::from_be_bytes([count_bytes[0], count_bytes[1]])
}

// Assert a projection result with a compact expected-column call site for the
// nullable additive schema tests, where most assertions are single-column
// predicate checks.
fn assert_projection_rows(
    result: SqlStatementResult,
    expected_columns: [&str; 1],
    expected_rows: Vec<Vec<OutputValue>>,
    context: &str,
) {
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("{context}: expected projection result");
    };

    assert_eq!(
        columns,
        expected_columns
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
        "{context}: column mismatch",
    );
    assert_eq!(
        row_count,
        u32::try_from(expected_rows.len()).expect("expected row count should fit in u32"),
        "{context}: row count mismatch"
    );
    assert_eq!(rows, expected_rows, "{context}: row mismatch");
}

// Assert that one unsupported SQL feature is surfaced with the same parser
// detail label through the selected SQL surface.
fn assert_sql_surface_preserves_unsupported_feature_detail<T, F>(
    cases: &[(&str, &'static str)],
    mut execute: F,
) where
    F: FnMut(&str) -> Result<T, QueryError>,
{
    for (sql, feature) in cases {
        let Err(err) = execute(sql) else {
            panic!("unsupported SQL feature should fail through the SQL surface: {sql}");
        };
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

// Require one session-compiled SELECT artifact to preserve the same canonical
// structural and logical identity as the directly lowered internal query.
fn assert_compiled_select_query_matches_lowered_identity_for_entity<E>(
    compiled: &crate::db::session::sql::CompiledSqlCommand,
    sql: &str,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let lowered = compile_sql_command::<E>(sql, MissingRowPolicy::Ignore)
        .unwrap_or_else(|err| panic!("{context} should lower into one canonical query: {err:?}"));
    let SqlCommand::Query(lowered_query) = lowered else {
        panic!("{context} should lower to one query command");
    };
    let crate::db::session::sql::CompiledSqlCommand::Select { query, .. } = compiled else {
        panic!("{context} should compile into one SELECT artifact");
    };

    assert_eq!(
        query.structural_cache_key(),
        lowered_query.structural().structural_cache_key(),
        "{context} must canonicalize onto the same structural query cache key before cache insertion",
    );
    assert_eq!(
        query
            .build_plan()
            .expect("compiled session query plan should build")
            .fingerprint(),
        lowered_query
            .plan()
            .expect("canonical lowered query plan should build")
            .into_inner()
            .fingerprint(),
        "{context} must preserve the same canonical logical plan identity as the lowered internal form",
    );
}

fn assert_compiled_select_query_matches_lowered_identity(
    compiled: &crate::db::session::sql::CompiledSqlCommand,
    sql: &str,
    context: &str,
) {
    assert_compiled_select_query_matches_lowered_identity_for_entity::<SessionSqlEntity>(
        compiled, sql, context,
    );
}

// Require two distinct SQL SELECT artifacts to preserve distinct canonical
// structural identity once lowered onto the shared query surface.
fn assert_compiled_select_queries_remain_distinct_for_entity(
    left: &crate::db::session::sql::CompiledSqlCommand,
    right: &crate::db::session::sql::CompiledSqlCommand,
    context: &str,
) {
    let crate::db::session::sql::CompiledSqlCommand::Select { query: left, .. } = left else {
        panic!("{context} left SQL should compile into one SELECT artifact");
    };
    let crate::db::session::sql::CompiledSqlCommand::Select { query: right, .. } = right else {
        panic!("{context} right SQL should compile into one SELECT artifact");
    };

    assert_ne!(
        left.structural_cache_key(),
        right.structural_cache_key(),
        "{context} must not collapse onto the same structural query cache key",
    );
}

fn assert_distinct_compiled_selects_execute_through_shared_query_plan_for_entity<E>(
    session: &DbSession<SessionSqlCanister>,
    left: &crate::db::session::sql::CompiledSqlCommand,
    right: &crate::db::session::sql::CompiledSqlCommand,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let _ = session
        .execute_compiled_sql::<E>(left)
        .expect("executing the left compiled SELECT should succeed through the shared lower cache");

    let _ = session.execute_compiled_sql::<E>(right).expect(
        "executing the right compiled SELECT should succeed through the shared lower cache",
    );
}

// This query-surface matrix keeps every non-query statement rejection on one
// outward contract table so the boundary stays easy to audit.
#[expect(
    clippy::too_many_lines,
    reason = "query-surface rejection matrix is intentionally tabular"
)]
#[test]
fn sql_query_surfaces_reject_non_query_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: keep the lowered query entrypoint fail-closed for every
    // non-query statement family.
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "SQL query lowering must reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "SQL query lowering must reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "SQL query lowering must reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "SQL query lowering must reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "SQL query lowering must reject SHOW ENTITIES statements",
            ),
            (
                "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
                "SQL query lowering must reject INSERT statements",
            ),
            (
                "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
                "SQL query lowering must reject INSERT statements",
            ),
            (
                "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
                "SQL query lowering must reject UPDATE statements",
            ),
        ],
        |sql| lower_select_query_for_tests::<SessionSqlEntity>(&session, sql),
    );

    // Phase 2: require both executable query entrypoints to preserve their
    // own surface-local lane boundary messages for the same non-query SQL.
    let message_cases = [
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "scalar SELECT helper rejects EXPLAIN",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "scalar SELECT helper rejects DESCRIBE",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "scalar SELECT helper rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "scalar SELECT helper rejects SHOW COLUMNS",
        ),
        (
            "SHOW ENTITIES",
            "scalar SELECT helper rejects SHOW ENTITIES",
        ),
        (
            "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "scalar SELECT helper rejects INSERT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            "scalar SELECT helper rejects INSERT",
        ),
        (
            "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
            "scalar SELECT helper rejects UPDATE",
        ),
    ];
    assert_sql_surface_rejects_statement_lanes_with_message(
        &message_cases,
        |sql| execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql),
        "scalar SELECT helper",
    );

    let grouped_cases = [
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "grouped SELECT helper rejects EXPLAIN",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "grouped SELECT helper rejects DESCRIBE",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "grouped SELECT helper rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "grouped SELECT helper rejects SHOW COLUMNS",
        ),
        (
            "SHOW ENTITIES",
            "grouped SELECT helper rejects SHOW ENTITIES",
        ),
        (
            "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "grouped SELECT helper rejects INSERT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            "grouped SELECT helper rejects INSERT",
        ),
        (
            "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
            "grouped SELECT helper rejects UPDATE",
        ),
    ];

    assert_sql_surface_rejects_statement_lanes_with_message(
        &grouped_cases,
        |sql| execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None),
        "grouped SELECT helper",
    );
}

#[test]
fn sql_query_lowering_projection_matrix_normalizes_to_scalar_fields() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_field_names, context) in [
        (
            "SELECT name, age FROM SessionSqlEntity",
            vec!["name".to_string(), "age".to_string()],
            "field-list SQL projection",
        ),
        (
            "SELECT alias.name \
             FROM SessionSqlEntity alias \
             WHERE alias.age >= 21 \
             ORDER BY alias.age DESC LIMIT 1",
            vec!["name".to_string()],
            "single-table alias SQL projection",
        ),
    ] {
        let query = lower_select_query_for_tests::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should lower: {err}"));
        let projection = query
            .plan()
            .unwrap_or_else(|err| panic!("{context} plan should build: {err}"))
            .projection_spec();
        let field_names = projection
            .fields()
            .map(|field| match field {
                ProjectionField::Scalar {
                    expr: Expr::Field(field),
                    alias: None,
                } => field.as_str().to_string(),
                other @ ProjectionField::Scalar { .. } => {
                    panic!("{context} should lower to canonical field exprs: {other:?}")
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(
            field_names, expected_field_names,
            "{context} should normalize to scalar field selection",
        );
    }
}

#[test]
fn sql_surface_text_specific_computed_projection_rejection_matrix_preserves_lane_messages() {
    reset_session_sql_store();
    let session = sql_session();

    let query_err = lower_select_query_for_tests::<SessionSqlEntity>(&session, "SELECT TRIM(name) FROM SessionSqlEntity")
        .expect_err(
            "SQL query lowering should stay on the structural lowered-query lane and reject text-specific computed projection forms",
        );
    assert!(
        query_err
            .to_string()
            .contains("SQL query lowering does not accept text-specific computed projection"),
        "SQL query lowering should reject text-specific computed projection with an actionable boundary message",
    );

    let execute_err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT TRIM(name) FROM SessionSqlEntity",
    )
    .expect_err(
        "scalar SELECT helper should keep text-specific computed projection on the statement-owned lane",
    );
    assert!(
        execute_err
            .to_string()
            .contains("scalar SELECT helper rejects text-specific computed projection"),
        "scalar SELECT helper should reject text-specific computed projection with an actionable boundary message",
    );
}

#[test]
fn execute_sql_rejects_quoted_identifiers_in_reduced_parser() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT \"name\" FROM SessionSqlEntity",
    )
    .expect_err("quoted identifiers should be rejected by reduced SQL parser");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "quoted identifiers should fail closed through unsupported SQL boundary",
    );
}

#[test]
fn sql_metadata_surfaces_match_typed_payloads() {
    reset_session_sql_store();
    let session = sql_session();

    let describe_from_sql =
        statement_describe_sql::<SessionSqlEntity>(&session, "DESCRIBE SessionSqlEntity")
            .expect("describe_sql should succeed");
    let show_indexes_from_sql =
        statement_show_indexes_sql::<SessionSqlEntity>(&session, "SHOW INDEXES SessionSqlEntity")
            .expect("show_indexes_sql should succeed");
    let show_columns_from_sql =
        statement_show_columns_sql::<SessionSqlEntity>(&session, "SHOW COLUMNS SessionSqlEntity")
            .expect("show_columns_sql should succeed");
    let show_entities_from_sql = statement_show_entities_sql(&session, "SHOW ENTITIES")
        .expect("show_entities_sql should succeed");
    let show_tables_from_sql = statement_show_entities_sql(&session, "SHOW TABLES")
        .expect("show tables alias should succeed");

    assert_eq!(
        describe_from_sql,
        session.describe_entity::<SessionSqlEntity>(),
        "describe_sql should project through canonical describe_entity payload",
    );
    assert_eq!(
        show_indexes_from_sql,
        session.show_indexes::<SessionSqlEntity>(),
        "show_indexes_sql should project through canonical show_indexes payload",
    );
    assert_eq!(
        show_columns_from_sql,
        session.show_columns::<SessionSqlEntity>(),
        "show_columns_sql should project through canonical show_columns payload",
    );
    assert_eq!(
        show_entities_from_sql,
        session.show_entities(),
        "show_entities_sql should project through canonical show_entities payload",
    );
    assert_eq!(
        session.show_tables(),
        session.show_entities(),
        "typed show_tables helper should stay a direct alias of show_entities",
    );
    assert_eq!(
        show_tables_from_sql,
        session.show_entities(),
        "SHOW TABLES should stay a direct alias of SHOW ENTITIES",
    );
}

// This metadata/explain matrix keeps every non-owned statement rejection on
// one outward surface contract instead of splitting the statement families.
#[expect(
    clippy::too_many_lines,
    reason = "metadata and explain rejection matrix is intentionally tabular"
)]
#[test]
fn sql_metadata_and_explain_surfaces_reject_non_owned_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "SELECT * FROM SessionSqlEntity",
                "describe_sql should reject SELECT statements",
            ),
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "describe_sql should reject EXPLAIN statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "describe_sql should reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "describe_sql should reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "describe_sql should reject SHOW ENTITIES statements",
            ),
        ],
        |sql| statement_describe_sql::<SessionSqlEntity>(&session, sql),
    );
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "SELECT * FROM SessionSqlEntity",
                "show_indexes_sql should reject SELECT statements",
            ),
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "show_indexes_sql should reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "show_indexes_sql should reject DESCRIBE statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "show_indexes_sql should reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "show_indexes_sql should reject SHOW ENTITIES statements",
            ),
        ],
        |sql| statement_show_indexes_sql::<SessionSqlEntity>(&session, sql),
    );
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "SELECT * FROM SessionSqlEntity",
                "show_columns_sql should reject SELECT statements",
            ),
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "show_columns_sql should reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "show_columns_sql should reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "show_columns_sql should reject SHOW INDEXES statements",
            ),
            (
                "SHOW ENTITIES",
                "show_columns_sql should reject SHOW ENTITIES statements",
            ),
        ],
        |sql| statement_show_columns_sql::<SessionSqlEntity>(&session, sql),
    );
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "SELECT * FROM SessionSqlEntity",
                "show_entities_sql should reject SELECT statements",
            ),
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "show_entities_sql should reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "show_entities_sql should reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "show_entities_sql should reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "show_entities_sql should reject SHOW COLUMNS statements",
            ),
        ],
        |sql| statement_show_entities_sql(&session, sql),
    );
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "DESCRIBE SessionSqlEntity",
                "explain_sql should reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "explain_sql should reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "explain_sql should reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "explain_sql should reject SHOW ENTITIES statements",
            ),
        ],
        |sql| statement_explain_sql::<SessionSqlEntity>(&session, sql),
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "this matrix test intentionally proves one shared unsupported-feature contract across several SQL surfaces"
)]
fn sql_surfaces_preserve_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();
    let parser_owned_cases = unsupported_sql_parser_feature_cases();
    let lowering_owned_cases = unsupported_sql_feature_cases();

    assert_sql_surface_preserves_unsupported_feature_detail(&parser_owned_cases, |sql| {
        parse_sql_statement_for_tests(&session, sql).map(|_| ())
    });
    assert_sql_surface_preserves_unsupported_feature_detail(&lowering_owned_cases, |sql| {
        lower_select_query_for_tests::<SessionSqlEntity>(&session, sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(&lowering_owned_cases, |sql| {
        execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(&lowering_owned_cases, |sql| {
        statement_projection_rows::<SessionSqlEntity>(&session, sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(&lowering_owned_cases, |sql| {
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(&lowering_owned_cases, |sql| {
        let explain_sql = format!("EXPLAIN {sql}");
        statement_explain_sql::<SessionSqlEntity>(&session, explain_sql.as_str())
    });
    let sql = "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING id";

    assert_unsupported_sql_surface_result(
        lower_select_query_for_tests::<SessionSqlEntity>(&session, sql),
        "SQL query lowering should reject INSERT lane even when RETURNING is present",
    );
    assert_unsupported_sql_surface_result(
        execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql),
        "scalar SELECT helper should reject INSERT lane even when RETURNING is present",
    );
    let returning_rows = statement_projection_rows::<SessionSqlEntity>(&session, sql)
        .expect("statement execution should admit INSERT RETURNING");
    assert_eq!(returning_rows.len(), 1);
    assert_eq!(returning_rows[0].len(), 1);
    assert!(
        matches!(returning_rows[0][0], Value::Ulid(_)),
        "statement INSERT RETURNING should project the requested generated id",
    );
    assert_unsupported_sql_surface_result(
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None),
        "grouped SELECT helper should reject INSERT lane even when RETURNING is present",
    );

    let update_returning_sql =
        "UPDATE SessionSqlEntity SET age = 22 WHERE name = 'Ada' RETURNING id, age";

    assert_unsupported_sql_surface_result(
        lower_select_query_for_tests::<SessionSqlEntity>(&session, update_returning_sql),
        "SQL query lowering should reject UPDATE lane even when RETURNING is present",
    );
    assert_unsupported_sql_surface_result(
        execute_scalar_select_for_tests::<SessionSqlEntity>(&session, update_returning_sql),
        "scalar SELECT helper should reject UPDATE lane even when RETURNING is present",
    );
    let updated_rows =
        statement_projection_rows::<SessionSqlEntity>(&session, update_returning_sql)
            .expect("statement execution should admit UPDATE RETURNING");
    assert_eq!(updated_rows.len(), 1);
    assert_eq!(updated_rows[0].len(), 2);
    assert!(
        matches!(updated_rows[0][0], Value::Ulid(_)),
        "statement UPDATE RETURNING should project the requested generated id",
    );
    assert_eq!(
        updated_rows[0][1],
        Value::Uint(22),
        "statement UPDATE RETURNING should project the updated scalar field",
    );
    assert_unsupported_sql_surface_result(
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, update_returning_sql, None),
        "grouped SELECT helper should reject UPDATE lane even when RETURNING is present",
    );

    let delete_returning_sql =
        "DELETE FROM SessionSqlEntity WHERE age > 20 ORDER BY age ASC LIMIT 1 RETURNING id";

    let query_from_err =
        lower_select_query_for_tests::<SessionSqlEntity>(&session, delete_returning_sql)
            .map(|_| ())
            .expect_err("SQL query lowering should reject DELETE RETURNING");
    assert!(
        query_from_err
            .to_string()
            .contains("DELETE RETURNING; use execute_sql_update::<E>()"),
        "SQL query lowering should preserve explicit DELETE RETURNING guidance",
    );

    let execute_sql_err =
        execute_scalar_select_for_tests::<SessionSqlEntity>(&session, delete_returning_sql)
            .map(|_| ())
            .expect_err("scalar SELECT helper should reject DELETE entirely");
    assert!(
        execute_sql_err
            .to_string()
            .contains("scalar SELECT helper rejects DELETE; use execute_sql_update::<E>()"),
        "scalar SELECT helper should preserve explicit fluent delete guidance",
    );

    let grouped_err =
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, delete_returning_sql, None)
            .map(|_| ())
            .expect_err("grouped SELECT helper should still reject DELETE at the grouped surface");
    assert!(
        grouped_err
            .to_string()
            .contains("grouped SELECT helper rejects DELETE"),
        "grouped SQL surface should preserve its own lane boundary before RETURNING guidance",
    );
}

#[test]
fn execute_sql_statement_admits_supported_single_entity_read_shapes() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let scalar = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
    )
    .expect("execute_sql_statement should admit scalar SELECT");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = scalar
    else {
        panic!("execute_sql_statement scalar SELECT should emit projection rows");
    };
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(rows, vec![vec![output(Value::Text("ada".to_string()))]]);
    assert_eq!(row_count, 1);

    let grouped = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
    )
    .expect("execute_sql_statement should admit grouped SELECT");
    let SqlStatementResult::Grouped {
        columns, row_count, ..
    } = grouped
    else {
        panic!("execute_sql_statement grouped SELECT should emit grouped rows");
    };
    assert_eq!(columns, vec!["age".to_string(), "COUNT(*)".to_string()]);
    assert_eq!(row_count, 2);

    let aggregate = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity",
    )
    .expect("execute_sql_statement should admit global aggregate SELECT");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = aggregate
    else {
        panic!("execute_sql_statement aggregate SELECT should emit projection rows");
    };
    assert_eq!(columns, vec!["COUNT(*)".to_string()]);
    assert_eq!(rows, vec![vec![output(Value::Uint(3))]]);
    assert_eq!(row_count, 1);
}

#[test]
fn execute_sql_statement_rejects_unsupported_schema_transition_before_select_compile() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlWriteEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("SQL SELECT schema-transition fixture row should save");
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_session_sql_write_old_accepted_schema_prefix();

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "SELECT id, name FROM SessionSqlWriteEntity WHERE id = 1",
    )
    .expect_err("SQL SELECT should reject unsupported accepted schema drift");
    let err_text = err.to_string();

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert!(
        err_text.contains("schema evolution is not yet supported")
            && err_text.contains("unsupported additive field transition"),
        "SQL SELECT should surface the schema-transition barrier: {err_text}",
    );
}

#[test]
fn execute_sql_statement_reads_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1480);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let result = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name, nickname FROM SessionNullableSqlEntity",
    )
    .expect("SQL SELECT should accept nullable append-only schema transition");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("SQL SELECT over old nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(columns, vec!["name".to_string(), "nickname".to_string()]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada".to_string())),
            output(Value::Null)
        ]],
    );
    assert_eq!(row_count, 1);
}

#[test]
fn compiled_sql_query_reads_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    insert_old_nullable_sql_row_for_test(Ulid::from_u128(1493), "Ada");

    let compiled = session
        .compile_sql_query::<SessionNullableSqlEntity>(
            "SELECT name, nickname FROM SessionNullableSqlEntity",
        )
        .expect("compiled SQL SELECT should accept nullable append-only schema transition");
    let result = session
        .execute_compiled_sql::<SessionNullableSqlEntity>(&compiled)
        .expect("compiled SQL SELECT should read old row after nullable transition");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("compiled SQL SELECT over old nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(columns, vec!["name".to_string(), "nickname".to_string()]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada".to_string())),
            output(Value::Null)
        ]],
    );
    assert_eq!(row_count, 1);
}

#[test]
fn execute_sql_statement_filters_old_rows_by_added_nullable_field_null_semantics() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    insert_old_nullable_sql_row_for_test(Ulid::from_u128(1497), "Ada");

    let null_result = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name FROM SessionNullableSqlEntity WHERE nickname IS NULL",
    )
    .expect("SQL SELECT should evaluate appended nullable field IS NULL on old row");
    let not_null_result = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name FROM SessionNullableSqlEntity WHERE nickname IS NOT NULL",
    )
    .expect("SQL SELECT should evaluate appended nullable field IS NOT NULL on old row");
    let eq_null_result = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name FROM SessionNullableSqlEntity WHERE nickname = NULL",
    )
    .expect("SQL SELECT should preserve = NULL semantics on appended nullable field");

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_projection_rows(
        null_result,
        ["name"],
        vec![vec![output(Value::Text("Ada".to_string()))]],
        "appended nullable field IS NULL should match old missing-slot rows",
    );
    assert_projection_rows(
        not_null_result,
        ["name"],
        Vec::<Vec<OutputValue>>::new(),
        "appended nullable field IS NOT NULL should not match old missing-slot rows",
    );
    assert_projection_rows(
        eq_null_result,
        ["name"],
        Vec::<Vec<OutputValue>>::new(),
        "appended nullable field = NULL should remain distinct from IS NULL",
    );
}

#[test]
fn compiled_sql_query_filters_old_rows_by_added_nullable_field_null_semantics() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    insert_old_nullable_sql_row_for_test(Ulid::from_u128(1498), "Ada");

    let compiled = session
        .compile_sql_query::<SessionNullableSqlEntity>(
            "SELECT name FROM SessionNullableSqlEntity WHERE nickname IS NULL",
        )
        .expect("compiled SQL SELECT should accept appended nullable field predicate");
    let result = session
        .execute_compiled_sql::<SessionNullableSqlEntity>(&compiled)
        .expect("compiled SQL SELECT should evaluate appended nullable field predicate");

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_projection_rows(
        result,
        ["name"],
        vec![vec![output(Value::Text("Ada".to_string()))]],
        "compiled appended nullable field IS NULL should match old missing-slot rows",
    );
}

#[test]
fn sql_metadata_surfaces_show_added_nullable_field_after_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();

    let described = statement_describe_sql::<SessionNullableSqlEntity>(
        &session,
        "DESCRIBE SessionNullableSqlEntity",
    )
    .expect("DESCRIBE should accept nullable append-only schema transition");
    let columns = statement_show_columns_sql::<SessionNullableSqlEntity>(
        &session,
        "SHOW COLUMNS SessionNullableSqlEntity",
    )
    .expect("SHOW COLUMNS should accept nullable append-only schema transition");

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(
        described.fields(),
        columns.as_slice(),
        "DESCRIBE and SHOW COLUMNS should share accepted-schema column rows",
    );
    assert_eq!(
        columns
            .iter()
            .map(|field| (
                field.name().to_string(),
                field.slot(),
                field.kind().to_string()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("id".to_string(), Some(0), "ulid".to_string()),
            ("name".to_string(), Some(1), "text(unbounded)".to_string()),
            (
                "nickname".to_string(),
                Some(2),
                "text(unbounded)".to_string(),
            ),
        ],
    );
}

#[test]
fn compiled_sql_update_rewrites_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1494);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let compiled = session
        .compile_sql_update::<SessionNullableSqlEntity>(
            "UPDATE SessionNullableSqlEntity SET name = 'Ada Lovelace' WHERE name = 'Ada'",
        )
        .expect("compiled SQL UPDATE should accept nullable append-only schema transition");
    let result = session
        .execute_compiled_sql::<SessionNullableSqlEntity>(&compiled)
        .expect("compiled SQL UPDATE should rewrite old row after nullable transition");
    let SqlStatementResult::Count { row_count } = result else {
        panic!("compiled SQL UPDATE over old nullable row should emit count result");
    };
    let raw_row = nullable_sql_raw_row_for_test(id);
    let decoded = raw_row
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("compiled SQL UPDATE should write back a current-layout row");

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_count, 1);
    assert_eq!(decoded.name, "Ada Lovelace");
    assert_eq!(decoded.nickname, None);
}

#[test]
fn compiled_sql_delete_removes_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1495);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let compiled = session
        .compile_sql_update::<SessionNullableSqlEntity>(
            "DELETE FROM SessionNullableSqlEntity WHERE name = 'Ada'",
        )
        .expect("compiled SQL DELETE should accept nullable append-only schema transition");
    let result = session
        .execute_compiled_sql::<SessionNullableSqlEntity>(&compiled)
        .expect("compiled SQL DELETE should remove old row after nullable transition");
    let SqlStatementResult::Count { row_count } = result else {
        panic!("compiled SQL DELETE over old nullable row should emit count result");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_count, 1);
    SESSION_SQL_DATA_STORE.with_borrow(|store| {
        let key = DataKey::try_new::<SessionNullableSqlEntity>(id)
            .expect("old nullable SQL data key should build")
            .to_raw()
            .expect("old nullable SQL data key should encode");

        assert!(store.get(&key).is_none());
    });
}

#[test]
fn compiled_sql_delete_returning_projects_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1496);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let compiled = session
        .compile_sql_update::<SessionNullableSqlEntity>(
            "DELETE FROM SessionNullableSqlEntity WHERE name = 'Ada' RETURNING name, nickname",
        )
        .expect(
            "compiled SQL DELETE RETURNING should accept nullable append-only schema transition",
        );
    let result = session
        .execute_compiled_sql::<SessionNullableSqlEntity>(&compiled)
        .expect("compiled SQL DELETE RETURNING should project old row after nullable transition");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("compiled SQL DELETE RETURNING over old nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(columns, vec!["name".to_string(), "nickname".to_string()]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada".to_string())),
            output(Value::Null)
        ]],
    );
    assert_eq!(row_count, 1);
    SESSION_SQL_DATA_STORE.with_borrow(|store| {
        let key = DataKey::try_new::<SessionNullableSqlEntity>(id)
            .expect("old nullable SQL data key should build")
            .to_raw()
            .expect("old nullable SQL data key should encode");

        assert!(store.get(&key).is_none());
    });
}

#[test]
fn fluent_load_reads_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1484);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let rows = session
        .execute_query(&Query::<SessionNullableSqlEntity>::new(
            MissingRowPolicy::Ignore,
        ))
        .expect("fluent load should accept old row after nullable append-only schema transition")
        .rows();
    let [(row_id, entity)] = rows
        .into_iter()
        .map(Row::into_parts)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|rows: Vec<_>| {
            panic!("fluent load should return exactly one nullable row, got {rows:?}")
        });

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_id.key(), id);
    assert_eq!(entity.name, "Ada");
    assert_eq!(entity.nickname, None);
}

#[test]
fn fluent_take_reads_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1486);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let rows = session
        .load::<SessionNullableSqlEntity>()
        .take(1)
        .expect("fluent take should accept old row after nullable append-only schema transition")
        .rows();
    let [(row_id, entity)] = rows
        .into_iter()
        .map(Row::into_parts)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|rows: Vec<_>| {
            panic!("fluent take should return exactly one nullable row, got {rows:?}")
        });

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_id.key(), id);
    assert_eq!(entity.name, "Ada");
    assert_eq!(entity.nickname, None);
}

#[test]
fn fluent_paged_load_reads_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1487);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let (response, continuation_cursor) = session
        .load::<SessionNullableSqlEntity>()
        .order_term(asc("id"))
        .limit(1)
        .execute_paged()
        .expect(
            "fluent paged load should accept old row after nullable append-only schema transition",
        )
        .into_parts();
    let [(row_id, entity)] = response
        .rows()
        .into_iter()
        .map(Row::into_parts)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|rows: Vec<_>| {
            panic!("fluent paged load should return exactly one nullable row, got {rows:?}")
        });

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_id.key(), id);
    assert_eq!(entity.name, "Ada");
    assert_eq!(entity.nickname, None);
    assert!(continuation_cursor.is_none());
}

#[test]
fn fluent_top_k_reads_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1488);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let rows = session
        .load::<SessionNullableSqlEntity>()
        .top_k_by("name", 1)
        .expect("fluent top-k should accept old row after nullable append-only schema transition")
        .rows();
    let [(row_id, entity)] = rows
        .into_iter()
        .map(Row::into_parts)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|rows: Vec<_>| {
            panic!("fluent top-k should return exactly one nullable row, got {rows:?}")
        });

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_id.key(), id);
    assert_eq!(entity.name, "Ada");
    assert_eq!(entity.nickname, None);
}

#[test]
fn fluent_delete_returns_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1485);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let rows = session
        .delete::<SessionNullableSqlEntity>()
        .filter(FieldRef::new("name").eq("Ada"))
        .execute_rows()
        .expect(
            "fluent delete rows should accept old row after nullable append-only schema transition",
        )
        .rows();
    let [(row_id, entity)] = rows
        .into_iter()
        .map(Row::into_parts)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|rows: Vec<_>| {
            panic!("fluent delete rows should return exactly one nullable row, got {rows:?}")
        });

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_id.key(), id);
    assert_eq!(entity.name, "Ada");
    assert_eq!(entity.nickname, None);
    SESSION_SQL_DATA_STORE.with_borrow(|store| {
        let key = DataKey::try_new::<SessionNullableSqlEntity>(id)
            .expect("old nullable SQL data key should build")
            .to_raw()
            .expect("old nullable SQL data key should encode");

        assert!(store.get(&key).is_none());
    });
}

#[test]
fn fluent_delete_count_removes_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1491);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let count = session
        .delete::<SessionNullableSqlEntity>()
        .filter(FieldRef::new("name").eq("Ada"))
        .execute()
        .expect("fluent delete count should accept old row after nullable append-only schema transition");

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(count, 1);
    SESSION_SQL_DATA_STORE.with_borrow(|store| {
        let key = DataKey::try_new::<SessionNullableSqlEntity>(id)
            .expect("old nullable SQL data key should build")
            .to_raw()
            .expect("old nullable SQL data key should encode");

        assert!(store.get(&key).is_none());
    });
}

#[test]
fn structural_update_rewrites_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1490);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let patch = session
        .structural_patch::<SessionNullableSqlEntity, _, _>([(
            "name",
            Value::Text("Ada Byron".to_string()),
        )])
        .expect("structural patch should resolve accepted nullable field layout");
    let updated = session
        .mutate_structural::<SessionNullableSqlEntity>(id, patch, MutationMode::Update)
        .expect("structural update should rewrite old row through accepted nullable transition");
    let raw_row = nullable_sql_raw_row_for_test(id);
    let decoded = raw_row
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("structural update should write back a current-layout row");
    let selected = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name, nickname FROM SessionNullableSqlEntity",
    )
    .expect("SQL SELECT should read the structurally updated nullable row");
    let SqlStatementResult::Projection { rows, .. } = selected else {
        panic!("SQL SELECT over structurally updated nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(updated.name, "Ada Byron");
    assert_eq!(updated.nickname, None);
    assert_eq!(decoded.name, "Ada Byron");
    assert_eq!(decoded.nickname, None);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada Byron".to_string())),
            output(Value::Null)
        ]],
    );
}

#[test]
fn structural_update_sets_appended_nullable_field_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1492);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let patch = session
        .structural_patch::<SessionNullableSqlEntity, _, _>([(
            "nickname",
            Value::Text("Countess".to_string()),
        )])
        .expect("structural patch should resolve appended nullable field");
    let updated = session
        .mutate_structural::<SessionNullableSqlEntity>(id, patch, MutationMode::Update)
        .expect("structural update should set appended nullable field on old row");
    let raw_row = nullable_sql_raw_row_for_test(id);
    let decoded = raw_row
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("structural update of appended field should write current-layout row");
    let selected = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name, nickname FROM SessionNullableSqlEntity",
    )
    .expect("SQL SELECT should read the structurally updated appended field");
    let SqlStatementResult::Projection { rows, .. } = selected else {
        panic!("SQL SELECT over appended-field structural update should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(updated.name, "Ada");
    assert_eq!(updated.nickname.as_deref(), Some("Countess"));
    assert_eq!(decoded.name, "Ada");
    assert_eq!(decoded.nickname.as_deref(), Some("Countess"));
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada".to_string())),
            output(Value::Text("Countess".to_string()))
        ]],
    );
}

#[test]
fn typed_update_rewrites_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1497);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let updated = session
        .update(SessionNullableSqlEntity {
            id,
            name: "Ada King".to_string(),
            nickname: None,
        })
        .expect("typed update should rewrite old row through accepted nullable transition");
    let raw_row = nullable_sql_raw_row_for_test(id);
    let decoded = raw_row
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("typed update should write back a current-layout row");
    let selected = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name, nickname FROM SessionNullableSqlEntity",
    )
    .expect("SQL SELECT should read the typed-updated nullable row");
    let SqlStatementResult::Projection { rows, .. } = selected else {
        panic!("SQL SELECT over typed-updated nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(updated.name, "Ada King");
    assert_eq!(updated.nickname, None);
    assert_eq!(decoded.name, "Ada King");
    assert_eq!(decoded.nickname, None);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada King".to_string())),
            output(Value::Null)
        ]],
    );
}

#[test]
fn typed_replace_rewrites_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1498);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let replaced = session
        .replace(SessionNullableSqlEntity {
            id,
            name: "Ada Augusta".to_string(),
            nickname: Some("Enchantress".to_string()),
        })
        .expect("typed replace should rewrite old row through accepted nullable transition");
    let raw_row = nullable_sql_raw_row_for_test(id);
    let decoded = raw_row
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("typed replace should write back a current-layout row");
    let selected = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name, nickname FROM SessionNullableSqlEntity",
    )
    .expect("SQL SELECT should read the typed-replaced nullable row");
    let SqlStatementResult::Projection { rows, .. } = selected else {
        panic!("SQL SELECT over typed-replaced nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(replaced.name, "Ada Augusta");
    assert_eq!(replaced.nickname.as_deref(), Some("Enchantress"));
    assert_eq!(decoded.name, "Ada Augusta");
    assert_eq!(decoded.nickname.as_deref(), Some("Enchantress"));
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada Augusta".to_string())),
            output(Value::Text("Enchantress".to_string()))
        ]],
    );
}

#[test]
fn typed_insert_writes_current_layout_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1504);

    let inserted = session
        .insert(SessionNullableSqlEntity {
            id,
            name: "Ada Fresh".to_string(),
            nickname: None,
        })
        .expect("typed insert should write current layout after nullable transition");
    let raw_row = nullable_sql_raw_row_for_test(id);
    let decoded = raw_row
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("typed insert should emit a current-layout row");

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(inserted.name, "Ada Fresh");
    assert_eq!(inserted.nickname, None);
    assert_eq!(decoded.name, "Ada Fresh");
    assert_eq!(decoded.nickname, None);
    assert_eq!(
        raw_row_slot_count_for_test(&raw_row),
        3,
        "fresh inserts after transition must emit the current accepted slot count",
    );
}

#[test]
fn typed_insert_existing_old_row_reports_conflict_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1499);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let err = session
        .insert(SessionNullableSqlEntity {
            id,
            name: "Ada Insert".to_string(),
            nickname: Some("Duplicate".to_string()),
        })
        .expect_err("typed insert should report conflict for an existing old row");
    let selected = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name, nickname FROM SessionNullableSqlEntity",
    )
    .expect("SQL SELECT should still read the unchanged old nullable row");
    let SqlStatementResult::Projection { rows, .. } = selected else {
        panic!("SQL SELECT over unchanged nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(err.class(), ErrorClass::Conflict);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada".to_string())),
            output(Value::Null)
        ]],
    );
}

#[test]
fn typed_update_many_atomic_rewrites_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let first_id = Ulid::from_u128(1500);
    let second_id = Ulid::from_u128(1501);
    insert_old_nullable_sql_row_for_test(first_id, "Ada");
    insert_old_nullable_sql_row_for_test(second_id, "Grace");

    let updated = session
        .update_many_atomic([
            SessionNullableSqlEntity {
                id: first_id,
                name: "Ada King".to_string(),
                nickname: Some("Countess".to_string()),
            },
            SessionNullableSqlEntity {
                id: second_id,
                name: "Grace Hopper".to_string(),
                nickname: None,
            },
        ])
        .expect("typed atomic update batch should rewrite old rows through accepted nullable transition");
    let first_decoded = nullable_sql_raw_row_for_test(first_id)
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("typed atomic update batch should rewrite first row to current layout");
    let second_decoded = nullable_sql_raw_row_for_test(second_id)
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("typed atomic update batch should rewrite second row to current layout");

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(updated.len(), 2);
    assert_eq!(first_decoded.name, "Ada King");
    assert_eq!(first_decoded.nickname.as_deref(), Some("Countess"));
    assert_eq!(second_decoded.name, "Grace Hopper");
    assert_eq!(second_decoded.nickname, None);
}

#[test]
fn typed_replace_many_non_atomic_rewrites_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let first_id = Ulid::from_u128(1502);
    let second_id = Ulid::from_u128(1503);
    insert_old_nullable_sql_row_for_test(first_id, "Ada");
    insert_old_nullable_sql_row_for_test(second_id, "Grace");

    let replaced = session
        .replace_many_non_atomic([
            SessionNullableSqlEntity {
                id: first_id,
                name: "Ada Byron".to_string(),
                nickname: None,
            },
            SessionNullableSqlEntity {
                id: second_id,
                name: "Grace Brewster".to_string(),
                nickname: Some("Amazing Grace".to_string()),
            },
        ])
        .expect("typed non-atomic replace batch should rewrite old rows through accepted nullable transition");
    let first_decoded = nullable_sql_raw_row_for_test(first_id)
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("typed non-atomic replace batch should rewrite first row to current layout");
    let second_decoded = nullable_sql_raw_row_for_test(second_id)
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("typed non-atomic replace batch should rewrite second row to current layout");

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(replaced.len(), 2);
    assert_eq!(first_decoded.name, "Ada Byron");
    assert_eq!(first_decoded.nickname, None);
    assert_eq!(second_decoded.name, "Grace Brewster");
    assert_eq!(second_decoded.nickname.as_deref(), Some("Amazing Grace"));
}

#[test]
fn execute_sql_update_rewrites_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1481);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let result = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "UPDATE SessionNullableSqlEntity SET name = 'Ada Lovelace' WHERE name = 'Ada'",
    )
    .expect("SQL UPDATE should accept old row after nullable append-only schema transition");
    let SqlStatementResult::Count { row_count } = result else {
        panic!("SQL UPDATE over old nullable row should emit a count result");
    };
    let raw_row = nullable_sql_raw_row_for_test(id);
    let decoded = raw_row
        .try_decode_with_generated_model_for_test::<SessionNullableSqlEntity>()
        .expect("SQL UPDATE should rewrite the old short row as a current-layout row");
    let selected = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name, nickname FROM SessionNullableSqlEntity",
    )
    .expect("SQL SELECT should read the updated nullable row");
    let SqlStatementResult::Projection { rows, .. } = selected else {
        panic!("SQL SELECT over updated nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_count, 1);
    assert_eq!(decoded.name, "Ada Lovelace");
    assert_eq!(decoded.nickname, None);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada Lovelace".to_string())),
            output(Value::Null)
        ]],
    );
}

#[test]
fn execute_sql_delete_removes_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1482);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let result = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "DELETE FROM SessionNullableSqlEntity WHERE name = 'Ada'",
    )
    .expect("SQL DELETE should accept old row after nullable append-only schema transition");
    let SqlStatementResult::Count { row_count } = result else {
        panic!("SQL DELETE over old nullable row should emit a count result");
    };
    let selected = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "SELECT name, nickname FROM SessionNullableSqlEntity",
    )
    .expect("SQL SELECT should read after deleting the old nullable row");
    let SqlStatementResult::Projection {
        rows,
        row_count: remaining_count,
        ..
    } = selected
    else {
        panic!("SQL SELECT after deleting old nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(row_count, 1);
    assert_eq!(remaining_count, 0);
    assert!(rows.is_empty());
}

#[test]
fn execute_sql_delete_returning_projects_old_rows_after_nullable_additive_schema_transition() {
    reset_session_sql_store();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_nullable_sql_old_accepted_schema_prefix();
    let session = sql_session();
    let id = Ulid::from_u128(1483);
    insert_old_nullable_sql_row_for_test(id, "Ada");

    let result = execute_sql_statement_for_tests::<SessionNullableSqlEntity>(
        &session,
        "DELETE FROM SessionNullableSqlEntity WHERE name = 'Ada' RETURNING name, nickname",
    )
    .expect("SQL DELETE RETURNING should project old row through accepted nullable transition");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("SQL DELETE RETURNING over old nullable row should emit projection rows");
    };

    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);

    assert_eq!(columns, vec!["name".to_string(), "nickname".to_string()]);
    assert_eq!(row_count, 1);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada".to_string())),
            output(Value::Null)
        ]],
    );
    SESSION_SQL_DATA_STORE.with_borrow(|store| {
        let key = DataKey::try_new::<SessionNullableSqlEntity>(id)
            .expect("old nullable SQL data key should build")
            .to_raw()
            .expect("old nullable SQL data key should encode");

        assert!(store.get(&key).is_none());
    });
}

#[test]
fn execute_sql_statement_admits_supported_single_entity_mutation_shapes() {
    reset_session_sql_store();
    let session = sql_session();

    let insert = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
    )
    .expect("execute_sql_statement should admit INSERT");
    let SqlStatementResult::Count { row_count } = insert else {
        panic!("execute_sql_statement INSERT should emit count payload");
    };
    assert_eq!(row_count, 1);

    let update = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
    )
    .expect("execute_sql_statement should admit UPDATE");
    let SqlStatementResult::Count { row_count } = update else {
        panic!("execute_sql_statement UPDATE should emit count payload");
    };
    assert_eq!(row_count, 1);

    let delete = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "DELETE FROM SessionSqlWriteEntity WHERE name = 'Ada' RETURNING name",
    )
    .expect("execute_sql_statement should admit DELETE RETURNING");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = delete
    else {
        panic!("execute_sql_statement DELETE RETURNING should emit projection rows");
    };
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(rows, vec![vec![output(Value::Text("Ada".to_string()))]]);
    assert_eq!(row_count, 1);
}

#[test]
fn execute_sql_query_rejects_supported_single_entity_mutation_shapes() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected, context) in [
        (
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "execute_sql_query rejects INSERT; use execute_sql_update::<E>()",
            "query SQL surface should reject INSERT",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
            "execute_sql_query rejects UPDATE; use execute_sql_update::<E>()",
            "query SQL surface should reject UPDATE",
        ),
        (
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1",
            "execute_sql_query rejects DELETE; use execute_sql_update::<E>()",
            "query SQL surface should reject DELETE",
        ),
    ] {
        let err = session
            .execute_sql_query::<SessionSqlWriteEntity>(sql)
            .expect_err(context);
        assert!(
            err.to_string().contains(expected),
            "{context} should preserve the query-to-update guidance",
        );
    }
}

#[test]
fn execute_sql_query_admits_supported_single_entity_read_shapes() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let scalar = session
        .execute_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("execute_sql_query should admit scalar SELECT");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = scalar
    else {
        panic!("execute_sql_query scalar SELECT should emit projection rows");
    };
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(rows, vec![vec![output(Value::Text("ada".to_string()))]]);
    assert_eq!(row_count, 1);

    let grouped = session
        .execute_sql_query::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect("execute_sql_query should admit grouped SELECT");
    let SqlStatementResult::Grouped {
        columns, row_count, ..
    } = grouped
    else {
        panic!("execute_sql_query grouped SELECT should emit grouped rows");
    };
    assert_eq!(columns, vec!["age".to_string(), "COUNT(*)".to_string()]);
    assert_eq!(row_count, 2);

    let aggregate = session
        .execute_sql_query::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("execute_sql_query should admit global aggregate SELECT");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = aggregate
    else {
        panic!("execute_sql_query aggregate SELECT should emit projection rows");
    };
    assert_eq!(columns, vec!["COUNT(*)".to_string()]);
    assert_eq!(rows, vec![vec![output(Value::Uint(3))]]);
    assert_eq!(row_count, 1);
}

#[expect(
    clippy::too_many_lines,
    reason = "artifact-family matrix intentionally keeps one representative read-lane table together"
)]
#[test]
fn compile_sql_query_and_execute_compiled_preserve_supported_read_families() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let scalar = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("scalar SELECT should compile");
    assert!(
        matches!(
            scalar,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "scalar SELECT should compile to lowered SELECT artifact",
    );
    let SqlStatementResult::Projection { row_count, .. } = session
        .execute_compiled_sql::<SessionSqlEntity>(&scalar)
        .expect("compiled scalar SELECT should execute")
    else {
        panic!("compiled scalar SELECT should emit projection rows");
    };
    assert_eq!(row_count, 1);

    let grouped = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect("grouped SELECT should compile");
    assert!(
        matches!(
            grouped,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "grouped SELECT should stay on the lowered SELECT artifact family",
    );
    let SqlStatementResult::Grouped { row_count, .. } = session
        .execute_compiled_sql::<SessionSqlEntity>(&grouped)
        .expect("compiled grouped SELECT should execute")
    else {
        panic!("compiled grouped SELECT should emit grouped rows");
    };
    assert_eq!(row_count, 2);

    let aggregate = session
        .compile_sql_query::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("global aggregate SELECT should compile");
    assert!(
        matches!(
            aggregate,
            crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. }
        ),
        "global aggregate SELECT should compile to dedicated aggregate artifact",
    );
    let SqlStatementResult::Projection { row_count, .. } = session
        .execute_compiled_sql::<SessionSqlEntity>(&aggregate)
        .expect("compiled aggregate SELECT should execute")
    else {
        panic!("compiled aggregate SELECT should emit projection rows");
    };
    assert_eq!(row_count, 1);

    let aggregate_with_having_alias = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT COUNT(*) AS total_rows \
             FROM SessionSqlEntity \
             HAVING total_rows > 1",
        )
        .expect("aliased global aggregate HAVING should compile");
    assert!(
        matches!(
            aggregate_with_having_alias,
            crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. }
        ),
        "aliased global aggregate HAVING should stay on the dedicated aggregate artifact family",
    );

    let aggregate_without_else_truth_wrapper = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT COUNT(*) \
             FROM SessionSqlEntity \
             HAVING CASE WHEN (COUNT(*) > 1) = TRUE THEN TRUE END",
        )
        .expect("truth-wrapped global aggregate omitted-ELSE HAVING should compile");
    let aggregate_explicit_null = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT COUNT(*) \
             FROM SessionSqlEntity \
             HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE NULL END",
        )
        .expect("explicit ELSE NULL global aggregate HAVING should compile");
    assert!(
        matches!(
            aggregate_without_else_truth_wrapper,
            crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. }
        ),
        "truth-wrapped global aggregate omitted-ELSE HAVING should stay on the dedicated aggregate artifact family",
    );
    assert!(
        matches!(
            aggregate_explicit_null,
            crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. }
        ),
        "explicit ELSE NULL global aggregate HAVING should stay on the dedicated aggregate artifact family",
    );

    let explain = session
        .compile_sql_query::<SessionSqlEntity>(
            "EXPLAIN SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("EXPLAIN SELECT should compile");
    assert!(
        matches!(
            explain,
            crate::db::session::sql::CompiledSqlCommand::Explain(_)
        ),
        "EXPLAIN SELECT should compile to lowered explain artifact",
    );
    let SqlStatementResult::Explain(rendered) = session
        .execute_compiled_sql::<SessionSqlEntity>(&explain)
        .expect("compiled EXPLAIN should execute")
    else {
        panic!("compiled EXPLAIN should emit explain text");
    };
    assert!(
        !rendered.is_empty(),
        "compiled EXPLAIN should render a non-empty explain payload",
    );
}

#[test]
fn execute_sql_update_admits_supported_single_entity_mutation_shapes() {
    reset_session_sql_store();
    let session = sql_session();

    let insert = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("execute_sql_update should admit INSERT");
    let SqlStatementResult::Count { row_count } = insert else {
        panic!("execute_sql_update INSERT should emit count payload");
    };
    assert_eq!(row_count, 1);

    let update = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        )
        .expect("execute_sql_update should admit UPDATE");
    let SqlStatementResult::Count { row_count } = update else {
        panic!("execute_sql_update UPDATE should emit count payload");
    };
    assert_eq!(row_count, 1);

    let delete = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE name = 'Ada' RETURNING name",
        )
        .expect("execute_sql_update should admit DELETE RETURNING");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = delete
    else {
        panic!("execute_sql_update DELETE RETURNING should emit projection rows");
    };
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(rows, vec![vec![output(Value::Text("Ada".to_string()))]]);
    assert_eq!(row_count, 1);
}

#[test]
fn compile_sql_update_and_execute_compiled_preserve_supported_mutation_families() {
    reset_session_sql_store();
    let session = sql_session();

    let insert = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("INSERT should compile");
    assert!(
        matches!(
            insert,
            crate::db::session::sql::CompiledSqlCommand::Insert(_)
        ),
        "INSERT should compile to prepared INSERT artifact",
    );
    let SqlStatementResult::Count { row_count } = session
        .execute_compiled_sql::<SessionSqlWriteEntity>(&insert)
        .expect("compiled INSERT should execute")
    else {
        panic!("compiled INSERT should emit count payload");
    };
    assert_eq!(row_count, 1);

    let update = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        )
        .expect("UPDATE should compile");
    assert!(
        matches!(
            update,
            crate::db::session::sql::CompiledSqlCommand::Update(_)
        ),
        "UPDATE should compile to prepared UPDATE artifact",
    );
    let SqlStatementResult::Count { row_count } = session
        .execute_compiled_sql::<SessionSqlWriteEntity>(&update)
        .expect("compiled UPDATE should execute")
    else {
        panic!("compiled UPDATE should emit count payload");
    };
    assert_eq!(row_count, 1);

    let delete = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE name = 'Ada' RETURNING name",
        )
        .expect("DELETE RETURNING should compile through update surface");
    let crate::db::session::sql::CompiledSqlCommand::Delete { returning, .. } = &delete else {
        panic!("DELETE RETURNING should compile to lowered DELETE artifact");
    };
    assert!(
        matches!(
            returning,
            Some(crate::db::sql::parser::SqlReturningProjection::Fields(fields))
                if matches!(fields.as_slice(), [field] if field == "name")
        ),
        "compiled DELETE artifact should retain only the RETURNING projection contract",
    );
    let SqlStatementResult::Projection { row_count, .. } = session
        .execute_compiled_sql::<SessionSqlWriteEntity>(&delete)
        .expect("compiled DELETE RETURNING should execute")
    else {
        panic!("compiled DELETE RETURNING should emit projection rows");
    };
    assert_eq!(row_count, 1);
}

#[test]
fn sql_compile_cache_keeps_query_and_update_surfaces_separate() {
    reset_session_sql_store();
    let session = sql_session();

    let insert_sql = "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)";
    let insert = session
        .compile_sql_update::<SessionSqlWriteEntity>(insert_sql)
        .expect("update surface should compile INSERT into the session-local cache");
    assert!(
        matches!(
            insert,
            crate::db::session::sql::CompiledSqlCommand::Insert(_)
        ),
        "update surface should cache the INSERT artifact under the update lane only",
    );

    let err = session
        .compile_sql_query::<SessionSqlWriteEntity>(insert_sql)
        .expect_err("query surface must not reuse the cached update artifact");
    assert!(
        err.to_string()
            .contains("execute_sql_query rejects INSERT; use execute_sql_update::<E>()"),
        "query surface should preserve its own lane boundary after an update-surface cache fill",
    );
}

#[test]
fn sql_compile_cache_covers_query_surface_read_explain_and_metadata_families() {
    reset_session_sql_store();
    let session = sql_session();

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "new SQL session should start with an empty compiled-command cache",
    );

    let scalar = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("scalar SELECT should compile into the query-surface cache");
    assert!(
        matches!(
            scalar,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "scalar SELECT should stay on the lowered SELECT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "first query-surface compile should populate one cache entry",
    );

    let scalar_repeat = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("same scalar SELECT should compile from the existing cache entry");
    assert!(
        matches!(
            scalar_repeat,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "repeated scalar SELECT should still resolve to the same artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical query-surface compile must not grow the cache",
    );
    assert_scalar_compiled_select_executes_through_shared_query_plan(&session, &scalar_repeat);

    let explain = session
        .compile_sql_query::<SessionSqlEntity>(
            "EXPLAIN SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("EXPLAIN should compile into the query-surface cache");
    assert!(
        matches!(
            explain,
            crate::db::session::sql::CompiledSqlCommand::Explain(_)
        ),
        "EXPLAIN should use its dedicated compiled artifact family",
    );

    let describe = session
        .compile_sql_query::<SessionSqlEntity>("DESCRIBE SessionSqlEntity")
        .expect("DESCRIBE should compile into the query-surface cache");
    assert!(
        matches!(
            describe,
            crate::db::session::sql::CompiledSqlCommand::DescribeEntity
        ),
        "DESCRIBE should cache its dedicated metadata artifact",
    );

    let show_indexes = session
        .compile_sql_query::<SessionSqlEntity>("SHOW INDEXES SessionSqlEntity")
        .expect("SHOW INDEXES should compile into the query-surface cache");
    assert!(
        matches!(
            show_indexes,
            crate::db::session::sql::CompiledSqlCommand::ShowIndexesEntity
        ),
        "SHOW INDEXES should cache its dedicated metadata artifact",
    );

    let show_columns = session
        .compile_sql_query::<SessionSqlEntity>("SHOW COLUMNS SessionSqlEntity")
        .expect("SHOW COLUMNS should compile into the query-surface cache");
    assert!(
        matches!(
            show_columns,
            crate::db::session::sql::CompiledSqlCommand::ShowColumnsEntity
        ),
        "SHOW COLUMNS should cache its dedicated metadata artifact",
    );

    let show_entities = session
        .compile_sql_query::<SessionSqlEntity>("SHOW ENTITIES")
        .expect("SHOW ENTITIES should compile into the query-surface cache");
    assert!(
        matches!(
            show_entities,
            crate::db::session::sql::CompiledSqlCommand::ShowEntities
        ),
        "SHOW ENTITIES should cache its dedicated metadata artifact",
    );

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        6,
        "query-surface cache should retain distinct entries for SELECT, EXPLAIN, and metadata families",
    );
}

fn assert_compiled_select_executes_through_shared_query_plan_for_entity<E>(
    session: &DbSession<SessionSqlCanister>,
    compiled: &crate::db::session::sql::CompiledSqlCommand,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let _ = session
        .execute_compiled_sql::<E>(compiled)
        .expect("executing one compiled SELECT should succeed through the shared lower cache");

    let _ = session.execute_compiled_sql::<E>(compiled).expect(
        "repeating one compiled SELECT should still succeed through the shared lower cache",
    );
}

fn assert_scalar_compiled_select_executes_through_shared_query_plan(
    session: &DbSession<SessionSqlCanister>,
    compiled: &crate::db::session::sql::CompiledSqlCommand,
) {
    assert_compiled_select_executes_through_shared_query_plan_for_entity::<SessionSqlEntity>(
        session, compiled,
    );
}

#[test]
fn shared_query_plan_cache_is_reused_by_fluent_and_sql_select_surfaces() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 21), ("Bob", 32)]);

    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "new session should start with an empty shared query-plan cache",
    );

    let sql = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("scalar SELECT * should compile");
    let _ = session
        .execute_compiled_sql::<SessionSqlEntity>(&sql)
        .expect("executing one compiled SQL select should populate the shared query-plan cache");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first SQL execution should populate one shared query-plan cache entry",
    );

    let fluent = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(1);
    let _ = session
        .execute_query(fluent.query())
        .expect("equivalent fluent query should execute");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "equivalent fluent execution should reuse the shared structural query-plan entry",
    );
}

#[test]
fn shared_query_plan_cache_reuses_canonical_equivalent_scalar_filter_forms() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 20), ("Bea", 30), ("Cara", 31)]);

    let searched_case_sql = "SELECT name \
                             FROM SessionSqlEntity \
                             WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
                             ORDER BY age ASC, id ASC LIMIT 2";
    let canonical_sql = "SELECT name \
                         FROM SessionSqlEntity \
                         WHERE COALESCE(age >= 30, FALSE) \
                            OR (NOT COALESCE(age >= 30, FALSE) AND age = 20) \
                         ORDER BY age ASC, id ASC LIMIT 2";

    let searched_case = session
        .compile_sql_query::<SessionSqlEntity>(searched_case_sql)
        .expect("searched CASE scalar filter query should compile");
    let canonical = session
        .compile_sql_query::<SessionSqlEntity>(canonical_sql)
        .expect("canonical scalar filter query should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        2,
        "different SQL spellings should remain distinct compiled-command cache entries",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "query-plan cache should still start empty before execution",
    );

    let _ = session
        .execute_compiled_sql::<SessionSqlEntity>(&searched_case)
        .expect("searched CASE scalar filter query should execute");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first canonical-equivalent scalar filter execution should populate one shared query-plan cache entry",
    );

    let _ = session
        .execute_compiled_sql::<SessionSqlEntity>(&canonical)
        .expect("canonical scalar filter query should execute");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "canonical-equivalent scalar filter execution should reuse the same shared query-plan cache entry",
    );
}

#[test]
fn shared_query_plan_cache_keeps_semantically_distinct_expression_filters_separate() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 20), ("Bea", 30), ("Cara", 31)]);

    let left_sql = "SELECT name \
                    FROM SessionSqlEntity \
                    WHERE COALESCE(age >= 30, FALSE) \
                       OR (NOT COALESCE(age >= 30, FALSE) AND age = 20) \
                    ORDER BY age ASC, id ASC LIMIT 2";
    let right_sql = "SELECT name \
                     FROM SessionSqlEntity \
                     WHERE COALESCE(age >= 31, FALSE) \
                        OR (NOT COALESCE(age >= 31, FALSE) AND age = 20) \
                     ORDER BY age ASC, id ASC LIMIT 2";

    let left = session
        .compile_sql_query::<SessionSqlEntity>(left_sql)
        .expect("left scalar filter query should compile");
    let right = session
        .compile_sql_query::<SessionSqlEntity>(right_sql)
        .expect("right scalar filter query should compile");

    let _ = session
        .execute_compiled_sql::<SessionSqlEntity>(&left)
        .expect("left scalar filter query should execute");
    let _ = session
        .execute_compiled_sql::<SessionSqlEntity>(&right)
        .expect("right scalar filter query should execute");

    assert_eq!(
        session.query_plan_cache_len(),
        2,
        "semantically distinct expression-owned scalar filters must not alias on the shared query-plan cache",
    );
}

#[test]
fn shared_query_plan_cache_keeps_is_true_and_is_not_true_filters_separate() {
    reset_session_sql_store();
    let session = sql_session();

    for (label, active, archived) in [
        ("bool-a", true, false),
        ("bool-b", false, false),
        ("bool-c", true, true),
    ] {
        session
            .insert(SessionSqlBoolCompareEntity {
                id: Ulid::generate(),
                label: label.to_string(),
                active,
                archived,
            })
            .expect("bool compare fixture insert should succeed");
    }

    let is_true = session
        .compile_sql_query::<SessionSqlBoolCompareEntity>(
            "SELECT label \
             FROM SessionSqlBoolCompareEntity \
             WHERE active IS TRUE \
             ORDER BY label ASC",
        )
        .expect("IS TRUE query should compile");
    let is_not_true = session
        .compile_sql_query::<SessionSqlBoolCompareEntity>(
            "SELECT label \
             FROM SessionSqlBoolCompareEntity \
             WHERE active IS NOT TRUE \
             ORDER BY label ASC",
        )
        .expect("IS NOT TRUE query should compile");

    let true_rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label FROM SessionSqlBoolCompareEntity WHERE active IS TRUE ORDER BY label ASC",
    )
    .expect("IS TRUE query should execute");
    let not_true_rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label FROM SessionSqlBoolCompareEntity WHERE active IS NOT TRUE ORDER BY label ASC",
    )
    .expect("IS NOT TRUE query should execute");

    assert_eq!(
        session.query_plan_cache_len(),
        2,
        "semantic negation must not alias shared query-plan cache identity with the positive boolean filter",
    );
    assert_ne!(
        true_rows, not_true_rows,
        "IS TRUE and IS NOT TRUE must not reuse the same compiled semantic plan",
    );

    let _ = is_true;
    let _ = is_not_true;
}

#[expect(
    clippy::too_many_lines,
    reason = "scalar truth-wrapper identity matrix intentionally proves one semantic boundary"
)]
#[test]
fn scalar_bool_truth_wrappers_reuse_semantic_identity() {
    reset_session_sql_store();
    let session = sql_session();

    for (label, active, archived) in [
        ("bool-a", true, false),
        ("bool-b", false, false),
        ("bool-c", true, true),
    ] {
        session
            .insert(SessionSqlBoolCompareEntity {
                id: Ulid::generate(),
                label: label.to_string(),
                active,
                archived,
            })
            .expect("bool compare fixture insert should succeed");
    }

    // Phase 1: compile four syntax-distinct spellings that should collapse into
    // two canonical scalar truth-condition identities.
    let is_true_sql = "SELECT label \
                       FROM SessionSqlBoolCompareEntity \
                       WHERE active IS TRUE \
                       ORDER BY label ASC";
    let bare_sql = "SELECT label \
                    FROM SessionSqlBoolCompareEntity \
                    WHERE active \
                    ORDER BY label ASC";
    let is_false_sql = "SELECT label \
                        FROM SessionSqlBoolCompareEntity \
                        WHERE active IS FALSE \
                        ORDER BY label ASC";
    let not_sql = "SELECT label \
                   FROM SessionSqlBoolCompareEntity \
                   WHERE NOT active \
                   ORDER BY label ASC";
    let is_true = session
        .compile_sql_query::<SessionSqlBoolCompareEntity>(is_true_sql)
        .expect("IS TRUE query should compile");
    let bare = session
        .compile_sql_query::<SessionSqlBoolCompareEntity>(bare_sql)
        .expect("bare bool truth query should compile");
    let is_false = session
        .compile_sql_query::<SessionSqlBoolCompareEntity>(is_false_sql)
        .expect("IS FALSE query should compile");
    let not_active = session
        .compile_sql_query::<SessionSqlBoolCompareEntity>(not_sql)
        .expect("NOT bool truth query should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        4,
        "scalar bool wrapper spellings should still occupy distinct compiled-command cache entries",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<SessionSqlBoolCompareEntity>(
        &is_true,
        is_true_sql,
        "IS TRUE bool truth query should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<SessionSqlBoolCompareEntity>(
        &bare,
        bare_sql,
        "bare bool truth query should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<SessionSqlBoolCompareEntity>(
        &is_false,
        is_false_sql,
        "IS FALSE bool truth query should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<SessionSqlBoolCompareEntity>(
        &not_active,
        not_sql,
        "NOT bool truth query should preserve canonical lowered identity",
    );

    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: is_true_query,
        ..
    } = &is_true
    else {
        panic!("IS TRUE query should compile into one SELECT artifact");
    };
    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: bare_query, ..
    } = &bare
    else {
        panic!("bare bool truth query should compile into one SELECT artifact");
    };
    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: is_false_query,
        ..
    } = &is_false
    else {
        panic!("IS FALSE query should compile into one SELECT artifact");
    };
    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: not_query, ..
    } = &not_active
    else {
        panic!("NOT bool truth query should compile into one SELECT artifact");
    };

    // Phase 2: require canonical truth-wrapper identities to agree on both the
    // structural cache key and the semantic plan fingerprint.
    assert_eq!(
        is_true_query.structural_cache_key(),
        bare_query.structural_cache_key(),
        "IS TRUE must collapse onto the same structural cache identity as the bare bool truth condition",
    );
    assert_eq!(
        is_true_query
            .build_plan()
            .expect("IS TRUE bool plan should build")
            .fingerprint(),
        bare_query
            .build_plan()
            .expect("bare bool truth plan should build")
            .fingerprint(),
        "IS TRUE must share semantic plan fingerprint identity with the bare bool truth condition",
    );
    assert_eq!(
        is_false_query.structural_cache_key(),
        not_query.structural_cache_key(),
        "IS FALSE must collapse onto the same structural cache identity as NOT <bool expr>",
    );
    assert_eq!(
        is_false_query
            .build_plan()
            .expect("IS FALSE bool plan should build")
            .fingerprint(),
        not_query
            .build_plan()
            .expect("NOT bool truth plan should build")
            .fingerprint(),
        "IS FALSE must share semantic plan fingerprint identity with NOT <bool expr>",
    );

    // Phase 3: execution should still allocate one shared query-plan cache
    // entry per canonical truth-condition identity.
    let _ = session
        .execute_compiled_sql::<SessionSqlBoolCompareEntity>(&is_true)
        .expect("executing IS TRUE query should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first scalar truth-wrapper execution should populate one shared query-plan cache entry",
    );
    let _ = session
        .execute_compiled_sql::<SessionSqlBoolCompareEntity>(&bare)
        .expect("executing bare bool truth query should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "bare bool truth condition should reuse the IS TRUE semantic plan identity",
    );
    let _ = session
        .execute_compiled_sql::<SessionSqlBoolCompareEntity>(&is_false)
        .expect("executing IS FALSE query should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        2,
        "second scalar truth-condition family should allocate a second shared query-plan cache entry",
    );
    let _ = session
        .execute_compiled_sql::<SessionSqlBoolCompareEntity>(&not_active)
        .expect("executing NOT bool truth query should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        2,
        "NOT <bool expr> should reuse the IS FALSE semantic plan identity",
    );
}

#[test]
fn trace_query_reuses_canonical_equivalent_scalar_filter_plan_identity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 20), ("Bea", 30), ("Cara", 31)]);

    // Phase 1: lower two SQL spellings that now belong to the same canonical
    // scalar filter identity after 0.107/0.108 normalization.
    let searched_case_sql = "SELECT name \
                             FROM SessionSqlEntity \
                             WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
                             ORDER BY age ASC, id ASC LIMIT 2";
    let canonical_sql = "SELECT name \
                         FROM SessionSqlEntity \
                         WHERE COALESCE(age >= 30, FALSE) \
                            OR (NOT COALESCE(age >= 30, FALSE) AND age = 20) \
                         ORDER BY age ASC, id ASC LIMIT 2";
    let searched_case =
        lower_select_query_for_tests::<SessionSqlEntity>(&session, searched_case_sql)
            .expect("searched CASE scalar filter query should lower for trace parity");
    let canonical = lower_select_query_for_tests::<SessionSqlEntity>(&session, canonical_sql)
        .expect("canonical scalar filter query should lower for trace parity");

    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "trace parity fixture should start with an empty shared query-plan cache",
    );

    // Phase 2: require trace-query planning to reuse the same canonical plan
    // identity and outward trace payload for both lowered query spellings.
    let searched_trace = session
        .trace_query(&searched_case)
        .expect("searched CASE scalar filter trace should build");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first canonical-equivalent trace should populate one shared query-plan entry",
    );

    let canonical_trace = session
        .trace_query(&canonical)
        .expect("canonical scalar filter trace should build");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "canonical-equivalent trace should reuse the same shared query-plan entry",
    );
    assert_eq!(
        searched_trace.plan_hash(),
        canonical_trace.plan_hash(),
        "trace plan hashes must follow canonical scalar filter identity rather than SQL spelling",
    );
    assert_eq!(
        searched_trace.reuse().artifact_class(),
        crate::db::TraceReuseArtifactClass::SharedPreparedQueryPlan,
        "trace should surface the shipped semantic-reuse artifact class",
    );
    assert!(
        !searched_trace.reuse().is_hit(),
        "first canonical trace should miss shared prepared-plan reuse before the cache is warm",
    );
    assert!(
        canonical_trace.reuse().is_hit(),
        "canonical-equivalent second trace should hit shared prepared-plan reuse",
    );
    assert_eq!(
        searched_trace.explain(),
        canonical_trace.explain(),
        "trace explain payloads must follow the same canonical scalar filter identity",
    );
    assert_eq!(
        searched_trace.access_strategy(),
        canonical_trace.access_strategy(),
        "trace access summaries must stay aligned once canonical filter identity collapses the SQL spellings",
    );
}

#[test]
fn fluent_trace_and_plan_hash_reuse_canonical_equivalent_filter_order() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 20), ("Bea", 30), ("Cara", 31)]);

    // Phase 1: build two fluent queries whose scalar filters are semantically
    // identical but arrive through different mutation order.
    let left = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("age").gte(20_u64))
        .filter(crate::db::FieldRef::new("age").lt(40_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2);
    let right = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("age").lt(40_u64))
        .filter(crate::db::FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2);

    let left_hash = left
        .plan_hash_hex()
        .expect("left fluent query should derive one canonical plan hash");
    let right_hash = right
        .plan_hash_hex()
        .expect("right fluent query should derive one canonical plan hash");
    assert_eq!(
        left_hash, right_hash,
        "canonical-equivalent fluent filter order must share one outward plan hash",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "plan-hash derivation should populate the shared prepared query-plan cache",
    );

    // Phase 2: require the public fluent trace wrapper to reuse the same
    // canonical shared query-plan entry warmed by plan-hash derivation.
    let left_trace = left
        .trace()
        .expect("left fluent trace should build through the shared session surface");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first fluent trace should reuse the shared query-plan entry warmed by plan-hash derivation",
    );

    let right_trace = right
        .trace()
        .expect("right fluent trace should build through the shared session surface");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "canonical-equivalent fluent trace should reuse the same shared query-plan entry",
    );
    assert_eq!(
        left_trace.plan_hash(),
        right_trace.plan_hash(),
        "fluent trace plan hash must follow canonical filter identity rather than append order",
    );
    assert_eq!(
        left_trace.explain(),
        right_trace.explain(),
        "fluent trace explain payloads must stay identical for canonical-equivalent filter order",
    );
}

#[test]
fn fluent_trace_and_plan_hash_reuse_canonical_equivalent_grouped_having_order() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 20), ("Bea", 30), ("Cara", 31)]);

    // Phase 1: build two grouped fluent queries whose HAVING clauses are
    // semantically identical but arrive through different builder order.
    let left = session
        .load::<SessionSqlEntity>()
        .group_by("age")
        .expect("left grouped fluent query should resolve group field")
        .aggregate(crate::db::count())
        .having_group(
            "age",
            crate::db::CompareOp::Gte,
            crate::value::InputValue::from(Value::Int(20)),
        )
        .expect("left grouped fluent query should accept group-field HAVING")
        .having_aggregate(
            0,
            crate::db::CompareOp::Gt,
            crate::value::InputValue::from(Value::Uint(0)),
        )
        .expect("left grouped fluent query should accept aggregate HAVING");
    let right = session
        .load::<SessionSqlEntity>()
        .group_by("age")
        .expect("right grouped fluent query should resolve group field")
        .aggregate(crate::db::count())
        .having_aggregate(
            0,
            crate::db::CompareOp::Gt,
            crate::value::InputValue::from(Value::Uint(0)),
        )
        .expect("right grouped fluent query should accept aggregate HAVING")
        .having_group(
            "age",
            crate::db::CompareOp::Gte,
            crate::value::InputValue::from(Value::Int(20)),
        )
        .expect("right grouped fluent query should accept group-field HAVING");

    let left_hash = left
        .plan_hash_hex()
        .expect("left grouped fluent query should derive one canonical plan hash");
    let right_hash = right
        .plan_hash_hex()
        .expect("right grouped fluent query should derive one canonical plan hash");
    assert_eq!(
        left_hash, right_hash,
        "canonical-equivalent grouped HAVING order must share one outward plan hash",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "grouped plan-hash derivation should populate the shared query-plan cache",
    );

    // Phase 2: require grouped fluent trace reuse to follow the same canonical
    // HAVING identity and shared query-plan cache entry warmed by plan-hash
    // derivation.
    let left_trace = left
        .trace()
        .expect("left grouped fluent trace should build through the shared session surface");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first grouped fluent trace should reuse the shared query-plan entry warmed by plan-hash derivation",
    );

    let right_trace = right
        .trace()
        .expect("right grouped fluent trace should build through the shared session surface");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "canonical-equivalent grouped fluent trace should reuse the same shared query-plan entry",
    );
    assert_eq!(
        left_trace.plan_hash(),
        right_trace.plan_hash(),
        "grouped fluent trace plan hash must follow canonical HAVING identity rather than append order",
    );
    assert_eq!(
        left_trace.reuse().artifact_class(),
        crate::db::TraceReuseArtifactClass::SharedPreparedQueryPlan,
        "grouped trace should surface the shipped semantic-reuse artifact class",
    );
    assert!(
        left_trace.reuse().is_hit(),
        "first grouped trace should hit shared prepared-plan reuse after plan-hash derivation warms the cache",
    );
    assert!(
        right_trace.reuse().is_hit(),
        "canonical-equivalent grouped trace should hit shared prepared-plan reuse",
    );
    assert_eq!(
        left_trace.explain(),
        right_trace.explain(),
        "grouped fluent trace explain payloads must stay identical for canonical-equivalent HAVING order",
    );
}

#[test]
fn trace_query_reports_reuse_miss_for_distinct_semantic_identity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 20), ("Bea", 30), ("Cara", 31)]);

    let left_sql = "SELECT name \
                    FROM SessionSqlEntity \
                    WHERE age >= 20 \
                    ORDER BY age ASC, id ASC LIMIT 2";
    let right_sql = "SELECT name \
                     FROM SessionSqlEntity \
                     WHERE age >= 20 \
                     ORDER BY age DESC, id DESC LIMIT 1";
    let left = lower_select_query_for_tests::<SessionSqlEntity>(&session, left_sql)
        .expect("left trace identity fixture should lower");
    let right = lower_select_query_for_tests::<SessionSqlEntity>(&session, right_sql)
        .expect("right trace identity fixture should lower");

    let left_trace = session
        .trace_query(&left)
        .expect("left distinct-identity trace should build");
    let right_trace = session
        .trace_query(&right)
        .expect("right distinct-identity trace should build");

    assert!(
        !left_trace.reuse().is_hit(),
        "first distinct query should miss shared prepared-plan reuse",
    );
    assert!(
        !right_trace.reuse().is_hit(),
        "different semantic identity should build a second plan instead of reusing the first",
    );
    assert_ne!(
        left_trace.plan_hash(),
        right_trace.plan_hash(),
        "distinct semantic identity fixture should stay on different plan hashes",
    );
}

#[test]
fn trace_query_reports_reuse_miss_for_distinct_projection_identity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 20), ("Bea", 30), ("Cara", 31)]);

    let left_sql = "SELECT name \
                    FROM SessionSqlEntity \
                    WHERE age >= 20 \
                    ORDER BY age ASC, id ASC LIMIT 2";
    let right_sql = "SELECT age \
                     FROM SessionSqlEntity \
                     WHERE age >= 20 \
                     ORDER BY age ASC, id ASC LIMIT 2";
    let left = lower_select_query_for_tests::<SessionSqlEntity>(&session, left_sql)
        .expect("left projection-identity fixture should lower");
    let right = lower_select_query_for_tests::<SessionSqlEntity>(&session, right_sql)
        .expect("right projection-identity fixture should lower");

    let left_trace = session
        .trace_query(&left)
        .expect("left projection-identity trace should build");
    let right_trace = session
        .trace_query(&right)
        .expect("right projection-identity trace should build");

    assert!(
        !left_trace.reuse().is_hit(),
        "first projection shape should miss shared prepared-plan reuse",
    );
    assert!(
        !right_trace.reuse().is_hit(),
        "different projection shape should build a second plan instead of reusing the first",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        2,
        "distinct projection identity should occupy separate shared query-plan cache entries",
    );
    assert_ne!(
        left_trace.plan_hash(),
        right_trace.plan_hash(),
        "distinct projection identity should stay on different plan hashes",
    );
}

#[test]
fn trace_query_reports_reuse_miss_for_distinct_grouping_identity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 20), ("Bea", 30), ("Cara", 31)]);

    let left_sql = "SELECT age \
                    FROM SessionSqlEntity \
                    WHERE age >= 20 \
                    ORDER BY age ASC, id ASC LIMIT 2";
    let right_sql = "SELECT age, COUNT(*) \
                     FROM SessionSqlEntity \
                     WHERE age >= 20 \
                     GROUP BY age \
                     ORDER BY age ASC LIMIT 2";
    let left = lower_select_query_for_tests::<SessionSqlEntity>(&session, left_sql)
        .expect("left grouping-identity fixture should lower");
    let right = lower_select_query_for_tests::<SessionSqlEntity>(&session, right_sql)
        .expect("right grouping-identity fixture should lower");

    let left_trace = session
        .trace_query(&left)
        .expect("left grouping-identity trace should build");
    let right_trace = session
        .trace_query(&right)
        .expect("right grouping-identity trace should build");

    assert!(
        !left_trace.reuse().is_hit(),
        "first grouping shape should miss shared prepared-plan reuse",
    );
    assert!(
        !right_trace.reuse().is_hit(),
        "different grouping shape should build a second plan instead of reusing the first",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        2,
        "distinct grouping identity should occupy separate shared query-plan cache entries",
    );
    assert_ne!(
        left_trace.plan_hash(),
        right_trace.plan_hash(),
        "distinct grouping identity should stay on different plan hashes",
    );
}

#[test]
fn shared_query_plan_cache_key_version_mismatch_fails_closed() {
    reset_session_sql_store();
    let session = sql_session();
    let query = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(1);
    let schema_fingerprint = session_sql_entity_initial_accepted_schema_cache_fingerprint();
    let authority = EntityAuthority::for_generated_type_for_test::<SessionSqlEntity>();
    let old_key = DbSession::<SessionSqlCanister>::query_plan_cache_key_for_tests(
        authority.clone(),
        schema_fingerprint,
        QueryPlanVisibility::StoreReady,
        query.query().structural(),
        1,
    );
    let new_key = DbSession::<SessionSqlCanister>::query_plan_cache_key_for_tests(
        authority,
        schema_fingerprint,
        QueryPlanVisibility::StoreReady,
        query.query().structural(),
        2,
    );
    let mut cache = HashSet::new();
    cache.insert(old_key.clone());

    assert_ne!(
        old_key, new_key,
        "shared lower-plan cache identity must include one explicit method version",
    );
    assert!(
        !cache.contains(&new_key),
        "shared lower-plan cache version mismatch must fail closed instead of reusing an older entry",
    );
}

#[test]
fn sql_cache_key_version_mismatch_fails_closed() {
    let compiled_v1 =
        SqlCompiledCommandCacheKey::query_for_entity_with_method_version::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
            1,
        );
    let compiled_v2 =
        SqlCompiledCommandCacheKey::query_for_entity_with_method_version::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
            2,
        );
    let mut compiled_cache = HashSet::new();
    compiled_cache.insert(compiled_v1.clone());

    assert_ne!(
        compiled_v1, compiled_v2,
        "compiled SQL cache identity must include one explicit method version",
    );
    assert!(
        !compiled_cache.contains(&compiled_v2),
        "compiled SQL cache version mismatch must fail closed instead of reusing an older entry",
    );
}

#[test]
fn sql_cache_key_version_keeps_query_and_update_surfaces_separate() {
    let query_key =
        SqlCompiledCommandCacheKey::query_for_entity_with_method_version::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
            1,
        );
    let update_key =
        SqlCompiledCommandCacheKey::update_for_entity_with_method_version::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
            1,
        );
    let mut cache = HashSet::new();
    cache.insert(query_key.clone());

    assert_ne!(
        query_key, update_key,
        "cache method versioning must not collapse query and update surface identity",
    );
    assert!(
        !cache.contains(&update_key),
        "query/update surface mismatch must fail closed even when the cache method version matches",
    );
}

#[test]
fn bounded_numeric_order_terms_use_the_normal_sql_surface_identity_and_cache_path() {
    for (sql, compile_context, identity_context) in [
        (
            "SELECT age + 1 AS next_age FROM SessionSqlEntity ORDER BY next_age ASC LIMIT 2",
            "bounded numeric ORDER BY alias",
            "admitted ORDER BY aliases",
        ),
        (
            "SELECT age FROM SessionSqlEntity ORDER BY age + 1 ASC LIMIT 2",
            "direct bounded numeric ORDER BY",
            "direct bounded numeric ORDER BY terms",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();

        assert_eq!(
            session.sql_compiled_command_cache_len(),
            0,
            "new SQL session should start with an empty compiled-command cache",
        );
        let compiled = session
            .compile_sql_query::<SessionSqlEntity>(sql)
            .unwrap_or_else(|err| {
                panic!("{compile_context} should compile through the SQL surface: {err:?}")
            });
        let repeat = session
            .compile_sql_query::<SessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("repeating one {compile_context} compile should hit the same compiled-command cache entry: {err:?}"));

        assert!(
            matches!(
                compiled,
                crate::db::session::sql::CompiledSqlCommand::Select { .. }
            ),
            "{compile_context} should stay on the normal SELECT compile lane",
        );
        assert!(
            matches!(
                repeat,
                crate::db::session::sql::CompiledSqlCommand::Select { .. }
            ),
            "repeating one {compile_context} compile should stay on the normal SELECT compile lane",
        );
        assert_eq!(
            session.sql_compiled_command_cache_len(),
            1,
            "repeating one identical {compile_context} compile must not grow the compiled-command cache",
        );

        assert_compiled_select_query_matches_lowered_identity(
            &compiled,
            sql,
            format!(
                "{identity_context} must canonicalize onto the same structural query cache key before cache insertion"
            )
            .as_str(),
        );

        assert_scalar_compiled_select_executes_through_shared_query_plan(&session, &compiled);
    }
}

#[test]
fn grouped_aggregate_order_alias_uses_the_normal_sql_surface_identity_and_cache_path() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let sql = "SELECT name, AVG(age) AS avg_age \
               FROM IndexedSessionSqlEntity \
               GROUP BY name \
               ORDER BY avg_age DESC, name ASC LIMIT 2";

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "new indexed SQL session should start with an empty compiled-command cache",
    );
    let compiled = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("grouped aggregate ORDER BY alias should compile through the normal SQL surface");
    let repeat = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("repeating one grouped aggregate ORDER BY alias compile should hit the same compiled-command cache entry");

    assert!(
        matches!(
            compiled,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "grouped aggregate ORDER BY alias should stay on the lowered SELECT artifact family",
    );
    assert!(
        matches!(
            repeat,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "repeating one grouped aggregate ORDER BY alias compile should stay on the lowered SELECT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical grouped aggregate ORDER BY alias compile must not grow the compiled-command cache",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &compiled,
        sql,
        "grouped aggregate ORDER BY aliases must canonicalize onto the same structural query cache key before cache insertion",
    );
}

#[test]
fn grouped_aggregate_input_order_alias_uses_the_normal_sql_surface_identity_and_cache_path() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let sql = "SELECT name, AVG(age + 1) AS avg_plus_one \
               FROM IndexedSessionSqlEntity \
               GROUP BY name \
               ORDER BY avg_plus_one DESC, name ASC LIMIT 2";

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "new indexed SQL session should start with an empty compiled-command cache",
    );
    let compiled = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect(
            "grouped aggregate input ORDER BY alias should compile through the normal SQL surface",
        );
    let repeat = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("repeating one grouped aggregate input ORDER BY alias compile should hit the same compiled-command cache entry");

    assert!(
        matches!(
            compiled,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "grouped aggregate input ORDER BY alias should stay on the lowered SELECT artifact family",
    );
    assert!(
        matches!(
            repeat,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "repeating one grouped aggregate input ORDER BY alias compile should stay on the lowered SELECT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical grouped aggregate input ORDER BY alias compile must not grow the compiled-command cache",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &compiled,
        sql,
        "grouped aggregate input ORDER BY aliases must canonicalize onto the same structural query cache key before cache insertion",
    );
}

#[test]
fn searched_case_scalar_projection_uses_the_normal_sql_surface_identity_and_cache_path() {
    reset_session_sql_store();
    let session = sql_session();

    let sql = "SELECT CASE WHEN age >= 30 THEN name ELSE 'young' END AS age_bucket \
               FROM SessionSqlEntity \
               ORDER BY id ASC LIMIT 2";

    let compiled = session
        .compile_sql_query::<SessionSqlEntity>(sql)
        .expect("searched CASE scalar projection should compile through the normal SQL surface");
    let repeat = session
        .compile_sql_query::<SessionSqlEntity>(sql)
        .expect("repeating one searched CASE scalar projection compile should hit the same compiled-command cache entry");

    assert!(
        matches!(
            compiled,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "searched CASE scalar projection should stay on the lowered SELECT artifact family",
    );
    assert!(
        matches!(
            repeat,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "repeating one searched CASE scalar projection compile should stay on the lowered SELECT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical searched CASE scalar projection compile must not grow the compiled-command cache",
    );

    assert_compiled_select_query_matches_lowered_identity(
        &compiled,
        sql,
        "searched CASE scalar projections must canonicalize onto the same structural query cache key before cache insertion",
    );
    assert_scalar_compiled_select_executes_through_shared_query_plan(&session, &compiled);
}

#[test]
fn searched_case_grouped_projection_uses_the_normal_sql_surface_identity_and_cache_path() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let sql = "SELECT age, CASE WHEN COUNT(*) > 1 THEN 'multi' ELSE 'single' END AS bucket \
               FROM IndexedSessionSqlEntity \
               GROUP BY age \
               ORDER BY age ASC LIMIT 2";

    let compiled = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("grouped searched CASE projection should compile through the normal SQL surface");
    let repeat = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("repeating one grouped searched CASE projection compile should hit the same compiled-command cache entry");

    assert!(
        matches!(
            compiled,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "grouped searched CASE projection should stay on the lowered SELECT artifact family",
    );
    assert!(
        matches!(
            repeat,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "repeating one grouped searched CASE projection compile should stay on the lowered SELECT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical grouped searched CASE projection compile must not grow the compiled-command cache",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &compiled,
        sql,
        "grouped searched CASE projections must canonicalize onto the same structural query cache key before cache insertion",
    );
    assert_compiled_select_executes_through_shared_query_plan_for_entity::<IndexedSessionSqlEntity>(
        &session, &compiled,
    );
}

#[test]
fn searched_case_grouped_having_uses_the_normal_sql_surface_identity_and_cache_path() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let sql = "SELECT age, COUNT(*) \
               FROM IndexedSessionSqlEntity \
               GROUP BY age \
               HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1 \
               ORDER BY age ASC LIMIT 2";

    let compiled = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("grouped searched CASE HAVING should compile through the normal SQL surface");
    let repeat = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("repeating one grouped searched CASE HAVING compile should hit the same compiled-command cache entry");

    assert!(
        matches!(
            compiled,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "grouped searched CASE HAVING should stay on the lowered SELECT artifact family",
    );
    assert!(
        matches!(
            repeat,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "repeating one grouped searched CASE HAVING compile should stay on the lowered SELECT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical grouped searched CASE HAVING compile must not grow the compiled-command cache",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &compiled,
        sql,
        "grouped searched CASE HAVING must canonicalize onto the same structural query cache key before cache insertion",
    );
    assert_compiled_select_executes_through_shared_query_plan_for_entity::<IndexedSessionSqlEntity>(
        &session, &compiled,
    );
}

#[test]
fn searched_case_aggregate_input_alias_uses_the_normal_sql_surface_identity_and_cache_path() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let sql = "SELECT age, SUM(CASE WHEN age > 10 THEN 1 ELSE 0 END) AS high_count \
               FROM IndexedSessionSqlEntity \
               GROUP BY age \
               ORDER BY high_count DESC, age ASC LIMIT 2";

    let compiled = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("grouped searched CASE aggregate input ORDER BY alias should compile through the normal SQL surface");
    let repeat = session
        .compile_sql_query::<IndexedSessionSqlEntity>(sql)
        .expect("repeating one grouped searched CASE aggregate input ORDER BY alias compile should hit the same compiled-command cache entry");

    assert!(
        matches!(
            compiled,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "grouped searched CASE aggregate input ORDER BY alias should stay on the lowered SELECT artifact family",
    );
    assert!(
        matches!(
            repeat,
            crate::db::session::sql::CompiledSqlCommand::Select { .. }
        ),
        "repeating one grouped searched CASE aggregate input ORDER BY alias compile should stay on the lowered SELECT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical grouped searched CASE aggregate input ORDER BY alias compile must not grow the compiled-command cache",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &compiled,
        sql,
        "grouped searched CASE aggregate input ORDER BY aliases must canonicalize onto the same structural query cache key before cache insertion",
    );
    assert_compiled_select_executes_through_shared_query_plan_for_entity::<IndexedSessionSqlEntity>(
        &session, &compiled,
    );
}

#[test]
fn searched_case_semantic_differences_do_not_alias_sql_cache_identity() {
    reset_session_sql_store();
    let session = sql_session();

    let left_sql = "SELECT CASE WHEN age >= 30 THEN name ELSE 'young' END AS age_bucket \
                    FROM SessionSqlEntity \
                    ORDER BY id ASC LIMIT 2";
    let right_sql = "SELECT CASE WHEN age >= 31 THEN name ELSE 'young' END AS age_bucket \
                     FROM SessionSqlEntity \
                     ORDER BY id ASC LIMIT 2";

    let left = session
        .compile_sql_query::<SessionSqlEntity>(left_sql)
        .expect("left searched CASE scalar projection should compile");
    let right = session
        .compile_sql_query::<SessionSqlEntity>(right_sql)
        .expect("right searched CASE scalar projection should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        2,
        "searched CASE condition changes must not collapse onto one compiled-command cache entry",
    );

    assert_compiled_select_query_matches_lowered_identity(
        &left,
        left_sql,
        "left searched CASE scalar projection should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity(
        &right,
        right_sql,
        "right searched CASE scalar projection should preserve canonical lowered identity",
    );
    assert_compiled_select_queries_remain_distinct_for_entity(
        &left,
        &right,
        "searched CASE condition changes",
    );
    assert_distinct_compiled_selects_execute_through_shared_query_plan_for_entity::<SessionSqlEntity>(
        &session, &left, &right,
    );
}

#[test]
fn grouped_case_having_semantic_differences_do_not_alias_sql_cache_identity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let left_sql = "SELECT age, COUNT(*) \
                    FROM IndexedSessionSqlEntity \
                    GROUP BY age \
                    HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1 \
                    ORDER BY age ASC LIMIT 2";
    let right_sql = "SELECT age, COUNT(*) \
                     FROM IndexedSessionSqlEntity \
                     GROUP BY age \
                     HAVING CASE WHEN COUNT(*) > 2 THEN 1 ELSE 0 END = 1 \
                     ORDER BY age ASC LIMIT 2";

    let left = session
        .compile_sql_query::<IndexedSessionSqlEntity>(left_sql)
        .expect("left grouped searched CASE HAVING should compile");
    let right = session
        .compile_sql_query::<IndexedSessionSqlEntity>(right_sql)
        .expect("right grouped searched CASE HAVING should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        2,
        "grouped searched CASE HAVING threshold changes must not collapse onto one compiled-command cache entry",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &left,
        left_sql,
        "left grouped searched CASE HAVING should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &right,
        right_sql,
        "right grouped searched CASE HAVING should preserve canonical lowered identity",
    );
    assert_compiled_select_queries_remain_distinct_for_entity(
        &left,
        &right,
        "grouped searched CASE HAVING threshold changes",
    );
    assert_distinct_compiled_selects_execute_through_shared_query_plan_for_entity::<
        IndexedSessionSqlEntity,
    >(&session, &left, &right);
}

#[test]
fn grouped_boolean_case_having_canonical_equivalence_reuses_semantic_identity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let case_sql = "SELECT age, COUNT(*) \
                    FROM IndexedSessionSqlEntity \
                    GROUP BY age \
                    HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END \
                    ORDER BY age ASC LIMIT 2";
    let canonical_sql = "SELECT age, COUNT(*) \
                         FROM IndexedSessionSqlEntity \
                         GROUP BY age \
                         HAVING COALESCE(COUNT(*) > 1, FALSE) OR FALSE \
                         ORDER BY age ASC LIMIT 2";

    let case = session
        .compile_sql_query::<IndexedSessionSqlEntity>(case_sql)
        .expect("grouped boolean searched CASE HAVING should compile");
    let canonical = session
        .compile_sql_query::<IndexedSessionSqlEntity>(canonical_sql)
        .expect("canonical grouped boolean HAVING should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        2,
        "different grouped HAVING SQL spellings should still occupy distinct compiled-command cache entries",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &case,
        case_sql,
        "grouped boolean searched CASE HAVING should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &canonical,
        canonical_sql,
        "canonical grouped boolean HAVING should preserve canonical lowered identity",
    );

    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: case_query, ..
    } = &case
    else {
        panic!("grouped boolean searched CASE HAVING should compile into one SELECT artifact");
    };
    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: canonical_query,
        ..
    } = &canonical
    else {
        panic!("canonical grouped boolean HAVING should compile into one SELECT artifact");
    };

    assert_eq!(
        case_query.structural_cache_key(),
        canonical_query.structural_cache_key(),
        "explicit-ELSE grouped searched CASE HAVING must collapse onto the same structural cache identity as its canonical grouped boolean form",
    );
    assert_eq!(
        case_query
            .build_plan()
            .expect("grouped searched CASE plan should build")
            .fingerprint(),
        canonical_query
            .build_plan()
            .expect("canonical grouped boolean plan should build")
            .fingerprint(),
        "explicit-ELSE grouped searched CASE HAVING must share semantic plan fingerprint identity with its canonical grouped boolean form",
    );

    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&case)
        .expect("executing grouped searched CASE HAVING should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first grouped canonical-equivalent execution should populate one shared query-plan cache entry",
    );
    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&canonical)
        .expect("executing canonical grouped boolean HAVING should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "canonical-equivalent grouped searched CASE HAVING should reuse the same shared query-plan cache entry",
    );
}

#[test]
fn grouped_boolean_case_having_truth_wrapper_reuses_semantic_identity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let canonical_sql = "SELECT age, COUNT(*) \
                         FROM IndexedSessionSqlEntity \
                         GROUP BY age \
                         HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END \
                         ORDER BY age ASC LIMIT 2";
    let wrapped_sql = "SELECT age, COUNT(*) \
                       FROM IndexedSessionSqlEntity \
                       GROUP BY age \
                       HAVING CASE WHEN (COUNT(*) > 1) = TRUE THEN TRUE ELSE FALSE END \
                       ORDER BY age ASC LIMIT 2";

    let canonical = session
        .compile_sql_query::<IndexedSessionSqlEntity>(canonical_sql)
        .expect("canonical grouped boolean searched CASE HAVING should compile");
    let wrapped = session
        .compile_sql_query::<IndexedSessionSqlEntity>(wrapped_sql)
        .expect("truth-wrapped grouped boolean searched CASE HAVING should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        2,
        "truth-wrapper grouped HAVING spellings should still occupy distinct compiled-command cache entries",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &canonical,
        canonical_sql,
        "canonical grouped boolean searched CASE HAVING should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &wrapped,
        wrapped_sql,
        "truth-wrapped grouped boolean searched CASE HAVING should preserve canonical lowered identity",
    );

    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: canonical_query,
        ..
    } = &canonical
    else {
        panic!(
            "canonical grouped boolean searched CASE HAVING should compile into one SELECT artifact"
        );
    };
    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: wrapped_query,
        ..
    } = &wrapped
    else {
        panic!(
            "truth-wrapped grouped boolean searched CASE HAVING should compile into one SELECT artifact"
        );
    };

    assert_eq!(
        canonical_query.structural_cache_key(),
        wrapped_query.structural_cache_key(),
        "truth-wrapper grouped searched CASE HAVING must collapse onto the same structural cache identity as the canonical grouped boolean spelling",
    );
    assert_eq!(
        canonical_query
            .build_plan()
            .expect("canonical grouped searched CASE plan should build")
            .fingerprint(),
        wrapped_query
            .build_plan()
            .expect("truth-wrapped grouped searched CASE plan should build")
            .fingerprint(),
        "truth-wrapper grouped searched CASE HAVING must share semantic plan fingerprint identity with the canonical grouped boolean spelling",
    );

    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&canonical)
        .expect("executing canonical grouped searched CASE HAVING should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first truth-wrapper grouped canonical-equivalent execution should populate one shared query-plan cache entry",
    );
    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&wrapped)
        .expect("executing truth-wrapped grouped searched CASE HAVING should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "truth-wrapper grouped canonical-equivalent execution should reuse the same shared query-plan cache entry",
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "grouped omitted-ELSE identity matrix intentionally proves one semantic boundary"
)]
#[test]
fn grouped_boolean_case_having_without_else_reuses_explicit_null_semantic_identity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let omitted_else_sql = "SELECT age, COUNT(*) \
                            FROM IndexedSessionSqlEntity \
                            GROUP BY age \
                            HAVING CASE WHEN COUNT(*) > 1 THEN TRUE END \
                            ORDER BY age ASC LIMIT 2";
    let explicit_null_sql = "SELECT age, COUNT(*) \
                             FROM IndexedSessionSqlEntity \
                             GROUP BY age \
                             HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE NULL END \
                             ORDER BY age ASC LIMIT 2";
    let explicit_false_sql = "SELECT age, COUNT(*) \
                              FROM IndexedSessionSqlEntity \
                              GROUP BY age \
                              HAVING COALESCE(COUNT(*) > 1, FALSE) OR FALSE \
                              ORDER BY age ASC LIMIT 2";

    let omitted_else = session
        .compile_sql_query::<IndexedSessionSqlEntity>(omitted_else_sql)
        .expect("grouped searched CASE HAVING without ELSE should compile");
    let explicit_null = session
        .compile_sql_query::<IndexedSessionSqlEntity>(explicit_null_sql)
        .expect("grouped searched CASE HAVING with explicit ELSE NULL should compile");
    let explicit_false = session
        .compile_sql_query::<IndexedSessionSqlEntity>(explicit_false_sql)
        .expect("canonical explicit-ELSE FALSE grouped boolean HAVING should compile");

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &omitted_else,
        omitted_else_sql,
        "grouped searched CASE HAVING without ELSE should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &explicit_null,
        explicit_null_sql,
        "grouped searched CASE HAVING with explicit ELSE NULL should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &explicit_false,
        explicit_false_sql,
        "canonical explicit-ELSE FALSE grouped boolean HAVING should preserve canonical lowered identity",
    );
    let SqlCommand::Query(omitted_else_lowered) =
        compile_sql_command::<IndexedSessionSqlEntity>(omitted_else_sql, MissingRowPolicy::Ignore)
            .expect("grouped searched CASE HAVING without ELSE should lower into one query")
    else {
        panic!("grouped searched CASE HAVING without ELSE should lower into one query command");
    };
    let SqlCommand::Query(explicit_null_lowered) =
        compile_sql_command::<IndexedSessionSqlEntity>(explicit_null_sql, MissingRowPolicy::Ignore)
            .expect(
                "grouped searched CASE HAVING with explicit ELSE NULL should lower into one query",
            )
    else {
        panic!(
            "grouped searched CASE HAVING with explicit ELSE NULL should lower into one query command"
        );
    };
    let SqlCommand::Query(explicit_false_lowered) = compile_sql_command::<IndexedSessionSqlEntity>(
        explicit_false_sql,
        MissingRowPolicy::Ignore,
    )
    .expect("canonical explicit-ELSE FALSE grouped boolean HAVING should lower into one query") else {
        panic!(
            "canonical explicit-ELSE FALSE grouped boolean HAVING should lower into one query command"
        );
    };

    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: omitted_else_query,
        ..
    } = &omitted_else
    else {
        panic!("grouped searched CASE HAVING without ELSE should compile into one SELECT artifact");
    };
    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: explicit_null_query,
        ..
    } = &explicit_null
    else {
        panic!(
            "grouped searched CASE HAVING with explicit ELSE NULL should compile into one SELECT artifact"
        );
    };
    assert_eq!(
        omitted_else_query.structural_cache_key(),
        explicit_null_query.structural_cache_key(),
        "grouped searched CASE HAVING without ELSE must collapse onto the same structural cache identity as the explicit ELSE NULL grouped boolean family",
    );
    assert_eq!(
        omitted_else_lowered
            .plan_hash_hex()
            .expect("grouped searched CASE HAVING without ELSE plan hash should build"),
        explicit_null_lowered
            .plan_hash_hex()
            .expect("grouped searched CASE HAVING with explicit ELSE NULL plan hash should build"),
        "grouped searched CASE HAVING without ELSE must share one outward plan hash with the explicit ELSE NULL grouped boolean family",
    );
    assert_eq!(
        omitted_else_query
            .build_plan()
            .expect("grouped searched CASE HAVING without ELSE plan should build")
            .fingerprint(),
        explicit_null_query
            .build_plan()
            .expect("grouped searched CASE HAVING with explicit ELSE NULL plan should build")
            .fingerprint(),
        "grouped searched CASE HAVING without ELSE must share semantic plan identity with the explicit ELSE NULL grouped boolean family",
    );
    assert_compiled_select_queries_remain_distinct_for_entity(
        &omitted_else,
        &explicit_false,
        "grouped searched CASE HAVING without ELSE versus explicit-ELSE FALSE grouped boolean form",
    );
    assert_ne!(
        omitted_else_lowered
            .plan_hash_hex()
            .expect("grouped searched CASE HAVING without ELSE plan hash should build"),
        explicit_false_lowered
            .plan_hash_hex()
            .expect("canonical explicit-ELSE FALSE grouped boolean HAVING plan hash should build"),
        "grouped searched CASE HAVING without ELSE must stay outward-hash distinct from the explicit-ELSE FALSE grouped boolean family",
    );

    let omitted_else_trace = session
        .trace_query(&omitted_else_lowered)
        .expect("grouped searched CASE HAVING without ELSE trace should build");
    let explicit_null_trace = session
        .trace_query(&explicit_null_lowered)
        .expect("grouped searched CASE HAVING with explicit ELSE NULL trace should build");
    assert_eq!(
        omitted_else_trace.plan_hash(),
        explicit_null_trace.plan_hash(),
        "grouped searched CASE HAVING without ELSE trace must share one outward plan hash with the explicit ELSE NULL grouped boolean family",
    );
    assert_eq!(
        omitted_else_trace.explain(),
        explicit_null_trace.explain(),
        "grouped searched CASE HAVING without ELSE trace explain must stay identical to the explicit ELSE NULL grouped boolean family",
    );
    assert_eq!(
        omitted_else_trace.reuse().artifact_class(),
        crate::db::TraceReuseArtifactClass::SharedPreparedQueryPlan,
        "grouped omitted-ELSE trace should surface the shared prepared query-plan reuse artifact",
    );
    assert!(
        !omitted_else_trace.reuse().is_hit(),
        "first grouped omitted-ELSE trace should miss shared prepared-plan reuse before the cache is warm",
    );
    assert!(
        explicit_null_trace.reuse().is_hit(),
        "grouped explicit ELSE NULL trace should hit shared prepared-plan reuse after the omitted-ELSE canonical equivalent warm-up",
    );

    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&omitted_else)
        .expect("executing grouped searched CASE HAVING without ELSE should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first grouped omitted-ELSE HAVING execution should populate one shared query-plan cache entry",
    );
    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&explicit_null)
        .expect("executing grouped searched CASE HAVING with explicit ELSE NULL should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "omitted-ELSE grouped searched CASE HAVING should reuse the explicit ELSE NULL grouped boolean plan identity",
    );
    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&explicit_false)
        .expect("executing canonical explicit-ELSE FALSE grouped boolean HAVING should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        2,
        "omitted-ELSE grouped searched CASE HAVING must stay distinct from the explicit-ELSE FALSE grouped boolean family",
    );
}

#[test]
fn grouped_boolean_case_having_without_else_truth_wrapper_reuses_explicit_null_semantic_identity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let explicit_null_sql = "SELECT age, COUNT(*) \
                             FROM IndexedSessionSqlEntity \
                             GROUP BY age \
                             HAVING CASE WHEN COUNT(*) > 1 THEN TRUE ELSE NULL END \
                             ORDER BY age ASC LIMIT 2";
    let wrapped_omitted_else_sql = "SELECT age, COUNT(*) \
                                    FROM IndexedSessionSqlEntity \
                                    GROUP BY age \
                                    HAVING CASE WHEN (COUNT(*) > 1) = TRUE THEN TRUE END \
                                    ORDER BY age ASC LIMIT 2";

    let explicit_null = session
        .compile_sql_query::<IndexedSessionSqlEntity>(explicit_null_sql)
        .expect("grouped searched CASE HAVING with explicit ELSE NULL should compile");
    let wrapped_omitted_else = session
        .compile_sql_query::<IndexedSessionSqlEntity>(wrapped_omitted_else_sql)
        .expect("truth-wrapped grouped searched CASE HAVING without ELSE should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        2,
        "truth-wrapped omitted-ELSE and explicit ELSE NULL grouped HAVING spellings should still occupy distinct compiled-command cache entries",
    );

    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &explicit_null,
        explicit_null_sql,
        "grouped searched CASE HAVING with explicit ELSE NULL should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity_for_entity::<IndexedSessionSqlEntity>(
        &wrapped_omitted_else,
        wrapped_omitted_else_sql,
        "truth-wrapped grouped searched CASE HAVING without ELSE should preserve canonical lowered identity",
    );

    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: explicit_null_query,
        ..
    } = &explicit_null
    else {
        panic!(
            "grouped searched CASE HAVING with explicit ELSE NULL should compile into one SELECT artifact"
        );
    };
    let crate::db::session::sql::CompiledSqlCommand::Select {
        query: wrapped_omitted_else_query,
        ..
    } = &wrapped_omitted_else
    else {
        panic!(
            "truth-wrapped grouped searched CASE HAVING without ELSE should compile into one SELECT artifact"
        );
    };

    assert_eq!(
        explicit_null_query.structural_cache_key(),
        wrapped_omitted_else_query.structural_cache_key(),
        "truth-wrapped grouped searched CASE HAVING without ELSE must collapse onto the same structural cache identity as the explicit ELSE NULL grouped boolean family",
    );
    assert_eq!(
        explicit_null_query
            .build_plan()
            .expect("grouped searched CASE HAVING with explicit ELSE NULL plan should build")
            .fingerprint(),
        wrapped_omitted_else_query
            .build_plan()
            .expect("truth-wrapped grouped searched CASE HAVING without ELSE plan should build")
            .fingerprint(),
        "truth-wrapped grouped searched CASE HAVING without ELSE must share semantic plan identity with the explicit ELSE NULL grouped boolean family",
    );

    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&explicit_null)
        .expect("executing grouped searched CASE HAVING with explicit ELSE NULL should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "first truth-wrapped omitted-ELSE grouped canonical-equivalent execution should populate one shared query-plan cache entry",
    );
    let _ = session
        .execute_compiled_sql::<IndexedSessionSqlEntity>(&wrapped_omitted_else)
        .expect("executing truth-wrapped grouped searched CASE HAVING without ELSE should succeed");
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "truth-wrapped grouped searched CASE HAVING without ELSE should reuse the explicit ELSE NULL grouped boolean plan identity",
    );
}

#[test]
fn aggregate_distinct_and_order_direction_changes_do_not_alias_sql_cache_identity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let distinct_sql = "SELECT name, AVG(DISTINCT age) AS avg_age \
                        FROM IndexedSessionSqlEntity \
                        GROUP BY name \
                        ORDER BY avg_age DESC, name ASC LIMIT 2";
    let desc_sql = "SELECT name, AVG(age) AS avg_age \
                    FROM IndexedSessionSqlEntity \
                    GROUP BY name \
                    ORDER BY avg_age DESC, name ASC LIMIT 2";
    let asc_sql = "SELECT name, AVG(age) AS avg_age \
                   FROM IndexedSessionSqlEntity \
                   GROUP BY name \
                   ORDER BY avg_age ASC, name ASC LIMIT 2";

    let distinct = session
        .compile_sql_query::<IndexedSessionSqlEntity>(distinct_sql)
        .expect("DISTINCT aggregate query should compile");
    let desc = session
        .compile_sql_query::<IndexedSessionSqlEntity>(desc_sql)
        .expect("descending aggregate ORDER BY query should compile");
    let asc = session
        .compile_sql_query::<IndexedSessionSqlEntity>(asc_sql)
        .expect("ascending aggregate ORDER BY query should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        3,
        "aggregate DISTINCT and ORDER BY direction changes must stay on distinct compiled-command cache entries",
    );

    assert_compiled_select_queries_remain_distinct_for_entity(
        &distinct,
        &desc,
        "aggregate DISTINCT changes",
    );
    assert_compiled_select_queries_remain_distinct_for_entity(
        &desc,
        &asc,
        "aggregate ORDER BY direction changes",
    );
    assert_distinct_compiled_selects_execute_through_shared_query_plan_for_entity::<
        IndexedSessionSqlEntity,
    >(&session, &distinct, &desc);
}

#[test]
fn aggregate_filter_semantic_differences_do_not_alias_sql_cache_identity() {
    reset_session_sql_store();
    let session = sql_session();

    let filtered_sql = "SELECT COUNT(*) FILTER (WHERE age >= 30) \
                        FROM SessionSqlEntity";
    let threshold_sql = "SELECT COUNT(*) FILTER (WHERE age >= 31) \
                         FROM SessionSqlEntity";
    let unfiltered_sql = "SELECT COUNT(*) FROM SessionSqlEntity";

    let filtered = session
        .compile_sql_query::<SessionSqlEntity>(filtered_sql)
        .expect("filtered global aggregate query should compile");
    let threshold = session
        .compile_sql_query::<SessionSqlEntity>(threshold_sql)
        .expect("threshold-varied filtered global aggregate query should compile");
    let unfiltered = session
        .compile_sql_query::<SessionSqlEntity>(unfiltered_sql)
        .expect("unfiltered global aggregate query should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        3,
        "aggregate FILTER semantic changes must stay on distinct compiled-command cache entries",
    );

    assert!(
        matches!(
            filtered,
            crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. }
        ),
        "filtered global aggregate query should stay on the dedicated global aggregate artifact family",
    );
    assert!(
        matches!(
            threshold,
            crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. }
        ),
        "threshold-varied filtered global aggregate query should stay on the dedicated global aggregate artifact family",
    );
    assert!(
        matches!(
            unfiltered,
            crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. }
        ),
        "unfiltered global aggregate query should stay on the dedicated global aggregate artifact family",
    );

    let _ = session
        .execute_compiled_sql::<SessionSqlEntity>(&filtered)
        .expect("filtered global aggregate query should execute after compile");
    let _ = session
        .execute_compiled_sql::<SessionSqlEntity>(&threshold)
        .expect("threshold-varied filtered global aggregate query should execute after compile");
    let _ = session
        .execute_compiled_sql::<SessionSqlEntity>(&unfiltered)
        .expect("unfiltered global aggregate query should execute after compile");
}

#[test]
fn grouped_aggregate_filter_semantic_differences_do_not_alias_sql_cache_identity() {
    reset_session_sql_store();
    let session = sql_session();

    let filtered_sql = "SELECT age, COUNT(*) FILTER (WHERE age >= 20) \
                        FROM SessionSqlEntity \
                        GROUP BY age \
                        ORDER BY age ASC LIMIT 2";
    let operator_sql = "SELECT age, COUNT(*) FILTER (WHERE age > 20) \
                        FROM SessionSqlEntity \
                        GROUP BY age \
                        ORDER BY age ASC LIMIT 2";
    let unfiltered_sql = "SELECT age, COUNT(*) \
                          FROM SessionSqlEntity \
                          GROUP BY age \
                          ORDER BY age ASC LIMIT 2";

    let filtered = session
        .compile_sql_query::<SessionSqlEntity>(filtered_sql)
        .expect("filtered grouped aggregate query should compile");
    let operator = session
        .compile_sql_query::<SessionSqlEntity>(operator_sql)
        .expect("operator-varied grouped aggregate query should compile");
    let unfiltered = session
        .compile_sql_query::<SessionSqlEntity>(unfiltered_sql)
        .expect("unfiltered grouped aggregate query should compile");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        3,
        "grouped aggregate FILTER semantic changes must stay on distinct compiled-command cache entries",
    );

    assert_compiled_select_query_matches_lowered_identity(
        &filtered,
        filtered_sql,
        "filtered grouped aggregate query should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity(
        &operator,
        operator_sql,
        "operator-varied grouped aggregate query should preserve canonical lowered identity",
    );
    assert_compiled_select_query_matches_lowered_identity(
        &unfiltered,
        unfiltered_sql,
        "unfiltered grouped aggregate query should preserve canonical lowered identity",
    );
    assert_compiled_select_queries_remain_distinct_for_entity(
        &filtered,
        &operator,
        "grouped aggregate FILTER operator changes",
    );
    assert_compiled_select_queries_remain_distinct_for_entity(
        &filtered,
        &unfiltered,
        "filtered versus unfiltered grouped aggregate changes",
    );
    assert_distinct_compiled_selects_execute_through_shared_query_plan_for_entity::<SessionSqlEntity>(
        &session, &filtered, &operator,
    );
}

#[test]
fn sql_compile_cache_covers_insert_update_and_delete_mutation_families() {
    reset_session_sql_store();
    let session = sql_session();

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "new SQL session should start with an empty compiled-command cache",
    );

    let insert = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("INSERT should compile into the update-surface cache");
    assert!(
        matches!(
            insert,
            crate::db::session::sql::CompiledSqlCommand::Insert(_)
        ),
        "INSERT should cache the prepared INSERT artifact",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "first update-surface compile should populate one cache entry",
    );

    let insert_repeat = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("same INSERT should compile from the existing update-surface cache entry");
    assert!(
        matches!(
            insert_repeat,
            crate::db::session::sql::CompiledSqlCommand::Insert(_)
        ),
        "repeated INSERT should stay on the prepared INSERT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical update-surface compile must not grow the cache",
    );

    let update = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        )
        .expect("UPDATE should compile into the update-surface cache");
    assert!(
        matches!(
            update,
            crate::db::session::sql::CompiledSqlCommand::Update(_)
        ),
        "UPDATE should cache the prepared UPDATE artifact",
    );

    let delete = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE name = 'Ada' RETURNING name",
        )
        .expect("DELETE RETURNING should compile into the update-surface cache");
    assert!(
        matches!(
            delete,
            crate::db::session::sql::CompiledSqlCommand::Delete { .. }
        ),
        "DELETE RETURNING should cache the lowered DELETE artifact",
    );

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        3,
        "update-surface cache should retain distinct entries for INSERT, UPDATE, and DELETE",
    );
}
