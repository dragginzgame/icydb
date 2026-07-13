use super::*;
use crate::{
    db::session::sql::{
        DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT, DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
        DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES, DEFAULT_PUBLIC_INSERT_STAGED_ROWS,
        DEFAULT_PUBLIC_UPDATE_RETURNING_RESPONSE_BYTES,
    },
    db::{
        AuthoredStructuralPatch, MutationMode, SqlDeleteExposurePolicy, SqlDeletePolicyContext,
        SqlPublicBoundedDeletePlan, SqlPublicBoundedUpdatePlan, SqlPublicPrimaryKeyDeletePlan,
        SqlPublicPrimaryKeyUpdatePlan, SqlUpdateExposurePolicy, SqlUpdatePolicyContext,
        SqlValidatedDeletePlan, SqlValidatedUpdatePlan, classify_sql_delete_policy,
        classify_sql_update_policy,
    },
    error::InternalError,
    metrics::sink::SqlWriteKind,
};

// Execute one write statement through the statement SQL boundary and assert it
// returns the canonical count payload for non-RETURNING write forms.
fn assert_statement_count<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_row_count: u32,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let payload = execute_sql_statement_for_tests::<E>(session, sql)
        .unwrap_or_else(|err| panic!("{context} should return count payload: {err}"));
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("{context} should return count payload");
    };

    assert_eq!(
        row_count, expected_row_count,
        "{context} should follow traditional SQL count semantics without RETURNING",
    );
}

// Execute one write statement that must stay fail-closed and assert it carries
// the compact SQL write boundary code instead of relying on message text.
fn assert_statement_write_boundary<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_boundary: SqlWriteBoundaryCode,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let err = execute_sql_statement_for_tests::<E>(session, sql).expect_err(context);

    assert_sql_write_boundary_detail(err, expected_boundary);
}

// Execute one signed write statement that widens parser literals and assert it
// returns the canonical count payload plus the expected persisted signed rows.
fn assert_signed_write_count_and_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_rows: &[Vec<Value>],
    context: &str,
) {
    assert_statement_count::<SessionSqlSignedWriteEntity>(session, sql, 1, context);

    let persisted = statement_projection_rows::<SessionSqlSignedWriteEntity>(
        session,
        "SELECT id, delta FROM SessionSqlSignedWriteEntity ORDER BY id ASC",
    )
    .unwrap_or_else(|err| panic!("{context} post-write projection should succeed: {err}"));

    assert_eq!(
        persisted, expected_rows,
        "{context} should persist the widened signed literal values",
    );
}

// Execute one write statement with RETURNING through the projection-row helper
// and assert the projected value rows stay stable for the requested surface.
fn assert_statement_returning_rows<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_rows: &[Vec<Value>],
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let rows = statement_projection_rows::<E>(session, sql)
        .unwrap_or_else(|err| panic!("{context} should return projection rows: {err}"));

    assert_eq!(
        rows, expected_rows,
        "{context} should preserve the requested RETURNING projection rows",
    );
}

// Seed one deterministic `SessionSqlWriteEntity` cohort so write-boundary tests
// can share the same setup path without repeating row literals inline.
fn seed_write_entities(session: &DbSession<SessionSqlCanister>, rows: &[(u64, &str, u64)]) {
    for (id, name, age) in rows {
        session
            .insert(SessionSqlWriteEntity {
                id: *id,
                name: (*name).to_string(),
                age: *age,
            })
            .expect("typed setup insert should succeed");
    }
}

fn oversized_public_update_returning_text() -> String {
    "x".repeat(DEFAULT_PUBLIC_UPDATE_RETURNING_RESPONSE_BYTES as usize + 1)
}

fn oversized_public_delete_returning_text() -> String {
    "x".repeat(DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES as usize + 1)
}

fn public_bounded_delete_plan_with_response_cap(
    sql: &str,
    max_returning_response_bytes: Option<u32>,
) -> SqlPublicBoundedDeletePlan {
    let report = classify_sql_delete_policy(
        sql,
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        SqlDeletePolicyContext {
            primary_key_fields: &["id"],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes,
        },
    )
    .expect("public bounded DELETE SQL should parse");

    let Some(SqlValidatedDeletePlan::PublicBoundedDeterministic(plan)) = report.plan else {
        panic!("public bounded DELETE SQL should produce a bounded plan");
    };

    plan
}

fn public_primary_key_delete_plan_with_response_cap(
    sql: &str,
    max_returning_response_bytes: Option<u32>,
) -> SqlPublicPrimaryKeyDeletePlan {
    let report = classify_sql_delete_policy(
        sql,
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        SqlDeletePolicyContext {
            primary_key_fields: &["id"],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT,
            max_returning_rows: None,
            max_returning_response_bytes,
        },
    )
    .expect("public primary-key DELETE SQL should parse");

    let Some(SqlValidatedDeletePlan::PublicPrimaryKeyOnly(plan)) = report.plan else {
        panic!("public primary-key DELETE SQL should produce a primary-key plan");
    };

    plan
}

fn public_primary_key_update_plan(sql: &str) -> SqlPublicPrimaryKeyUpdatePlan {
    public_primary_key_update_plan_with_returning_caps(sql, None, None)
}

fn public_primary_key_update_plan_with_response_cap(
    sql: &str,
    max_returning_response_bytes: Option<u32>,
) -> SqlPublicPrimaryKeyUpdatePlan {
    public_primary_key_update_plan_with_returning_caps(sql, None, max_returning_response_bytes)
}

fn public_primary_key_update_plan_with_returning_caps(
    sql: &str,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlPublicPrimaryKeyUpdatePlan {
    let report = classify_sql_update_policy(
        sql,
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        SqlUpdatePolicyContext {
            primary_key_fields: &["id"],
            generated_fields: &[],
            managed_fields: &[],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
            max_returning_rows,
            max_returning_response_bytes,
        },
    )
    .expect("public primary-key UPDATE SQL should parse");

    let Some(SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(plan)) = report.plan else {
        panic!("public primary-key UPDATE SQL should produce a primary-key plan");
    };

    plan
}

fn public_bounded_update_plan(sql: &str) -> SqlPublicBoundedUpdatePlan {
    public_bounded_update_plan_with_returning_caps(sql, None, None)
}

fn public_bounded_update_plan_with_response_cap(
    sql: &str,
    max_returning_response_bytes: Option<u32>,
) -> SqlPublicBoundedUpdatePlan {
    public_bounded_update_plan_with_returning_caps(sql, None, max_returning_response_bytes)
}

fn public_bounded_update_plan_with_returning_caps(
    sql: &str,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlPublicBoundedUpdatePlan {
    let report = classify_sql_update_policy(
        sql,
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        SqlUpdatePolicyContext {
            primary_key_fields: &["id"],
            generated_fields: &[],
            managed_fields: &[],
            max_public_bounded_limit: DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT,
            max_returning_rows,
            max_returning_response_bytes,
        },
    )
    .expect("public bounded UPDATE SQL should parse");

    let Some(SqlValidatedUpdatePlan::PublicBoundedDeterministic(plan)) = report.plan else {
        panic!("public bounded UPDATE SQL should produce a bounded plan");
    };

    plan
}

fn public_bounded_update_plan_with_caps(
    sql: &str,
    max_staged_rows: Option<u32>,
    max_returning_rows: Option<u32>,
) -> SqlPublicBoundedUpdatePlan {
    let mut plan = public_bounded_update_plan_with_returning_caps(sql, max_returning_rows, None);
    let mut execution_bounds = plan.execution_bounds();
    execution_bounds.max_staged_rows = max_staged_rows;
    plan.set_execution_bounds_for_tests(execution_bounds);

    plan
}

fn rejected_public_primary_key_update_has_no_plan(sql: &str) {
    let report = classify_sql_update_policy(
        sql,
        SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        SqlUpdatePolicyContext::new(&["id"]),
    )
    .expect("public primary-key UPDATE rejection SQL should parse");

    assert!(
        report.plan.is_none(),
        "rejected public primary-key UPDATE must not expose an executable plan",
    );
}

fn rejected_public_bounded_update_has_no_plan(sql: &str) {
    let report = classify_sql_update_policy(
        sql,
        SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        SqlUpdatePolicyContext::new(&["id"]),
    )
    .expect("public bounded UPDATE rejection SQL should parse");

    assert!(
        report.plan.is_none(),
        "rejected public bounded UPDATE must not expose an executable plan",
    );
}

// Assert one SQL write rejection from the accepted-schema transition barrier
// while preserving the row surface after the schema fixture is cleared.
fn assert_sql_write_unsupported_transition(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) {
    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(session, sql)
        .expect_err("SQL write should reject unsupported accepted schema drift");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
        "{context} should surface the schema-transition barrier as a compact unsupported diagnostic",
    );
}

fn captured_sql_write_events(
    events: &[MetricsEvent],
) -> Vec<(&'static str, SqlWriteKind, [u64; 4])> {
    events
        .iter()
        .filter_map(|event| match event {
            MetricsEvent::SqlWrite {
                entity_path,
                kind,
                staged_rows,
                matched_rows,
                mutated_rows,
                returning_rows,
            } => Some((
                *entity_path,
                *kind,
                [*staged_rows, *matched_rows, *mutated_rows, *returning_rows],
            )),
            _ => None,
        })
        .collect()
}

fn capture_sql_write_events(run: impl FnOnce()) -> Vec<(&'static str, SqlWriteKind, [u64; 4])> {
    let ((), events) = capture_session_metrics(run);

    captured_sql_write_events(&events)
}

const BROAD_SQL_WRITE_ROWS: [(u64, &str, u64); 6] = [
    (1, "Ada", 21),
    (2, "Bea", 22),
    (3, "Cid", 23),
    (4, "Dee", 24),
    (5, "Eli", 25),
    (6, "Fay", 26),
];

fn capture_seeded_write_entity_events(
    run: impl FnOnce(&DbSession<SessionSqlCanister>),
) -> Vec<(&'static str, SqlWriteKind, [u64; 4])> {
    capture_sql_write_events(|| {
        reset_session_sql_store();
        let session = sql_session();
        seed_write_entities(&session, &BROAD_SQL_WRITE_ROWS);

        run(&session);
    })
}

fn broad_write_id_rows() -> Vec<Vec<Value>> {
    (1..=BROAD_SQL_WRITE_ROWS.len() as u64)
        .map(|id| vec![Value::Nat64(id)])
        .collect()
}

fn assert_single_sql_write_event(
    actual: Vec<(&'static str, SqlWriteKind, [u64; 4])>,
    entity_path: &'static str,
    kind: SqlWriteKind,
    rows: [u64; 4],
) {
    assert_eq!(actual, vec![(entity_path, kind, rows)]);
}

fn captured_sql_write_error_events(
    events: &[MetricsEvent],
) -> Vec<(&'static str, SqlWriteKind, ErrorClass)> {
    events
        .iter()
        .filter_map(|event| match event {
            MetricsEvent::SqlWriteError {
                entity_path,
                kind,
                class,
            } => Some((*entity_path, *kind, *class)),
            _ => None,
        })
        .collect()
}

// Read back the canonical `SessionSqlWriteEntity` ordered row surface used by
// the SQL write tests that assert persisted post-write state.
fn persisted_write_rows(session: &DbSession<SessionSqlCanister>) -> Vec<Vec<Value>> {
    statement_projection_rows::<SessionSqlWriteEntity>(
        session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-write SQL projection should succeed")
}

fn write_rows(rows: &[(u64, &str, u64)]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|(id, name, age)| {
            vec![
                Value::Nat64(*id),
                Value::Text((*name).to_string()),
                Value::Nat64(*age),
            ]
        })
        .collect()
}

fn persisted_write_ages(session: &DbSession<SessionSqlCanister>) -> Vec<Vec<Value>> {
    statement_projection_rows::<SessionSqlWriteEntity>(
        session,
        "SELECT id, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-write age SQL projection should succeed")
}

fn persisted_composite_write_rows(session: &DbSession<SessionSqlCanister>) -> Vec<Vec<Value>> {
    statement_projection_rows::<SessionSqlCompositeWriteEntity>(
        session,
        "SELECT tenant_id, local_id, name, age FROM SessionSqlCompositeWriteEntity \
         ORDER BY tenant_id ASC, local_id ASC",
    )
    .expect("post-write composite SQL projection should succeed")
}

fn persisted_generated_timestamp_rows(session: &DbSession<SessionSqlCanister>) -> Vec<Vec<Value>> {
    statement_projection_rows::<SessionSqlGeneratedTimestampEntity>(
        session,
        "SELECT id, created_on_insert, name FROM SessionSqlGeneratedTimestampEntity ORDER BY id ASC",
    )
    .expect("post-write generated timestamp projection should succeed")
}

fn persisted_managed_write_rows(session: &DbSession<SessionSqlCanister>) -> Vec<Vec<Value>> {
    statement_projection_rows::<SessionSqlManagedWriteEntity>(
        session,
        "SELECT id, name, created_at, updated_at FROM SessionSqlManagedWriteEntity ORDER BY id ASC",
    )
    .expect("post-write managed timestamp projection should succeed")
}

// Execute one `SessionSqlWriteEntity` UPDATE statement and assert both the
// returned count payload and the persisted ordered row surface stay stable.
fn assert_write_update_count_and_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_row_count: u32,
    expected_rows: &[Vec<Value>],
    context: &str,
) {
    assert_statement_count::<SessionSqlWriteEntity>(session, sql, expected_row_count, context);

    let persisted = persisted_write_rows(session);
    assert_eq!(
        persisted, expected_rows,
        "{context} should preserve the expected persisted write rows",
    );
}

// Execute one SQL statement that returns a single unsigned id column and decode
// it into the compact key list used by update/delete target convergence tests.
fn statement_nat_ids<E>(session: &DbSession<SessionSqlCanister>, sql: &str) -> Vec<u64>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    statement_projection_rows::<E>(session, sql)
        .unwrap_or_else(|err| panic!("id-returning SQL should succeed: {err}"))
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Nat64(id)] => *id,
            other => panic!("id-returning SQL should emit one nat id column, got {other:?}"),
        })
        .collect()
}

// Run one selector-shaped statement against a fresh deterministic write fixture
// so SELECT, UPDATE RETURNING, and DELETE RETURNING can be compared without
// mutation side effects leaking between surfaces.
fn write_selector_ids(sql: &str) -> Vec<u64> {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(
        &session,
        &[
            (1, "Ada", 21),
            (2, "Bea", 30),
            (3, "Cid", 25),
            (4, "Dee", 40),
        ],
    );

    statement_nat_ids::<SessionSqlWriteEntity>(&session, sql)
}

// Run one count-returning write statement against the same fresh deterministic
// write fixture used by selector convergence tests.
fn write_count(sql: &str) -> u32 {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(
        &session,
        &[
            (1, "Ada", 21),
            (2, "Bea", 30),
            (3, "Cid", 25),
            (4, "Dee", 40),
        ],
    );

    let payload = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(&session, sql)
        .unwrap_or_else(|err| panic!("count-returning SQL write should succeed: {err}"));
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("count-returning SQL write should return a count payload");
    };

    row_count
}

// Compare selector keys while allowing explicitly unordered SQL surfaces to
// differ in row order but never in the target key set.
fn assert_selector_ids_match(
    mut expected: Vec<u64>,
    mut actual: Vec<u64>,
    ordered: bool,
    context: &str,
) {
    if !ordered {
        expected.sort_unstable();
        actual.sort_unstable();
    }

    assert_eq!(
        actual, expected,
        "{context} should select the same target ids"
    );
}

// Seed one generated-timestamp row so SQL and structural rewrite tests can
// share the same persisted setup without restating the entity literal.
fn seed_generated_timestamp_entity(
    session: &DbSession<SessionSqlCanister>,
    id: u64,
    name: &str,
    created_on_insert_nanos: i64,
) {
    session
        .insert(SessionSqlGeneratedTimestampEntity {
            id,
            created_on_insert: Timestamp::from_nanos(created_on_insert_nanos),
            name: name.to_string(),
        })
        .expect("generated timestamp setup insert should succeed");
}

fn seed_composite_key_terminal_entities(
    session: &DbSession<SessionSqlCanister>,
) -> (
    SessionSqlCompositeWriteEntityKey,
    SessionSqlCompositeWriteEntityKey,
) {
    let first_key = SessionSqlCompositeWriteEntityKey {
        tenant_id: 7,
        local_id: 1,
    };
    let second_key = SessionSqlCompositeWriteEntityKey {
        tenant_id: 7,
        local_id: 2,
    };

    session
        .insert_many_atomic([
            SessionSqlCompositeWriteEntity {
                tenant_id: first_key.tenant_id,
                local_id: first_key.local_id,
                name: "Ada".to_string(),
                age: 21,
            },
            SessionSqlCompositeWriteEntity {
                tenant_id: second_key.tenant_id,
                local_id: second_key.local_id,
                name: "Bea".to_string(),
                age: 22,
            },
        ])
        .expect("typed composite-key setup insert should succeed");

    (first_key, second_key)
}

// Build one structural insert/replace patch that explicitly writes the
// generated timestamp field so generated-field rejection matrices can share it.
fn generated_timestamp_insert_patch(
    session: &DbSession<SessionSqlCanister>,
    id: u64,
    name: &str,
    created_on_insert_nanos: i64,
    context: &str,
) -> AuthoredStructuralPatch {
    session
        .structural_patch::<SessionSqlGeneratedTimestampEntity, _, _, _>([
            ("id", Value::Nat64(id)),
            (
                "created_on_insert",
                Value::Timestamp(Timestamp::from_nanos(created_on_insert_nanos)),
            ),
            ("name", Value::Text(name.to_string())),
        ])
        .unwrap_or_else(|err| panic!("{context} should resolve accepted-schema fields: {err}"))
}

#[test]
fn execute_sql_write_rejects_unsupported_schema_transition_before_staging() {
    reset_session_sql_store();
    let session = sql_session();
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_session_sql_write_old_accepted_schema_prefix();

    assert_sql_write_unsupported_transition(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        "SQL INSERT",
    );
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    assert!(
        persisted_write_rows(&session).is_empty(),
        "unsupported INSERT transition must fail before mutation staging",
    );

    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_session_sql_write_old_accepted_schema_prefix();
    assert_sql_write_unsupported_transition(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        "SQL UPDATE",
    );
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    assert_eq!(
        persisted_write_rows(&session),
        vec![vec![
            Value::Nat64(1),
            Value::Text("Ada".to_string()),
            Value::Nat64(21),
        ]],
        "unsupported UPDATE transition must fail before mutation staging",
    );

    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    install_session_sql_write_old_accepted_schema_prefix();
    assert_sql_write_unsupported_transition(
        &session,
        "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING id",
        "SQL DELETE RETURNING",
    );
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    assert_eq!(
        persisted_write_rows(&session),
        vec![vec![
            Value::Nat64(1),
            Value::Text("Ada".to_string()),
            Value::Nat64(21),
        ]],
        "unsupported DELETE transition must fail before delete staging",
    );
}

#[test]
fn session_structural_patch_resolves_fields_through_accepted_schema_descriptor() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);

    let patch = session
        .structural_patch::<SessionSqlWriteEntity, _, _, _>([
            ("name", Value::Text("Ari".to_string())),
            ("age", Value::Nat64(31)),
        ])
        .expect("session structural patch should resolve accepted schema fields");
    let updated = session
        .mutate_structural::<SessionSqlWriteEntity>(1, patch, MutationMode::Update)
        .expect("accepted-schema structural patch should update the row");

    assert_eq!(updated.name, "Ari");
    assert_eq!(updated.age, 31);
    assert_eq!(
        persisted_write_rows(&session),
        vec![vec![
            Value::Nat64(1),
            Value::Text("Ari".to_string()),
            Value::Nat64(31),
        ]],
    );
}

// Assert one structural generated-field rejection keeps the Unsupported class
// and names the ownership-protected generated field.
fn assert_structural_generated_field_rejection(err: &InternalError, context: &str) {
    assert_eq!(err.class(), ErrorClass::Unsupported);
    assert_eq!(
        err.diagnostic_code(),
        DiagnosticCode::RuntimeUnsupported,
        "{context} should preserve compact generated-field ownership diagnostics",
    );
}

// Execute one supported `INSERT ... SELECT ... RETURNING *` statement and
// assert it synthesizes a fresh primary key while preserving the projected row
// payload and the expected persisted post-insert surface.
fn assert_insert_select_returning_and_persisted_rows(
    session: &DbSession<SessionSqlCanister>,
    returning_sql: &str,
    persisted_sql: &str,
    expected_inserted_name: &str,
    expected_persisted: &[Vec<Value>],
    context: &str,
) {
    let rows = statement_projection_rows::<SessionSqlEntity>(session, returning_sql)
        .unwrap_or_else(|err| panic!("{context} should succeed with RETURNING: {err}"));
    let persisted = statement_projection_rows::<SessionSqlEntity>(session, persisted_sql)
        .unwrap_or_else(|err| {
            panic!("{context} post-insert-select projection should succeed: {err}")
        });

    assert_eq!(rows.len(), 1, "{context} should insert one row");
    assert!(
        matches!(rows[0][0], Value::Ulid(_)),
        "{context} should synthesize one schema-generated Ulid primary key",
    );
    assert_ne!(
        rows[0][0],
        Value::Ulid(Ulid::from_u128(1)),
        "{context} should allocate a fresh generated primary key",
    );
    assert_eq!(
        rows[0][1..],
        [
            Value::Text(expected_inserted_name.to_string()),
            Value::Nat64(21)
        ],
        "{context} should preserve the projected source payload",
    );
    assert_eq!(
        persisted, expected_persisted,
        "{context} should persist the expected post-insert rows",
    );
}

#[test]
fn execute_sql_statement_single_row_insert_matrix_returns_count_without_returning() {
    let cases = [
        (
            "explicit-column insert",
            Some(
                "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21) RETURNING id",
            ),
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (2, 'Bea', 22)",
            1_u32,
        ),
        (
            "single-table-alias insert",
            None,
            "INSERT INTO SessionSqlWriteEntity s (id, name, age) VALUES (6, 'Fae', 26)",
            1_u32,
        ),
        (
            "canonical-order insert",
            None,
            "INSERT INTO SessionSqlWriteEntity VALUES (4, 'Dee', 24)",
            1_u32,
        ),
    ];

    for (context, columns_sql, row_sql, expected_row_count) in cases {
        reset_session_sql_store();
        let session = sql_session();

        if let Some(columns_sql) = columns_sql {
            let columns =
                statement_projection_columns::<SessionSqlWriteEntity>(&session, columns_sql)
                    .unwrap_or_else(|err| {
                        panic!("{context} should return projection payload: {err}")
                    });
            assert_eq!(columns, vec!["id"]);
        }

        assert_statement_count::<SessionSqlWriteEntity>(
            &session,
            row_sql,
            expected_row_count,
            context,
        );
    }
}

#[test]
fn execute_sql_statement_multi_row_insert_matrix_returns_count_without_returning() {
    for (sql, expected_row_count, check_persisted, context) in [
        (
            "INSERT INTO SessionSqlWriteEntity (id, name, age) \
             VALUES (2, 'Bea', 22), (3, 'Cid', 23)",
            2_u32,
            true,
            "explicit-column multi-row insert",
        ),
        (
            "INSERT INTO SessionSqlWriteEntity VALUES (4, 'Dee', 24), (5, 'Eli', 25)",
            2_u32,
            false,
            "canonical-order multi-row insert",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();

        assert_statement_count::<SessionSqlWriteEntity>(&session, sql, expected_row_count, context);

        if check_persisted {
            let persisted = statement_projection_rows::<SessionSqlWriteEntity>(
                &session,
                "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
            )
            .unwrap_or_else(|err| panic!("{context} post-insert projection should succeed: {err}"));
            assert_eq!(
                persisted.len(),
                usize::try_from(expected_row_count).unwrap_or(usize::MAX),
                "{context} should persist the counted insert rows",
            );
        }
    }
}

#[test]
fn execute_sql_insert_update_supports_composite_primary_key_fields() {
    reset_session_sql_store();
    let session = sql_session();

    assert_statement_returning_rows::<SessionSqlCompositeWriteEntity>(
        &session,
        "INSERT INTO SessionSqlCompositeWriteEntity \
         (tenant_id, local_id, name, age) \
         VALUES (1, 10, 'Ada', 21), (1, 11, 'Bea', 22) \
         RETURNING tenant_id, local_id, name, age",
        &[
            vec![
                Value::Nat64(1),
                Value::Nat64(10),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(1),
                Value::Nat64(11),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
        ],
        "composite primary-key SQL INSERT",
    );

    assert_statement_returning_rows::<SessionSqlCompositeWriteEntity>(
        &session,
        "UPDATE SessionSqlCompositeWriteEntity \
         SET age = 30 \
         WHERE tenant_id = 1 AND local_id = 10 \
         RETURNING tenant_id, local_id, name, age",
        &[vec![
            Value::Nat64(1),
            Value::Nat64(10),
            Value::Text("Ada".to_string()),
            Value::Nat64(30),
        ]],
        "composite primary-key SQL UPDATE",
    );

    assert_eq!(
        persisted_composite_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Nat64(10),
                Value::Text("Ada".to_string()),
                Value::Nat64(30),
            ],
            vec![
                Value::Nat64(1),
                Value::Nat64(11),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
        ],
    );
}

#[test]
fn execute_sql_select_order_by_composite_primary_key_component_preserves_declared_order() {
    reset_session_sql_store();
    let session = sql_session();

    assert_statement_count::<SessionSqlCompositeWriteEntity>(
        &session,
        "INSERT INTO SessionSqlCompositeWriteEntity \
         (tenant_id, local_id, name, age) \
         VALUES \
         (1, 1, 'Ada', 21), \
         (2, 1, 'Bea', 22), \
         (1, 2, 'Cy', 23), \
         (2, 2, 'Dee', 24), \
         (1, 3, 'Eli', 25), \
         (2, 3, 'Fay', 26)",
        6,
        "composite primary-key SQL ordering fixture",
    );

    let rows = statement_projection_rows::<SessionSqlCompositeWriteEntity>(
        &session,
        "SELECT tenant_id, local_id FROM SessionSqlCompositeWriteEntity ORDER BY local_id DESC",
    )
    .expect("composite primary-key component ORDER BY should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Nat64(1), Value::Nat64(3)],
            vec![Value::Nat64(2), Value::Nat64(3)],
            vec![Value::Nat64(1), Value::Nat64(2)],
            vec![Value::Nat64(2), Value::Nat64(2)],
            vec![Value::Nat64(1), Value::Nat64(1)],
            vec![Value::Nat64(2), Value::Nat64(1)],
        ],
        "ORDER BY over a composite primary-key component must not be rewritten as full primary-key order",
    );
}

#[test]
fn fluent_exact_key_paths_support_composite_primary_keys() {
    reset_session_sql_store();
    let session = sql_session();
    let (first_key, second_key) = seed_composite_key_terminal_entities(&session);

    let first = session
        .load::<SessionSqlCompositeWriteEntity>()
        .trusted_read_unchecked()
        .by_id(Id::from_key(first_key))
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("composite by_id load should succeed");
    assert_eq!(first.len(), 1);
    assert_eq!(first.as_slice()[0].id(), Id::from_key(first_key));
    assert_eq!(first.as_slice()[0].entity_ref().name, "Ada");

    let both = session
        .load::<SessionSqlCompositeWriteEntity>()
        .trusted_read_unchecked()
        .by_ids([Id::from_key(second_key), Id::from_key(first_key)])
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("composite by_ids load should succeed");
    assert_eq!(both.len(), 2);
    assert!(both.contains_id(&Id::from_key(first_key)));
    assert!(both.contains_id(&Id::from_key(second_key)));

    let deleted = session
        .delete::<SessionSqlCompositeWriteEntity>()
        .by_id(Id::from_key(first_key))
        .execute()
        .expect("composite by_id delete should succeed");
    assert_eq!(deleted, 1);
    assert_eq!(
        persisted_composite_write_rows(&session),
        vec![vec![
            Value::Nat64(second_key.tenant_id),
            Value::Nat64(second_key.local_id),
            Value::Text("Bea".to_string()),
            Value::Nat64(22),
        ]],
    );
}

#[test]
fn fluent_id_terminals_support_composite_primary_keys() {
    reset_session_sql_store();
    let session = sql_session();
    let (first_key, second_key) = seed_composite_key_terminal_entities(&session);

    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .min()
            .expect("composite min id terminal should succeed"),
        Some(Id::from_key(first_key)),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .max()
            .expect("composite max id terminal should succeed"),
        Some(Id::from_key(second_key)),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .min_by("age")
            .expect("composite min_by id terminal should succeed"),
        Some(Id::from_key(first_key)),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .max_by("age")
            .expect("composite max_by id terminal should succeed"),
        Some(Id::from_key(second_key)),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .nth_by("age", 1)
            .expect("composite nth_by id terminal should succeed"),
        Some(Id::from_key(second_key)),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .median_by("age")
            .expect("composite median_by id terminal should succeed"),
        Some(Id::from_key(first_key)),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .min_max_by("age")
            .expect("composite min_max_by id terminal should succeed"),
        Some((Id::from_key(first_key), Id::from_key(second_key))),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .order_term(crate::db::asc("local_id"))
            .values_by_with_ids("age")
            .expect("composite values_by_with_ids terminal should succeed"),
        outputs_with_ids::<SessionSqlCompositeWriteEntity>(vec![
            (Id::from_key(first_key), Value::Nat64(21)),
            (Id::from_key(second_key), Value::Nat64(22)),
        ]),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .top_k_by_with_ids("age", 1)
            .expect("composite top_k_by_with_ids terminal should succeed"),
        outputs_with_ids::<SessionSqlCompositeWriteEntity>(vec![(
            Id::from_key(second_key),
            Value::Nat64(22),
        )]),
    );
    assert_eq!(
        session
            .load::<SessionSqlCompositeWriteEntity>()
            .trusted_read_unchecked()
            .bottom_k_by_with_ids("age", 1)
            .expect("composite bottom_k_by_with_ids terminal should succeed"),
        outputs_with_ids::<SessionSqlCompositeWriteEntity>(vec![(
            Id::from_key(first_key),
            Value::Nat64(21),
        )]),
    );
}

#[test]
fn fluent_paged_load_resumes_composite_primary_key_rows_without_duplicates() {
    reset_session_sql_store();
    let session = sql_session();
    let (first_key, second_key) = seed_composite_key_terminal_entities(&session);

    session
        .insert(SessionSqlCompositeWriteEntity {
            tenant_id: first_key.tenant_id,
            local_id: 3,
            name: "Cid".to_string(),
            age: 22,
        })
        .expect("third composite-key setup insert should succeed");

    let first_page = session
        .load::<SessionSqlCompositeWriteEntity>()
        .trusted_read_unchecked()
        .order_term(crate::db::asc("age"))
        .page(2)
        .expect("first composite-key page should execute");
    assert_eq!(
        first_page.response().ids().collect::<Vec<_>>(),
        vec![Id::from_key(first_key), Id::from_key(second_key)],
        "first composite-key page should preserve ordered row identity",
    );
    let cursor = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first composite-key page should emit a cursor"),
    );

    let second_page = session
        .load::<SessionSqlCompositeWriteEntity>()
        .trusted_read_unchecked()
        .order_term(crate::db::asc("age"))
        .next_page(2, cursor)
        .expect("second composite-key page should resume after the first page");
    assert_eq!(
        second_page.response().ids().collect::<Vec<_>>(),
        vec![Id::from_key(SessionSqlCompositeWriteEntityKey {
            tenant_id: first_key.tenant_id,
            local_id: 3,
        })],
        "second composite-key page should not repeat rows from the first page",
    );
    assert!(
        second_page.continuation_cursor().is_none(),
        "second composite-key page should exhaust the ordered result set",
    );
}

#[test]
fn execute_sql_insert_rejects_missing_composite_primary_key_component() {
    reset_session_sql_store();
    let session = sql_session();

    assert_statement_write_boundary::<SessionSqlCompositeWriteEntity>(
        &session,
        "INSERT INTO SessionSqlCompositeWriteEntity \
         (tenant_id, name, age) \
         VALUES (1, 'Ada', 21)",
        SqlWriteBoundaryCode::MissingPrimaryKey,
        "composite primary-key SQL INSERT missing component",
    );
}

#[test]
fn execute_sql_update_rejects_composite_primary_key_mutation() {
    reset_session_sql_store();
    let session = sql_session();

    assert_statement_count::<SessionSqlCompositeWriteEntity>(
        &session,
        "INSERT INTO SessionSqlCompositeWriteEntity \
         (tenant_id, local_id, name, age) \
         VALUES (1, 10, 'Ada', 21)",
        1,
        "composite primary-key SQL INSERT setup",
    );
    assert_statement_write_boundary::<SessionSqlCompositeWriteEntity>(
        &session,
        "UPDATE SessionSqlCompositeWriteEntity \
         SET local_id = 12 \
         WHERE tenant_id = 1 AND local_id = 10",
        SqlWriteBoundaryCode::UpdatePrimaryKeyMutation,
        "composite primary-key SQL UPDATE mutation",
    );
}

#[test]
fn execute_sql_statement_multi_row_insert_late_failure_is_statement_atomic() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(2, "Existing", 20)]);

    execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) \
         VALUES (1, 'Ada', 21), (2, 'Dup', 22)",
    )
    .expect_err("late duplicate-key INSERT failure should reject the whole statement");

    assert_eq!(
        persisted_write_rows(&session),
        vec![vec![
            Value::Nat64(2),
            Value::Text("Existing".to_string()),
            Value::Nat64(20),
        ]],
        "late INSERT failure must not commit the earlier row",
    );
}

#[test]
fn execute_sql_statement_multi_row_insert_duplicate_keys_is_statement_atomic() {
    reset_session_sql_store();
    let session = sql_session();

    execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) \
         VALUES (1, 'Ada', 21), (1, 'Dup', 22)",
    )
    .expect_err("duplicate keys inside one INSERT statement should fail atomically");

    assert!(
        persisted_write_rows(&session).is_empty(),
        "duplicate-key INSERT must commit zero rows",
    );
}

#[test]
fn execute_sql_statement_insert_with_schema_generated_primary_key_matrix_accepts_omission() {
    let cases = [
        (
            "named-column omission",
            "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21)",
            "Ada",
            21_u64,
            true,
        ),
        (
            "positional omission",
            "INSERT INTO SessionSqlEntity VALUES ('Bea', 22)",
            "Bea",
            22_u64,
            false,
        ),
    ];

    for (context, sql, expected_name, expected_age, check_persisted) in cases {
        reset_session_sql_store();
        let session = sql_session();

        let rows = statement_projection_rows::<SessionSqlEntity>(
            &session,
            match sql {
                "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21)" => {
                    "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING *"
                }
                "INSERT INTO SessionSqlEntity VALUES ('Bea', 22)" => {
                    "INSERT INTO SessionSqlEntity VALUES ('Bea', 22) RETURNING *"
                }
                _ => unreachable!("generated-key insert matrix uses fixed SQL cases"),
            },
        )
        .unwrap_or_else(|err| {
            panic!("{context} should synthesize one schema-generated Ulid: {err}")
        });

        assert_eq!(rows.len(), 1);
        std::assert_matches!(rows[0][0], Value::Ulid(_));
        assert_eq!(
            rows[0][1..],
            [
                Value::Text(expected_name.to_string()),
                Value::Nat64(expected_age),
            ],
        );

        if check_persisted {
            let persisted = statement_projection_rows::<SessionSqlEntity>(
                &session,
                "SELECT name, age FROM SessionSqlEntity ORDER BY name ASC",
            )
            .unwrap_or_else(|err| panic!("{context} post-insert projection should succeed: {err}"));
            assert_eq!(
                persisted,
                vec![vec![
                    Value::Text(expected_name.to_string()),
                    Value::Nat64(expected_age)
                ]],
            );
        }
    }
}

#[test]
fn execute_sql_statement_insert_rejects_missing_required_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_boundary, context) in [
        (
            "INSERT INTO SessionSqlWriteEntity (id, name) VALUES (1, 'Ada')",
            SqlWriteBoundaryCode::MissingRequiredFields,
            "missing non-generated field",
        ),
        (
            "INSERT INTO SessionSqlWriteEntity (name, age) VALUES ('Ada', 21)",
            SqlWriteBoundaryCode::MissingPrimaryKey,
            "missing primary key field",
        ),
    ] {
        assert_statement_write_boundary::<SessionSqlWriteEntity>(
            &session,
            sql,
            expected_boundary,
            context,
        );
    }
}

#[test]
fn execute_sql_statement_write_rejects_explicit_managed_timestamp_fields_matrix() {
    let cases = [
        (
            "INSERT INTO SessionSqlManagedWriteEntity (id, name, created_at) VALUES (1, 'Ada', 0)",
            SqlWriteBoundaryCode::ExplicitManagedField,
            "INSERT explicit managed timestamp write",
            false,
        ),
        (
            "UPDATE SessionSqlManagedWriteEntity SET updated_at = 0 WHERE id = 1",
            SqlWriteBoundaryCode::ExplicitManagedField,
            "UPDATE explicit managed timestamp write",
            true,
        ),
    ];

    for (sql, expected_boundary, context, seed_row) in cases {
        reset_session_sql_store();
        let session = sql_session();

        if seed_row {
            session
                .insert(SessionSqlManagedWriteEntity {
                    id: 1,
                    name: "Ada".to_string(),
                    created_at: Timestamp::from_nanos(1),
                    updated_at: Timestamp::from_nanos(1),
                })
                .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));
        }

        assert_statement_write_boundary::<SessionSqlManagedWriteEntity>(
            &session,
            sql,
            expected_boundary,
            context,
        );
    }
}

#[test]
fn execute_sql_statement_insert_synthesizes_omitted_managed_timestamp_fields_matrix() {
    for (sql, expected_id, expected_name, context) in [
        (
            "INSERT INTO SessionSqlManagedWriteEntity (id, name) VALUES (1, 'Ada') \
             RETURNING id, name, created_at, updated_at",
            1,
            "Ada",
            "named-column managed timestamp insert",
        ),
        (
            "INSERT INTO SessionSqlManagedWriteEntity VALUES (2, 'Bea') \
             RETURNING id, name, created_at, updated_at",
            2,
            "Bea",
            "positional managed timestamp insert",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();

        let rows = statement_projection_rows::<SessionSqlManagedWriteEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should synthesize managed fields: {err}"));

        assert_eq!(rows.len(), 1, "{context} should return one inserted row");
        assert_eq!(rows[0][0], Value::Nat64(expected_id));
        assert_eq!(rows[0][1], Value::Text(expected_name.to_string()));
        let created_at = match &rows[0][2] {
            Value::Timestamp(value) => *value,
            other => panic!("{context} should return created_at timestamp, got {other:?}"),
        };
        let updated_at = match &rows[0][3] {
            Value::Timestamp(value) => *value,
            other => panic!("{context} should return updated_at timestamp, got {other:?}"),
        };
        assert_ne!(
            created_at,
            Timestamp::EPOCH,
            "{context} should not persist an epoch created_at",
        );
        assert_eq!(
            created_at, updated_at,
            "{context} should use one statement write timestamp for managed insert fields",
        );

        let persisted = statement_projection_rows::<SessionSqlManagedWriteEntity>(
            &session,
            "SELECT id, name, created_at, updated_at FROM SessionSqlManagedWriteEntity \
             ORDER BY id ASC",
        )
        .unwrap_or_else(|err| panic!("{context} post-insert projection should succeed: {err}"));
        assert_eq!(
            persisted, rows,
            "{context} should persist synthesized fields"
        );
    }
}

#[test]
fn execute_sql_statement_insert_select_synthesizes_omitted_managed_timestamp_fields() {
    reset_session_sql_store();
    let session = sql_session();
    let source_rows = statement_projection_rows::<SessionSqlGeneratedKeyManagedWriteEntity>(
        &session,
        "INSERT INTO SessionSqlGeneratedKeyManagedWriteEntity (name) VALUES ('Ada') \
         RETURNING id, name, created_at, updated_at",
    )
    .expect("managed generated-key source row setup should succeed");

    let rows = statement_projection_rows::<SessionSqlGeneratedKeyManagedWriteEntity>(
        &session,
        "INSERT INTO SessionSqlGeneratedKeyManagedWriteEntity (name) \
         SELECT name FROM SessionSqlGeneratedKeyManagedWriteEntity WHERE name = 'Ada' \
         RETURNING id, name, created_at, updated_at",
    )
    .unwrap_or_else(|err| panic!("INSERT SELECT should synthesize omitted managed fields: {err}"));

    assert_eq!(rows.len(), 1, "INSERT SELECT should return one row");
    assert_ne!(
        rows[0][0], source_rows[0][0],
        "INSERT SELECT should synthesize a distinct generated primary key",
    );
    assert!(
        matches!(rows[0][0], Value::Ulid(_)),
        "INSERT SELECT should return a generated Ulid primary key",
    );
    assert_eq!(rows[0][1], Value::Text("Ada".to_string()));
    let created_at = match &rows[0][2] {
        Value::Timestamp(value) => *value,
        other => panic!("INSERT SELECT should return created_at timestamp, got {other:?}"),
    };
    let updated_at = match &rows[0][3] {
        Value::Timestamp(value) => *value,
        other => panic!("INSERT SELECT should return updated_at timestamp, got {other:?}"),
    };
    assert_ne!(
        created_at,
        Timestamp::EPOCH,
        "INSERT SELECT should not persist an epoch created_at",
    );
    assert_eq!(
        created_at, updated_at,
        "INSERT SELECT should use one statement write timestamp for managed insert fields",
    );

    let persisted = statement_projection_rows::<SessionSqlGeneratedKeyManagedWriteEntity>(
        &session,
        "SELECT id, name, created_at, updated_at FROM SessionSqlGeneratedKeyManagedWriteEntity \
         ORDER BY name ASC, id ASC",
    )
    .expect("managed INSERT SELECT post-state projection should succeed");
    assert_eq!(persisted.len(), 2);
    assert!(
        persisted
            .iter()
            .any(|persisted_row| persisted_row == &rows[0]),
        "INSERT SELECT should persist the synthesized managed-field row",
    );
}

#[test]
fn execute_sql_statement_insert_rejects_explicit_generated_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_boundary, context) in [
        (
            "INSERT INTO SessionSqlGeneratedTimestampEntity (id, created_on_insert, name) VALUES (1, 7, 'Ada')",
            SqlWriteBoundaryCode::ExplicitGeneratedField,
            "named-column generated timestamp insert",
        ),
        (
            "INSERT INTO SessionSqlGeneratedTimestampEntity VALUES (2, 9, 'Bea')",
            SqlWriteBoundaryCode::ExplicitGeneratedField,
            "positional generated timestamp insert",
        ),
    ] {
        assert_statement_write_boundary::<SessionSqlGeneratedTimestampEntity>(
            &session,
            sql,
            expected_boundary,
            context,
        );
    }
}

#[test]
fn execute_sql_statement_insert_synthesizes_schema_generated_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    for (named_rows, positional_rows, generated_kind, context) in [
        (
            statement_projection_rows::<SessionSqlGeneratedFieldEntity>(
                &session,
                "INSERT INTO SessionSqlGeneratedFieldEntity (id, name) VALUES (1, 'Ada') RETURNING *",
            )
            .expect("SQL INSERT should synthesize omitted schema-generated non-primary fields"),
            statement_projection_rows::<SessionSqlGeneratedFieldEntity>(
                &session,
                "INSERT INTO SessionSqlGeneratedFieldEntity VALUES (2, 'Bea') RETURNING *",
            )
            .expect(
                "positional SQL INSERT should omit schema-generated non-primary fields by width",
            ),
            "ulid",
            "schema-generated non-primary field",
        ),
        (
            statement_projection_rows::<SessionSqlGeneratedTimestampEntity>(
                &session,
                "INSERT INTO SessionSqlGeneratedTimestampEntity (id, name) VALUES (1, 'Ada') RETURNING *",
            )
            .expect("SQL INSERT should synthesize omitted schema-generated timestamp fields"),
            statement_projection_rows::<SessionSqlGeneratedTimestampEntity>(
                &session,
                "INSERT INTO SessionSqlGeneratedTimestampEntity VALUES (2, 'Bea') RETURNING *",
            )
            .expect("positional SQL INSERT should omit schema-generated timestamp fields by width"),
            "timestamp",
            "schema-generated timestamp field",
        ),
    ] {
        assert_eq!(
            named_rows.len(),
            1,
            "{context} named insert should return one row"
        );
        assert_eq!(
            positional_rows.len(),
            1,
            "{context} positional insert should return one row",
        );
        assert_eq!(named_rows[0][0], Value::Nat64(1));
        assert_eq!(positional_rows[0][0], Value::Nat64(2));
        match generated_kind {
            "ulid" => {
                assert!(
                    matches!(named_rows[0][1], Value::Ulid(_)),
                    "{context} named insert should synthesize a Ulid field",
                );
                assert!(
                    matches!(positional_rows[0][1], Value::Ulid(_)),
                    "{context} positional insert should synthesize a Ulid field",
                );
            }
            "timestamp" => {
                assert!(
                    matches!(named_rows[0][1], Value::Timestamp(_)),
                    "{context} named insert should synthesize a timestamp field",
                );
                assert!(
                    matches!(positional_rows[0][1], Value::Timestamp(_)),
                    "{context} positional insert should synthesize a timestamp field",
                );
            }
            other => panic!("unexpected generated field kind: {other}"),
        }
        assert_eq!(named_rows[0][2], Value::Text("Ada".to_string()));
        assert_eq!(positional_rows[0][2], Value::Text("Bea".to_string()));
    }
}

#[test]
fn structural_create_rejects_explicit_generated_insert_fields_matrix() {
    let cases = [
        (
            MutationMode::Insert,
            1_u64,
            "Ada",
            7_i64,
            "structural insert explicit generated timestamp",
        ),
        (
            MutationMode::Replace,
            2_u64,
            "Bea",
            9_i64,
            "structural replace-on-missing explicit generated timestamp",
        ),
    ];

    for (mode, key, name, created_on_insert_nanos, context) in cases {
        reset_session_sql_store();
        let session = sql_session();
        let patch = generated_timestamp_insert_patch(
            &session,
            key,
            name,
            created_on_insert_nanos,
            "generated timestamp structural create",
        );

        let err = session
            .mutate_structural::<SessionSqlGeneratedTimestampEntity>(key, patch, mode)
            .expect_err("structural create lanes should reject explicit insert-generated fields");

        assert_structural_generated_field_rejection(&err, context);
    }
}

#[test]
fn execute_sql_statement_update_rejects_explicit_generated_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_timestamp_entity(&session, 1, "Ada", 1);

    let err = execute_sql_statement_for_tests::<SessionSqlGeneratedTimestampEntity>(
        &session,
        "UPDATE SessionSqlGeneratedTimestampEntity SET created_on_insert = 7 WHERE id = 1",
    )
    .expect_err("insert-generated fields should stay system-owned on SQL UPDATE");
    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ExplicitGeneratedField);
}

#[test]
fn structural_rewrite_rejects_explicit_generated_insert_fields_matrix() {
    let cases = [
        (
            MutationMode::Update,
            "structural update explicit generated timestamp",
        ),
        (
            MutationMode::Replace,
            "structural replace-existing explicit generated timestamp",
        ),
    ];

    for (mode, context) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_generated_timestamp_entity(&session, 1, "Ada", 1);

        let patch = session
            .structural_patch::<SessionSqlGeneratedTimestampEntity, _, _, _>([(
                "created_on_insert",
                Value::Timestamp(Timestamp::from_nanos(9)),
            )])
            .expect("generated timestamp structural rewrite should resolve accepted-schema field");
        let err = session
            .mutate_structural::<SessionSqlGeneratedTimestampEntity>(1, patch, mode)
            .expect_err("structural rewrites should reject explicit insert-generated fields");

        assert_structural_generated_field_rejection(&err, context);
    }
}

#[test]
fn execute_sql_statement_single_row_update_matrix_returns_count_without_returning() {
    let cases = [
        (
            "plain update",
            "UPDATE SessionSqlWriteEntity SET name = 'Bea', age = 22 WHERE id = 1",
            true,
        ),
        (
            "aliased update",
            "UPDATE SessionSqlWriteEntity s SET s.name = 'Bea', s.age = 22 WHERE s.id = 1",
            false,
        ),
    ];

    for (context, sql, check_persisted) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_write_entities(&session, &[(1, "Ada", 21)]);

        assert_statement_count::<SessionSqlWriteEntity>(&session, sql, 1, context);

        if check_persisted {
            let persisted = persisted_write_rows(&session);
            assert_eq!(
                persisted,
                vec![vec![
                    Value::Nat64(1),
                    Value::Text("Bea".to_string()),
                    Value::Nat64(22),
                ]],
            );
        }
    }
}

#[test]
fn execute_validated_sql_public_primary_key_update_plan_mutates_one_row() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);

    let plan =
        public_primary_key_update_plan("UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1");
    let result = session
        .execute_validated_sql_public_primary_key_update::<SessionSqlWriteEntity>(&plan)
        .expect("validated public primary-key UPDATE plan should execute");
    let SqlStatementResult::Count { row_count } = result else {
        panic!("validated public primary-key UPDATE should return count payload");
    };

    assert_eq!(row_count, 1);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(30),
            ],
        ],
    );
}

#[test]
fn public_pk_update_rejects_oversized_returning_byte_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);

    let plan = public_primary_key_update_plan_with_response_cap(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING id",
        Some(1),
    );
    let err = session
        .execute_validated_sql_public_primary_key_update::<SessionSqlWriteEntity>(&plan)
        .expect_err("oversized RETURNING response should reject public primary-key UPDATE");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningResponseTooLarge);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(30),
            ],
        ],
    );
}

#[test]
fn public_pk_delete_rejects_oversized_returning_byte_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);

    let plan = public_primary_key_delete_plan_with_response_cap(
        "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING id",
        Some(1),
    );
    let err = session
        .execute_validated_sql_public_primary_key_delete::<SessionSqlWriteEntity>(&plan)
        .expect_err("oversized RETURNING response should reject public primary-key DELETE");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningResponseTooLarge);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(30),
            ],
        ],
        "primary-key DELETE RETURNING byte cap should reject before mutation",
    );
}

#[test]
fn public_pk_update_allows_sized_returning_byte_cap() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);

    let plan = public_primary_key_update_plan_with_response_cap(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING id",
        Some(4096),
    );
    let result = session
        .execute_validated_sql_public_primary_key_update::<SessionSqlWriteEntity>(&plan)
        .expect("RETURNING response inside byte cap should execute");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("bounded RETURNING response should return projection payload");
    };

    assert_eq!(columns, ["id"]);
    assert_eq!(rows, vec![vec![output(Value::Nat64(1))]]);
    assert_eq!(row_count, 1);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(30),
            ],
        ],
    );
}

#[test]
fn public_pk_update_rejects_returning_row_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);

    let plan = public_primary_key_update_plan_with_returning_caps(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING id",
        Some(0),
        None,
    );
    let err = session
        .execute_validated_sql_public_primary_key_update::<SessionSqlWriteEntity>(&plan)
        .expect_err("RETURNING row cap should reject public primary-key UPDATE");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningRowsTooMany);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(30),
            ],
        ],
    );
}

#[test]
fn public_pk_update_allows_configured_returning_byte_cap_without_returning() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);

    let plan = public_primary_key_update_plan_with_response_cap(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        Some(1),
    );
    let result = session
        .execute_validated_sql_public_primary_key_update::<SessionSqlWriteEntity>(&plan)
        .expect("configured returning byte cap should not reject count-only public UPDATE");

    let SqlStatementResult::Count { row_count } = result else {
        panic!("count-only public primary-key UPDATE should return count payload");
    };

    assert_eq!(row_count, 1);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(30),
            ],
        ],
    );
}

#[test]
fn public_pk_update_allows_configured_returning_row_cap_without_returning() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);

    let plan = public_primary_key_update_plan_with_returning_caps(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        Some(0),
        None,
    );
    let result = session
        .execute_validated_sql_public_primary_key_update::<SessionSqlWriteEntity>(&plan)
        .expect("configured returning row cap should not reject count-only public UPDATE");

    let SqlStatementResult::Count { row_count } = result else {
        panic!("count-only public primary-key UPDATE should return count payload");
    };

    assert_eq!(row_count, 1);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(30),
            ],
        ],
    );
}

#[test]
fn execute_validated_sql_public_primary_key_update_rejects_non_pk_plan_before_execution() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21)]);

    rejected_public_primary_key_update_has_no_plan(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21",
    );

    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn execute_sql_public_primary_key_update_derives_context_and_mutates_one_row() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);

    let result = session
        .execute_sql_public_primary_key_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        )
        .expect("schema-derived public primary-key UPDATE should execute");
    let SqlStatementResult::Count { row_count } = result else {
        panic!("schema-derived public primary-key UPDATE should return count payload");
    };

    assert_eq!(row_count, 1);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(30),
            ],
        ],
    );
}

#[test]
fn execute_sql_public_primary_key_update_derives_default_returning_byte_cap() {
    reset_session_sql_store();
    let session = sql_session();
    let oversized_name = oversized_public_update_returning_text();
    session
        .insert_many_atomic([
            SessionSqlWriteEntity {
                id: 1,
                name: oversized_name,
                age: 21,
            },
            SessionSqlWriteEntity {
                id: 2,
                name: "Bea".to_string(),
                age: 30,
            },
        ])
        .expect("oversized public UPDATE fixture insert should succeed");

    let err = session
        .execute_sql_public_primary_key_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING name",
        )
        .expect_err("schema-derived public primary-key UPDATE should enforce response budget");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningResponseTooLarge);
    assert_eq!(
        persisted_write_ages(&session),
        vec![
            vec![Value::Nat64(1), Value::Nat64(21)],
            vec![Value::Nat64(2), Value::Nat64(30)],
        ],
        "default public primary-key RETURNING byte cap should reject before mutation",
    );
}

#[test]
fn execute_sql_public_primary_key_delete_derives_default_returning_byte_cap() {
    reset_session_sql_store();
    let session = sql_session();
    let oversized_name = oversized_public_delete_returning_text();
    session
        .insert_many_atomic([
            SessionSqlWriteEntity {
                id: 1,
                name: oversized_name,
                age: 21,
            },
            SessionSqlWriteEntity {
                id: 2,
                name: "Bea".to_string(),
                age: 30,
            },
        ])
        .expect("oversized public DELETE fixture insert should succeed");

    let err = session
        .execute_sql_public_primary_key_delete::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING name",
        )
        .expect_err("schema-derived public primary-key DELETE should enforce response budget");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningResponseTooLarge);
    assert_eq!(
        persisted_write_ages(&session),
        vec![
            vec![Value::Nat64(1), Value::Nat64(21)],
            vec![Value::Nat64(2), Value::Nat64(30)],
        ],
        "default public primary-key DELETE RETURNING byte cap should reject before mutation",
    );
}

#[test]
fn execute_sql_public_primary_key_update_rejects_non_pk_where_without_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21)]);

    let err = session
        .execute_sql_public_primary_key_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21",
        )
        .expect_err("schema-derived public primary-key UPDATE should reject non-PK WHERE");

    assert_runtime_unsupported_query_execution_diagnostic(
        err,
        "public primary-key UPDATE should fail closed before execution",
    );
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn execute_sql_public_primary_key_update_rejects_schema_owned_assignments_before_execution() {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_timestamp_entity(&session, 1, "Ada", 1);

    let err = session
        .execute_sql_public_primary_key_update::<SessionSqlGeneratedTimestampEntity>(
            "UPDATE SessionSqlGeneratedTimestampEntity SET created_on_insert = 7 WHERE id = 1",
        )
        .expect_err("public primary-key UPDATE should reject generated assignment pre-execution");

    assert_runtime_unsupported_query_execution_diagnostic(
        err,
        "generated-field assignment should be rejected by schema-derived public policy context",
    );

    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlManagedWriteEntity {
            id: 1,
            name: "Ada".to_string(),
            created_at: Timestamp::from_nanos(1),
            updated_at: Timestamp::from_nanos(1),
        })
        .expect("managed-field setup insert should succeed");

    let err = session
        .execute_sql_public_primary_key_update::<SessionSqlManagedWriteEntity>(
            "UPDATE SessionSqlManagedWriteEntity SET updated_at = 0 WHERE id = 1",
        )
        .expect_err("public primary-key UPDATE should reject managed assignment pre-execution");

    assert_runtime_unsupported_query_execution_diagnostic(
        err,
        "managed-field assignment should be rejected by schema-derived public policy context",
    );
}

#[test]
fn execute_validated_sql_public_primary_key_update_allows_generated_returning_all() {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_timestamp_entity(&session, 1, "Ada", 1);

    let plan = public_primary_key_update_plan(
        "UPDATE SessionSqlGeneratedTimestampEntity SET name = 'Bea' WHERE id = 1 RETURNING *",
    );
    let result = session
        .execute_validated_sql_public_primary_key_update::<SessionSqlGeneratedTimestampEntity>(
            &plan,
        )
        .expect("validated public UPDATE should return visible generated fields");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("generated RETURNING * should return projection payload");
    };

    assert_eq!(columns, ["id", "created_on_insert", "name"]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Nat64(1)),
            output(Value::Timestamp(Timestamp::from_nanos(1))),
            output(Value::Text("Bea".to_string())),
        ]],
    );
    assert_eq!(row_count, 1);
    assert_eq!(
        persisted_generated_timestamp_rows(&session),
        vec![vec![
            Value::Nat64(1),
            Value::Timestamp(Timestamp::from_nanos(1)),
            Value::Text("Bea".to_string()),
        ]],
    );
}

#[test]
fn execute_sql_public_primary_key_update_allows_generated_returning_field() {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_timestamp_entity(&session, 1, "Ada", 1);

    let result = session
        .execute_sql_public_primary_key_update::<SessionSqlGeneratedTimestampEntity>(
            "UPDATE SessionSqlGeneratedTimestampEntity SET name = 'Bea' WHERE id = 1 \
             RETURNING created_on_insert",
        )
        .expect("schema-derived public UPDATE should return visible generated fields");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("generated field-list RETURNING should return projection payload");
    };

    assert_eq!(columns, ["created_on_insert"]);
    assert_eq!(
        rows,
        vec![vec![output(Value::Timestamp(Timestamp::from_nanos(1)))]],
    );
    assert_eq!(row_count, 1);
    assert_eq!(
        persisted_generated_timestamp_rows(&session),
        vec![vec![
            Value::Nat64(1),
            Value::Timestamp(Timestamp::from_nanos(1)),
            Value::Text("Bea".to_string()),
        ]],
    );
}

#[test]
fn execute_sql_public_primary_key_update_allows_visible_returning_fields() {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_timestamp_entity(&session, 1, "Ada", 1);

    let result = session
        .execute_sql_public_primary_key_update::<SessionSqlGeneratedTimestampEntity>(
            "UPDATE SessionSqlGeneratedTimestampEntity SET name = 'Bea' WHERE id = 1 \
             RETURNING id, name",
        )
        .expect("schema-derived public UPDATE should return ordinary visible fields");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("visible field-list RETURNING should return projection payload");
    };

    assert_eq!(columns, ["id", "name"]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Nat64(1)),
            output(Value::Text("Bea".to_string())),
        ]],
    );
    assert_eq!(row_count, 1);
    assert_eq!(
        persisted_generated_timestamp_rows(&session),
        vec![vec![
            Value::Nat64(1),
            Value::Timestamp(Timestamp::from_nanos(1)),
            Value::Text("Bea".to_string()),
        ]],
    );
}

#[test]
fn execute_sql_public_bounded_update_allows_managed_returning_all() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert_many_atomic([
            SessionSqlManagedWriteEntity {
                id: 1,
                name: "Ada".to_string(),
                created_at: Timestamp::from_nanos(1),
                updated_at: Timestamp::from_nanos(1),
            },
            SessionSqlManagedWriteEntity {
                id: 2,
                name: "Bea".to_string(),
                created_at: Timestamp::from_nanos(2),
                updated_at: Timestamp::from_nanos(2),
            },
        ])
        .expect("managed-field setup insert should succeed");

    let result = session
        .execute_sql_public_bounded_update::<SessionSqlManagedWriteEntity>(
            "UPDATE SessionSqlManagedWriteEntity SET name = 'Cid' WHERE id > 0 \
             ORDER BY id ASC LIMIT 2 RETURNING *",
        )
        .expect("schema-derived public bounded UPDATE should return managed visible fields");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("managed RETURNING * should return projection payload");
    };

    assert_eq!(columns, ["id", "name", "created_at", "updated_at"]);
    assert_eq!(
        rows,
        vec![
            vec![
                output(Value::Nat64(1)),
                output(Value::Text("Cid".to_string())),
                output(Value::Timestamp(Timestamp::from_nanos(1))),
                output(Value::Timestamp(Timestamp::from_nanos(1))),
            ],
            vec![
                output(Value::Nat64(2)),
                output(Value::Text("Cid".to_string())),
                output(Value::Timestamp(Timestamp::from_nanos(2))),
                output(Value::Timestamp(Timestamp::from_nanos(2))),
            ],
        ],
    );
    assert_eq!(row_count, 2);
    assert_eq!(
        persisted_managed_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Cid".to_string()),
                Value::Timestamp(Timestamp::from_nanos(1)),
                Value::Timestamp(Timestamp::from_nanos(1)),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Cid".to_string()),
                Value::Timestamp(Timestamp::from_nanos(2)),
                Value::Timestamp(Timestamp::from_nanos(2)),
            ],
        ],
    );
}

#[test]
fn execute_validated_sql_public_bounded_update_plan_mutates_limited_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    let plan = public_bounded_update_plan(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2",
    );
    let mut row_count = None;
    let events = capture_sql_write_events(|| {
        let result = session
            .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
            .expect("validated public bounded UPDATE plan should execute");
        let SqlStatementResult::Count { row_count: count } = result else {
            panic!("validated public bounded UPDATE should return count payload");
        };
        row_count = Some(count);
    });

    assert_eq!(row_count, Some(2));
    assert_single_sql_write_event(
        events,
        SessionSqlWriteEntity::PATH,
        SqlWriteKind::Update,
        [2, 2, 2, 0],
    );
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn public_bounded_update_characterizes_exact_staged_bounds() {
    let cases = [
        (
            "empty UPDATE at zero staged bound",
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age > 99 ORDER BY id ASC LIMIT 1",
            0,
            0,
            vec![(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)],
        ),
        (
            "single selected row at exact staged bound",
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 1",
            1,
            1,
            vec![(1, "Ada", 22), (2, "Bea", 21), (3, "Cid", 21)],
        ),
        (
            "limit-windowed rows at exact staged bound",
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2",
            2,
            2,
            vec![(1, "Ada", 22), (2, "Bea", 22), (3, "Cid", 21)],
        ),
    ];

    for (context, sql, max_staged_rows, expected_row_count, expected_rows) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);
        let plan = public_bounded_update_plan_with_caps(sql, Some(max_staged_rows), None);
        let result = session
            .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
            .unwrap_or_else(|err| panic!("{context} should execute: {err:?}"));
        let SqlStatementResult::Count { row_count } = result else {
            panic!("{context} should return count payload");
        };

        assert_eq!(row_count, expected_row_count, "{context}");
        assert_eq!(persisted_write_rows(&session), write_rows(&expected_rows));
    }
}

#[test]
fn public_bounded_update_characterizes_over_bound_atomicity() {
    let cases = [
        (
            "one selected row over zero staged bound",
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 1",
            0,
        ),
        (
            "two selected rows over one-row staged bound",
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2",
            1,
        ),
    ];

    for (context, sql, max_staged_rows) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);
        let plan = public_bounded_update_plan_with_caps(sql, Some(max_staged_rows), None);
        let err = session
            .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
            .expect_err(context);

        assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::StagedRowsTooMany);
        assert_eq!(
            persisted_write_rows(&session),
            write_rows(&[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]),
            "{context} should reject before mutation",
        );
    }
}

#[test]
fn public_bounded_update_returning_characterizes_order_and_limit_precedence() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);
    let plan = public_bounded_update_plan_with_caps(
        "UPDATE SessionSqlWriteEntity SET age = 22 \
         WHERE age = 21 ORDER BY id ASC LIMIT 2 RETURNING id",
        Some(2),
        Some(2),
    );

    let result = session
        .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
        .expect("exactly bounded UPDATE RETURNING should execute");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("UPDATE RETURNING should return projection payload");
    };

    assert_eq!(columns, ["id"]);
    assert_eq!(
        rows,
        vec![vec![output(Value::Nat64(1))], vec![output(Value::Nat64(2))],],
        "UPDATE RETURNING should preserve ordered candidate output",
    );
    assert_eq!(row_count, 2);
    assert_eq!(
        persisted_write_rows(&session),
        write_rows(&[(1, "Ada", 22), (2, "Bea", 22), (3, "Cid", 21)]),
    );

    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);
    let plan = public_bounded_update_plan_with_caps(
        "UPDATE SessionSqlWriteEntity SET age = 22 \
         WHERE age = 21 ORDER BY id ASC LIMIT 2 RETURNING id",
        Some(1),
        Some(1),
    );
    let err = session
        .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
        .expect_err("combined staged and RETURNING row cap should reject before mutation");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::StagedRowsTooMany);
    assert_eq!(
        persisted_write_rows(&session),
        write_rows(&[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]),
        "combined staged and RETURNING row cap should preserve staged-row precedence",
    );
}

#[test]
fn public_bounded_update_rejects_oversized_returning_byte_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    let plan = public_bounded_update_plan_with_response_cap(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2 RETURNING id",
        Some(1),
    );
    let err = session
        .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
        .expect_err("oversized RETURNING response should reject public bounded UPDATE");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningResponseTooLarge);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn public_bounded_delete_rejects_oversized_returning_byte_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    let plan = public_bounded_delete_plan_with_response_cap(
        "DELETE FROM SessionSqlWriteEntity \
         WHERE age = 21 ORDER BY id ASC LIMIT 2 RETURNING id",
        Some(1),
    );
    let err = session
        .execute_validated_sql_public_bounded_delete::<SessionSqlWriteEntity>(&plan)
        .expect_err("oversized RETURNING response should reject public bounded DELETE");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningResponseTooLarge);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
        "bounded DELETE RETURNING byte cap should reject before mutation",
    );
}

#[test]
fn public_bounded_update_allows_sized_returning_byte_cap() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    let plan = public_bounded_update_plan_with_response_cap(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2 RETURNING id",
        Some(4096),
    );
    let result = session
        .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
        .expect("bounded RETURNING response inside byte cap should execute");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = result
    else {
        panic!("bounded RETURNING response should return projection payload");
    };

    assert_eq!(columns, ["id"]);
    assert_eq!(
        rows,
        vec![vec![output(Value::Nat64(1))], vec![output(Value::Nat64(2))],],
    );
    assert_eq!(row_count, 2);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn public_bounded_update_rejects_returning_row_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    let plan = public_bounded_update_plan_with_returning_caps(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2 RETURNING id",
        Some(1),
        None,
    );
    let err = session
        .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
        .expect_err("RETURNING row cap should reject public bounded UPDATE");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningRowsTooMany);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn public_bounded_update_allows_configured_returning_byte_cap_without_returning() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    let plan = public_bounded_update_plan_with_response_cap(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2",
        Some(1),
    );
    let result = session
        .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
        .expect("configured returning byte cap should not reject count-only public bounded UPDATE");

    let SqlStatementResult::Count { row_count } = result else {
        panic!("count-only public bounded UPDATE should return count payload");
    };

    assert_eq!(row_count, 2);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn public_bounded_update_allows_configured_returning_row_cap_without_returning() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    let plan = public_bounded_update_plan_with_returning_caps(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2",
        Some(1),
        None,
    );
    let result = session
        .execute_validated_sql_public_bounded_update::<SessionSqlWriteEntity>(&plan)
        .expect("configured returning row cap should not reject count-only public bounded UPDATE");

    let SqlStatementResult::Count { row_count } = result else {
        panic!("count-only public bounded UPDATE should return count payload");
    };

    assert_eq!(row_count, 2);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn execute_validated_sql_public_bounded_update_rejects_unordered_plan_before_execution() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21)]);

    rejected_public_bounded_update_has_no_plan(
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 LIMIT 2",
    );

    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn execute_sql_public_bounded_update_derives_context_and_mutates_limited_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    let result = session
        .execute_sql_public_bounded_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 ORDER BY id ASC LIMIT 2",
        )
        .expect("schema-derived public bounded UPDATE should execute");
    let SqlStatementResult::Count { row_count } = result else {
        panic!("schema-derived public bounded UPDATE should return count payload");
    };

    assert_eq!(row_count, 2);
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn execute_sql_public_bounded_update_rejects_limit_above_default_without_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21)]);
    let excessive_limit = DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT
        .checked_add(1)
        .expect("test default public bounded update limit should fit u32");

    let err = session
        .execute_sql_public_bounded_update::<SessionSqlWriteEntity>(
            format!(
                "UPDATE SessionSqlWriteEntity SET age = 22 \
                 WHERE age = 21 ORDER BY id ASC LIMIT {excessive_limit}"
            )
            .as_str(),
        )
        .expect_err("schema-derived public bounded UPDATE should reject excessive LIMIT");

    assert_runtime_unsupported_query_execution_diagnostic(
        err,
        "public bounded UPDATE should enforce the default maximum limit before execution",
    );
    assert_eq!(
        persisted_write_ages(&session),
        vec![
            vec![Value::Nat64(1), Value::Nat64(21)],
            vec![Value::Nat64(2), Value::Nat64(21)],
        ],
    );
}

#[test]
fn execute_sql_public_bounded_update_derives_default_returning_byte_cap() {
    reset_session_sql_store();
    let session = sql_session();
    let oversized_name = oversized_public_update_returning_text();
    session
        .insert_many_atomic([
            SessionSqlWriteEntity {
                id: 1,
                name: oversized_name,
                age: 21,
            },
            SessionSqlWriteEntity {
                id: 2,
                name: "Bea".to_string(),
                age: 21,
            },
            SessionSqlWriteEntity {
                id: 3,
                name: "Cid".to_string(),
                age: 21,
            },
        ])
        .expect("oversized bounded public UPDATE fixture insert should succeed");

    let err = session
        .execute_sql_public_bounded_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 \
             WHERE age = 21 ORDER BY id ASC LIMIT 2 RETURNING name",
        )
        .expect_err("schema-derived public bounded UPDATE should enforce response budget");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningResponseTooLarge);
    assert_eq!(
        persisted_write_ages(&session),
        vec![
            vec![Value::Nat64(1), Value::Nat64(21)],
            vec![Value::Nat64(2), Value::Nat64(21)],
            vec![Value::Nat64(3), Value::Nat64(21)],
        ],
        "default public bounded RETURNING byte cap should reject before mutation",
    );
}

#[test]
fn execute_sql_public_bounded_delete_derives_default_returning_byte_cap() {
    reset_session_sql_store();
    let session = sql_session();
    let oversized_name = oversized_public_delete_returning_text();
    session
        .insert_many_atomic([
            SessionSqlWriteEntity {
                id: 1,
                name: oversized_name,
                age: 21,
            },
            SessionSqlWriteEntity {
                id: 2,
                name: "Bea".to_string(),
                age: 21,
            },
            SessionSqlWriteEntity {
                id: 3,
                name: "Cid".to_string(),
                age: 21,
            },
        ])
        .expect("oversized bounded public DELETE fixture insert should succeed");

    let err = session
        .execute_sql_public_bounded_delete::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity \
             WHERE age = 21 ORDER BY id ASC LIMIT 2 RETURNING name",
        )
        .expect_err("schema-derived public bounded DELETE should enforce response budget");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::ReturningResponseTooLarge);
    assert_eq!(
        persisted_write_ages(&session),
        vec![
            vec![Value::Nat64(1), Value::Nat64(21)],
            vec![Value::Nat64(2), Value::Nat64(21)],
            vec![Value::Nat64(3), Value::Nat64(21)],
        ],
        "default public bounded DELETE RETURNING byte cap should reject before mutation",
    );
}

#[test]
fn execute_sql_public_bounded_update_rejects_unordered_without_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21)]);

    let err = session
        .execute_sql_public_bounded_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 LIMIT 2",
        )
        .expect_err("schema-derived public bounded UPDATE should reject implicit ordering");

    assert_runtime_unsupported_query_execution_diagnostic(
        err,
        "public bounded UPDATE should require explicit primary-key order before execution",
    );
    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(21),
            ],
        ],
    );
}

#[test]
fn execute_sql_statement_write_metrics_capture_sql_boundary_shape() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);
    seed_session_sql_entities(&session, &[("Ada", 21)]);

    let events = capture_sql_write_events(|| {
        execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
            &session,
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (3, 'Cid', 31)",
        )
        .expect("SQL INSERT should succeed");
        execute_sql_statement_for_tests::<SessionSqlEntity>(
            &session,
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' RETURNING *",
        )
        .expect("SQL INSERT SELECT RETURNING should succeed");
        execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
            &session,
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age >= 21 RETURNING id",
        )
        .expect("SQL UPDATE RETURNING should succeed");
        execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
            &session,
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING id",
        )
        .expect("SQL DELETE RETURNING should succeed");
    });

    assert_eq!(
        events,
        vec![
            (
                SessionSqlWriteEntity::PATH,
                SqlWriteKind::Insert,
                [1, 1, 1, 0],
            ),
            (
                SessionSqlEntity::PATH,
                SqlWriteKind::InsertSelect,
                [1, 1, 1, 1],
            ),
            (
                SessionSqlWriteEntity::PATH,
                SqlWriteKind::Update,
                [3, 3, 3, 3],
            ),
            (
                SessionSqlWriteEntity::PATH,
                SqlWriteKind::Delete,
                [1, 1, 1, 1],
            ),
        ],
    );
}

#[test]
fn execute_sql_statement_broad_write_metrics_capture_staged_row_pressure() {
    let insert_select_events = capture_sql_write_events(|| {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &[("Ada", 21), ("Bea", 22), ("Cid", 23)]);

        assert_statement_count::<SessionSqlEntity>(
            &session,
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, age FROM SessionSqlEntity WHERE age >= 21",
            3,
            "broad INSERT SELECT",
        );
    });
    assert_single_sql_write_event(
        insert_select_events,
        SessionSqlEntity::PATH,
        SqlWriteKind::InsertSelect,
        [3, 3, 3, 0],
    );

    let update_events = capture_seeded_write_entity_events(|session| {
        assert_statement_count::<SessionSqlWriteEntity>(
            session,
            "UPDATE SessionSqlWriteEntity SET age = 99 WHERE age >= 21",
            6,
            "broad UPDATE",
        );
    });
    assert_single_sql_write_event(
        update_events,
        SessionSqlWriteEntity::PATH,
        SqlWriteKind::Update,
        [6, 6, 6, 0],
    );

    let update_returning_events = capture_seeded_write_entity_events(|session| {
        assert_statement_returning_rows::<SessionSqlWriteEntity>(
            session,
            "UPDATE SessionSqlWriteEntity SET age = 99 WHERE age >= 21 RETURNING id",
            broad_write_id_rows().as_slice(),
            "broad UPDATE RETURNING",
        );
    });
    assert_single_sql_write_event(
        update_returning_events,
        SessionSqlWriteEntity::PATH,
        SqlWriteKind::Update,
        [6, 6, 6, 6],
    );

    let delete_events = capture_seeded_write_entity_events(|session| {
        assert_statement_count::<SessionSqlWriteEntity>(
            session,
            "DELETE FROM SessionSqlWriteEntity WHERE age >= 21",
            6,
            "broad DELETE",
        );
    });
    assert_single_sql_write_event(
        delete_events,
        SessionSqlWriteEntity::PATH,
        SqlWriteKind::Delete,
        [6, 6, 6, 0],
    );

    let delete_returning_events = capture_seeded_write_entity_events(|session| {
        assert_statement_returning_rows::<SessionSqlWriteEntity>(
            session,
            "DELETE FROM SessionSqlWriteEntity WHERE age >= 21 RETURNING id",
            broad_write_id_rows().as_slice(),
            "broad DELETE RETURNING",
        );
    });
    assert_single_sql_write_event(
        delete_returning_events,
        SessionSqlWriteEntity::PATH,
        SqlWriteKind::Delete,
        [6, 6, 6, 6],
    );
}

#[test]
fn execute_sql_statement_write_error_metrics_capture_command_shape_and_class() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);

    let ((), events) = capture_session_metrics(|| {
        execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
            &session,
            "UPDATE SessionSqlWriteEntity SET age = 'old' WHERE id = 1",
        )
        .expect_err("invalid SQL UPDATE literal should fail");
    });

    assert_eq!(
        captured_sql_write_error_events(&events),
        vec![(
            SessionSqlWriteEntity::PATH,
            SqlWriteKind::Update,
            ErrorClass::Unsupported,
        )],
    );
}

#[test]
fn execute_sql_statement_signed_numeric_write_matrix_widens_parser_literals() {
    let cases = [
        (
            "signed SQL UPDATE",
            Some((1_i64, -5_i64)),
            "UPDATE SessionSqlSignedWriteEntity SET delta = 7 WHERE id = 1",
            vec![vec![Value::Int64(1), Value::Int64(7)]],
        ),
        (
            "signed SQL INSERT",
            None,
            "INSERT INTO SessionSqlSignedWriteEntity (id, delta) VALUES (2, 9)",
            vec![vec![Value::Int64(2), Value::Int64(9)]],
        ),
    ];

    for (context, seed_row, sql, expected_rows) in cases {
        reset_session_sql_store();
        let session = sql_session();

        if let Some((id, delta)) = seed_row {
            session
                .insert(SessionSqlSignedWriteEntity { id, delta })
                .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));
        }

        assert_signed_write_count_and_rows(&session, sql, expected_rows.as_slice(), context);
    }
}

#[test]
fn execute_sql_statement_rejects_incompatible_assignment_literal_for_signed_field() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSignedWriteEntity { id: 1, delta: -5 })
        .expect("signed write setup insert should succeed");

    let err = execute_sql_statement_for_tests::<SessionSqlSignedWriteEntity>(
        &session,
        "UPDATE SessionSqlSignedWriteEntity SET delta = 'Ada' WHERE id = 1",
    )
    .expect_err("signed field assignment should stay fail-closed for incompatible literals");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::InvalidFieldLiteral);
}

#[test]
fn execute_sql_statement_update_with_non_primary_key_predicate_updates_matching_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 30)]);

    assert_write_update_count_and_rows(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21",
        2,
        &[
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(30),
            ],
        ],
        "SQL UPDATE with non-primary-key predicate",
    );
}

#[test]
fn execute_sql_statement_update_with_order_limit_and_offset_updates_one_ordered_window() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(
        &session,
        &[
            (1, "Ada", 21),
            (2, "Bea", 30),
            (3, "Cid", 25),
            (4, "Dee", 40),
        ],
    );

    assert_write_update_count_and_rows(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 99 WHERE age >= 21 ORDER BY age DESC LIMIT 2 OFFSET 1",
        2,
        &[
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(99),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(99),
            ],
            vec![
                Value::Nat64(4),
                Value::Text("Dee".to_string()),
                Value::Nat64(40),
            ],
        ],
        "SQL UPDATE ordered window",
    );
}

#[test]
fn execute_sql_statement_update_with_limit_and_offset_uses_primary_key_order_fallback() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    assert_write_update_count_and_rows(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 LIMIT 1 OFFSET 1",
        1,
        &[
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(3),
                Value::Text("Cid".to_string()),
                Value::Nat64(21),
            ],
        ],
        "SQL UPDATE window without ORDER BY",
    );
}

#[test]
fn execute_sql_statement_update_selector_converges_with_select_and_delete_targets() {
    for (clause, ordered, context) in [
        ("WHERE age = 21", false, "WHERE predicate"),
        (
            "WHERE age >= 21 ORDER BY age ASC",
            true,
            "ORDER BY ASC selector",
        ),
        (
            "WHERE age >= 21 ORDER BY age DESC",
            true,
            "ORDER BY DESC selector",
        ),
        (
            "WHERE age >= 21 ORDER BY id ASC LIMIT 2",
            true,
            "LIMIT selector",
        ),
        (
            "WHERE age >= 21 ORDER BY id ASC LIMIT 2 OFFSET 1",
            true,
            "OFFSET selector",
        ),
        (
            "WHERE age >= 21 ORDER BY age DESC LIMIT 2",
            true,
            "WHERE ORDER BY LIMIT selector",
        ),
    ] {
        let select_ids =
            write_selector_ids(&format!("SELECT id FROM SessionSqlWriteEntity {clause}"));
        let update_ids = write_selector_ids(&format!(
            "UPDATE SessionSqlWriteEntity SET age = 99 {clause} RETURNING id"
        ));
        let delete_ids = write_selector_ids(&format!(
            "DELETE FROM SessionSqlWriteEntity {clause} RETURNING id"
        ));

        assert_selector_ids_match(select_ids.clone(), update_ids, ordered, context);
        assert_selector_ids_match(select_ids, delete_ids, ordered, context);
    }
}

#[test]
fn execute_sql_statement_write_residual_filters_converge_with_select_returning_and_count() {
    for (clause, context) in [
        (
            "WHERE CASE WHEN age > 25 THEN TRUE ELSE NULL END",
            "CASE ELSE NULL residual",
        ),
        (
            "WHERE (age = 21 OR age = 25) \
             AND CASE WHEN name != 'Bea' THEN TRUE ELSE NULL END",
            "OR plus CASE residual",
        ),
        (
            "WHERE (age = 30 OR NULL) AND name = 'Bea'",
            "OR NULL residual",
        ),
        (
            "WHERE (age < 25 AND TRUE) \
             OR CASE WHEN age = 40 THEN TRUE ELSE NULL END",
            "AND plus CASE OR residual",
        ),
    ] {
        let select_ids =
            write_selector_ids(&format!("SELECT id FROM SessionSqlWriteEntity {clause}"));
        let update_ids = write_selector_ids(&format!(
            "UPDATE SessionSqlWriteEntity SET age = 99 {clause} RETURNING id"
        ));
        let delete_ids = write_selector_ids(&format!(
            "DELETE FROM SessionSqlWriteEntity {clause} RETURNING id"
        ));
        let delete_count = write_count(&format!("DELETE FROM SessionSqlWriteEntity {clause}"));

        assert_selector_ids_match(select_ids.clone(), update_ids, false, context);
        assert_selector_ids_match(select_ids.clone(), delete_ids, false, context);
        assert_eq!(
            delete_count as usize,
            select_ids.len(),
            "{context} DELETE count should match the selected residual-filter target set",
        );
    }
}

#[test]
fn execute_sql_statement_write_rejects_entity_mismatch_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, context) in [
        (
            "INSERT INTO SessionSqlGeneratedFieldEntity (id, name) VALUES (1, 'Ada')",
            "insert entity mismatch",
        ),
        (
            "UPDATE SessionSqlGeneratedTimestampEntity SET name = 'Ada' WHERE id = 1",
            "update entity mismatch",
        ),
    ] {
        let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(&session, sql)
            .expect_err(context);

        assert_sql_lowering_detail(err, SqlLoweringCode::EntityMismatch);
    }
}

#[test]
fn execute_sql_statement_insert_select_matrix_accepts_supported_source_shapes() {
    for (returning_sql, expected_inserted_name, persisted_sql, expected_persisted, context) in [
        (
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1 RETURNING *",
            "Ada",
            "SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY age ASC LIMIT 10",
            vec![
                vec![Value::Text("Ada".to_string()), Value::Nat64(21)],
                vec![Value::Text("Ada".to_string()), Value::Nat64(21)],
            ],
            "plain INSERT SELECT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT LOWER(name), age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1 RETURNING *",
            "ada",
            "SELECT name, age FROM SessionSqlEntity ORDER BY name ASC LIMIT 10",
            vec![
                vec![Value::Text("Ada".to_string()), Value::Nat64(21)],
                vec![Value::Text("ada".to_string()), Value::Nat64(21)],
            ],
            "computed INSERT SELECT",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &[("Ada", 21)]);

        assert_insert_select_returning_and_persisted_rows(
            &session,
            returning_sql,
            persisted_sql,
            expected_inserted_name,
            expected_persisted.as_slice(),
            context,
        );
    }
}

#[test]
fn compile_sql_insert_select_carries_bound_source_query_artifact() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 21)]);

    let compiled = session
        .compile_sql_update::<SessionSqlEntity>(
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' RETURNING *",
        )
        .expect("INSERT SELECT should compile");
    let crate::db::session::sql::CompiledSqlCommand::Insert(command) = &compiled else {
        panic!("INSERT SELECT should compile to the INSERT command family");
    };
    assert!(
        command.source_query().is_some(),
        "compiled INSERT SELECT should carry the bound source query artifact",
    );

    let SqlStatementResult::Projection { rows, .. } = session
        .execute_compiled_sql::<SessionSqlEntity>(&compiled)
        .expect("compiled INSERT SELECT should execute")
    else {
        panic!("compiled INSERT SELECT should return projected rows");
    };
    assert_eq!(
        rows.len(),
        1,
        "compiled INSERT SELECT should insert and return one projected row",
    );
}

#[test]
fn execute_sql_statement_insert_select_late_failure_is_statement_atomic() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(
        &session,
        &[(1, "Ada", 21), (2, "Bea", 22), (12, "Existing", 32)],
    );

    let events = capture_sql_write_events(|| {
        execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
            &session,
            "INSERT INTO SessionSqlWriteEntity (id, name, age) \
             SELECT id + 10, name, age FROM SessionSqlWriteEntity WHERE id <= 2 ORDER BY id ASC",
        )
        .expect_err("late INSERT SELECT conflict should reject the whole statement");
    });
    assert!(
        events.is_empty(),
        "failed staged INSERT SELECT must not emit successful SQL write row metrics",
    );

    assert_eq!(
        persisted_write_rows(&session),
        vec![
            vec![
                Value::Nat64(1),
                Value::Text("Ada".to_string()),
                Value::Nat64(21),
            ],
            vec![
                Value::Nat64(2),
                Value::Text("Bea".to_string()),
                Value::Nat64(22),
            ],
            vec![
                Value::Nat64(12),
                Value::Text("Existing".to_string()),
                Value::Nat64(32),
            ],
        ],
        "late INSERT SELECT failure must not commit the earlier projected row",
    );
}

#[test]
fn execute_sql_update_insert_values_rejects_public_staged_row_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    let row_count = DEFAULT_PUBLIC_INSERT_STAGED_ROWS + 1;
    let values = (1..=row_count)
        .map(|id| format!("({id}, 'Name{id}', 21)"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES {values}");

    let err = session
        .execute_sql_update::<SessionSqlWriteEntity>(&sql)
        .expect_err("public update surface INSERT VALUES should enforce staged-row cap");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::StagedRowsTooMany);
    assert!(
        persisted_write_rows(&session).is_empty(),
        "oversized public INSERT VALUES must not commit partial rows",
    );
}

#[test]
fn execute_sql_update_insert_select_rejects_public_staged_row_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    let source_count = DEFAULT_PUBLIC_INSERT_STAGED_ROWS + 1;
    for id in 1..=source_count {
        session
            .insert(SessionSqlEntity {
                id: Ulid::from_u128(u128::from(id)),
                name: format!("Name{id}"),
                age: 21,
            })
            .expect("typed source setup insert should succeed");
    }
    let baseline = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name, age FROM SessionSqlEntity ORDER BY id ASC",
    )
    .expect("baseline projection should succeed");

    let err = session
        .execute_sql_update::<SessionSqlEntity>(
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, age FROM SessionSqlEntity ORDER BY id ASC",
        )
        .expect_err("public update surface INSERT SELECT should enforce staged-row cap");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::StagedRowsTooMany);
    assert_eq!(
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "SELECT name, age FROM SessionSqlEntity ORDER BY id ASC",
        )
        .expect("post-rejection projection should succeed"),
        baseline,
        "oversized public INSERT SELECT must reject before mutation commit",
    );
}

#[test]
fn execute_sql_statement_insert_select_rejection_matrix_preserves_boundary_codes() {
    let cases = [
        (
            "aggregate source",
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT COUNT(*), COUNT(*) FROM SessionSqlEntity",
            SqlWriteBoundaryCode::InsertSelectAggregateProjection,
            vec![(Ulid::from_u128(1), "Ada", 21_u64)],
        ),
        (
            "grouped source",
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, COUNT(*) FROM SessionSqlEntity GROUP BY name",
            SqlWriteBoundaryCode::InsertSelectRequiresScalar,
            vec![
                (Ulid::from_u128(1), "Ada", 21_u64),
                (Ulid::from_u128(2), "Bea", 22_u64),
            ],
        ),
    ];

    for (context, sql, expected_boundary, seed_rows) in cases {
        reset_session_sql_store();
        let session = sql_session();

        for (id, name, age) in seed_rows {
            session
                .insert(SessionSqlEntity {
                    id,
                    name: name.to_string(),
                    age,
                })
                .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));
        }

        assert_statement_write_boundary::<SessionSqlEntity>(
            &session,
            sql,
            expected_boundary,
            context,
        );
    }
}

#[test]
fn execute_sql_statement_update_unique_conflict_is_statement_atomic() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_unique_prefix_offset_session_entities(
        &session,
        &[(1, "gold", "alpha", "first"), (2, "gold", "beta", "second")],
    );

    execute_sql_statement_for_tests::<SessionUniquePrefixOffsetEntity>(
        &session,
        "UPDATE SessionUniquePrefixOffsetEntity SET handle = 'shared' WHERE tier = 'gold' ORDER BY id ASC",
    )
    .expect_err("same-batch unique-index UPDATE conflict should fail atomically");

    let persisted = statement_projection_rows::<SessionUniquePrefixOffsetEntity>(
        &session,
        "SELECT tier, handle, note FROM SessionUniquePrefixOffsetEntity ORDER BY id ASC",
    )
    .expect("post-update projection should succeed");
    assert_eq!(
        persisted,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("alpha".to_string()),
                Value::Text("first".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("beta".to_string()),
                Value::Text("second".to_string()),
            ],
        ],
        "late UPDATE unique conflict must not commit the earlier matched row",
    );
}

#[test]
fn execute_sql_statement_insert_strong_relation_same_statement_target_stays_committed_only() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 1,
            parent: None,
        })
        .expect("committed nullable root setup should save");

    assert_statement_count::<SessionSqlSelfRelationEntity>(
        &session,
        "INSERT INTO SessionSqlSelfRelationEntity (id, parent) VALUES (2, 1)",
        1,
        "committed strong relation target insert",
    );

    execute_sql_statement_for_tests::<SessionSqlSelfRelationEntity>(
        &session,
        "INSERT INTO SessionSqlSelfRelationEntity (id, parent) VALUES (3, 1), (4, 3)",
    )
    .expect_err("same-statement strong relation target should still be rejected");

    let persisted = statement_projection_rows::<SessionSqlSelfRelationEntity>(
        &session,
        "SELECT id, parent FROM SessionSqlSelfRelationEntity ORDER BY id ASC",
    )
    .expect("post-relation projection should succeed");
    assert_eq!(
        persisted,
        vec![
            vec![Value::Nat64(1), Value::Null],
            vec![Value::Nat64(2), Value::Nat64(1)],
        ],
        "same-statement relation failure must not persist the staged parent or child",
    );
}

#[test]
fn execute_sql_statement_write_rejects_incompatible_primary_key_literal() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (-1, 'Ada', 21)",
    )
    .expect_err("unsigned SQL insert key boundary should stay fail-closed for signed literals");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible);
}

#[test]
fn execute_sql_statement_insert_rejects_tuple_length_mismatch() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21), (2, 'Bea')",
    )
    .expect_err("SQL INSERT with tuple length mismatch should stay fail-closed");

    assert_runtime_unsupported_query_execution_diagnostic(
        err,
        "INSERT tuple length mismatch should keep the compact unsupported diagnostic",
    );
}

#[test]
fn execute_sql_statement_insert_and_update_returning_projection_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    assert_statement_returning_rows::<SessionSqlEntity>(
        &session,
        "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING name, age",
        &[vec![Value::Text("Ada".to_string()), Value::Nat64(21)]],
        "SQL INSERT RETURNING field list",
    );

    seed_write_entities(&session, &[(1, "Ada", 21)]);

    assert_statement_returning_rows::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING *",
        &[vec![
            Value::Nat64(1),
            Value::Text("Ada".to_string()),
            Value::Nat64(22),
        ]],
        "SQL UPDATE RETURNING star",
    );
}

#[test]
fn execute_sql_update_reuses_authority_schema_info_for_selector() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);
    let compiled = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING id",
        )
        .expect("SQL UPDATE RETURNING should compile before counter reset");

    DbSession::<SessionSqlCanister>::reset_accepted_catalog_runtime_counters_for_tests();
    let result = session
        .execute_compiled_sql::<SessionSqlWriteEntity>(&compiled)
        .expect("compiled SQL UPDATE RETURNING should execute");
    let counters =
        DbSession::<SessionSqlCanister>::accepted_catalog_runtime_counter_snapshot_for_tests();

    let SqlStatementResult::Projection { row_count, .. } = result else {
        panic!("SQL UPDATE RETURNING should project rows");
    };
    assert_eq!(row_count, 1);
    assert_eq!(
        counters.schema_info_projections, 1,
        "SQL UPDATE should reuse the authority-carried selector schema view for save execution and skip no-index commit-preflight schema projection",
    );
}

#[test]
fn execute_sql_update_returning_star_public_entrypoint_projects_rows() {
    reset_session_sql_store();
    let session = sql_session();

    let insert = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) \
             VALUES (1, 'Ada', 21) RETURNING *",
        )
        .expect("public SQL update entrypoint should admit INSERT RETURNING *");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = insert
    else {
        panic!("INSERT RETURNING * should return a projection payload");
    };
    assert_eq!(columns, ["id", "name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Nat64(1)),
            output(Value::Text("Ada".to_string())),
            output(Value::Nat64(21)),
        ]],
    );
    assert_eq!(row_count, 1);

    let update = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING *",
        )
        .expect("public SQL update entrypoint should admit UPDATE RETURNING *");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = update
    else {
        panic!("UPDATE RETURNING * should return a projection payload");
    };
    assert_eq!(columns, ["id", "name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Nat64(1)),
            output(Value::Text("Ada".to_string())),
            output(Value::Nat64(22)),
        ]],
    );
    assert_eq!(row_count, 1);

    let delete = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING *",
        )
        .expect("public SQL update entrypoint should admit DELETE RETURNING *");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = delete
    else {
        panic!("DELETE RETURNING * should return a projection payload");
    };
    assert_eq!(columns, ["id", "name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Nat64(1)),
            output(Value::Text("Ada".to_string())),
            output(Value::Nat64(22)),
        ]],
    );
    assert_eq!(row_count, 1);
    assert!(
        persisted_write_rows(&session).is_empty(),
        "DELETE RETURNING * should still remove the matched row",
    );
}

#[test]
fn execute_sql_update_returning_field_list_public_entrypoint_projects_rows() {
    reset_session_sql_store();
    let session = sql_session();

    let insert = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) \
             VALUES (1, 'Ada', 21) RETURNING name, age",
        )
        .expect("public SQL update entrypoint should admit INSERT field-list RETURNING");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = insert
    else {
        panic!("INSERT field-list RETURNING should return a projection payload");
    };
    assert_eq!(columns, ["name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            output(Value::Text("Ada".to_string())),
            output(Value::Nat64(21)),
        ]],
    );
    assert_eq!(row_count, 1);

    let update = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING id, age",
        )
        .expect("public SQL update entrypoint should admit UPDATE field-list RETURNING");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = update
    else {
        panic!("UPDATE field-list RETURNING should return a projection payload");
    };
    assert_eq!(columns, ["id", "age"]);
    assert_eq!(
        rows,
        vec![vec![output(Value::Nat64(1)), output(Value::Nat64(22))]],
    );
    assert_eq!(row_count, 1);

    let delete = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING name",
        )
        .expect("public SQL update entrypoint should admit DELETE field-list RETURNING");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = delete
    else {
        panic!("DELETE field-list RETURNING should return a projection payload");
    };
    assert_eq!(columns, ["name"]);
    assert_eq!(rows, vec![vec![output(Value::Text("Ada".to_string()))]]);
    assert_eq!(row_count, 1);
    assert!(
        persisted_write_rows(&session).is_empty(),
        "DELETE field-list RETURNING should still remove the matched row",
    );
}

#[test]
fn execute_sql_statement_write_rejects_unsupported_returning_projection_matrix() {
    reset_session_sql_store();
    for (entity_kind, sql) in [
        (
            "insert",
            "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING LOWER(name)",
        ),
        (
            "update",
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING LOWER(name)",
        ),
    ] {
        let session = sql_session();
        let err = match entity_kind {
            "insert" => execute_sql_statement_for_tests::<SessionSqlEntity>(&session, sql)
                .expect_err("unsupported INSERT RETURNING projection should stay fail-closed"),
            "update" => {
                seed_write_entities(&session, &[(1, "Ada", 21)]);
                execute_sql_statement_for_tests::<SessionSqlWriteEntity>(&session, sql)
                    .expect_err("unsupported UPDATE RETURNING projection should stay fail-closed")
            }
            other => panic!("unexpected write RETURNING case: {other}"),
        };

        assert_sql_unsupported_feature_detail(
            err,
            icydb_diagnostic_code::SqlFeatureCode::UnsupportedFunctionNamespace,
        );
    }
}

#[test]
fn execute_sql_statement_returning_field_list_rejects_invalid_fields_before_mutation() {
    let cases = [
        (
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (2, 'Bob', 22) \
             RETURNING missing",
            SqlWriteBoundaryCode::UnknownReturningField,
            "INSERT unknown RETURNING field",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING missing",
            SqlWriteBoundaryCode::UnknownReturningField,
            "UPDATE unknown RETURNING field",
        ),
        (
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING missing",
            SqlWriteBoundaryCode::UnknownReturningField,
            "DELETE unknown RETURNING field",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING name, name",
            SqlWriteBoundaryCode::DuplicateReturningField,
            "UPDATE duplicate RETURNING field",
        ),
    ];

    for (sql, boundary, context) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_write_entities(&session, &[(1, "Ada", 21)]);
        let baseline = persisted_write_rows(&session);

        assert_statement_write_boundary::<SessionSqlWriteEntity>(&session, sql, boundary, context);
        assert_eq!(
            persisted_write_rows(&session),
            baseline,
            "{context} should reject before mutation",
        );
    }
}

#[test]
fn execute_sql_update_rejects_unsupported_sql_without_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);
    let baseline = persisted_write_rows(&session);

    let cases = [
        (
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (2, 'Bob', 22) \
             RETURNING LOWER(name)",
            "computed INSERT RETURNING should fail before inserting",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING LOWER(name)",
            "computed UPDATE RETURNING should fail before updating",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING COUNT(*)",
            "aggregate UPDATE RETURNING should fail before updating",
        ),
        (
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING LOWER(name)",
            "computed DELETE RETURNING should fail before deleting",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 \
             WHERE id IN (SELECT id FROM SessionSqlWriteEntity)",
            "subquery UPDATE should fail before updating",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = CAST(22 AS nat64) WHERE id = 1",
            "cast UPDATE should fail before updating",
        ),
        (
            "DELETE FROM SessionSqlWriteEntity \
             WHERE id IN (SELECT id FROM SessionSqlWriteEntity)",
            "subquery DELETE should fail before deleting",
        ),
        (
            "WITH selected AS (SELECT * FROM SessionSqlWriteEntity) \
             UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
            "CTE UPDATE should fail before updating",
        ),
    ];

    for (sql, context) in cases {
        assert!(
            session
                .execute_sql_update::<SessionSqlWriteEntity>(sql)
                .is_err(),
            "{context}",
        );
        assert_eq!(
            persisted_write_rows(&session),
            baseline,
            "{context}: rows should remain unchanged",
        );
    }
}

#[test]
fn execute_sql_statement_update_requires_where_predicate() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22",
    )
    .expect_err("SQL UPDATE without WHERE predicate should stay fail-closed");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::UpdateMissingWherePredicate);
}

#[test]
fn execute_sql_statement_update_rejects_expression_only_where_before_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);
    let baseline = persisted_write_rows(&session);

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity \
         SET age = 22 \
         WHERE COALESCE(NULLIF(age, 21), 99) = 99",
    )
    .expect_err("UPDATE expression-only WHERE should stay predicate-gated");

    assert_sql_lowering_detail(err, SqlLoweringCode::WhereExpressionShape);
    assert_eq!(
        persisted_write_rows(&session),
        baseline,
        "unsupported UPDATE WHERE shape should reject before mutation",
    );
}

#[test]
fn execute_sql_statement_update_rejects_unsupported_order_by_shape_before_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);
    let baseline = persisted_write_rows(&session);

    assert_statement_write_boundary::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 ORDER BY age + 1",
        SqlWriteBoundaryCode::WriteOrderByUnsupportedShape,
        "UPDATE expression ORDER BY",
    );
    assert_eq!(
        persisted_write_rows(&session),
        baseline,
        "unsupported UPDATE ORDER BY shape should reject before mutation",
    );
}

#[test]
fn execute_sql_statement_update_rejects_invalid_window_clause_order() {
    reset_session_sql_store();
    let session = sql_session();

    for sql in [
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 LIMIT 1 ORDER BY id",
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 OFFSET 1 LIMIT 1",
    ] {
        let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(&session, sql)
            .expect_err("invalid UPDATE window clause ordering should stay fail-closed");

        assert_runtime_unsupported_query_execution_diagnostic(
            err,
            "invalid UPDATE window clause ordering should keep the compact unsupported diagnostic",
        );
    }
}

#[test]
fn execute_sql_statement_update_rejects_primary_key_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET id = 2, age = 22 WHERE id = 1",
    )
    .expect_err("SQL UPDATE primary-key mutation should stay fail-closed");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::UpdatePrimaryKeyMutation);
}
