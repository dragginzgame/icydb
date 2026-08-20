#![allow(
    clippy::significant_drop_tightening,
    reason = "each test intentionally retains its exclusive pooled fixture lease for its full scope"
)]

#[expect(
    dead_code,
    unused_imports,
    reason = "this boundary target consumes only the verdict subset of the shared test harness"
)]
mod sql_harness;

use std::collections::BTreeSet;

use crate::sql_harness::{
    CorrectnessObservation, CorrectnessScenario, CorrectnessVerdict, EligibleProvider,
    EvidenceStrength, ExpectedAcceptance, MutationKind, NormalizedCell, NormalizedResult,
    NullabilityClass, ObservedOutcome, PredicateFamily, QueryShape, RouteExpectation, RouteFact,
    RouteFamily, RouteOutcome, RouteReason, RowOrder, ScenarioMetadata, StatementFamily,
    ValueTypeFamily, WindowSpec, correctness_verdict,
};
use candid::{CandidType, Principal};
use ic_testkit::pic::{
    CachedStandaloneCanisterFixtureGuard, CachedStandaloneCanisterFixturePool,
    StandaloneCanisterFixture,
};
use icydb::{
    Error, ErrorCode, ErrorOrigin,
    db::{
        EntitySchemaDescription, IntegrityCheckResult, QuickIntegrityStatus, RowProjectionOutput,
        SqlColumnDefault, SqlColumnExtra, SqlColumnKey, SqlDescribeOutput, SqlIntegrityError,
        SqlQueryExecutionAttribution, SqlShowColumnsOutput, StorageReport,
        sql::{SqlGroupedRowsOutput, SqlQueryPerfResult, SqlQueryResult},
    },
    diagnostic::{DiagnosticCode, RuntimeBoundaryCode},
    metrics::CompactMetricsReport,
    types::Decimal,
    value::OutputValue,
};
use icydb_testing_integration::{
    deliver_fixture_startup_watchdog, install_fixture_canister, reset_icydb_fixtures,
    upgrade_fixture_canister,
};
use icydb_testing_sqlite_reference::{
    SqliteReferenceColumnKind, SqliteReferenceFamily, SqliteReferencePredicateFamily,
    SqliteReferenceResult, SqliteReferenceRowOrder, SqliteReferenceScenario, SqliteReferenceValue,
    SqliteReferenceWindow, execute_sqlite_reference_scenario, required_sqlite_reference_scenarios,
};
use serde::Deserialize;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct IdentityCloseoutPerfResult {
    caller_nat64_instructions: u64,
    generated_nat64_instructions: u64,
    generated_nat128_instructions: u64,
    one_row_batch_instructions: u64,
    maximum_batch_instructions: u64,
    maximum_batch_rows: u32,
    sequential_three_entity_instructions: u64,
    atomic_three_entity_instructions: u64,
    maximum_entity_context_instructions: u64,
    maximum_entity_context_count: u32,
    sequential_typed_enrollment_instructions: u64,
    atomic_typed_enrollment_instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ApplicationBehaviorPerfResult {
    normalize_instructions: u64,
    validate_instructions: u64,
    normalize_and_validate_instructions: u64,
    normalized_bytes: u64,
    validated_bytes: u64,
    composed_bytes: u64,
    iterations: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlExecutionInstructionResult {
    result: Result<SqlQueryResult, Error>,
    local_instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct AcceptedSchemaReadInstructionResult {
    description: EntitySchemaDescription,
    local_instructions: u64,
}

const SQL_FIXTURE_POOL_CAPACITY: usize = 8;
const SQL_BOUNDED_FIXTURE_POOL_CAPACITY: usize = 4;

static SQL_FIXTURE_POOL: CachedStandaloneCanisterFixturePool<SQL_FIXTURE_POOL_CAPACITY> =
    CachedStandaloneCanisterFixturePool::new();
static SQL_BOUNDED_FIXTURE_POOL: CachedStandaloneCanisterFixturePool<
    SQL_BOUNDED_FIXTURE_POOL_CAPACITY,
> = CachedStandaloneCanisterFixturePool::new();

fn install_sql_canister_fixture() -> CachedStandaloneCanisterFixtureGuard<'static> {
    // Bound concurrent PocketIC ownership while restoring the installed
    // canister to its exact post-install baseline for every test lease.
    SQL_FIXTURE_POOL
        .acquire(|| install_fixture_canister("sql"))
        .unwrap_or_else(|error| panic!("SQL fixture pool should restore cleanly: {error}"))
        .0
}

fn install_sql_bounded_canister_fixture() -> CachedStandaloneCanisterFixtureGuard<'static> {
    SQL_BOUNDED_FIXTURE_POOL
        .acquire(|| install_fixture_canister("sql_bounded"))
        .unwrap_or_else(|error| panic!("bounded SQL fixture pool should restore cleanly: {error}"))
        .0
}

fn install_demo_rpg_canister_fixture() -> StandaloneCanisterFixture {
    // The demo RPG canister has one generated entity, making it a useful
    // boundary fixture for proving generated DDL still requires explicit targets.
    install_fixture_canister("demo_rpg")
}

fn reset_sql_fixtures(fixture: &StandaloneCanisterFixture) {
    // Keep each test isolated by resetting and then loading the deterministic
    // baseline fixture set through the live canister update surface.
    reset_icydb_fixtures(fixture);
}

#[test]
fn sql_canister_schema_endpoint_exposes_exact_diagnostic_identity() {
    let fixture = install_sql_canister_fixture();
    let response: Result<Vec<EntitySchemaDescription>, Error> = fixture
        .query_candid("icydb_schema", ())
        .expect("schema endpoint response should decode");
    let report = response.expect("controller schema endpoint should succeed");
    assert!(
        !report.is_empty(),
        "test schema should expose accepted entities"
    );

    let mut entity_tags = BTreeSet::new();
    for entity in &report {
        assert!(
            entity_tags.insert(entity.entity_tag()),
            "accepted entity tags must remain unique"
        );
        assert_ne!(
            entity.accepted_schema_fingerprint_method(),
            0,
            "accepted fingerprint methods must be explicit"
        );
    }

    let endpoint_entity = report
        .iter()
        .find(|entity| entity.entity_name() == "SqlTestUser")
        .expect("schema endpoint should describe SqlTestUser");
    let SqlQueryResult::Describe(SqlDescribeOutput::Verbose {
        description: sql_entity,
    }) = query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE").expect("SQL DESCRIBE should succeed")
    else {
        panic!("SQL DESCRIBE should return one entity description");
    };
    assert_eq!(sql_entity.entity_tag(), endpoint_entity.entity_tag());
    assert_eq!(
        sql_entity.accepted_schema_fingerprint_method(),
        endpoint_entity.accepted_schema_fingerprint_method()
    );
    assert_eq!(
        sql_entity.accepted_schema_fingerprint(),
        endpoint_entity.accepted_schema_fingerprint()
    );
}

#[test]
fn sql_canister_compact_introspection_is_shared_typed_and_bounded() {
    let fixture = install_sql_canister_fixture();
    let describe =
        query_sql(&fixture, "DESCRIBE SqlTestUser").expect("compact DESCRIBE should succeed");
    let SqlQueryResult::Describe(SqlDescribeOutput::Compact { entity, columns }) = describe else {
        panic!("default DESCRIBE must hard-cut to the compact result");
    };
    assert_eq!(entity, "SqlTestUser");

    let show_columns = query_sql(&fixture, "SHOW COLUMNS sqltestuser")
        .expect("case-insensitive compact SHOW COLUMNS should succeed");
    let SqlQueryResult::ShowColumns(SqlShowColumnsOutput::Compact {
        entity: show_entity,
        columns: show_columns,
    }) = show_columns
    else {
        panic!("default SHOW COLUMNS must hard-cut to the compact result");
    };
    assert_eq!(show_entity, "SqlTestUser");
    assert_eq!(
        columns, show_columns,
        "both commands must share one projector"
    );

    let id = columns
        .iter()
        .find(|column| column.name() == "id")
        .expect("compact rows should include id");
    assert_eq!(id.key(), SqlColumnKey::Primary);
    assert_eq!(id.default(), &SqlColumnDefault::Auto);
    assert_eq!(id.extra(), &[SqlColumnExtra::Generated]);

    let name = columns
        .iter()
        .find(|column| column.name() == "name")
        .expect("compact rows should include name");
    assert_eq!(name.key(), SqlColumnKey::Multiple);
    assert_eq!(name.default(), &SqlColumnDefault::Required);
    assert!(name.extra().is_empty());

    for timestamp in ["created_at", "updated_at"] {
        let column = columns
            .iter()
            .find(|column| column.name() == timestamp)
            .unwrap_or_else(|| panic!("compact rows should include {timestamp}"));
        assert_eq!(column.default(), &SqlColumnDefault::Auto);
        assert_eq!(column.extra(), &[SqlColumnExtra::Generated]);
    }

    let identity = query_sql(&fixture, "DESCRIBE SqlTestIdentityNat64")
        .expect("accepted Identity metadata should be compactly describable");
    let SqlQueryResult::Describe(SqlDescribeOutput::Compact {
        columns: identity_columns,
        ..
    }) = identity
    else {
        panic!("Identity DESCRIBE should use the compact envelope");
    };
    let identity_id = identity_columns
        .iter()
        .find(|column| column.name() == "id")
        .expect("Identity compact rows should include id");
    assert_eq!(identity_id.default(), &SqlColumnDefault::Auto);
    assert_eq!(
        identity_id.extra(),
        &[SqlColumnExtra::Identity, SqlColumnExtra::Generated],
    );

    let rendered = SqlQueryResult::Describe(SqlDescribeOutput::Compact {
        entity,
        columns: columns.clone(),
    })
    .render_lines();
    assert_eq!(
        rendered
            .iter()
            .filter(|line| {
                line.split('|')
                    .map(str::trim)
                    .filter(|cell| !cell.is_empty())
                    .eq(["name", "type", "nullable", "key", "default", "extra"])
            })
            .count(),
        1,
        "compact DESCRIBE renders exactly one table",
    );
    assert!(!rendered.iter().any(|line| line.starts_with("entity:")));

    let from = query_sql(&fixture, "SHOW RELATIONS FROM SqlTestUser")
        .expect("SHOW RELATIONS FROM should succeed");
    let in_form = query_sql(&fixture, "SHOW RELATIONS IN SqlTestUser")
        .expect("SHOW RELATIONS IN should succeed");
    assert_eq!(from, in_form);
    let SqlQueryResult::ShowRelations(relations) = from else {
        panic!("SHOW RELATIONS should return its dedicated typed result");
    };
    assert_eq!(relations.entity(), "SqlTestUser");
    assert!(relations.relations().is_empty());

    let compact_error = query_sql(&fixture, "DESCRIBE MissingIntrospectionEntity")
        .expect_err("compact missing entity should fail");
    let verbose_error = query_sql(&fixture, "DESCRIBE MissingIntrospectionEntity VERBOSE")
        .expect_err("verbose missing entity should fail");
    assert_eq!(compact_error.code(), verbose_error.code());
    assert_eq!(compact_error.class(), verbose_error.class());
}

#[test]
#[ignore = "release-closeout instruction probe over the exact prepared-commit work bound"]
fn identity_closeout_reports_one_row_and_maximum_batch_instruction_costs() {
    let fixture = install_sql_canister_fixture();
    let stable_before = stable_memory_fingerprint(&fixture);
    let result: Result<IdentityCloseoutPerfResult, Error> = fixture
        .update_candid("measure_identity_closeout_perf", ())
        .expect("Identity closeout perf result should decode");
    let result = result.expect("Identity closeout perf endpoint should succeed");

    assert!(result.caller_nat64_instructions > 0);
    assert!(result.generated_nat64_instructions > 0);
    assert!(result.generated_nat128_instructions > 0);
    assert!(result.one_row_batch_instructions > 0);
    assert!(result.maximum_batch_instructions > result.one_row_batch_instructions);
    assert!(result.sequential_three_entity_instructions > 0);
    assert!(result.atomic_three_entity_instructions > 0);
    assert!(result.maximum_entity_context_instructions > 0);
    assert!(result.sequential_typed_enrollment_instructions > 0);
    assert!(result.atomic_typed_enrollment_instructions > 0);
    assert!(result.atomic_three_entity_instructions < 40_000_000_000);
    assert!(result.maximum_entity_context_instructions < 40_000_000_000);
    assert!(result.atomic_typed_enrollment_instructions < 40_000_000_000);
    assert!(
        result.atomic_three_entity_instructions < result.sequential_three_entity_instructions,
        "one marker should cost less than three independently committed writes",
    );
    assert!(
        result.atomic_typed_enrollment_instructions
            < result.sequential_typed_enrollment_instructions,
        "one typed marker should cost less than three typed commits",
    );
    assert_eq!(result.maximum_batch_rows, (4 * 1024) - 1);
    assert_eq!(result.maximum_entity_context_count, 64);
    let membership_indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestEnrollmentUserPrincipal")
            .expect("Toko-shaped membership indexes should remain introspectable"),
    );
    assert!(membership_indexes.iter().any(|index| {
        index.contains("(user_id)") && !index.contains("authentication_principal")
    }));
    assert!(membership_indexes.iter().any(|index| {
        index.starts_with("UNIQUE INDEX") && index.contains("(user_id, authentication_principal)")
    }));
    let stable_after = stable_memory_fingerprint(&fixture);
    assert!(stable_after.1 >= stable_before.1);

    println!(
        "identity closeout instructions: caller_nat64={} generated_nat64={} generated_nat128={} one_row_batch={} maximum_batch={} maximum_batch_rows={} sequential_three_entity={} atomic_three_entity={} maximum_entity_context={} maximum_entity_context_count={} sequential_typed_enrollment={} atomic_typed_enrollment={} stable_before={} stable_after={} stable_delta={}",
        result.caller_nat64_instructions,
        result.generated_nat64_instructions,
        result.generated_nat128_instructions,
        result.one_row_batch_instructions,
        result.maximum_batch_instructions,
        result.maximum_batch_rows,
        result.sequential_three_entity_instructions,
        result.atomic_three_entity_instructions,
        result.maximum_entity_context_instructions,
        result.maximum_entity_context_count,
        result.sequential_typed_enrollment_instructions,
        result.atomic_typed_enrollment_instructions,
        stable_before.1,
        stable_after.1,
        stable_after.1.saturating_sub(stable_before.1),
    );
}

#[test]
#[ignore = "release evidence for explicit application-behavior instruction costs"]
fn application_behavior_reports_separate_and_composed_instruction_costs() {
    let fixture = install_sql_canister_fixture();
    let result: Result<ApplicationBehaviorPerfResult, String> = fixture
        .query_candid("measure_application_behavior_perf", ())
        .expect("application behavior perf result should decode");
    let result = result.expect("application behavior perf endpoint should succeed");

    assert!(result.normalize_instructions > 0);
    assert!(result.validate_instructions > 0);
    assert!(result.normalize_and_validate_instructions > 0);
    assert_eq!(result.iterations, 256);
    let expected_bytes = u64::from(result.iterations) * 9;
    assert_eq!(result.normalized_bytes, expected_bytes);
    assert_eq!(result.validated_bytes, expected_bytes);
    assert_eq!(result.composed_bytes, expected_bytes);

    println!(
        "application behavior instructions over {} calls: normalize={} validate={} normalize_and_validate={}",
        result.iterations,
        result.normalize_instructions,
        result.validate_instructions,
        result.normalize_and_validate_instructions,
    );
}

fn seed_oversized_sql_group_name(fixture: &StandaloneCanisterFixture) {
    let result: Result<(), Error> = fixture
        .update_candid("seed_oversized_sql_group_name", ())
        .expect("oversized SQL group-name seed call should decode");

    result.expect("oversized SQL group-name seed should succeed");
}

fn query_sql_with_perf(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> Result<SqlQueryPerfResult, Error> {
    fixture
        .query_candid("icydb_query", (sql.to_string(),))
        .expect("sql query canister call should decode")
}

fn query_sql_attribution(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> Result<SqlQueryExecutionAttribution, Error> {
    fixture
        .query_candid("measure_sql_query_attribution", (sql.to_string(),))
        .expect("SQL attribution canister call should decode")
}

fn measure_query_sql(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> SqlExecutionInstructionResult {
    fixture
        .query_candid("measure_sql_query_instructions", (sql.to_string(),))
        .expect("measured SQL query canister call should decode")
}

fn query_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    query_sql_with_perf(fixture, sql).map(|payload| payload.result)
}

fn query_numeric_types(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> Result<SqlQueryResult, Error> {
    query_sql(fixture, sql)
}

fn ddl_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    fixture
        .update_candid("icydb_ddl", (sql.to_string(),))
        .expect("sql DDL canister call should decode")
}

fn measure_ddl_sql(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> SqlExecutionInstructionResult {
    fixture
        .update_candid("measure_sql_ddl_admission_instructions", (sql.to_string(),))
        .expect("measured SQL DDL canister call should decode")
}

fn measure_mutation_sql(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> SqlExecutionInstructionResult {
    fixture
        .update_candid(
            "measure_trusted_sql_exact_update_instructions",
            (sql.to_string(),),
        )
        .expect("measured SQL mutation canister call should decode")
}

fn measure_accepted_schema_read(
    fixture: &StandaloneCanisterFixture,
    entity: &str,
) -> AcceptedSchemaReadInstructionResult {
    let result: Result<AcceptedSchemaReadInstructionResult, Error> = fixture
        .query_candid(
            "measure_accepted_schema_read_instructions",
            (entity.to_string(),),
        )
        .expect("measured accepted-schema read should decode");
    result.expect("measured accepted-schema read should succeed")
}

fn stable_memory_fingerprint(fixture: &StandaloneCanisterFixture) -> ([u8; 32], usize) {
    let stable = fixture.pocket_ic().get_stable_memory(fixture.canister_id());
    (*blake3::hash(&stable).as_bytes(), stable.len())
}

#[test]
fn reference_unknown_order_field_survives_real_canister_and_leaves_stable_bytes_unchanged() {
    const FAILURE_SQL: &str = "SELECT pokemon_card_id FROM PokemonCardMetadata \
        ORDER BY hp DESC, id DESC";
    const SUCCESS_SQL: &str = "SELECT pokemon_card_id FROM PokemonCardMetadata \
        ORDER BY hp DESC, pokemon_card_id DESC LIMIT 1";

    let fixture = install_sql_canister_fixture();
    let stable_before = stable_memory_fingerprint(&fixture);

    let direct_error = query_sql(&fixture, FAILURE_SQL)
        .expect_err("reference unknown ORDER BY field should fail through generated dispatch");
    assert_eq!(direct_error.code(), ErrorCode::QUERY_PLAN);
    assert_eq!(direct_error.facts().len(), 1);
    assert_eq!(
        direct_error.facts()[0].tag(),
        icydb::diagnostic::DiagnosticFactTag::TermIndex.raw()
    );
    assert_eq!(direct_error.facts()[0].value(), 1);
    assert_eq!(
        direct_error.validated_query_field(),
        Ok(Some((icydb::diagnostic::QueryFieldRole::OrderBy, "id")))
    );

    let measured_failure = measure_query_sql(&fixture, FAILURE_SQL);
    assert_eq!(measured_failure.result, Err(direct_error));
    assert!(measured_failure.local_instructions > 0);
    assert!(measured_failure.local_instructions < 40_000_000_000);

    let success = query_sql_with_perf(&fixture, SUCCESS_SQL)
        .expect("reference corrected query should succeed through generated dispatch");
    assert!(success.instructions > 0);
    assert!(success.instructions < 40_000_000_000);

    let stable_after = stable_memory_fingerprint(&fixture);
    assert_eq!(stable_after, stable_before);

    println!(
        "0.232 real-canister instructions: unknown_order_field={} corrected_query={} stable_bytes={} stable_blake3={}",
        measured_failure.local_instructions,
        success.instructions,
        stable_after.1,
        blake3::Hash::from_bytes(stable_after.0),
    );
}

fn update_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    fixture
        .update_candid("icydb_update", (sql.to_string(),))
        .expect("sql update canister call should decode")
}

fn expect_integrity_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> IntegrityCheckResult {
    let result: Result<IntegrityCheckResult, SqlIntegrityError> = fixture
        .update_candid("icydb_integrity", (sql.to_string(),))
        .expect("integrity canister call should decode");

    result.expect("integrity canister call should succeed")
}

#[derive(Clone, Copy, Debug)]
struct DdlSchemaVersion {
    current: u32,
}

impl DdlSchemaVersion {
    const fn initial() -> Self {
        Self { current: 1 }
    }

    fn publish(
        &mut self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        let result = ddl_sql(fixture, &ddl_transition_sql(sql, self.current));
        if result.is_ok() {
            self.current = self
                .current
                .checked_add(1)
                .expect("test schema version should fit u32");
        }
        result
    }

    fn reject(
        self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        ddl_sql(fixture, &ddl_transition_sql(sql, self.current))
    }

    fn measure_publish(
        &mut self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> SqlExecutionInstructionResult {
        let measured = measure_ddl_sql(fixture, &ddl_transition_sql(sql, self.current));
        if measured.result.is_ok() {
            self.current = self
                .current
                .checked_add(1)
                .expect("test schema version should fit u32");
        }
        measured
    }

    fn measure_reject(
        self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> SqlExecutionInstructionResult {
        measure_ddl_sql(fixture, &ddl_transition_sql(sql, self.current))
    }

    fn measure_no_op(
        self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> SqlExecutionInstructionResult {
        measure_ddl_sql(fixture, &ddl_expected_sql(sql, self.current))
    }

    fn no_op(
        self,
        fixture: &StandaloneCanisterFixture,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        ddl_sql(fixture, &ddl_expected_sql(sql, self.current))
    }

    fn validate_constraint_to_completion(
        &mut self,
        fixture: &StandaloneCanisterFixture,
        entity: &str,
        constraint: &str,
    ) -> Result<SqlQueryResult, Error> {
        let sql = format!("ALTER TABLE {entity} VALIDATE CONSTRAINT {constraint}");
        for _ in 0..4 {
            let result = ddl_sql(fixture, sql.as_str())?;
            if matches!(
                &result,
                SqlQueryResult::Ddl {
                    constraint_validation: Some(validation),
                    ..
                } if validation.complete
            ) {
                self.current = self
                    .current
                    .checked_add(1)
                    .expect("test schema version should fit u32");
                return Ok(result);
            }
        }
        panic!("bounded fixture validation should complete within four calls");
    }
}

fn ddl_transition_sql(sql: &str, expected_schema_version: u32) -> String {
    ddl_contract_sql(
        sql,
        &format!(
            "EXPECT SCHEMA VERSION {expected_schema_version} SET SCHEMA VERSION {}",
            expected_schema_version
                .checked_add(1)
                .expect("test schema version should fit u32"),
        ),
    )
}

fn ddl_expected_sql(sql: &str, expected_schema_version: u32) -> String {
    ddl_contract_sql(
        sql,
        &format!("EXPECT SCHEMA VERSION {expected_schema_version}"),
    )
}

fn ddl_contract_sql(sql: &str, contract: &str) -> String {
    if let Some(where_offset) = sql.find(" WHERE ") {
        format!(
            "{} {contract}{}",
            &sql[..where_offset],
            &sql[where_offset..],
        )
    } else {
        format!("{sql} {contract}")
    }
}

fn expect_projection(result: SqlQueryResult) -> RowProjectionOutput {
    match result {
        SqlQueryResult::Projection(rows) => rows,
        other => panic!("expected projection payload, got {other:?}"),
    }
}

fn first_projected_text(output: &RowProjectionOutput) -> String {
    output
        .rendered_rows()
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .expect("projection should include a first text cell")
}

fn assert_projection_rendered(
    output: &RowProjectionOutput,
    entity: &str,
    columns: &[&str],
    rows: &[&[&str]],
    row_count: u32,
    message: &str,
) {
    assert_eq!(output.entity, entity, "{message}");
    assert_eq!(output.columns, columns, "{message}");
    assert_eq!(output.rendered_rows(), string_rows(rows), "{message}");
    assert_eq!(output.row_count, row_count, "{message}");
}

fn string_rows(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|value| (*value).to_string()).collect())
        .collect()
}

fn expect_grouped(result: SqlQueryResult) -> SqlGroupedRowsOutput {
    match result {
        SqlQueryResult::Grouped(rows) => rows,
        other => panic!("expected grouped payload, got {other:?}"),
    }
}

fn expect_explain(result: SqlQueryResult) -> String {
    match result {
        SqlQueryResult::Explain { explain, .. } => explain,
        other => panic!("expected explain payload, got {other:?}"),
    }
}

fn expect_describe(result: SqlQueryResult) -> EntitySchemaDescription {
    match result {
        SqlQueryResult::Describe(SqlDescribeOutput::Verbose { description }) => description,
        other => panic!("expected DESCRIBE payload, got {other:?}"),
    }
}

fn expect_show_indexes(result: SqlQueryResult) -> Vec<String> {
    match result {
        SqlQueryResult::ShowIndexes { indexes, .. } => indexes,
        other => panic!("expected SHOW INDEXES FROM payload, got {other:?}"),
    }
}

fn active_constraint_name(
    fixture: &StandaloneCanisterFixture,
    entity: &str,
    kind: &str,
    field: &str,
) -> String {
    let sql = format!("SHOW CONSTRAINTS FROM {entity}");
    let result = query_sql(fixture, sql.as_str()).expect("SHOW CONSTRAINTS should succeed");
    let SqlQueryResult::ShowConstraints { constraints, .. } = result else {
        panic!("SHOW CONSTRAINTS should return constraint metadata");
    };
    constraints
        .into_iter()
        .find(|constraint| {
            constraint.kind() == kind
                && constraint.validation_state() != "validated"
                && constraint.fields() == [field.to_string()]
        })
        .map(|constraint| constraint.name().to_string())
        .expect("live activation should be discoverable through SHOW CONSTRAINTS")
}

fn sql_test_user_id_by_name(fixture: &StandaloneCanisterFixture, name: &str) -> String {
    let sql = format!("SELECT id FROM SqlTestUser WHERE name = '{name}'");
    let output = expect_projection(
        query_sql(fixture, sql.as_str()).expect("fixture id read should find the named user"),
    );

    assert_eq!(
        output.row_count, 1,
        "named SQL fixture user should be unique",
    );
    first_projected_text(&output)
}

fn sql_test_numeric_type_id_by_label(fixture: &StandaloneCanisterFixture, label: &str) -> String {
    let sql = format!("SELECT id FROM SqlTestNumericTypes WHERE label = '{label}'");
    let output = expect_projection(
        query_sql(fixture, sql.as_str()).expect("fixture id read should find the labeled row"),
    );

    assert_eq!(
        output.row_count, 1,
        "labeled numeric SQL fixture row should be unique",
    );
    first_projected_text(&output)
}

fn assert_ddl_no_op(result: SqlQueryResult, expected_kind: &str, expected_target: &str) {
    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = result
    else {
        panic!("no-op DDL should return a DDL payload");
    };

    assert_eq!(mutation_kind, expected_kind);
    assert_eq!(target_index, expected_target);
    assert_eq!(status, "no_op");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
}

fn assert_rename_column_ddl_report(result: SqlQueryResult) {
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        target_store,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = result
    else {
        panic!("RENAME COLUMN should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "rename_field");
    assert_eq!(target_index, "handle");
    assert_eq!(target_store, "SqlTestUser");
    assert_eq!(
        field_path,
        vec!["nickname".to_string(), "handle".to_string()],
    );
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
}

fn assert_rename_column_schema_visibility(
    before: &EntitySchemaDescription,
    after: &EntitySchemaDescription,
) {
    assert!(
        before
            .fields()
            .iter()
            .any(|field| field.name() == "nickname"),
        "setup should expose DDL-owned source field before RENAME COLUMN",
    );
    assert!(
        !after
            .fields()
            .iter()
            .any(|field| field.name() == "nickname"),
        "published RENAME COLUMN should remove the old accepted field name",
    );
    assert!(
        after.fields().iter().any(|field| field.name() == "handle"),
        "published RENAME COLUMN should expose the new accepted field name",
    );
}

fn assert_rename_column_index_visibility(indexes: &[String]) {
    assert!(
        indexes
            .iter()
            .any(|index| index
                == "INDEX sql_test_user_nickname_idx (handle) [state=ready] [origin=ddl]"),
        "published RENAME COLUMN should update field-path index metadata: {indexes:?}",
    );
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_lower_nickname_idx (expr:v1:LOWER(handle)) [state=ready] [origin=ddl]"),
        "published RENAME COLUMN should update expression index metadata: {indexes:?}",
    );
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_filtered_nickname_idx (handle) WHERE handle IS NOT NULL [state=ready] [origin=ddl]"),
        "published RENAME COLUMN should update filtered index predicate metadata: {indexes:?}",
    );
}

fn assert_runtime_unsupported_query_error(err: &Error, context: &str) {
    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "{context} should stay an unsupported runtime error at the canister boundary",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "{context} should keep query-owned origin metadata",
    );
}

fn assert_query_sql_surface_mismatch_error(err: &Error, expected: ErrorCode, context: &str) {
    assert_eq!(
        err.diagnostic_code(),
        DiagnosticCode::QuerySqlSurfaceMismatch,
        "{context} should stay at the compact SQL surface mismatch boundary",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "{context} should keep query-owned origin metadata",
    );
    assert_eq!(
        err.code(),
        expected,
        "{context} should preserve the numeric SQL surface mismatch leaf code",
    );
}

fn assert_ddl_rejection_error(err: &Error, context: &str) {
    assert!(
        matches!(
            err.origin(),
            ErrorOrigin::Query | ErrorOrigin::Store | ErrorOrigin::Interface
        ),
        "{context} should keep query, store, or interface origin metadata, got {:?}",
        err.origin(),
    );

    match err.diagnostic_code() {
        DiagnosticCode::SchemaDdlAdmission => assert_ne!(
            err.code(),
            ErrorCode::SCHEMA_DDL_ADMISSION,
            "{context} should preserve a specific schema DDL admission leaf code",
        ),
        DiagnosticCode::QueryUnsupportedSqlFeature => assert!(
            err.code() != ErrorCode::QUERY_UNSUPPORTED_SQL_FEATURE,
            "{context} should preserve a numeric unsupported SQL feature leaf code",
        ),
        DiagnosticCode::RuntimeUnsupported if err.origin() == ErrorOrigin::Interface => assert!(
            matches!(
                err.code(),
                ErrorCode::RUNTIME_BOUNDARY_SQL_DDL_TARGET_REQUIRED
                    | ErrorCode::RUNTIME_BOUNDARY_SQL_DDL_ENTITY_NOT_CONFIGURED
            ),
            "{context} should preserve a numeric generated DDL boundary leaf code",
        ),
        DiagnosticCode::RuntimeUnsupported => {}
        other => panic!(
            "{context} should reject as compact DDL admission, unsupported SQL feature, or unsupported runtime, got {other:?}"
        ),
    }
}

fn assert_numeric_query_error(err: Error, expected_code: ErrorCode, context: &str) {
    assert!(
        matches!(
            expected_code,
            ErrorCode::QUERY_NUMERIC_OVERFLOW | ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE
        ),
        "numeric query assertions must use numeric diagnostic codes",
    );
    assert_eq!(
        err.code(),
        expected_code,
        "{context} should preserve numeric compact diagnostic code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "{context} should keep query-owned origin metadata",
    );
}

fn assert_ddl_rejects_without_index_visibility_change(
    fixture: &StandaloneCanisterFixture,
    schema_version: DdlSchemaVersion,
    sql: &str,
    forbidden_visibility_fragment: &str,
) {
    let before = expect_show_indexes(
        query_sql(fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes before rejected DDL"),
    );
    let err = schema_version
        .reject(fixture, sql)
        .expect_err("invalid DDL should reject");

    assert_ddl_rejection_error(
        &err,
        "invalid DDL should stay at the schema DDL admission boundary",
    );
    let after = expect_show_indexes(
        query_sql(fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should still read accepted indexes after rejected DDL"),
    );
    assert_eq!(
        after, before,
        "rejected DDL must leave accepted index visibility unchanged",
    );
    assert!(
        after
            .iter()
            .all(|index| !index.contains(forbidden_visibility_fragment)),
        "rejected DDL output fragment must not become visible: {after:?}",
    );
}

fn assert_ddl_rejects_with_index_visibility_unchanged(
    fixture: &StandaloneCanisterFixture,
    schema_version: DdlSchemaVersion,
    sql: &str,
) -> Error {
    assert_ddl_rejects_with_entity_index_visibility_unchanged(
        fixture,
        schema_version,
        "SqlTestUser",
        sql,
    )
}

fn assert_ddl_rejects_with_entity_index_visibility_unchanged(
    fixture: &StandaloneCanisterFixture,
    schema_version: DdlSchemaVersion,
    entity: &str,
    sql: &str,
) -> Error {
    let before = expect_show_indexes(
        query_sql(fixture, &format!("SHOW INDEXES FROM {entity}"))
            .expect("SHOW INDEXES FROM should read accepted indexes before rejected DDL"),
    );
    let err = schema_version
        .reject(fixture, sql)
        .expect_err("invalid DDL should reject");

    assert_ddl_rejection_error(
        &err,
        "invalid DDL should stay at the schema DDL admission boundary",
    );
    let after = expect_show_indexes(
        query_sql(fixture, &format!("SHOW INDEXES FROM {entity}"))
            .expect("SHOW INDEXES FROM should still read accepted indexes after rejected DDL"),
    );
    assert_eq!(
        after, before,
        "rejected DDL must leave accepted index visibility unchanged",
    );

    err
}

#[test]
fn sql_canister_required_sqlite_reference_profile_matches_bundled_reference() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let scenarios = required_sqlite_reference_scenarios();
    assert!(
        scenarios.len() <= 96,
        "required live SQLite profile must stay within the design cap",
    );

    for scenario in scenarios {
        let expected = execute_sqlite_reference_scenario(*scenario).unwrap_or_else(|error| {
            panic!(
                "bundled SQLite scenario {:?} failed as {:?}: {}",
                scenario.id(),
                error.kind(),
                error.detail(),
            )
        });
        let sql = scenario
            .render_sql("SqlTestUser")
            .expect("maintained live entity identifier should render");
        let live = query_sql(&fixture, &sql).unwrap_or_else(|error| {
            panic!(
                "live IcyDB scenario {:?} rejected with {:?}/{:?}: {error}",
                scenario.id(),
                error.code(),
                error.diagnostic_code(),
            )
        });
        let subject = normalize_live_sqlite_result(*scenario, live).unwrap_or_else(|error| {
            panic!(
                "live IcyDB scenario {:?} should normalize: {error}",
                scenario.id()
            )
        });
        let reference = normalize_bundled_sqlite_result(&expected);
        let correctness_scenario = live_sqlite_correctness_scenario(*scenario, sql);
        let observation = CorrectnessObservation {
            subject: ObservedOutcome::Accepted(subject),
            provider: Some(ObservedOutcome::Accepted(reference)),
            route: None,
        };

        assert_eq!(
            correctness_verdict(&correctness_scenario, &observation),
            CorrectnessVerdict::Passed,
            "live IcyDB should agree with bundled SQLite for scenario {:?}",
            scenario.id(),
        );
    }
}

fn live_sqlite_correctness_scenario(
    scenario: SqliteReferenceScenario,
    sql: String,
) -> CorrectnessScenario<()> {
    CorrectnessScenario {
        key: scenario.id().to_string(),
        surface: (),
        family: "sqlite.reference.live".to_string(),
        sql,
        metadata: ScenarioMetadata {
            contract_features: scenario.contract_features(),
            provider_id: "canister.query.sqlite_reference_profile",
            provider: EligibleProvider::SqliteReference,
            evidence_strength: EvidenceStrength::ReferenceOracle,
            statement: StatementFamily::Select,
            shape: sqlite_reference_query_shape(scenario),
            value_type: sqlite_reference_value_type(scenario),
            nullability: if scenario.nullable() {
                NullabilityClass::Nullable
            } else {
                NullabilityClass::NonNullable
            },
            predicate: sqlite_reference_predicate(scenario.predicate()),
            window: sqlite_reference_window(scenario.window()),
            mutation: MutationKind::None,
            row_order: sqlite_reference_row_order(scenario.row_order()),
            route: RouteExpectation::Fixed(RouteFact::new(
                RouteFamily::UnsupportedAccessKind,
                RouteOutcome::Unsupported,
                RouteReason::OrderExpressionNotClassified,
            )),
            required_route: None,
            expected: ExpectedAcceptance::Accepted,
        },
    }
}

fn sqlite_reference_query_shape(scenario: SqliteReferenceScenario) -> QueryShape {
    if scenario
        .families()
        .contains(&SqliteReferenceFamily::Grouped)
    {
        QueryShape::Grouped
    } else if scenario
        .families()
        .contains(&SqliteReferenceFamily::Aggregate)
    {
        QueryShape::GlobalAggregate
    } else {
        QueryShape::Scalar
    }
}

fn sqlite_reference_value_type(scenario: SqliteReferenceScenario) -> ValueTypeFamily {
    let columns = scenario.columns();
    if columns
        .iter()
        .all(|kind| *kind == SqliteReferenceColumnKind::Blob)
    {
        ValueTypeFamily::Blob
    } else if columns
        .iter()
        .all(|kind| *kind == SqliteReferenceColumnKind::Boolean)
    {
        ValueTypeFamily::Boolean
    } else if columns.iter().all(|kind| {
        matches!(
            kind,
            SqliteReferenceColumnKind::Decimal | SqliteReferenceColumnKind::Integer
        )
    }) {
        ValueTypeFamily::Numeric
    } else if columns
        .iter()
        .all(|kind| *kind == SqliteReferenceColumnKind::Text)
    {
        ValueTypeFamily::Text
    } else {
        ValueTypeFamily::Mixed
    }
}

const fn sqlite_reference_predicate(predicate: SqliteReferencePredicateFamily) -> PredicateFamily {
    match predicate {
        SqliteReferencePredicateFamily::Compound => PredicateFamily::Compound,
        SqliteReferencePredicateFamily::FieldComparison => PredicateFamily::FieldComparison,
        SqliteReferencePredicateFamily::Membership => PredicateFamily::Membership,
        SqliteReferencePredicateFamily::None => PredicateFamily::None,
        SqliteReferencePredicateFamily::Range => PredicateFamily::Range,
    }
}

const fn sqlite_reference_window(window: SqliteReferenceWindow) -> WindowSpec {
    match window {
        SqliteReferenceWindow::Ordered => {
            WindowSpec::ordered_unbounded("declared SQLite reference order")
        }
        SqliteReferenceWindow::OrderedLimit { limit, offset } => {
            WindowSpec::ordered(limit, offset, "declared SQLite reference order")
        }
        SqliteReferenceWindow::Unordered => WindowSpec::NONE,
    }
}

const fn sqlite_reference_row_order(order: SqliteReferenceRowOrder) -> RowOrder {
    match order {
        SqliteReferenceRowOrder::Ordered => RowOrder::Ordered,
        SqliteReferenceRowOrder::Unordered => RowOrder::Unordered,
    }
}

fn normalize_bundled_sqlite_result(result: &SqliteReferenceResult) -> NormalizedResult {
    NormalizedResult {
        columns: result.columns().to_vec(),
        rows: result
            .rows()
            .iter()
            .map(|row| row.iter().map(normalize_bundled_sqlite_value).collect())
            .collect(),
        row_order: sqlite_reference_row_order(result.row_order()),
    }
}

fn normalize_bundled_sqlite_value(value: &SqliteReferenceValue) -> NormalizedCell {
    match value {
        SqliteReferenceValue::Blob(value) => NormalizedCell::Bytes(value.clone()),
        SqliteReferenceValue::Boolean(value) => NormalizedCell::Bool(*value),
        SqliteReferenceValue::Decimal { mantissa, scale } => NormalizedCell::Decimal {
            coefficient: *mantissa,
            scale: *scale,
        },
        SqliteReferenceValue::Integer(value) => NormalizedCell::Int(i128::from(*value)),
        SqliteReferenceValue::Null => NormalizedCell::Null,
        SqliteReferenceValue::Text(value) => NormalizedCell::Text(value.clone()),
    }
}

fn normalize_live_sqlite_result(
    scenario: SqliteReferenceScenario,
    result: SqlQueryResult,
) -> Result<NormalizedResult, String> {
    let (columns, rows) = match result {
        SqlQueryResult::Projection(output) => {
            verify_live_row_count(scenario, output.row_count, output.rows.len())?;
            let rows = output
                .rows
                .into_iter()
                .enumerate()
                .map(|(row_index, row)| normalize_live_projection_row(scenario, row_index, row))
                .collect::<Result<Vec<_>, _>>()?;
            (output.columns, rows)
        }
        SqlQueryResult::Grouped(output) => {
            verify_live_row_count(scenario, output.row_count, output.rows.len())?;
            if output.next_cursor.is_some() {
                return Err("compact live SQLite profile unexpectedly produced a cursor".into());
            }
            let rows = output
                .rows
                .into_iter()
                .enumerate()
                .map(|(row_index, row)| normalize_live_grouped_row(scenario, row_index, row))
                .collect::<Result<Vec<_>, _>>()?;
            (output.columns, rows)
        }
        other => {
            return Err(format!(
                "scenario {:?} returned unsupported live payload {other:?}",
                scenario.id()
            ));
        }
    };
    if columns.len() != scenario.columns().len() {
        return Err(format!(
            "scenario {:?} returned {} columns for {} declared mappings",
            scenario.id(),
            columns.len(),
            scenario.columns().len(),
        ));
    }

    Ok(NormalizedResult {
        columns,
        rows,
        row_order: sqlite_reference_row_order(scenario.row_order()),
    })
}

fn normalize_live_projection_row(
    scenario: SqliteReferenceScenario,
    row_index: usize,
    row: Vec<OutputValue>,
) -> Result<Vec<NormalizedCell>, String> {
    verify_live_row_shape(scenario, row_index, row.len())?;
    row.into_iter()
        .zip(scenario.columns().iter().copied())
        .enumerate()
        .map(|(column, (value, kind))| {
            normalize_live_projection_value(scenario, column, kind, value)
        })
        .collect()
}

fn normalize_live_projection_value(
    scenario: SqliteReferenceScenario,
    column: usize,
    kind: SqliteReferenceColumnKind,
    value: OutputValue,
) -> Result<NormalizedCell, String> {
    let value = match (kind, value) {
        (_, OutputValue::Null) => NormalizedCell::Null,
        (SqliteReferenceColumnKind::Blob, OutputValue::Blob(value)) => NormalizedCell::Bytes(value),
        (SqliteReferenceColumnKind::Boolean, OutputValue::Bool(value)) => {
            NormalizedCell::Bool(value)
        }
        (SqliteReferenceColumnKind::Decimal, OutputValue::Decimal(value)) => {
            NormalizedCell::Decimal {
                coefficient: value.mantissa(),
                scale: value.scale(),
            }
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Int64(value)) => {
            NormalizedCell::Int(i128::from(value))
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Int128(value)) => {
            let value = i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {:?} column {column} returned non-SQLite Int128 {value}",
                    scenario.id()
                )
            })?;
            NormalizedCell::Int(i128::from(value))
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Nat64(value)) => {
            let value = i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {:?} column {column} returned non-SQLite Nat64 {value}",
                    scenario.id()
                )
            })?;
            NormalizedCell::Int(i128::from(value))
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Nat128(value)) => {
            let value = i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {:?} column {column} returned non-SQLite Nat128 {value}",
                    scenario.id()
                )
            })?;
            NormalizedCell::Int(i128::from(value))
        }
        (SqliteReferenceColumnKind::Text, OutputValue::Text(value)) => NormalizedCell::Text(value),
        (kind, value) => {
            return Err(format!(
                "scenario {:?} column {column} returned {value:?} for {kind:?}",
                scenario.id()
            ));
        }
    };

    Ok(value)
}

fn normalize_live_grouped_row(
    scenario: SqliteReferenceScenario,
    row_index: usize,
    row: Vec<String>,
) -> Result<Vec<NormalizedCell>, String> {
    verify_live_row_shape(scenario, row_index, row.len())?;
    row.into_iter()
        .zip(scenario.columns().iter().copied())
        .enumerate()
        .map(|(column, (value, kind))| {
            let value = match kind {
                SqliteReferenceColumnKind::Decimal => {
                    let value = value.parse::<Decimal>().map_err(|error| {
                        format!(
                            "scenario {:?} grouped column {column} returned non-decimal {value:?}: {error}",
                            scenario.id()
                        )
                    })?;
                    NormalizedCell::Decimal {
                        coefficient: value.mantissa(),
                        scale: value.scale(),
                    }
                }
                SqliteReferenceColumnKind::Integer => {
                    let value = value.parse::<i64>().map_err(|error| {
                        format!(
                            "scenario {:?} grouped column {column} returned non-integer {value:?}: {error}",
                            scenario.id()
                        )
                    })?;
                    NormalizedCell::Int(i128::from(value))
                }
                SqliteReferenceColumnKind::Text => NormalizedCell::Text(value),
                SqliteReferenceColumnKind::Blob | SqliteReferenceColumnKind::Boolean => {
                    return Err(format!(
                        "scenario {:?} grouped column {column} uses unsupported rendered {kind:?} mapping",
                        scenario.id()
                    ));
                }
            };
            Ok(value)
        })
        .collect()
}

#[test]
fn sql_canister_sqlite_normalization_preserves_exact_decimal_identity() {
    let expected = NormalizedCell::Decimal {
        coefficient: 123,
        scale: 2,
    };
    assert_eq!(
        normalize_bundled_sqlite_value(&SqliteReferenceValue::Decimal {
            mantissa: 123,
            scale: 2,
        }),
        expected,
    );

    let scenario = required_sqlite_reference_scenarios()[0];
    let actual = normalize_live_projection_value(
        scenario,
        0,
        SqliteReferenceColumnKind::Decimal,
        OutputValue::Decimal(Decimal::new(123, 2)),
    )
    .expect("exact decimal output should normalize without coercion");
    assert_eq!(actual, expected);
}

fn verify_live_row_shape(
    scenario: SqliteReferenceScenario,
    row_index: usize,
    row_len: usize,
) -> Result<(), String> {
    if row_len != scenario.columns().len() {
        return Err(format!(
            "scenario {:?} row {row_index} returned {row_len} values for {} declared mappings",
            scenario.id(),
            scenario.columns().len(),
        ));
    }

    Ok(())
}

fn verify_live_row_count(
    scenario: SqliteReferenceScenario,
    row_count: u32,
    rows_len: usize,
) -> Result<(), String> {
    let rows_len = u32::try_from(rows_len).map_err(|_| {
        format!(
            "scenario {:?} returned a row vector too large for its public count",
            scenario.id()
        )
    })?;
    if row_count != rows_len {
        return Err(format!(
            "scenario {:?} reported {row_count} rows but returned {rows_len}",
            scenario.id()
        ));
    }

    Ok(())
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("supported CREATE INDEX DDL should publish through the canister endpoint");

    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after DDL publication"),
    );
    assert!(
        indexes
            .iter()
            .any(|index| index == "INDEX sql_test_user_rank_idx (rank) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published accepted index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_expression_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_lower_name_idx ON SqlTestUser (LOWER(name))",
        )
        .expect(
            "supported expression CREATE INDEX DDL should publish through the canister endpoint",
        );

    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported expression CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_expression_index");
    assert_eq!(target_index, "sql_test_user_lower_name_idx");
    assert_eq!(field_path, vec!["LOWER(name)".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes =
        expect_show_indexes(query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser").expect(
            "SHOW INDEXES FROM should read accepted indexes after expression DDL publication",
        ));
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_lower_name_idx (expr:v1:LOWER(name)) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published expression index: {indexes:?}",
    );

    let no_op = schema_version
        .no_op(
            &fixture,
            "CREATE INDEX IF NOT EXISTS sql_test_user_lower_name_idx ON SqlTestUser (LOWER(name))",
        )
        .expect(
            "matching expression CREATE INDEX IF NOT EXISTS should no-op at the canister endpoint",
        );
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = no_op
    else {
        panic!("matching expression CREATE INDEX IF NOT EXISTS should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_expression_index");
    assert_eq!(target_index, "sql_test_user_lower_name_idx");
    assert_eq!(field_path, vec!["LOWER(name)".to_string()]);
    assert_eq!(status, "no_op");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_unique_expression_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
        &fixture,
        "CREATE UNIQUE INDEX sql_test_user_lower_name_unique_idx ON SqlTestUser (LOWER(name))",
    )
    .expect(
        "supported unique expression CREATE INDEX DDL should publish through the canister endpoint",
    );

    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported unique expression CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_expression_index");
    assert_eq!(target_index, "sql_test_user_lower_name_unique_idx");
    assert_eq!(field_path, vec!["LOWER(name)".to_string()]);
    assert_eq!(status, "activation_published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
    schema_version
        .validate_constraint_to_completion(
            &fixture,
            "SqlTestUser",
            "sql_test_user_lower_name_unique_idx",
        )
        .expect("unique expression validation should promote the candidate index");

    let indexes = expect_show_indexes(query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser").expect(
        "SHOW INDEXES FROM should read accepted indexes after unique expression DDL publication",
    ));
    assert!(
        indexes.iter().any(|index| index
            == "UNIQUE INDEX sql_test_user_lower_name_unique_idx (expr:v1:LOWER(name)) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published unique expression index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_filtered_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_filtered_rank_idx ON SqlTestUser (rank) WHERE age > 30",
        )
        .expect("supported filtered CREATE INDEX DDL should publish through the canister endpoint");

    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported filtered CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_filtered_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 2);

    let indexes =
        expect_show_indexes(query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser").expect(
            "SHOW INDEXES FROM should read accepted indexes after filtered DDL publication",
        ));
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_filtered_rank_idx (rank) WHERE age > 30 [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published filtered index: {indexes:?}",
    );
}

fn assert_nullable_unique_route_evidence(
    before: &SqlQueryPerfResult,
    after: &SqlQueryPerfResult,
    forced_full_scan: &SqlQueryPerfResult,
    proven_attribution: &SqlQueryExecutionAttribution,
    full_scan_attribution: &SqlQueryExecutionAttribution,
) {
    assert_eq!(
        after.result, before.result,
        "the newly eligible filtered-index route must preserve pre-index full-scan output",
    );
    assert_eq!(
        after.result, forced_full_scan.result,
        "the proven index route and equivalent conservative full scan must agree",
    );
    assert_projection_rendered(
        &expect_projection(after.result.clone()),
        "SqlTestUser",
        &["name"],
        &[&["alice"]],
        1,
        "filtered unique access should return exactly the matching present value",
    );
    assert!(
        before.instructions > 0 && after.instructions > 0,
        "both production-shaped query routes should expose instruction attribution",
    );
    assert!(
        proven_attribution.index_store_entry_reads > 0,
        "the selected filtered index route should read its physical entry",
    );
    println!(
        "nullable unique query instructions: pre_index_full_scan_total={} pre_index_full_scan_compiler={} pre_index_full_scan_planner={} pre_index_full_scan_store={} pre_index_full_scan_executor={} pre_index_full_scan_decode={} proven_index_total={} proven_index_compiler={} proven_index_planner={} proven_index_store={} proven_index_executor={} proven_index_decode={} equivalent_full_scan_total={} equivalent_full_scan_compiler={} equivalent_full_scan_planner={} equivalent_full_scan_store={} equivalent_full_scan_executor={} equivalent_full_scan_decode={}",
        before.instructions,
        before.compiler_instructions,
        before.planner_instructions,
        before.store_instructions,
        before.executor_instructions,
        before.decode_instructions,
        after.instructions,
        after.compiler_instructions,
        after.planner_instructions,
        after.store_instructions,
        after.executor_instructions,
        after.decode_instructions,
        forced_full_scan.instructions,
        forced_full_scan.compiler_instructions,
        forced_full_scan.planner_instructions,
        forced_full_scan.store_instructions,
        forced_full_scan.executor_instructions,
        forced_full_scan.decode_instructions,
    );
    println!(
        "nullable unique query reads: proven_data_gets={} proven_index_gets={} proven_index_ranges={} proven_index_entries={} equivalent_full_scan_data_gets={} equivalent_full_scan_index_gets={} equivalent_full_scan_index_ranges={} equivalent_full_scan_index_entries={}",
        proven_attribution.store_get_calls,
        proven_attribution.index_store_get_calls,
        proven_attribution.index_store_range_scan_calls,
        proven_attribution.index_store_entry_reads,
        full_scan_attribution.store_get_calls,
        full_scan_attribution.index_store_get_calls,
        full_scan_attribution.index_store_range_scan_calls,
        full_scan_attribution.index_store_entry_reads,
    );
}

fn populate_nullable_unique_nicknames(fixture: &StandaloneCanisterFixture) {
    for (name, nickname) in [("alice", "ally"), ("bob", "bravo"), ("charlie", "zulu")] {
        let id = sql_test_user_id_by_name(fixture, name);
        update_sql(
            fixture,
            format!("UPDATE SqlTestUser SET nickname = '{nickname}' WHERE id = '{id}'").as_str(),
        )
        .unwrap_or_else(|error| panic!("primary-key nickname update should succeed: {error:?}"));
    }
}

fn publish_and_assert_nullable_unique_constraints(
    fixture: &StandaloneCanisterFixture,
    schema_version: &mut DdlSchemaVersion,
) {
    for (ddl, index) in [
        (
            "CREATE UNIQUE INDEX sql_test_user_unique_nickname_idx ON SqlTestUser (nickname) WHERE nickname IS NOT NULL",
            "sql_test_user_unique_nickname_idx",
        ),
        (
            "CREATE UNIQUE INDEX sql_test_user_unique_rank_contract_idx ON SqlTestUser (rank)",
            "sql_test_user_unique_rank_contract_idx",
        ),
    ] {
        schema_version
            .publish(fixture, ddl)
            .unwrap_or_else(|error| panic!("unique index should enter validation: {error:?}"));
        schema_version
            .validate_constraint_to_completion(fixture, "SqlTestUser", index)
            .unwrap_or_else(|error| panic!("unique index should validate: {error:?}"));
    }

    let constraints = match query_sql(fixture, "SHOW CONSTRAINTS FROM SqlTestUser")
        .expect("SHOW CONSTRAINTS should expose accepted unique contracts")
    {
        SqlQueryResult::ShowConstraints { constraints, .. } => constraints,
        other => panic!("expected SHOW CONSTRAINTS payload, got {other:?}"),
    };
    let filtered = constraints
        .iter()
        .find(|constraint| constraint.index() == Some("sql_test_user_unique_nickname_idx"))
        .expect("filtered accepted unique constraint should expose its backing index");
    assert!(filtered.index_id().is_some());
    assert_eq!(filtered.predicate_sql(), Some("nickname IS NOT NULL"));
    assert_eq!(filtered.semantics(), "partial_unique_index_v1");
    let unfiltered = constraints
        .iter()
        .find(|constraint| constraint.index() == Some("sql_test_user_unique_rank_contract_idx"))
        .expect("unfiltered accepted unique constraint should expose its backing index");
    assert!(unfiltered.index_id().is_some());
    assert_eq!(unfiltered.predicate_sql(), None);
    assert_eq!(unfiltered.semantics(), "unique_index_v1");
}

fn assert_nullable_unique_range_route_parity(fixture: &StandaloneCanisterFixture) {
    let range_sql = "SELECT name FROM SqlTestUser WHERE nickname > 'b' ORDER BY age ASC";
    let range_explain = expect_explain(
        query_sql(fixture, format!("EXPLAIN EXECUTION {range_sql}").as_str())
            .expect("proven range EXPLAIN should succeed"),
    );
    assert!(
        range_explain.contains("IndexRange(sql_test_user_unique_nickname_idx)"),
        "a concrete non-null range should prove filtered-index eligibility: {range_explain}",
    );
    let indexed = query_sql(fixture, range_sql).expect("proven range query should succeed");
    let forced_sql = "SELECT name FROM SqlTestUser WHERE nickname > 'b' OR name = '__no_such_user__' ORDER BY age ASC";
    let forced_explain = expect_explain(
        query_sql(fixture, format!("EXPLAIN EXECUTION {forced_sql}").as_str())
            .expect("conservative range EXPLAIN should succeed"),
    );
    assert!(
        forced_explain.contains("FullScan")
            && !forced_explain.contains("sql_test_user_unique_nickname_idx"),
        "unsupported OR range proof must preserve a full scan: {forced_explain}",
    );
    assert_eq!(
        indexed,
        query_sql(fixture, forced_sql).expect("equivalent conservative range query should succeed"),
        "newly eligible range output must match an independently forced full scan",
    );
}

#[test]
fn sql_canister_filtered_unique_index_requires_and_uses_non_null_query_proof() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN nickname text")
        .expect("nullable test field should publish through the canister endpoint");
    populate_nullable_unique_nicknames(&fixture);

    let select_sql = "SELECT name FROM SqlTestUser WHERE nickname = 'ally'";
    let before_explain = expect_explain(
        query_sql(&fixture, format!("EXPLAIN EXECUTION {select_sql}").as_str())
            .expect("pre-index EXPLAIN should succeed"),
    );
    assert!(
        before_explain.contains("FullScan")
            && !before_explain.contains("sql_test_user_unique_nickname_idx"),
        "pre-index EXPLAIN must retain the full scan and not select the future index: {before_explain}",
    );
    let before = query_sql_with_perf(&fixture, select_sql)
        .expect("pre-index full-scan query should succeed");

    publish_and_assert_nullable_unique_constraints(&fixture, &mut schema_version);

    let after_explain = expect_explain(
        query_sql(&fixture, format!("EXPLAIN EXECUTION {select_sql}").as_str())
            .expect("post-index EXPLAIN should succeed"),
    );
    assert!(
        after_explain.contains("IndexPrefix(sql_test_user_unique_nickname_idx)"),
        "a concrete non-null equality should prove filtered-index eligibility: {after_explain}",
    );
    let after = query_sql_with_perf(&fixture, select_sql).expect("post-index query should succeed");
    let missing_proof_explain = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION SELECT name FROM SqlTestUser WHERE nickname IS NULL",
        )
        .expect("nullable query EXPLAIN should succeed"),
    );
    assert!(
        !missing_proof_explain.contains("sql_test_user_unique_nickname_idx"),
        "a query that can observe omitted rows must not select the filtered index: {missing_proof_explain}",
    );
    let forced_full_scan_sql =
        "SELECT name FROM SqlTestUser WHERE nickname = 'ally' OR nickname = 'never'";
    let forced_full_scan_explain = expect_explain(
        query_sql(
            &fixture,
            format!("EXPLAIN EXECUTION {forced_full_scan_sql}").as_str(),
        )
        .expect("conservative OR query EXPLAIN should succeed"),
    );
    assert!(
        forced_full_scan_explain.contains("FullScan")
            && !forced_full_scan_explain.contains("sql_test_user_unique_nickname_idx"),
        "an unsupported OR proof must preserve the full-scan route: {forced_full_scan_explain}",
    );
    let forced_full_scan = query_sql_with_perf(&fixture, forced_full_scan_sql)
        .expect("equivalent conservative query should execute through a full scan");
    assert_nullable_unique_range_route_parity(&fixture);
    let proven_attribution = query_sql_attribution(&fixture, select_sql)
        .expect("proven index route should expose detailed attribution");
    let full_scan_attribution = query_sql_attribution(&fixture, forced_full_scan_sql)
        .expect("conservative route should expose detailed attribution");
    assert_nullable_unique_route_evidence(
        &before,
        &after,
        &forced_full_scan,
        &proven_attribution,
        &full_scan_attribution,
    );
}

fn require_measured_ddl_success(measured: SqlExecutionInstructionResult, context: &str) -> u64 {
    measured
        .result
        .unwrap_or_else(|error| panic!("{context} should succeed: {error:?}"));
    assert!(
        measured.local_instructions > 0,
        "{context} should report local instructions",
    );
    measured.local_instructions
}

fn prepare_nullable_unique_measurement_fields(
    fixture: &StandaloneCanisterFixture,
    schema_version: &mut DdlSchemaVersion,
) {
    for field in ["closeout_nullable_valid", "closeout_nullable_rejected"] {
        schema_version
            .publish(
                fixture,
                format!("ALTER TABLE SqlTestUser ADD COLUMN {field} text").as_str(),
            )
            .unwrap_or_else(|error| panic!("closeout nullable field should publish: {error:?}"));
    }
}

fn measure_nullable_unique_ddl_matrix(
    fixture: &StandaloneCanisterFixture,
    schema_version: &mut DdlSchemaVersion,
) -> [u64; 4] {
    let non_unique = require_measured_ddl_success(
        schema_version.measure_publish(
            fixture,
            "CREATE INDEX closeout_age_name_idx ON SqlTestUser (age, name)",
        ),
        "non-unique DDL admission",
    );
    let non_null_unique = require_measured_ddl_success(
        schema_version.measure_publish(
            fixture,
            "CREATE UNIQUE INDEX closeout_rank_unique_idx ON SqlTestUser (rank)",
        ),
        "non-null unique DDL admission",
    );
    schema_version
        .validate_constraint_to_completion(fixture, "SqlTestUser", "closeout_rank_unique_idx")
        .expect("non-null unique closeout index should validate");

    let nullable_unique = require_measured_ddl_success(
        schema_version.measure_publish(
            fixture,
            "CREATE UNIQUE INDEX closeout_nullable_valid_idx ON SqlTestUser \
             (closeout_nullable_valid) WHERE closeout_nullable_valid IS NOT NULL",
        ),
        "guarded nullable unique DDL admission",
    );
    schema_version
        .validate_constraint_to_completion(fixture, "SqlTestUser", "closeout_nullable_valid_idx")
        .expect("guarded nullable unique closeout index should validate");

    let rejected = schema_version.measure_reject(
        fixture,
        "CREATE UNIQUE INDEX closeout_nullable_rejected_idx ON SqlTestUser \
         (closeout_nullable_rejected)",
    );
    assert!(
        rejected.result.is_err(),
        "unguarded nullable unique admission must reject",
    );
    assert!(rejected.local_instructions > 0);

    [
        non_unique,
        non_null_unique,
        nullable_unique,
        rejected.local_instructions,
    ]
}

#[test]
fn nullable_unique_closeout_measures_admission_stable_bytes_and_unchanged_write_path() {
    let fixture = install_fixture_canister("sql");
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();
    prepare_nullable_unique_measurement_fields(&fixture, &mut schema_version);
    let ddl_instructions = measure_nullable_unique_ddl_matrix(&fixture, &mut schema_version);

    let stable_before = stable_memory_fingerprint(&fixture);
    let no_op = schema_version.measure_no_op(
        &fixture,
        "CREATE UNIQUE INDEX IF NOT EXISTS closeout_nullable_valid_idx ON SqlTestUser \
         (closeout_nullable_valid) WHERE closeout_nullable_valid IS NOT NULL",
    );
    require_measured_ddl_success(no_op, "identical accepted nullable unique DDL");
    let stable_after = stable_memory_fingerprint(&fixture);
    assert_eq!(
        stable_after, stable_before,
        "identical accepted schema and physical index authority must not change stable bytes",
    );

    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let write = measure_mutation_sql(
        &fixture,
        format!("UPDATE SqlTestUser SET rank = 101 WHERE id = '{alice_id}'").as_str(),
    );
    write
        .result
        .unwrap_or_else(|error| panic!("accepted unique write should succeed: {error:?}"));
    assert!(write.local_instructions > 0);

    println!(
        "0.231 nullable-unique closeout: non_unique_ddl={} non_null_unique_ddl={} \
         guarded_nullable_unique_ddl={} rejected_nullable_unique_ddl={} \
         stable_bytes_before={} stable_bytes_after={} unique_write={}",
        ddl_instructions[0],
        ddl_instructions[1],
        ddl_instructions[2],
        ddl_instructions[3],
        stable_before.1,
        stable_after.1,
        write.local_instructions,
    );
}

fn add_closeout_indexes_to_eight(
    fixture: &StandaloneCanisterFixture,
    schema_version: &mut DdlSchemaVersion,
) {
    for ordinal in 1..=7 {
        let field = format!("closeout_reopen_{ordinal}");
        schema_version
            .publish(
                fixture,
                format!("ALTER TABLE SqlTestUser ADD COLUMN {field} text").as_str(),
            )
            .unwrap_or_else(|error| panic!("reopen measurement field should publish: {error:?}"));
        schema_version
            .publish(
                fixture,
                format!("CREATE INDEX closeout_reopen_{ordinal}_idx ON SqlTestUser ({field})")
                    .as_str(),
            )
            .unwrap_or_else(|error| panic!("reopen measurement index should publish: {error:?}"));
    }
}

#[test]
fn accepted_schema_reopen_reads_are_measured_at_one_and_eight_indexes() {
    let fixture = install_fixture_canister("sql");
    deliver_fixture_startup_watchdog(&fixture);
    let one = measure_accepted_schema_read(&fixture, "SqlTestUser");
    assert_eq!(one.description.indexes().len(), 1);
    assert!(one.local_instructions > 0);

    let mut schema_version = DdlSchemaVersion::initial();
    add_closeout_indexes_to_eight(&fixture, &mut schema_version);
    assert_eq!(
        measure_accepted_schema_read(&fixture, "SqlTestUser")
            .description
            .indexes()
            .len(),
        8,
    );
    upgrade_fixture_canister(&fixture, "sql");
    deliver_fixture_startup_watchdog(&fixture);
    deliver_fixture_startup_watchdog(&fixture);
    let eight = measure_accepted_schema_read(&fixture, "SqlTestUser");
    assert_eq!(eight.description.indexes().len(), 8);
    assert!(eight.local_instructions > 0);

    println!(
        "0.231 accepted-schema reopen: one_index_instructions={} eight_index_instructions={}",
        one.local_instructions, eight.local_instructions,
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_supported_multi_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_age_idx ON SqlTestUser (rank, age)",
        )
        .expect(
            "supported multi-field CREATE INDEX DDL should publish through the canister endpoint",
        );

    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported multi-field CREATE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_age_idx");
    assert_eq!(field_path, vec!["rank,age".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after DDL publication"),
    );
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_rank_age_idx (rank, age) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published composite index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_treats_asc_index_order_as_default_order() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_age_asc_idx ON SqlTestUser (rank ASC, age ASC)",
        )
        .expect("CREATE INDEX with explicit ASC should publish through the canister endpoint");

    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported ASC CREATE INDEX should return a DDL payload");
    };
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_age_asc_idx");
    assert_eq!(field_path, vec!["rank,age".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after ASC DDL publication"),
    );
    assert!(
        indexes.iter().any(|index| index
            == "INDEX sql_test_user_rank_age_asc_idx (rank, age) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose explicit ASC as the default index order: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_and_drops_supported_unique_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE UNIQUE INDEX sql_test_user_unique_rank_idx ON SqlTestUser (rank)",
        )
        .expect("supported CREATE UNIQUE INDEX DDL should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("supported CREATE UNIQUE INDEX should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_unique_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "activation_published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
    schema_version
        .validate_constraint_to_completion(&fixture, "SqlTestUser", "sql_test_user_unique_rank_idx")
        .expect("unique field-path validation should promote the candidate index");

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after unique DDL publication"),
    );
    assert!(
        indexes.iter().any(|index| index
            == "UNIQUE INDEX sql_test_user_unique_rank_idx (rank) [state=ready] [origin=ddl]"),
        "SHOW INDEXES FROM should expose the DDL-published unique index: {indexes:?}",
    );

    let ddl = schema_version
        .publish(
            &fixture,
            "DROP INDEX sql_test_user_unique_rank_idx ON SqlTestUser",
        )
        .expect("supported DROP INDEX should remove a DDL-published unique field-path index");
    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        field_path,
        status,
        ..
    } = ddl
    else {
        panic!("supported DROP INDEX should return a DDL payload");
    };
    assert_eq!(mutation_kind, "drop_secondary_index");
    assert_eq!(target_index, "sql_test_user_unique_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after unique DROP INDEX"),
    );
    assert!(
        indexes
            .iter()
            .all(|index| !index.contains("sql_test_user_unique_rank_idx")),
        "SHOW INDEXES FROM should hide the dropped DDL unique index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_drops_supported_ddl_field_path_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before DROP INDEX");

    let ddl = schema_version
        .publish(&fixture, "DROP INDEX sql_test_user_rank_idx ON SqlTestUser")
        .expect("supported DROP INDEX DDL should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        ..
    } = ddl
    else {
        panic!("supported DROP INDEX should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_secondary_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after DROP INDEX"),
    );
    assert!(
        indexes
            .iter()
            .all(|index| !index.contains("sql_test_user_rank_idx")),
        "SHOW INDEXES FROM should hide the dropped DDL index: {indexes:?}",
    );
}

#[test]
fn demo_rpg_ddl_endpoint_rejects_targetless_drop_index() {
    let fixture = install_demo_rpg_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX character_renown_idx ON Character (renown)",
        )
        .expect("setup CREATE INDEX should publish before targetless DROP INDEX");

    assert_ddl_rejects_with_entity_index_visibility_unchanged(
        &fixture,
        schema_version,
        "Character",
        "DROP INDEX character_renown_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_ambiguous_drop_index_shorthand() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before ambiguous DROP INDEX");

    assert_ddl_rejects_with_index_visibility_unchanged(
        &fixture,
        schema_version,
        "DROP INDEX sql_test_user_rank_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_generated_index_drop_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    let err = assert_ddl_rejects_with_index_visibility_unchanged(
        &fixture,
        schema_version,
        "DROP INDEX idx_sql_test_user__name ON SqlTestUser",
    );
    assert_eq!(
        err.code(),
        ErrorCode::SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED,
        "generated index drop should preserve the compact DDL leaf code",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_create_index_if_not_exists_for_absent_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let ddl = schema_version
        .publish(
            &fixture,
            "CREATE INDEX IF NOT EXISTS sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("absent CREATE INDEX IF NOT EXISTS should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("absent CREATE INDEX IF NOT EXISTS should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 3);
    assert_eq!(index_keys_written, 3);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after idempotent CREATE INDEX"),
    );
    assert!(
        indexes
            .iter()
            .any(|index| index == "INDEX sql_test_user_rank_idx (rank) [state=ready] [origin=ddl]"),
        "CREATE INDEX IF NOT EXISTS should expose the published accepted index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_noops_create_index_if_not_exists_for_existing_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before idempotent CREATE INDEX");

    let ddl = schema_version
        .no_op(
            &fixture,
            "CREATE INDEX IF NOT EXISTS sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("matching CREATE INDEX IF NOT EXISTS should no-op through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("matching CREATE INDEX IF NOT EXISTS should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "add_field_path_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "no_op");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after no-op CREATE INDEX"),
    );
    let rank_index = "INDEX sql_test_user_rank_idx (rank) [state=ready] [origin=ddl]";
    let occurrences = indexes
        .iter()
        .filter(|index| index.as_str() == rank_index)
        .count();
    assert_eq!(
        occurrences, 1,
        "no-op CREATE INDEX IF NOT EXISTS should not duplicate accepted indexes: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_conflicting_create_index_if_not_exists() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before conflicting idempotent CREATE INDEX");

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        schema_version,
        "CREATE INDEX IF NOT EXISTS sql_test_user_rank_idx ON SqlTestUser (age)",
        "INDEX sql_test_user_rank_idx (age)",
    );
}

#[test]
fn sql_canister_ddl_endpoint_drops_existing_index_with_if_exists() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before idempotent DROP INDEX");

    let ddl = schema_version
        .publish(
            &fixture,
            "DROP INDEX IF EXISTS sql_test_user_rank_idx ON SqlTestUser",
        )
        .expect("existing DROP INDEX IF EXISTS should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("existing DROP INDEX IF EXISTS should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_secondary_index");
    assert_eq!(target_index, "sql_test_user_rank_idx");
    assert_eq!(field_path, vec!["rank".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after idempotent DROP INDEX"),
    );
    assert!(
        indexes
            .iter()
            .all(|index| !index.contains("sql_test_user_rank_idx")),
        "DROP INDEX IF EXISTS should hide the dropped DDL index: {indexes:?}",
    );
}

#[test]
fn sql_canister_ddl_endpoint_noops_drop_index_if_exists_for_missing_index() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    let before = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes before no-op DROP INDEX"),
    );
    let ddl = schema_version
        .no_op(
            &fixture,
            "DROP INDEX IF EXISTS sql_test_user_missing_idx ON SqlTestUser",
        )
        .expect("missing DROP INDEX IF EXISTS should no-op through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = ddl
    else {
        panic!("missing DROP INDEX IF EXISTS should return a DDL payload");
    };

    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_secondary_index");
    assert_eq!(target_index, "sql_test_user_missing_idx");
    assert!(field_path.is_empty());
    assert_eq!(status, "no_op");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let after = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES FROM should read accepted indexes after no-op DROP INDEX"),
    );
    assert_eq!(
        after, before,
        "no-op DROP INDEX IF EXISTS should leave accepted index visibility unchanged",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_generated_index_drop_with_if_exists() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    let err = assert_ddl_rejects_with_index_visibility_unchanged(
        &fixture,
        schema_version,
        "DROP INDEX IF EXISTS idx_sql_test_user__name ON SqlTestUser",
    );
    assert_eq!(
        err.code(),
        ErrorCode::SCHEMA_DDL_GENERATED_INDEX_DROP_REJECTED,
        "generated index DROP IF EXISTS should preserve the compact DDL leaf code",
    );
}

#[test]
fn sql_canister_ddl_publication_updates_describe_explain_and_reads() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let before_describe = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema before DDL"),
    );
    assert!(
        before_describe
            .indexes()
            .iter()
            .all(|index| index.name() != "sql_test_user_rank_idx"),
        "pre-DDL DESCRIBE must not expose the future DDL index",
    );

    let before_explain = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION \
             SELECT name FROM SqlTestUser \
             WHERE rank >= 25 \
             ORDER BY rank ASC \
             LIMIT 2",
        )
        .expect("EXPLAIN should succeed before DDL"),
    );
    assert!(
        !before_explain.contains("sql_test_user_rank_idx"),
        "pre-DDL EXPLAIN must not select the future DDL index: {before_explain}",
    );

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("supported CREATE INDEX DDL should publish before post-DDL visibility checks");

    let after_describe = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema after DDL"),
    );
    assert!(
        after_describe.indexes().iter().any(|index| {
            index.name() == "sql_test_user_rank_idx"
                && index.fields().iter().map(String::as_str).eq(["rank"])
                && !index.unique()
                && index.origin() == "ddl"
        }),
        "post-DDL DESCRIBE should expose the published accepted index: {after_describe:?}",
    );

    let after_explain = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION \
             SELECT name FROM SqlTestUser \
             WHERE rank >= 25 \
             ORDER BY rank ASC \
             LIMIT 2",
        )
        .expect("EXPLAIN should succeed after DDL"),
    );
    assert!(
        after_explain.contains("IndexRange(sql_test_user_rank_idx)"),
        "post-DDL EXPLAIN should select the DDL-published accepted index: {after_explain}",
    );

    let rows = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE rank >= 25 ORDER BY rank ASC LIMIT 2",
        )
        .expect("indexed read should succeed after DDL"),
    );
    assert_projection_rendered(
        &rows,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["alice"]],
        2,
        "post-DDL indexed read should observe the accepted-after index without changing row semantics",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_unknown_field_path_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        schema_version,
        "CREATE INDEX sql_test_user_missing_idx ON SqlTestUser (missing)",
        "sql_test_user_missing_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_duplicate_index_name_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("setup CREATE INDEX should publish before duplicate-name rejection");

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        schema_version,
        "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (age)",
        "INDEX sql_test_user_rank_idx (age)",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_duplicate_field_path_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    assert_ddl_rejects_without_index_visibility_change(
        &fixture,
        schema_version,
        "CREATE INDEX sql_test_user_duplicate_name_idx ON SqlTestUser (name)",
        "sql_test_user_duplicate_name_idx",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_unsupported_create_index_shapes_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let schema_version = DdlSchemaVersion::initial();

    assert_ddl_rejects_with_index_visibility_unchanged(
        &fixture,
        schema_version,
        "CREATE INDEX sql_test_user_rank_desc_idx ON SqlTestUser (rank DESC)",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_alter_column_default() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN bonus nat64")
        .expect("setup nullable ADD COLUMN should publish through the canister endpoint");
    let set_default = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN bonus SET DEFAULT 7",
        )
        .expect("ALTER COLUMN SET DEFAULT should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = set_default
    else {
        panic!("ALTER COLUMN SET DEFAULT should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "set_field_default");
    assert_eq!(target_index, "bonus");
    assert_eq!(field_path, vec!["bonus".to_string()]);
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let describe_after_set = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema after SET DEFAULT"),
    );
    assert!(
        describe_after_set.fields().iter().any(|field| {
            field.name() == "bonus"
                && field.kind() == "nat64"
                && field.insert_omission() == Some("default")
                && field.insert_default() == Some("7")
                && field.insert_default_bytes().is_some()
                && field.insert_default_hash().is_some()
                && field.origin() == "ddl"
        }),
        "DESCRIBE should expose the accepted default change: {describe_after_set:?}",
    );
    let set_default_no_op = schema_version
        .no_op(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN bonus SET DEFAULT 7",
        )
        .expect("matching ALTER COLUMN SET DEFAULT should no-op through the canister endpoint");
    assert_ddl_no_op(set_default_no_op, "set_field_default", "bonus");

    schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ADD COLUMN nickname text DEFAULT 'anonymous'",
        )
        .expect("setup nullable defaulted ADD COLUMN should publish through the canister endpoint");
    let drop_default = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN nickname DROP DEFAULT",
        )
        .expect("ALTER COLUMN DROP DEFAULT should publish for nullable accepted fields");
    let SqlQueryResult::Ddl {
        entity,
        mutation_kind,
        target_index,
        field_path,
        status,
        ..
    } = drop_default
    else {
        panic!("ALTER COLUMN DROP DEFAULT should return a DDL payload");
    };
    assert_eq!(entity, "SqlTestUser");
    assert_eq!(mutation_kind, "drop_field_default");
    assert_eq!(target_index, "nickname");
    assert_eq!(field_path, vec!["nickname".to_string()]);
    assert_eq!(status, "published");

    let describe_after_drop = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema after DROP DEFAULT"),
    );
    assert!(
        describe_after_drop.fields().iter().any(|field| {
            field.name() == "nickname"
                && field.kind() == "text(unbounded)"
                && field.nullable()
                && field.origin() == "ddl"
        }),
        "DESCRIBE should expose the accepted default removal: {describe_after_drop:?}",
    );
}

#[test]
fn sql_canister_compact_default_fallback_uses_accepted_payload_facts() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();
    let literal = "accepted-default-".repeat(12);

    schema_version
        .publish(
            &fixture,
            format!("ALTER TABLE SqlTestUser ADD COLUMN profile text DEFAULT '{literal}'").as_str(),
        )
        .expect("long accepted text default should publish through the canister endpoint");

    let verbose = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("verbose DESCRIBE should expose accepted default payload facts"),
    );
    let profile = verbose
        .fields()
        .iter()
        .find(|field| field.name() == "profile")
        .expect("verbose accepted fields should include profile");
    let bytes = profile
        .insert_default_bytes()
        .expect("accepted default should expose its encoded byte count");
    let hash = profile
        .insert_default_hash()
        .expect("accepted default should expose its canonical payload hash");
    let expected = format!("text(bytes={bytes}, sha256={hash})");

    let SqlQueryResult::Describe(SqlDescribeOutput::Compact { columns, .. }) =
        query_sql(&fixture, "DESCRIBE SqlTestUser")
            .expect("compact DESCRIBE should render the accepted default")
    else {
        panic!("compact DESCRIBE should retain its typed compact envelope");
    };
    let profile = columns
        .iter()
        .find(|column| column.name() == "profile")
        .expect("compact accepted columns should include profile");
    assert_eq!(
        profile.default(),
        &SqlColumnDefault::Literal {
            text: expected.clone(),
        },
    );
    assert!(expected.len() <= 128);

    let SqlQueryResult::ShowColumns(SqlShowColumnsOutput::Compact {
        columns: repeated, ..
    }) = query_sql(&fixture, "SHOW COLUMNS SqlTestUser")
        .expect("SHOW COLUMNS should reuse the accepted compact projector")
    else {
        panic!("SHOW COLUMNS should retain its typed compact envelope");
    };
    assert_eq!(columns, repeated);
}

#[test]
fn sql_canister_ddl_endpoint_publishes_alter_column_nullability() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ADD COLUMN nickname text DEFAULT 'anonymous'",
        )
        .expect("setup nullable defaulted ADD COLUMN should publish through the canister endpoint");
    let set_not_null = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN nickname SET NOT NULL",
        )
        .expect("ALTER COLUMN SET NOT NULL should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = set_not_null
    else {
        panic!("ALTER COLUMN SET NOT NULL should return a DDL payload");
    };
    assert_eq!(mutation_kind, "set_field_not_null");
    assert_eq!(target_index, "nickname");
    assert_eq!(status, "activation_published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);
    let constraint_name = active_constraint_name(&fixture, "SqlTestUser", "not_null", "nickname");
    schema_version
        .validate_constraint_to_completion(&fixture, "SqlTestUser", constraint_name.as_str())
        .expect("bounded SET NOT NULL validation should promote the accepted constraint");

    let describe_after_set = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema after SET NOT NULL"),
    );
    assert!(
        describe_after_set.fields().iter().any(|field| {
            field.name() == "nickname"
                && !field.nullable()
                && field.kind() == "text(unbounded)"
                && field.insert_omission() == Some("default")
                && field.insert_default() == Some("'anonymous'")
                && field.insert_default_hash().is_some()
                && field.origin() == "ddl"
        }),
        "DESCRIBE should expose the accepted nullability change: {describe_after_set:?}",
    );

    let drop_not_null = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN nickname DROP NOT NULL",
        )
        .expect("ALTER COLUMN DROP NOT NULL should publish through the canister endpoint");
    let SqlQueryResult::Ddl {
        mutation_kind,
        target_index,
        status,
        rows_scanned,
        index_keys_written,
        ..
    } = drop_not_null
    else {
        panic!("ALTER COLUMN DROP NOT NULL should return a DDL payload");
    };
    assert_eq!(mutation_kind, "drop_field_not_null");
    assert_eq!(target_index, "nickname");
    assert_eq!(status, "published");
    assert_eq!(rows_scanned, 0);
    assert_eq!(index_keys_written, 0);

    let drop_not_null_no_op = schema_version
        .no_op(
            &fixture,
            "ALTER TABLE SqlTestUser ALTER COLUMN nickname DROP NOT NULL",
        )
        .expect("matching ALTER COLUMN DROP NOT NULL should no-op through the canister endpoint");
    assert_ddl_no_op(drop_not_null_no_op, "drop_field_not_null", "nickname");
}

#[test]
fn sql_canister_ddl_endpoint_rejects_unsupported_alter_column_without_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser ADD COLUMN required_score nat64 NOT NULL DEFAULT 7",
        )
        .expect("setup required ADD COLUMN DEFAULT should publish before unsupported DROP DEFAULT");
    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN bonus nat64")
        .expect("setup nullable ADD COLUMN should publish before invalid SET DEFAULT");

    for (sql, expected_code) in [
        (
            "ALTER TABLE SqlTestUser ALTER COLUMN rank SET DEFAULT 7",
            ErrorCode::SCHEMA_DDL_GENERATED_FIELD_DEFAULT_CHANGE_REJECTED,
        ),
        (
            "ALTER TABLE SqlTestUser ALTER COLUMN bonus SET DEFAULT 'seven'",
            ErrorCode::SCHEMA_DDL_INVALID_ALTER_COLUMN_DEFAULT,
        ),
        (
            "ALTER TABLE SqlTestUser ALTER COLUMN rank DROP NOT NULL",
            ErrorCode::SCHEMA_DDL_GENERATED_FIELD_NULLABILITY_CHANGE_REJECTED,
        ),
    ] {
        let before = expect_describe(
            query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
                .expect("DESCRIBE should read accepted schema before rejected ALTER COLUMN"),
        );
        let err = schema_version
            .reject(&fixture, sql)
            .expect_err("ALTER COLUMN should reject before publication");
        assert_ddl_rejection_error(
            &err,
            "ALTER COLUMN should stay at the schema DDL admission boundary",
        );
        assert_eq!(
            err.code(),
            expected_code,
            "{sql} should preserve the compact DDL admission leaf code",
        );
        let after = expect_describe(
            query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
                .expect("DESCRIBE should read accepted schema after rejected ALTER COLUMN"),
        );
        assert_eq!(
            after, before,
            "rejected ALTER COLUMN must leave accepted schema visibility unchanged",
        );
    }
}

#[test]
fn sql_canister_ddl_endpoint_rejects_nonempty_drop_column_before_publication() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let missing = schema_version
        .no_op(
            &fixture,
            "ALTER TABLE SqlTestUser DROP COLUMN IF EXISTS missing",
        )
        .expect("DROP COLUMN IF EXISTS should no-op for missing accepted fields");
    assert_ddl_no_op(missing, "drop_field", "missing");

    let generated_err = schema_version
        .reject(&fixture, "ALTER TABLE SqlTestUser DROP COLUMN rank")
        .expect_err("DROP COLUMN should reject generated accepted fields");
    assert_ddl_rejection_error(
        &generated_err,
        "generated DROP COLUMN rejection should stay at the schema DDL admission boundary",
    );

    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN nickname text")
        .expect("setup nullable ADD COLUMN should publish through the canister endpoint");
    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN handle text")
        .expect("setup second nullable ADD COLUMN should publish through the canister endpoint");
    let before = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema before DROP COLUMN"),
    );
    let error = schema_version
        .reject(&fixture, "ALTER TABLE SqlTestUser DROP COLUMN nickname")
        .expect_err("nonempty DROP COLUMN should require the future migration protocol");
    assert_ddl_rejection_error(
        &error,
        "nonempty DROP COLUMN rejection should stay at the schema DDL admission boundary",
    );
    assert_eq!(
        error.code(),
        ErrorCode::SCHEMA_DDL_REWRITE_REQUIRES_MIGRATION,
        "nonempty DROP COLUMN should preserve the migration-required leaf code",
    );
    let after = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema after DROP COLUMN"),
    );
    assert!(
        before
            .fields()
            .iter()
            .any(|field| field.name() == "nickname"),
        "setup should expose DDL-owned field before DROP COLUMN",
    );
    assert!(
        before.fields().iter().any(|field| field.name() == "handle"),
        "setup should expose later DDL-owned field before DROP COLUMN",
    );
    assert!(
        after
            .fields()
            .iter()
            .any(|field| field.name() == "nickname"),
        "rejected DROP COLUMN must preserve the accepted field",
    );
    assert!(
        after.fields().iter().any(|field| field.name() == "handle"),
        "rejected DROP COLUMN must preserve later active fields",
    );
    assert_eq!(
        after, before,
        "rejected DROP COLUMN must leave accepted schema visibility unchanged",
    );
}

#[test]
fn sql_canister_ddl_endpoint_publishes_rename_column_for_ddl_owned_field() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    let same_name = schema_version
        .no_op(
            &fixture,
            "ALTER TABLE SqlTestUser RENAME COLUMN rank TO rank",
        )
        .expect("same-name RENAME COLUMN should no-op through the canister endpoint");
    assert_ddl_no_op(same_name, "rename_field", "rank");

    let generated_err = schema_version
        .reject(
            &fixture,
            "ALTER TABLE SqlTestUser RENAME COLUMN rank TO score",
        )
        .expect_err("RENAME COLUMN should reject generated accepted fields");
    assert_ddl_rejection_error(
        &generated_err,
        "generated RENAME COLUMN rejection should stay at the schema DDL admission boundary",
    );

    schema_version
        .publish(&fixture, "ALTER TABLE SqlTestUser ADD COLUMN nickname text")
        .expect("setup nullable ADD COLUMN should publish through the canister endpoint");
    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_nickname_idx ON SqlTestUser (nickname)",
        )
        .expect("setup field-path CREATE INDEX should publish through the canister endpoint");
    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_lower_nickname_idx ON SqlTestUser (LOWER(nickname))",
        )
        .expect("setup expression CREATE INDEX should publish through the canister endpoint");
    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_filtered_nickname_idx ON SqlTestUser (nickname) WHERE nickname IS NOT NULL",
        )
        .expect("setup filtered CREATE INDEX should publish through the canister endpoint");
    let before = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema before RENAME COLUMN"),
    );
    let rename = schema_version
        .publish(
            &fixture,
            "ALTER TABLE SqlTestUser RENAME COLUMN nickname TO handle",
        )
        .expect("RENAME COLUMN should publish DDL-owned accepted field metadata");
    assert_rename_column_ddl_report(rename);

    let after = expect_describe(
        query_sql(&fixture, "DESCRIBE SqlTestUser VERBOSE")
            .expect("DESCRIBE should read accepted schema after RENAME COLUMN"),
    );
    assert_rename_column_schema_visibility(&before, &after);

    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("SHOW INDEXES should read accepted index metadata after RENAME COLUMN"),
    );
    assert_rename_column_index_visibility(&indexes);
}

#[test]
fn sql_canister_integrity_endpoint_executes_controller_gated_quick_check() {
    let fixture = install_sql_canister_fixture();

    let result = expect_integrity_sql(&fixture, "CHECK INTEGRITY SqlTestUser QUICK");
    let IntegrityCheckResult::Quick(result) = result else {
        panic!("Quick integrity SQL should return the canonical Quick payload");
    };

    assert_eq!(result.status(), &QuickIntegrityStatus::CompleteClean);
    assert_eq!(
        result.entity().entity_path(),
        "SqlTestUser",
        "integrity results should expose the accepted current entity name"
    );

    let outsider = Principal::self_authenticating([7_u8; 32]);
    let rejected: Result<IntegrityCheckResult, SqlIntegrityError> = fixture
        .update_candid_as(
            outsider,
            "icydb_integrity",
            ("CHECK INTEGRITY SqlTestUser QUICK".to_string(),),
        )
        .expect("non-controller integrity call should decode");
    assert_eq!(
        rejected,
        Err(SqlIntegrityError::Sql(Error::from_runtime_boundary(
            RuntimeBoundaryCode::SqlSurfaceControllerRequired,
            ErrorOrigin::Interface,
        ))),
    );
}

#[test]
fn source_declared_controller_endpoints_authorize_before_private_handlers() {
    let fixture = install_sql_canister_fixture();
    let outsider = Principal::self_authenticating([8_u8; 32]);
    let sql_error = Error::from_runtime_boundary(
        RuntimeBoundaryCode::SqlSurfaceControllerRequired,
        ErrorOrigin::Interface,
    );
    let operational_error = Error::from_runtime_boundary(
        RuntimeBoundaryCode::OperationalSurfaceControllerRequired,
        ErrorOrigin::Interface,
    );
    let schema_error = Error::from_runtime_boundary(
        RuntimeBoundaryCode::SchemaSurfaceControllerRequired,
        ErrorOrigin::Interface,
    );

    let query: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid_as(outsider, "icydb_query", ("not valid SQL".to_string(),))
        .expect("non-controller SQL query response should decode");
    assert_eq!(query, Err(sql_error.clone()));

    for method in ["icydb_ddl", "icydb_update"] {
        let result: Result<SqlQueryResult, Error> = fixture
            .update_candid_as(outsider, method, ("not valid SQL".to_string(),))
            .expect("non-controller SQL update response should decode");
        assert_eq!(result, Err(sql_error.clone()), "{method}");
    }
    for method in ["icydb_fixtures_reset", "icydb_fixtures_load"] {
        let result: Result<(), Error> = fixture
            .update_candid_as(outsider, method, ())
            .expect("non-controller fixture response should decode");
        assert_eq!(result, Err(sql_error.clone()), "{method}");
    }

    let metrics: Result<CompactMetricsReport, Error> = fixture
        .query_candid_as(outsider, "icydb_metrics", (None::<u64>,))
        .expect("public metrics response should decode");
    assert!(
        metrics.is_ok(),
        "public metrics must not require a controller"
    );

    let metrics_reset: Result<(), Error> = fixture
        .update_candid_as(outsider, "icydb_metrics_reset", ())
        .expect("non-controller metrics reset response should decode");
    assert_eq!(metrics_reset, Err(operational_error.clone()));
    let snapshot: Result<StorageReport, Error> = fixture
        .query_candid_as(outsider, "icydb_snapshot", ())
        .expect("non-controller snapshot response should decode");
    assert_eq!(
        snapshot.expect_err("a non-controller snapshot must fail before its handler"),
        operational_error,
    );
    let schema: Result<Vec<EntitySchemaDescription>, Error> = fixture
        .query_candid_as(outsider, "icydb_schema", ())
        .expect("non-controller schema response should decode");
    assert_eq!(schema, Err(schema_error));
}

#[test]
fn sql_canister_ddl_owned_schema_and_rows_survive_upgrade_reconciliation() {
    // Use an unpooled fixture because this test deliberately replaces the
    // installed Wasm and observes the real post-upgrade readiness boundary.
    let fixture = install_fixture_canister("sql");
    reset_sql_fixtures(&fixture);
    let mut schema_version = DdlSchemaVersion::initial();

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("accepted DDL index should publish before upgrade");

    upgrade_fixture_canister(&fixture, "sql");
    let pending = query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
        .expect_err("ordinary query must not drive post-upgrade recovery");
    assert_eq!(
        pending.code(),
        ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );

    deliver_fixture_startup_watchdog(&fixture);
    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("watchdog recovery should restore accepted DDL authority"),
    );
    assert!(
        indexes
            .iter()
            .any(|index| index == "INDEX sql_test_user_rank_idx (rank) [state=ready] [origin=ddl]"),
        "post-upgrade accepted indexes should retain the DDL-owned index: {indexes:?}",
    );

    let count = expect_projection(
        query_sql(&fixture, "SELECT COUNT(*) FROM SqlTestUser")
            .expect("post-upgrade count should observe durable rows"),
    );
    assert_projection_rendered(
        &count,
        "SqlTestUser",
        &["COUNT(*)"],
        &[&["3"]],
        1,
        "post-upgrade recovery must retain all durable rows",
    );

    let rows = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE rank >= 25 ORDER BY rank ASC LIMIT 2",
        )
        .expect("post-upgrade indexed read should retain original rows"),
    );
    assert_projection_rendered(
        &rows,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["alice"]],
        2,
        "post-upgrade reconciliation must preserve DDL authority and row semantics",
    );

    schema_version
        .publish(&fixture, "DROP INDEX sql_test_user_rank_idx ON SqlTestUser")
        .expect("accepted DDL index deletion should publish before upgrade");
    upgrade_fixture_canister(&fixture, "sql");
    let pending = query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
        .expect_err("ordinary query must remain gated during deletion recovery");
    assert_eq!(
        pending.code(),
        ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );
    deliver_fixture_startup_watchdog(&fixture);
    let indexes = expect_show_indexes(
        query_sql(&fixture, "SHOW INDEXES FROM SqlTestUser")
            .expect("watchdog recovery should restore the index deletion"),
    );
    assert!(
        indexes
            .iter()
            .all(|index| !index.contains("sql_test_user_rank_idx")),
        "post-upgrade accepted indexes must retain the DDL-owned deletion: {indexes:?}",
    );

    schema_version
        .publish(
            &fixture,
            "CREATE INDEX sql_test_user_rank_idx ON SqlTestUser (rank)",
        )
        .expect("re-creation should prove the recovered physical domain has no stale keys");
    let rows = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE rank >= 25 ORDER BY rank ASC LIMIT 2",
        )
        .expect("re-created index should observe the original durable rows"),
    );
    assert_projection_rendered(
        &rows,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["alice"]],
        2,
        "recovered index deletion and re-creation must retain row semantics",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_scalar_and_grouped_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let scalar = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("scalar SQL query should succeed"),
    );
    assert_projection_rendered(
        &scalar,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["alice"]],
        2,
        "query(sql) should preserve ordered scalar projection payloads",
    );

    let grouped = expect_grouped(
        query_sql(
            &fixture,
            "SELECT age, COUNT(*) FROM SqlTestUser GROUP BY age ORDER BY age ASC LIMIT 10",
        )
        .expect("grouped SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["age".to_string(), "COUNT(*)".to_string()],
            rows: vec![
                vec!["24".to_string(), "1".to_string()],
                vec!["31".to_string(), "1".to_string()],
                vec!["43".to_string(), "1".to_string()],
            ],
            row_count: 3,
            next_cursor: None,
        },
        "query(sql) should preserve grouped result payloads too",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_global_post_aggregate_value_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let post_aggregate = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age), 2) AS avg_rounded, COUNT(*) + 1 AS count_plus_one, MAX(age) - MIN(age) AS spread \
             FROM SqlTestUser",
        )
        .expect("global post-aggregate SQL query should succeed"),
    );

    assert_projection_rendered(
        &post_aggregate,
        "SqlTestUser",
        &["avg_rounded", "count_plus_one", "spread"],
        &[&["32.67", "4", "19"]],
        1,
        "query(sql) should preserve the real reduced values for global post-aggregate projection expressions at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_global_aggregate_having_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let matched = expect_projection(
        query_sql(
            &fixture,
            "SELECT COUNT(*) FROM SqlTestUser HAVING COUNT(*) > 1",
        )
        .expect("global aggregate HAVING SQL query should succeed"),
    );
    assert_projection_rendered(
        &matched,
        "SqlTestUser",
        &["COUNT(*)"],
        &[&["3"]],
        1,
        "query(sql) should keep the implicit aggregate row when global HAVING matches",
    );

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age), 2) AS avg_rounded FROM SqlTestUser HAVING AVG(age) > 40",
        )
        .expect("global aggregate HAVING should still return projection payload when filtered"),
    );
    assert_projection_rendered(
        &filtered,
        "SqlTestUser",
        &["avg_rounded"],
        &[],
        0,
        "query(sql) should filter away the implicit aggregate row while preserving the projection shape when global HAVING fails",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_grouped_aggregate_combo_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let grouped = expect_grouped(
        query_sql(
            &fixture,
            "SELECT age, AVG(age + 1) AS avg_plus_one \
             FROM SqlTestUser \
             GROUP BY age \
             HAVING AVG(age + 1) > 25 \
             ORDER BY avg_plus_one DESC, age ASC \
             LIMIT 2",
        )
        .expect("grouped aggregate combination SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["age".to_string(), "avg_plus_one".to_string()],
            rows: vec![
                vec!["43".to_string(), "44".to_string()],
                vec!["31".to_string(), "32".to_string()],
            ],
            row_count: 2,
            next_cursor: None,
        },
        "query(sql) should preserve grouped aggregate-input, HAVING, and Top-K ordering values together at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_grouped_wrapped_aggregate_input_order_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let grouped = expect_grouped(
        query_sql(
            &fixture,
            "SELECT name, ROUND(AVG(age + 1 * 2), 2) AS avg_boosted \
             FROM SqlTestUser \
             GROUP BY name \
             ORDER BY avg_boosted DESC, name ASC \
             LIMIT 2",
        )
        .expect("grouped wrapped aggregate-input ORDER BY alias SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "avg_boosted".to_string()],
            rows: vec![
                vec!["charlie".to_string(), "45.00".to_string()],
                vec!["alice".to_string(), "33.00".to_string()],
            ],
            row_count: 2,
            next_cursor: None,
        },
        "query(sql) should preserve wrapped grouped aggregate-input ordering values at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_grouped_parenthesized_aggregate_input_order_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let grouped = expect_grouped(
        query_sql(
            &fixture,
            "SELECT name, ROUND(AVG((age + age) / 2), 2) AS avg_balanced \
             FROM SqlTestUser \
             GROUP BY name \
             ORDER BY avg_balanced DESC, name ASC \
             LIMIT 2",
        )
        .expect("grouped parenthesized aggregate-input ORDER BY alias SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestUser".to_string(),
            columns: vec!["name".to_string(), "avg_balanced".to_string()],
            rows: vec![
                vec!["charlie".to_string(), "43.00".to_string()],
                vec!["alice".to_string(), "31.00".to_string()],
            ],
            row_count: 2,
            next_cursor: None,
        },
        "query(sql) should preserve parenthesized grouped aggregate-input ordering values at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_keeps_canonical_equivalent_grouped_having_explain_identity() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let left = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION VERBOSE \
             SELECT age, COUNT(*) \
             FROM SqlTestUser \
             GROUP BY age \
             HAVING age >= 24 AND COUNT(*) > 0 \
             ORDER BY age ASC \
             LIMIT 10",
        )
        .expect("left grouped HAVING explain query should succeed"),
    );
    let right = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION VERBOSE \
             SELECT age, COUNT(*) \
             FROM SqlTestUser \
             GROUP BY age \
             HAVING COUNT(*) > 0 AND age >= 24 \
             ORDER BY age ASC \
             LIMIT 10",
        )
        .expect("right grouped HAVING explain query should succeed"),
    );

    assert_eq!(
        left, right,
        "public SQL explain should keep canonical-equivalent grouped HAVING order on the same outward identity surface",
    );
}

#[test]
fn sql_canister_query_endpoint_surfaces_semantic_reuse_diagnostics_on_verbose_explain() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let first = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION VERBOSE \
             SELECT name \
             FROM SqlTestUser \
             WHERE age >= 24 AND age < 50 \
             ORDER BY age ASC \
             LIMIT 2",
        )
        .expect("first verbose explain query should succeed"),
    );
    let second = expect_explain(
        query_sql(
            &fixture,
            "EXPLAIN EXECUTION VERBOSE \
             SELECT name \
             FROM SqlTestUser \
             WHERE age < 50 AND age >= 24 \
             ORDER BY age ASC \
             LIMIT 2",
        )
        .expect("second verbose explain query should succeed"),
    );

    assert!(
        first.contains("diag.s.semantic_reuse_artifact=shared_prepared_query_plan")
            && first.contains("diag.s.semantic_reuse=miss"),
        "first public SQL verbose explain should report one shared query-plan miss: {first}",
    );
    assert!(
        second.contains("diag.s.semantic_reuse_artifact=shared_prepared_query_plan")
            && second.contains("diag.s.semantic_reuse=miss"),
        "public SQL query entrypoints should surface one honest shared query-plan miss on each isolated query call: {second}",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_scalar_arithmetic_and_round_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let arithmetic = expect_projection(
        query_sql(
            &fixture,
            "SELECT age - 1 FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("scalar arithmetic SQL query should succeed"),
    );
    assert_projection_rendered(
        &arithmetic,
        "SqlTestUser",
        &["age - 1"],
        &[&["23"], &["30"]],
        2,
        "query(sql) should preserve scalar arithmetic projection payloads at the live canister boundary",
    );

    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(age / 3, 2) FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("scalar ROUND SQL query should succeed"),
    );
    assert_projection_rendered(
        &rounded,
        "SqlTestUser",
        &["ROUND(age / 3, 2)"],
        &[&["8.00"], &["10.33"]],
        2,
        "query(sql) should preserve scalar ROUND projection payloads at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_chained_scalar_arithmetic_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let precedence = expect_projection(
        query_sql(
            &fixture,
            "SELECT age + 1 * 2 AS value FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("chained scalar precedence SQL query should succeed"),
    );
    assert_projection_rendered(
        &precedence,
        "SqlTestUser",
        &["value"],
        &[&["26"], &["33"]],
        2,
        "query(sql) should preserve multiplication precedence inside chained scalar arithmetic at the live canister boundary",
    );

    let associativity = expect_projection(
        query_sql(
            &fixture,
            "SELECT age - 1 - 2 AS value FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("chained scalar associativity SQL query should succeed"),
    );
    assert_projection_rendered(
        &associativity,
        "SqlTestUser",
        &["value"],
        &[&["21"], &["28"]],
        2,
        "query(sql) should preserve left-associative subtraction inside chained scalar arithmetic at the live canister boundary",
    );

    let parenthesized = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND((age + rank) / 2, 2) AS value FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("parenthesized scalar ROUND SQL query should succeed"),
    );
    assert_projection_rendered(
        &parenthesized,
        "SqlTestUser",
        &["value"],
        &[&["24.50"], &["29.50"]],
        2,
        "query(sql) should preserve parenthesized scalar arithmetic before ROUND at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_chained_global_aggregate_expression_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let result = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age + 1 * 2), 2) AS avg_shifted, ROUND(AVG((age + age) / 2), 2) AS avg_balanced FROM SqlTestUser",
        )
        .expect("chained global aggregate expression SQL query should succeed"),
    );
    assert_projection_rendered(
        &result,
        "SqlTestUser",
        &["avg_shifted", "avg_balanced"],
        &[&["34.67", "32.67"]],
        1,
        "query(sql) should preserve chained aggregate-input and parenthesized global post-aggregate values at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_round_field_to_field_arithmetic_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(age + rank, 2) AS total FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("ROUND(field + field) SQL query should succeed"),
    );
    assert_projection_rendered(
        &rounded,
        "SqlTestUser",
        &["total"],
        &[&["49.00"], &["59.00"]],
        2,
        "query(sql) should preserve ROUND(field + field) projection payloads at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_field_to_field_arithmetic_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let arithmetic = expect_projection(
        query_sql(
            &fixture,
            "SELECT age + rank AS total FROM SqlTestUser ORDER BY age ASC LIMIT 2",
        )
        .expect("field-to-field arithmetic SQL query should succeed"),
    );
    assert_projection_rendered(
        &arithmetic,
        "SqlTestUser",
        &["total"],
        &[&["49"], &["59"]],
        2,
        "query(sql) should preserve field-to-field arithmetic projection payloads at the live canister boundary",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_executes_small_width_numeric_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let small_width = expect_projection(
        query_numeric_types(
            &fixture,
            "SELECT label, nat16_value + 1, nat8_value + nat16_value, int8_value - 1 \
             FROM SqlTestNumericTypes \
             ORDER BY label \
             LIMIT 10",
        )
        .expect("mixed small-width numeric SQL query should succeed"),
    );
    assert_projection_rendered(
        &small_width,
        "SqlTestNumericTypes",
        &[
            "label",
            "nat16_value + 1",
            "nat8_value + nat16_value",
            "int8_value - 1",
        ],
        &[&["alpha", "4", "17", "-2"], &["beta", "8", "23", "1"]],
        2,
        "query(sql) should preserve Int8/Nat8/Nat16 arithmetic at the schema/test SQL canister boundary",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_executes_wide_integer_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let wide_width = expect_projection(
        query_numeric_types(
            &fixture,
            "SELECT label, int16_value + int32_value, int64_value + nat64_value, nat32_value + nat64_value \
             FROM SqlTestNumericTypes \
             ORDER BY nat16_value DESC \
             LIMIT 10",
        )
        .expect("mixed wide numeric SQL query should succeed"),
    );
    assert_projection_rendered(
        &wide_width,
        "SqlTestNumericTypes",
        &[
            "label",
            "int16_value + int32_value",
            "int64_value + nat64_value",
            "nat32_value + nat64_value",
        ],
        &[
            &["beta", "63", "18000", "9300"],
            &["alpha", "33", "500", "1120"],
        ],
        2,
        "query(sql) should preserve Int16/Int32/Int64 and Nat32/Nat64 arithmetic at the schema/test SQL canister boundary",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_executes_decimal_float_projection_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let decimal_float = expect_projection(
        query_numeric_types(
            &fixture,
            "SELECT label, ROUND(decimal_value * 100, 2), TRUNC(decimal_value / 3, 2), float64_value / 2, ROUND(float32_value + float64_value, 2) \
             FROM SqlTestNumericTypes \
             ORDER BY decimal_value DESC \
             LIMIT 10",
        )
        .expect("decimal and float numeric SQL query should succeed"),
    );
    assert_projection_rendered(
        &decimal_float,
        "SqlTestNumericTypes",
        &[
            "label",
            "ROUND(decimal_value * 100, 2)",
            "TRUNC(decimal_value / 3, 2)",
            "float64_value / 2",
            "ROUND(float32_value + float64_value, 2)",
        ],
        &[
            &["beta", "25.00", "0.08", "0.125", "0.50"],
            &["alpha", "15.00", "0.05", "0.25", "1.25"],
        ],
        2,
        "query(sql) should preserve Decimal/Float32/Float64 arithmetic at the schema/test SQL canister boundary",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_executes_mixed_numeric_aggregate_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let global = expect_projection(
        query_numeric_types(
            &fixture,
            "SELECT COUNT(*), SUM(nat16_value), AVG(int32_value), MIN(int16_value), MAX(nat64_value) \
             FROM SqlTestNumericTypes",
        )
        .expect("global mixed numeric aggregate SQL query should succeed"),
    );
    assert_projection_rendered(
        &global,
        "SqlTestNumericTypes",
        &[
            "COUNT(*)",
            "SUM(nat16_value)",
            "AVG(int32_value)",
            "MIN(int16_value)",
            "MAX(nat64_value)",
        ],
        &[&["2", "10", "46.5", "-2", "9000"]],
        1,
        "query(sql) should preserve mixed numeric global aggregates at the schema/test SQL canister boundary",
    );

    let grouped = expect_grouped(
        query_numeric_types(
            &fixture,
            "SELECT group_name, SUM(nat32_value), AVG(decimal_value), MAX(float64_value) \
             FROM SqlTestNumericTypes \
             GROUP BY group_name \
             ORDER BY group_name \
             LIMIT 50",
        )
        .expect("grouped mixed numeric aggregate SQL query should succeed"),
    );
    assert_eq!(
        grouped,
        SqlGroupedRowsOutput {
            entity: "SqlTestNumericTypes".to_string(),
            columns: vec![
                "group_name".to_string(),
                "SUM(nat32_value)".to_string(),
                "AVG(decimal_value)".to_string(),
                "MAX(float64_value)".to_string(),
            ],
            rows: vec![
                vec![
                    "fighter".to_string(),
                    "300".to_string(),
                    "0.25".to_string(),
                    "0.25".to_string(),
                ],
                vec![
                    "mage".to_string(),
                    "120".to_string(),
                    "0.15".to_string(),
                    "0.5".to_string(),
                ],
            ],
            row_count: 2,
            next_cursor: None,
        },
        "query(sql) should preserve mixed numeric grouped aggregates at the schema/test SQL canister boundary",
    );
}

#[test]
fn generated_sql_query_endpoint_rejects_an_undeliverable_complete_envelope() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);
    seed_oversized_sql_group_name(&fixture);

    let error = query_sql(
        &fixture,
        "SELECT group_name, COUNT(*) \
         FROM SqlTestNumericTypes \
         GROUP BY group_name \
         ORDER BY group_name \
         LIMIT 50",
    )
    .expect_err("an oversized successful SQL envelope must return a typed response error");
    assert_eq!(
        error.code(),
        ErrorCode::RUNTIME_BOUNDARY_SQL_QUERY_REPLY_BYTES_EXCEEDED,
    );
    assert_eq!(error.origin(), ErrorOrigin::Response);

    let count = expect_projection(
        query_sql(&fixture, "SELECT COUNT(*) FROM SqlTestNumericTypes")
            .expect("the typed oversize rejection must leave later endpoint replies usable"),
    );
    assert_projection_rendered(
        &count,
        "SqlTestNumericTypes",
        &["COUNT(*)"],
        &[&["3"]],
        1,
        "oversized reply rejection should not trap or alter stored rows",
    );
}

#[test]
fn sql_canister_numeric_type_endpoint_reports_numeric_overflow_errors() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    for sql in [
        "SELECT label, POWER(nat16_value + nat8_value, 100) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, POWER(nat64_value + 1, 20) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, POWER(decimal_value + 100, 80) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, POWER(int16_value - 1000, 99) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT SUM(POWER(nat16_value, 100)) \
         FROM SqlTestNumericTypes",
        "SELECT group_name, AVG(POWER(nat32_value, 50)) \
         FROM SqlTestNumericTypes \
         GROUP BY group_name \
         ORDER BY group_name \
         LIMIT 50",
    ] {
        let err = query_numeric_types(&fixture, sql)
            .expect_err("overflowing mixed numeric SQL should fail");

        assert_numeric_query_error(err, ErrorCode::QUERY_NUMERIC_OVERFLOW, sql);
    }
}

#[test]
fn sql_canister_numeric_type_endpoint_reports_numeric_not_representable_errors() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    for sql in [
        "SELECT label, nat16_value / 0 \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, MOD(nat64_value, 0) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
        "SELECT label, SQRT(int16_value - 1000) \
         FROM SqlTestNumericTypes \
         ORDER BY label \
         LIMIT 1",
    ] {
        let err = query_numeric_types(&fixture, sql)
            .expect_err("non-representable mixed numeric SQL should fail");

        assert_numeric_query_error(err, ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE, sql);
    }
}

#[test]
fn sql_canister_query_endpoint_executes_singleton_global_output_order_alias_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let ordered = expect_projection(
        query_sql(
            &fixture,
            "SELECT ROUND(AVG(age), 2) AS avg_rounded FROM SqlTestUser ORDER BY avg_rounded DESC",
        )
        .expect("singleton global aggregate output ORDER BY alias SQL query should succeed"),
    );
    assert_projection_rendered(
        &ordered,
        "SqlTestUser",
        &["avg_rounded"],
        &[&["32.67"]],
        1,
        "query(sql) should treat singleton global aggregate output ordering as an inert no-op while still returning the correct value",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_order_by_bounded_numeric_alias_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let arithmetic = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age + 1 AS next_age FROM SqlTestUser ORDER BY next_age ASC LIMIT 2",
        )
        .expect("ORDER BY arithmetic alias SQL query should succeed"),
    );
    let field_to_field = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age + rank AS total FROM SqlTestUser ORDER BY total ASC LIMIT 2",
        )
        .expect("ORDER BY field-to-field arithmetic alias SQL query should succeed"),
    );
    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, ROUND(age / 3, 2) AS rounded_age FROM SqlTestUser ORDER BY rounded_age DESC LIMIT 2",
        )
        .expect("ORDER BY ROUND alias SQL query should succeed"),
    );

    assert_projection_rendered(
        &arithmetic,
        "SqlTestUser",
        &["name", "next_age"],
        &[&["bob", "25"], &["alice", "32"]],
        2,
        "query(sql) should preserve arithmetic alias ordering at the live canister boundary",
    );
    assert_projection_rendered(
        &field_to_field,
        "SqlTestUser",
        &["name", "total"],
        &[&["bob", "49"], &["alice", "59"]],
        2,
        "query(sql) should preserve field-to-field arithmetic alias ordering at the live canister boundary",
    );
    assert_projection_rendered(
        &rounded,
        "SqlTestUser",
        &["name", "rounded_age"],
        &[&["charlie", "14.33"], &["alice", "10.33"]],
        2,
        "query(sql) should preserve ROUND alias ordering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_direct_bounded_numeric_order_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let arithmetic = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY age + 1 ASC LIMIT 2",
        )
        .expect("direct ORDER BY arithmetic SQL query should succeed"),
    );
    let rounded = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY ROUND(age / 3, 2) DESC LIMIT 2",
        )
        .expect("direct ORDER BY ROUND SQL query should succeed"),
    );

    assert_projection_rendered(
        &arithmetic,
        "SqlTestUser",
        &["name", "age"],
        &[&["bob", "24"], &["alice", "31"]],
        2,
        "query(sql) should preserve direct arithmetic ordering at the live canister boundary",
    );
    assert_projection_rendered(
        &rounded,
        "SqlTestUser",
        &["name", "age"],
        &[&["charlie", "43"], &["alice", "31"]],
        2,
        "query(sql) should preserve direct ROUND ordering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_field_to_field_predicate_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age > rank ORDER BY age ASC LIMIT 10",
        )
        .expect("field-to-field predicate SQL query should succeed"),
    );
    assert_projection_rendered(
        &filtered,
        "SqlTestUser",
        &["name"],
        &[&["alice"]],
        1,
        "query(sql) should preserve field-to-field predicate filtering at the live canister boundary",
    );

    let mixed = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age > 18 AND age > rank ORDER BY age ASC LIMIT 10",
        )
        .expect("mixed literal and field-to-field predicate SQL query should succeed"),
    );
    assert_projection_rendered(
        &mixed,
        "SqlTestUser",
        &["name"],
        &[&["alice"]],
        1,
        "query(sql) should preserve correct residual filtering when a literal predicate and a field-to-field predicate are combined at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_not_between_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age NOT BETWEEN 25 AND 40 ORDER BY age ASC LIMIT 10",
        )
        .expect("NOT BETWEEN SQL query should succeed"),
    );
    assert_projection_rendered(
        &filtered,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["charlie"]],
        2,
        "query(sql) should preserve NOT BETWEEN filtering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_executes_not_like_prefix_queries() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let filtered = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE LOWER(name) NOT LIKE 'a%' ORDER BY age ASC LIMIT 10",
        )
        .expect("NOT LIKE SQL query should succeed"),
    );
    assert_projection_rendered(
        &filtered,
        "SqlTestUser",
        &["name"],
        &[&["bob"], &["charlie"]],
        2,
        "query(sql) should preserve bounded NOT LIKE prefix filtering at the live canister boundary",
    );
}

#[test]
fn sql_canister_query_endpoint_rejects_mutation_sql() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let err = query_sql(
        &fixture,
        "DELETE FROM SqlTestUser WHERE name = 'bob' RETURNING name",
    )
    .expect_err("query(sql) must reject mutation statements");

    assert_query_sql_surface_mismatch_error(
        &err,
        ErrorCode::SQL_SURFACE_QUERY_REJECTS_DELETE,
        "wrong-lane SQL should keep query-owned origin metadata",
    );
}

#[test]
fn sql_canister_query_endpoint_rejects_update_sql() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let err = query_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 22 WHERE name = 'alice'",
    )
    .expect_err("query(sql) must reject UPDATE statements");

    assert_query_sql_surface_mismatch_error(
        &err,
        ErrorCode::SQL_SURFACE_QUERY_REJECTS_UPDATE,
        "query endpoint UPDATE rejection should stay at the SQL surface boundary",
    );
}

#[test]
fn sql_canister_ddl_endpoint_rejects_row_mutation_sql_without_row_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    for (label, sql) in [
        (
            "INSERT",
            "INSERT INTO SqlTestUser (name, age) VALUES ('zara', 50)",
        ),
        (
            "UPDATE",
            "UPDATE SqlTestUser SET age = 22 WHERE name = 'alice'",
        ),
        ("DELETE", "DELETE FROM SqlTestUser WHERE name = 'bob'"),
    ] {
        let before = expect_projection(
            query_sql(
                &fixture,
                "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
            )
            .expect("pre-rejection read should prove the row set exists"),
        );
        let err = ddl_sql(&fixture, sql).expect_err("DDL endpoint must reject row mutations");

        assert_eq!(
            err.diagnostic_code(),
            DiagnosticCode::SchemaDdlAdmission,
            "DDL endpoint {label} rejection should stay at the schema DDL admission boundary",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "DDL endpoint {label} rejection should keep query-owned origin metadata",
        );
        assert_eq!(
            err.code(),
            ErrorCode::SCHEMA_DDL_VALIDATION_FAILED,
            "DDL endpoint {label} rejection should preserve the NotDdl validation leaf code",
        );
        let after = expect_projection(
            query_sql(
                &fixture,
                "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
            )
            .expect("post-rejection read should still execute"),
        );
        assert_eq!(
            after, before,
            "rejected DDL endpoint {label} must not mutate rows",
        );
    }
}

#[test]
fn sql_canister_update_endpoint_admits_primary_key_update_only() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let alice = expect_projection(
        query_sql(
            &fixture,
            "SELECT id, age FROM SqlTestUser WHERE name = 'alice'",
        )
        .expect("pre-update read should find alice"),
    );
    let alice_id = first_projected_text(&alice);
    let result = update_sql(
        &fixture,
        format!("UPDATE SqlTestUser SET age = 32 WHERE id = '{alice_id}'").as_str(),
    )
    .expect("source-declared SQL update endpoint should admit primary-key UPDATE");

    assert_eq!(
        result,
        SqlQueryResult::Count {
            entity: "SqlTestUser".to_string(),
            row_count: 1,
        },
    );
    let after = expect_projection(
        query_sql(&fixture, "SELECT age FROM SqlTestUser WHERE name = 'alice'")
            .expect("post-update read should find alice"),
    );
    assert_eq!(after.rendered_rows(), string_rows(&[&["32"]]));
}

#[test]
fn sql_canister_update_endpoint_rejects_non_primary_key_update_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE name = 'alice'",
    )
    .expect_err("source-declared SQL update endpoint must reject non-PK UPDATE");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "generated SQL update endpoint should preserve policy rejection code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "generated SQL update endpoint policy rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected generated SQL update endpoint call must not mutate rows",
    );
}

#[test]
fn sql_canister_update_endpoint_rejects_primary_key_update_with_extra_guard_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let err = update_sql(
        &fixture,
        format!("UPDATE SqlTestUser SET age = 32 WHERE id = '{alice_id}' AND age = 31").as_str(),
    )
    .expect_err("source-declared SQL update endpoint must reject guarded PK UPDATE");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "generated SQL update endpoint should reject extra guard predicates under the current primary-key policy",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "guarded primary-key UPDATE rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected guarded primary-key generated UPDATE must not mutate rows",
    );
}

#[test]
fn sql_canister_update_endpoint_returns_primary_key_post_update_rows() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let returning = expect_projection(
        update_sql(
            &fixture,
            format!("UPDATE SqlTestUser SET age = 34 WHERE id = '{alice_id}' RETURNING name, age")
                .as_str(),
        )
        .expect("primary-key generated SQL update endpoint should admit RETURNING"),
    );

    assert_projection_rendered(
        &returning,
        "SqlTestUser",
        &["name", "age"],
        &[&["alice", "34"]],
        1,
        "primary-key generated UPDATE RETURNING should return the post-update row image",
    );
}

#[test]
fn sql_canister_update_endpoint_returns_primary_key_post_update_star_rows() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let returning = expect_projection(
        update_sql(
            &fixture,
            format!("UPDATE SqlTestUser SET age = 35 WHERE id = '{alice_id}' RETURNING *").as_str(),
        )
        .expect("primary-key generated SQL update endpoint should admit RETURNING *"),
    );

    assert_eq!(returning.entity, "SqlTestUser");
    assert_eq!(
        returning.columns,
        ["age", "created_at", "id", "name", "rank", "updated_at"],
        "primary-key generated UPDATE RETURNING * should preserve accepted row-layout order",
    );
    assert_eq!(returning.row_count, 1);
    let rows = returning.rendered_rows();
    let row = rows
        .first()
        .expect("primary-key generated UPDATE RETURNING * should return one row");
    assert_eq!(
        row.len(),
        returning.columns.len(),
        "primary-key generated UPDATE RETURNING * should return a complete row image",
    );
    assert_eq!(row[2], alice_id);
    assert_eq!(row[3], "alice");
    assert_eq!(
        row[0], "35",
        "primary-key generated UPDATE RETURNING * should expose the post-update value",
    );
    assert_eq!(
        row[4], "28",
        "primary-key generated UPDATE RETURNING * should preserve unchanged fields",
    );
}

#[test]
fn sql_canister_update_endpoint_rejects_oversized_returning_response_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let alpha_id = sql_test_numeric_type_id_by_label(&fixture, "alpha");
    seed_oversized_sql_group_name(&fixture);
    let err = update_sql(
        &fixture,
        format!(
            "UPDATE SqlTestNumericTypes SET int32_value = 37 \
             WHERE id = '{alpha_id}' RETURNING group_name"
        )
        .as_str(),
    )
    .expect_err("primary-key generated UPDATE should reject oversized RETURNING response");

    assert_eq!(
        err.code(),
        ErrorCode::SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE,
        "primary-key generated UPDATE should enforce the default RETURNING response budget",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "oversized primary-key UPDATE RETURNING rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            format!("SELECT int32_value FROM SqlTestNumericTypes WHERE id = '{alpha_id}'").as_str(),
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after.rendered_rows(),
        string_rows(&[&["35"]]),
        "oversized primary-key UPDATE RETURNING should reject before mutation",
    );
}

#[test]
fn sql_canister_update_endpoint_rejects_computed_returning_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let alice_id = sql_test_user_id_by_name(&fixture, "alice");
    let err = update_sql(
        &fixture,
        format!("UPDATE SqlTestUser SET age = 34 WHERE id = '{alice_id}' RETURNING LOWER(name)")
            .as_str(),
    )
    .expect_err("primary-key generated SQL update endpoint must reject computed RETURNING");

    assert_eq!(
        err.code(),
        ErrorCode::SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE,
        "computed primary-key UPDATE RETURNING should preserve the specific unsupported SQL feature code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "computed primary-key UPDATE RETURNING rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected primary-key UPDATE RETURNING must not mutate rows",
    );
}

#[test]
fn sql_canister_update_endpoint_rejects_invalid_returning_fields_without_mutation() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let alice_id = sql_test_user_id_by_name(&fixture, "alice");

    for (returning, expected_code) in [
        ("missing", ErrorCode::SQL_WRITE_UNKNOWN_RETURNING_FIELD),
        ("name, name", ErrorCode::SQL_WRITE_DUPLICATE_RETURNING_FIELD),
    ] {
        let err = update_sql(
            &fixture,
            format!(
                "UPDATE SqlTestUser SET age = 34 WHERE id = '{alice_id}' RETURNING {returning}"
            )
            .as_str(),
        )
        .expect_err(
            "primary-key generated SQL update endpoint must reject invalid RETURNING fields",
        );

        assert_eq!(
            err.code(),
            expected_code,
            "invalid primary-key UPDATE RETURNING field list should preserve its compact SQL write code",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "invalid primary-key UPDATE RETURNING rejection should stay query-owned",
        );
    }
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected invalid primary-key UPDATE RETURNING field lists must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_admits_explicit_limited_primary_key_order() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-update read should prove the row set exists"),
    );
    let result = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY id ASC LIMIT 2",
    )
    .expect("source-declared bounded SQL update endpoint should admit explicit bounded UPDATE");

    assert_eq!(
        result,
        SqlQueryResult::Count {
            entity: "SqlTestUser".to_string(),
            row_count: 2,
        },
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-update read should still execute"),
    );
    assert_ne!(
        after, before,
        "admitted bounded generated SQL update should mutate the limited target set",
    );
    assert_eq!(
        after
            .rendered_rows()
            .into_iter()
            .filter(|row| row.get(1).is_some_and(|age| age == "32"))
            .count(),
        2,
        "bounded generated SQL update should mutate exactly the admitted LIMIT window",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_unordered_limit_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 LIMIT 2",
    )
    .expect_err("source-declared bounded SQL update endpoint must reject implicit ordering");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should preserve policy rejection code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update endpoint policy rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected bounded generated SQL update endpoint call must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_limit_above_default_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY id ASC LIMIT 101",
    )
    .expect_err("source-declared bounded SQL update endpoint must reject excessive LIMIT");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should enforce the default row limit",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update limit rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "bounded generated SQL update over the default row limit must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_non_primary_key_order_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY age ASC LIMIT 2",
    )
    .expect_err("source-declared bounded SQL update endpoint must reject non-PK ordering");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should reject non-primary-key ordering",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update non-primary-key ordering rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "bounded generated SQL update with non-primary-key ordering must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_desc_order_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY id DESC LIMIT 2",
    )
    .expect_err("source-declared bounded SQL update endpoint must reject descending order");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should reject descending primary-key order",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update descending-order rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "bounded generated SQL update with descending order must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_offset_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 32 WHERE age >= 24 ORDER BY id ASC LIMIT 2 OFFSET 1",
    )
    .expect_err("source-declared bounded SQL update endpoint must reject OFFSET");

    assert_eq!(
        err.code(),
        ErrorCode::RUNTIME_UNSUPPORTED,
        "bounded generated SQL update endpoint should reject OFFSET",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "bounded generated SQL update OFFSET rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "bounded generated SQL update with OFFSET must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_returns_post_update_rows() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let target_names = expect_projection(
        query_sql(
            &fixture,
            "SELECT name FROM SqlTestUser WHERE age >= 24 ORDER BY id ASC LIMIT 2",
        )
        .expect("pre-update target read should prove the bounded order"),
    );
    let returning = expect_projection(
        update_sql(
            &fixture,
            "UPDATE SqlTestUser SET age = 33 \
             WHERE age >= 24 ORDER BY id ASC LIMIT 2 RETURNING name, age",
        )
        .expect("bounded generated SQL update endpoint should admit bounded RETURNING"),
    );

    let expected_rows = target_names
        .rendered_rows()
        .into_iter()
        .map(|row| vec![row[0].clone(), "33".to_string()])
        .collect::<Vec<_>>();
    assert_eq!(returning.entity, "SqlTestUser");
    assert_eq!(returning.columns, ["name", "age"]);
    assert_eq!(returning.rendered_rows(), expected_rows);
    assert_eq!(
        returning.row_count, 2,
        "bounded generated UPDATE RETURNING should return post-update rows in the frozen target order",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_returns_post_update_star_rows() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let targets = expect_projection(
        query_sql(
            &fixture,
            "SELECT id, name, rank FROM SqlTestUser WHERE age >= 24 ORDER BY id ASC LIMIT 2",
        )
        .expect("pre-update target read should prove the bounded order"),
    );
    let returning = expect_projection(
        update_sql(
            &fixture,
            "UPDATE SqlTestUser SET age = 36 \
             WHERE age >= 24 ORDER BY id ASC LIMIT 2 RETURNING *",
        )
        .expect("bounded generated SQL update endpoint should admit bounded RETURNING *"),
    );

    assert_eq!(returning.entity, "SqlTestUser");
    assert_eq!(
        returning.columns,
        ["age", "created_at", "id", "name", "rank", "updated_at"],
        "bounded generated UPDATE RETURNING * should preserve accepted row-layout order",
    );
    assert_eq!(returning.row_count, 2);
    assert_eq!(
        returning.rows.len(),
        targets.rows.len(),
        "bounded generated UPDATE RETURNING * should return the frozen target window",
    );
    let returning_rows = returning.rendered_rows();
    let target_rows = targets.rendered_rows();
    for (row, target) in returning_rows.iter().zip(target_rows.iter()) {
        assert_eq!(
            row.len(),
            returning.columns.len(),
            "bounded generated UPDATE RETURNING * should return complete row images",
        );
        assert_eq!(row[2], target[0]);
        assert_eq!(row[3], target[1]);
        assert_eq!(
            row[0], "36",
            "bounded generated UPDATE RETURNING * should expose post-update values",
        );
        assert_eq!(
            row[4], target[2],
            "bounded generated UPDATE RETURNING * should preserve unchanged fields",
        );
    }
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_oversized_returning_response_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    seed_oversized_sql_group_name(&fixture);
    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT id, int32_value FROM SqlTestNumericTypes ORDER BY id ASC",
        )
        .expect("pre-rejection read should still execute"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestNumericTypes SET int32_value = 37 \
         WHERE nat32_value >= 100 ORDER BY id ASC LIMIT 2 RETURNING group_name",
    )
    .expect_err("bounded generated UPDATE should reject oversized RETURNING response");

    assert_eq!(
        err.code(),
        ErrorCode::SQL_WRITE_RETURNING_RESPONSE_TOO_LARGE,
        "bounded generated UPDATE should enforce the default RETURNING response budget",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "oversized bounded UPDATE RETURNING rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT id, int32_value FROM SqlTestNumericTypes ORDER BY id ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "oversized bounded UPDATE RETURNING should reject before mutation",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_computed_returning_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );
    let err = update_sql(
        &fixture,
        "UPDATE SqlTestUser SET age = 33 \
         WHERE age >= 24 ORDER BY id ASC LIMIT 2 RETURNING LOWER(name)",
    )
    .expect_err("bounded generated SQL update endpoint must reject computed RETURNING");

    assert_eq!(
        err.code(),
        ErrorCode::SQL_FEATURE_UNSUPPORTED_FUNCTION_NAMESPACE,
        "computed bounded UPDATE RETURNING should preserve the specific unsupported SQL feature code",
    );
    assert_eq!(
        err.origin(),
        ErrorOrigin::Query,
        "computed bounded UPDATE RETURNING rejection should stay query-owned",
    );
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected bounded UPDATE RETURNING must not mutate rows",
    );
}

#[test]
fn sql_canister_bounded_update_endpoint_rejects_invalid_returning_fields_without_mutation() {
    let fixture = install_sql_bounded_canister_fixture();
    reset_sql_fixtures(&fixture);

    let before = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("pre-rejection read should prove the row set exists"),
    );

    for (returning, expected_code) in [
        ("missing", ErrorCode::SQL_WRITE_UNKNOWN_RETURNING_FIELD),
        ("name, name", ErrorCode::SQL_WRITE_DUPLICATE_RETURNING_FIELD),
    ] {
        let err = update_sql(
            &fixture,
            format!(
                "UPDATE SqlTestUser SET age = 33 \
                 WHERE age >= 24 ORDER BY id ASC LIMIT 2 RETURNING {returning}"
            )
            .as_str(),
        )
        .expect_err("bounded generated SQL update endpoint must reject invalid RETURNING fields");

        assert_eq!(
            err.code(),
            expected_code,
            "invalid bounded UPDATE RETURNING field list should preserve its compact SQL write code",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "invalid bounded UPDATE RETURNING rejection should stay query-owned",
        );
    }
    let after = expect_projection(
        query_sql(
            &fixture,
            "SELECT name, age FROM SqlTestUser ORDER BY name ASC",
        )
        .expect("post-rejection read should still execute"),
    );
    assert_eq!(
        after, before,
        "rejected invalid bounded UPDATE RETURNING field lists must not mutate rows",
    );
}

#[test]
fn sql_canister_query_endpoint_rejects_malformed_sql() {
    let fixture = install_sql_canister_fixture();
    reset_sql_fixtures(&fixture);

    let err = query_sql(&fixture, "SELECT FROM SqlTestUser")
        .expect_err("query(sql) must reject malformed SQL before execution");

    assert_runtime_unsupported_query_error(
        &err,
        "malformed SQL should keep query-owned origin metadata",
    );
}
