//!
//! Small SQL canister used for lightweight SQL fixture smoke tests.
//!

use candid::CandidType;
use ic_cdk::{query, update};
#[cfg(feature = "sql")]
use icydb::db::query::asc;
#[cfg(feature = "sql")]
use icydb::types::{Decimal, Float32, Float64};
use icydb::{
    ErrorKind, ErrorOrigin, QueryErrorKind,
    db::{
        DynamicQuery, StructuralMutation, StructuralPatch, TypedAdapterError, TypedEntityAdapter,
        TypedOperationError, TypedWriteAdapter, TypedWriteHandle, WriteCell,
    },
    prelude::FieldRef,
    types::{Id, Ulid},
    value::InputValue,
};
use icydb_model::base::types::web::MimeType;
use icydb_model::{Inner as _, NormalizeAndValidate as _, normalize, validate};
use icydb_testing_test_sql_fixtures::sql::{
    SqlTestEnrollmentRobotInsert, SqlTestEnrollmentUser, SqlTestEnrollmentUserInsert,
    SqlTestEnrollmentUserPrincipalInsert,
};

icydb::start!();

icydb::endpoints! {
    #[cfg(feature = "local-sql-query")]
    icydb_sql_query(introspection = true);
    #[cfg(feature = "sql")]
    icydb_ddl;
    #[cfg(all(feature = "sql", not(icydb_bounded_update)))]
    icydb_update(admission = primary_key_only);
    #[cfg(all(feature = "sql", icydb_bounded_update))]
    icydb_update(admission = bounded_deterministic);
    #[cfg(all(feature = "sql", not(icydb_bounded_update)))]
    icydb_integrity;
    icydb_metrics(authorization = public);
    icydb_metrics_reset;
    icydb_snapshot;
    icydb_schema(authorization = controller);
    #[cfg(feature = "schema-migration-api")]
    icydb_schema_migrate;
    #[cfg(feature = "schema-migration-api")]
    icydb_schema_migration;
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_reset;
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_load(handler = icydb_fixtures_load);
}

#[cfg(feature = "sql")]
const OVERSIZED_SQL_GROUP_NAME_LEN: usize = 1_050_000;
const IDENTITY_MAX_BATCH_ROWS: u32 = (4 * 1024) - 1;
const APPLICATION_BEHAVIOR_PERF_ITERATIONS: u32 = 256;

#[cfg(all(feature = "test-admin-api", feature = "diagnostics"))]
#[query]
fn measure_sql_query_attribution(
    sql: String,
) -> Result<icydb::db::SqlQueryExecutionAttribution, icydb::Error> {
    icydb::__macro::with_query_metrics_context(|| {
        icydb::db::with_request_execution(|| {
            let (_, attribution) =
                icydb::db!()?.execute_trusted_sql_query_with_attribution(&sql)?;
            Ok(attribution)
        })
    })
}

#[cfg(all(feature = "test-admin-api", feature = "diagnostics"))]
#[derive(CandidType, Clone, Debug)]
struct SqlExecutionInstructionResult {
    result: Result<icydb::db::sql::SqlQueryResult, icydb::Error>,
    local_instructions: u64,
}

#[cfg(all(feature = "test-admin-api", feature = "diagnostics"))]
#[derive(CandidType, Clone, Debug)]
struct AcceptedSchemaReadInstructionResult {
    description: icydb::db::EntitySchemaDescription,
    local_instructions: u64,
}

/// Measure one trusted SQL query while retaining either success or failure.
#[cfg(all(feature = "test-admin-api", feature = "diagnostics"))]
#[query]
fn measure_sql_query_instructions(sql: String) -> SqlExecutionInstructionResult {
    icydb::__macro::with_query_metrics_context(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = icydb::db::with_request_execution(|| {
            icydb::db!()?.execute_trusted_sql_query(sql.as_str())
        });
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        SqlExecutionInstructionResult {
            result,
            local_instructions,
        }
    })
}

/// Measure administrative DDL admission through the existing catalog owner.
#[cfg(all(feature = "test-admin-api", feature = "diagnostics"))]
#[update]
fn measure_sql_ddl_admission_instructions(sql: String) -> SqlExecutionInstructionResult {
    let start = ic_cdk::api::performance_counter(1);
    let result =
        icydb::db::with_request_execution(|| icydb::db!()?.execute_admin_sql_ddl(sql.as_str()));
    let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    SqlExecutionInstructionResult {
        result,
        local_instructions,
    }
}

/// Measure one trusted exact update through the existing complete write path.
#[cfg(all(feature = "test-admin-api", feature = "diagnostics"))]
#[update]
fn measure_trusted_sql_exact_update_instructions(sql: String) -> SqlExecutionInstructionResult {
    let start = ic_cdk::api::performance_counter(1);
    let result = icydb::db::with_request_execution(|| {
        icydb::db!()?.execute_trusted_sql_exact_update(sql.as_str(), 1)
    });
    let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    SqlExecutionInstructionResult {
        result,
        local_instructions,
    }
}

/// Measure an accepted schema read after startup has reopened catalog authority.
#[cfg(all(feature = "test-admin-api", feature = "diagnostics"))]
#[query]
fn measure_accepted_schema_read_instructions(
    entity: String,
) -> Result<AcceptedSchemaReadInstructionResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let description = icydb::db!()?.try_describe_entity_by_name(entity.as_str())?;
        let local_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        Ok(AcceptedSchemaReadInstructionResult {
            description,
            local_instructions,
        })
    })
}

#[cfg(feature = "metrics-context-audit")]
#[query]
fn audit_query_metrics_context_trap() -> Result<(), icydb::Error> {
    icydb::__macro::with_query_metrics_context(|| {
        ic_cdk::trap("intentional query-context rollback probe")
    })
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
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

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct ApplicationBehaviorPerfResult {
    normalize_instructions: u64,
    validate_instructions: u64,
    normalize_and_validate_instructions: u64,
    normalized_bytes: u64,
    validated_bytes: u64,
    composed_bytes: u64,
    iterations: u32,
}

/// Measure the three explicit application-behavior surfaces without database
/// access or generated write adapters.
#[query]
fn measure_application_behavior_perf() -> Result<ApplicationBehaviorPerfResult, String> {
    let mut normalized_bytes = 0_u64;
    let start = ic_cdk::api::performance_counter(1);
    for _ in 0..APPLICATION_BEHAVIOR_PERF_ITERATIONS {
        let mut value = MimeType::from("  Text/HTML  ");
        normalize(&mut value).map_err(|error| error.to_string())?;
        normalized_bytes = normalized_bytes.saturating_add(value.inner().len() as u64);
    }
    let normalize_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    let mut validated_bytes = 0_u64;
    let start = ic_cdk::api::performance_counter(1);
    for _ in 0..APPLICATION_BEHAVIOR_PERF_ITERATIONS {
        let value = MimeType::from("text/html");
        validate(&value).map_err(|error| error.to_string())?;
        validated_bytes = validated_bytes.saturating_add(value.inner().len() as u64);
    }
    let validate_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    let mut composed_bytes = 0_u64;
    let start = ic_cdk::api::performance_counter(1);
    for _ in 0..APPLICATION_BEHAVIOR_PERF_ITERATIONS {
        let value = MimeType::from("  Text/HTML  ")
            .normalize_and_validate()
            .map_err(|error| error.to_string())?;
        composed_bytes = composed_bytes.saturating_add(value.inner().len() as u64);
    }
    let normalize_and_validate_instructions =
        ic_cdk::api::performance_counter(1).saturating_sub(start);

    Ok(ApplicationBehaviorPerfResult {
        normalize_instructions,
        validate_instructions,
        normalize_and_validate_instructions,
        normalized_bytes,
        validated_bytes,
        composed_bytes,
        iterations: APPLICATION_BEHAVIOR_PERF_ITERATIONS,
    })
}

/// Load one deterministic baseline fixture dataset for SQL smoke tests.
#[cfg(feature = "test-admin-api")]
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    icydb::db!()?.execute_trusted_structural_insert_batch("SqlTestUser", sql_user_patches())?;
    icydb::db!()?.execute_trusted_structural_insert_batch(
        "SqlTestNumericTypes",
        sql_numeric_type_patches(),
    )?;

    Ok(())
}

/// Build one deterministic baseline SQL user fixture batch.
#[cfg(feature = "test-admin-api")]
fn sql_user_patches() -> Vec<StructuralPatch> {
    vec![
        sql_user_patch("alice", 31, 28),
        sql_user_patch("bob", 24, 25),
        sql_user_patch("charlie", 43, 43),
    ]
}

#[cfg(feature = "test-admin-api")]
fn sql_user_patch(name: &str, age: u16, rank: i32) -> StructuralPatch {
    #[cfg(not(feature = "schema-migration-v2"))]
    let patch = StructuralPatch::new()
        .field("name", WriteCell::Value(InputValue::text(name.to_string())))
        .field("age", WriteCell::Value(InputValue::from(i32::from(age))))
        .field("rank", WriteCell::Value(InputValue::from(rank)));

    #[cfg(feature = "schema-migration-v2")]
    let patch = StructuralPatch::new()
        .field("name", WriteCell::Value(InputValue::text(name.to_string())))
        .field("age", WriteCell::Value(InputValue::from(age)))
        .field("score", WriteCell::Value(InputValue::from(rank)));

    patch
}

/// Seed runtime-built oversized unindexed payloads for mutation and complete
/// generated-query response-budget tests without embedding literals in Wasm.
#[cfg(feature = "sql")]
#[update]
fn seed_oversized_sql_group_name() -> Result<(), icydb::Error> {
    icydb::db::with_request_execution(|| {
        let session = icydb::db!()?;
        set_sql_numeric_group_name(&session, "alpha", "a".repeat(OVERSIZED_SQL_GROUP_NAME_LEN))?;
        set_sql_numeric_group_name(&session, "beta", "b".repeat(OVERSIZED_SQL_GROUP_NAME_LEN))?;
        let gamma = "c".repeat(OVERSIZED_SQL_GROUP_NAME_LEN);
        session.execute_trusted_structural_insert_batch(
            "SqlTestNumericTypes",
            vec![sql_numeric_type_patch(
                "gamma",
                gamma.as_str(),
                3,
                7,
                89,
                12_000,
                18,
                9,
                500,
                12_000,
                30,
                Float32::from(0),
                Float64::from(0),
            )],
        )?;

        Ok(())
    })
}

#[cfg(feature = "sql")]
fn set_sql_numeric_group_name<C: icydb::traits::CanisterKind>(
    session: &icydb::db::DbSession<C>,
    label: &str,
    group_name: String,
) -> Result<(), icydb::Error> {
    let query = DynamicQuery::new("SqlTestNumericTypes")
        .filter(FieldRef::new("label").eq(label))
        .order_by(asc("id"))
        .select(["id"])
        .limit(1);
    let result = session.execute_trusted_live_page(&query, None)?;
    let id = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| match value.as_public() {
            icydb::value::PublicValue::Ulid(id) => Some(*id),
            _ => None,
        })
        .ok_or_else(|| {
            icydb::Error::from_kind(
                ErrorKind::Query(QueryErrorKind::NotFound),
                ErrorOrigin::Response,
            )
        })?;
    let patch =
        session.structural_patch([("group_name", WriteCell::Value(InputValue::from(group_name)))]);

    session.execute_trusted_structural_mutation(StructuralMutation::Update {
        entity: "SqlTestNumericTypes".to_string(),
        key: InputValue::from(id),
        patch,
    })?;

    Ok(())
}

/// Build one deterministic mixed numeric fixture batch for SQL type coverage.
#[cfg(feature = "test-admin-api")]
fn sql_numeric_type_patches() -> Vec<StructuralPatch> {
    vec![
        sql_numeric_type_patch(
            "alpha",
            "mage",
            -1,
            -2,
            35,
            -500,
            14,
            3,
            120,
            1_000,
            15,
            Float32::try_new(0.75).expect("finite float32 fixture value"),
            Float64::try_new(0.50).expect("finite float64 fixture value"),
        ),
        sql_numeric_type_patch(
            "beta",
            "fighter",
            2,
            5,
            58,
            9_000,
            16,
            7,
            300,
            9_000,
            25,
            Float32::try_new(0.25).expect("finite float32 fixture value"),
            Float64::try_new(0.25).expect("finite float64 fixture value"),
        ),
    ]
}

#[cfg(feature = "sql")]
#[expect(
    clippy::too_many_arguments,
    reason = "one fixture helper mirrors the maintained scalar SQL type matrix"
)]
fn sql_numeric_type_patch(
    label: &str,
    group_name: &str,
    int8_value: i8,
    int16_value: i16,
    int32_value: i32,
    int64_value: i64,
    nat8_value: u8,
    nat16_value: u16,
    nat32_value: u32,
    nat64_value: u64,
    decimal_value: i64,
    float32_value: Float32,
    float64_value: Float64,
) -> StructuralPatch {
    StructuralPatch::new()
        .field(
            "label",
            WriteCell::Value(InputValue::text(label.to_string())),
        )
        .field(
            "group_name",
            WriteCell::Value(InputValue::text(group_name.to_string())),
        )
        .field("int8_value", WriteCell::Value(InputValue::from(int8_value)))
        .field(
            "int16_value",
            WriteCell::Value(InputValue::from(int16_value)),
        )
        .field(
            "int32_value",
            WriteCell::Value(InputValue::from(int32_value)),
        )
        .field(
            "int64_value",
            WriteCell::Value(InputValue::from(int64_value)),
        )
        .field("nat8_value", WriteCell::Value(InputValue::from(nat8_value)))
        .field(
            "nat16_value",
            WriteCell::Value(InputValue::from(nat16_value)),
        )
        .field(
            "nat32_value",
            WriteCell::Value(InputValue::from(nat32_value)),
        )
        .field(
            "nat64_value",
            WriteCell::Value(InputValue::from(nat64_value)),
        )
        .field(
            "decimal_value",
            WriteCell::Value(InputValue::from(Decimal::new(decimal_value, 2))),
        )
        .field(
            "float32_value",
            WriteCell::Value(InputValue::from(float32_value)),
        )
        .field(
            "float64_value",
            WriteCell::Value(InputValue::from(float64_value)),
        )
}

fn identity_payload_patch(payload: u64) -> StructuralPatch {
    StructuralPatch::new().field("payload", WriteCell::Value(InputValue::nat64(payload)))
}

fn caller_nat64_patch(id: u64, payload: u64) -> StructuralPatch {
    identity_payload_patch(payload).field("id", WriteCell::Value(InputValue::nat64(id)))
}

const fn typed_fixture_invariant_error() -> icydb::Error {
    icydb::Error::from_kind(
        ErrorKind::Query(QueryErrorKind::Validate),
        ErrorOrigin::Executor,
    )
}

fn typed_operation_fixture_error(error: TypedOperationError) -> icydb::Error {
    match error {
        TypedOperationError::Adapter(_) => typed_fixture_invariant_error(),
        TypedOperationError::Database(error) => error,
    }
}

const fn typed_adapter_fixture_error(_error: TypedAdapterError) -> icydb::Error {
    typed_fixture_invariant_error()
}

fn enrollment_user_input(id: Id<SqlTestEnrollmentUser>, name: &str) -> SqlTestEnrollmentUserInsert {
    SqlTestEnrollmentUserInsert {
        id: WriteCell::Value(id),
        display_name: WriteCell::Value(name.to_string()),
    }
}

const fn enrollment_membership_input(
    principal: icydb::types::Principal,
    user_id: Id<SqlTestEnrollmentUser>,
) -> SqlTestEnrollmentUserPrincipalInsert {
    SqlTestEnrollmentUserPrincipalInsert {
        authentication_principal: WriteCell::Value(Id::from_key(principal)),
        user_id: WriteCell::Value(user_id),
    }
}

fn enrollment_robot_input(
    user_id: Id<SqlTestEnrollmentUser>,
    label: &str,
) -> SqlTestEnrollmentRobotInsert {
    SqlTestEnrollmentRobotInsert {
        user_id: WriteCell::Value(user_id),
        label: WriteCell::Value(label.to_string()),
    }
}

fn execute_one_typed<C, W>(session: &icydb::db::DbSession<C>, input: W) -> Result<(), icydb::Error>
where
    C: icydb::traits::CanisterKind,
    W: TypedWriteAdapter,
{
    let binding = W::Entity::typed_binding(session).map_err(|error| match error {
        icydb::db::TypedOperationError::Adapter(_) => typed_fixture_invariant_error(),
        icydb::db::TypedOperationError::Database(error) => error,
    })?;
    let write = input
        .encode_write(&binding)
        .map_err(typed_adapter_fixture_error)?;
    session
        .execute_trusted_typed_write(write)
        .map_err(typed_operation_fixture_error)?;
    Ok(())
}

fn verify_typed_enrollment_failure_controls<C: icydb::traits::CanisterKind>(
    session: &icydb::db::DbSession<C>,
    existing_user_id: Id<SqlTestEnrollmentUser>,
    existing_principal: icydb::types::Principal,
    foreign_user: &TypedWriteHandle<SqlTestEnrollmentUser>,
) -> Result<(), icydb::Error> {
    let other_user_id = Id::<SqlTestEnrollmentUser>::generate().map_err(icydb::Error::from)?;
    let mut other = session.trusted_typed_write_batch();
    let other_user = other
        .push(enrollment_user_input(other_user_id, "Other Owner"))
        .map_err(typed_operation_fixture_error)?;
    let other_results = other.execute().map_err(typed_operation_fixture_error)?;
    if !matches!(
        other_results.result(foreign_user),
        Err(TypedAdapterError::BatchHandleMismatch)
    ) || other_results
        .result(&other_user)
        .map_err(typed_adapter_fixture_error)?
        .affected_rows()
        != 1
    {
        return Err(typed_fixture_invariant_error());
    }

    let failed_user_id = Id::<SqlTestEnrollmentUser>::generate().map_err(icydb::Error::from)?;
    let mut failed = session.trusted_typed_write_batch();
    failed
        .push(enrollment_user_input(failed_user_id, "Must Remain Atomic"))
        .map_err(typed_operation_fixture_error)?;
    failed
        .push(enrollment_robot_input(failed_user_id, "Must Not Commit"))
        .map_err(typed_operation_fixture_error)?;
    failed
        .push(enrollment_membership_input(
            existing_principal,
            failed_user_id,
        ))
        .map_err(typed_operation_fixture_error)?;
    if !matches!(failed.execute(), Err(TypedOperationError::Database(_))) {
        return Err(typed_fixture_invariant_error());
    }
    let failed_robot_page = session.execute_trusted_live_page(
        &DynamicQuery::new("SqlTestEnrollmentRobot")
            .filter(FieldRef::new("user_id").eq(failed_user_id.key()))
            .select(["id"])
            .limit(1),
        None,
    )?;
    if !failed_robot_page.rows.is_empty() {
        return Err(typed_fixture_invariant_error());
    }
    execute_one_typed(
        session,
        enrollment_user_input(failed_user_id, "Atomic Retry"),
    )?;

    let explicit_generated_id = StructuralPatch::new()
        .field("id", WriteCell::Value(InputValue::ulid(Ulid::nil())))
        .field(
            "user_id",
            WriteCell::Value(InputValue::ulid(existing_user_id.key())),
        )
        .field(
            "label",
            WriteCell::Value(InputValue::text("Generated bypass".to_string())),
        );
    if session
        .execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: "SqlTestEnrollmentRobot".to_string(),
            patch: explicit_generated_id,
        })
        .is_ok()
    {
        return Err(typed_fixture_invariant_error());
    }
    Ok(())
}

fn measure_typed_enrollment_costs<C: icydb::traits::CanisterKind>(
    session: &icydb::db::DbSession<C>,
) -> Result<(u64, u64), icydb::Error> {
    let sequential_user_id = Id::<SqlTestEnrollmentUser>::generate().map_err(icydb::Error::from)?;
    let sequential_principal = icydb::types::Principal::from_slice(b"icydb-0.235-sequential");
    let start = ic_cdk::api::performance_counter(1);
    execute_one_typed(
        session,
        enrollment_user_input(sequential_user_id, "Sequential User"),
    )?;
    execute_one_typed(
        session,
        enrollment_membership_input(sequential_principal, sequential_user_id),
    )?;
    execute_one_typed(
        session,
        enrollment_robot_input(sequential_user_id, "Sequential Robot"),
    )?;
    let sequential = ic_cdk::api::performance_counter(1).saturating_sub(start);

    let atomic_user_id = Id::<SqlTestEnrollmentUser>::generate().map_err(icydb::Error::from)?;
    let atomic_principal = icydb::types::Principal::from_slice(b"icydb-0.235-atomic");
    let start = ic_cdk::api::performance_counter(1);
    let mut batch = session.trusted_typed_write_batch();
    let user = batch
        .push(enrollment_user_input(atomic_user_id, "Atomic User"))
        .map_err(typed_operation_fixture_error)?;
    let membership = batch
        .push(enrollment_membership_input(
            atomic_principal,
            atomic_user_id,
        ))
        .map_err(typed_operation_fixture_error)?;
    let robot = batch
        .push(enrollment_robot_input(atomic_user_id, "Atomic Robot"))
        .map_err(typed_operation_fixture_error)?;
    let mut results = batch.execute().map_err(typed_operation_fixture_error)?;
    let atomic = ic_cdk::api::performance_counter(1).saturating_sub(start);

    let user_row = results.row(&user).map_err(typed_operation_fixture_error)?;
    let membership_row = results
        .row(&membership)
        .map_err(typed_operation_fixture_error)?;
    let robot_row = results.row(&robot).map_err(typed_operation_fixture_error)?;
    if user_row.id != atomic_user_id.key()
        || membership_row.authentication_principal != atomic_principal
        || membership_row.user_id != atomic_user_id.key()
        || robot_row.user_id != atomic_user_id.key()
        || results
            .result(&robot)
            .map_err(typed_adapter_fixture_error)?
            .entity()
            != "SqlTestEnrollmentRobot"
        || !matches!(
            results.row(&robot),
            Err(TypedOperationError::Adapter(
                TypedAdapterError::BatchRowConsumed
            ))
        )
    {
        return Err(typed_fixture_invariant_error());
    }
    verify_typed_enrollment_failure_controls(session, atomic_user_id, atomic_principal, &user)?;

    Ok((sequential, atomic))
}

fn measure_three_entity_write_costs<C: icydb::traits::CanisterKind>(
    session: &icydb::db::DbSession<C>,
) -> Result<(u64, u64), icydb::Error> {
    let start = ic_cdk::api::performance_counter(1);
    for entity in [
        "SqlTestIdentityNat64",
        "SqlTestIdentityNat128",
        "SqlTestIdentityBatch",
    ] {
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: entity.to_string(),
            patch: identity_payload_patch(3),
        })?;
    }
    let sequential = ic_cdk::api::performance_counter(1).saturating_sub(start);

    let start = ic_cdk::api::performance_counter(1);
    let atomic = session.execute_trusted_structural_mutation_batch(vec![
        StructuralMutation::Insert {
            entity: "SqlTestIdentityNat64".to_string(),
            patch: identity_payload_patch(4),
        },
        StructuralMutation::Insert {
            entity: "SqlTestIdentityNat128".to_string(),
            patch: identity_payload_patch(4),
        },
        StructuralMutation::Insert {
            entity: "SqlTestIdentityBatch".to_string(),
            patch: identity_payload_patch(4),
        },
    ])?;
    let atomic_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    if atomic.len() != 3
        || atomic
            .iter()
            .any(|result| result.affected_rows != 1 || result.rows.len() != 1)
    {
        return Err(icydb::Error::from_kind(
            ErrorKind::Query(QueryErrorKind::Validate),
            ErrorOrigin::Executor,
        ));
    }

    Ok((sequential, atomic_instructions))
}

fn measure_maximum_entity_context_cost<C: icydb::traits::CanisterKind>(
    session: &icydb::db::DbSession<C>,
) -> Result<(u64, u32), icydb::Error> {
    const ENTITY_COUNT: u32 = 64;
    let batch = (0..ENTITY_COUNT)
        .map(|index| StructuralMutation::Insert {
            entity: format!("SqlTestContext{index:02}"),
            patch: caller_nat64_patch(1, u64::from(index)),
        })
        .collect();
    let start = ic_cdk::api::performance_counter(1);
    let results = session.execute_trusted_structural_mutation_batch(batch)?;
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    if results.len() != 64
        || results
            .iter()
            .any(|result| result.affected_rows != 1 || result.rows.len() != 1)
    {
        return Err(typed_fixture_invariant_error());
    }
    Ok((instructions, ENTITY_COUNT))
}

/// Measure the final Identity write matrix on one fresh test canister.
#[update]
fn measure_identity_closeout_perf() -> Result<IdentityCloseoutPerfResult, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let session = icydb::db!()?;

        // Warm each accepted entity and the shared journaled store before sampling.
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: "SqlTestCallerNat64".to_string(),
            patch: caller_nat64_patch(1, 1),
        })?;
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: "SqlTestIdentityNat64".to_string(),
            patch: identity_payload_patch(1),
        })?;
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: "SqlTestIdentityNat128".to_string(),
            patch: identity_payload_patch(1),
        })?;
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: "SqlTestIdentityBatch".to_string(),
            patch: identity_payload_patch(1),
        })?;

        let start = ic_cdk::api::performance_counter(1);
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: "SqlTestCallerNat64".to_string(),
            patch: caller_nat64_patch(2, 2),
        })?;
        let caller_nat64_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        let start = ic_cdk::api::performance_counter(1);
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: "SqlTestIdentityNat64".to_string(),
            patch: identity_payload_patch(2),
        })?;
        let generated_nat64_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);

        let start = ic_cdk::api::performance_counter(1);
        session.execute_trusted_structural_mutation(StructuralMutation::Insert {
            entity: "SqlTestIdentityNat128".to_string(),
            patch: identity_payload_patch(2),
        })?;
        let generated_nat128_instructions =
            ic_cdk::api::performance_counter(1).saturating_sub(start);

        let (sequential_three_entity_instructions, atomic_three_entity_instructions) =
            measure_three_entity_write_costs(&session)?;
        let (maximum_entity_context_instructions, maximum_entity_context_count) =
            measure_maximum_entity_context_cost(&session)?;
        let (sequential_typed_enrollment_instructions, atomic_typed_enrollment_instructions) =
            measure_typed_enrollment_costs(&session)?;

        let start = ic_cdk::api::performance_counter(1);
        let one_row = session.execute_trusted_structural_insert_batch(
            "SqlTestIdentityBatch",
            vec![identity_payload_patch(2)],
        )?;
        let one_row_batch_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        if one_row.affected_rows != 1 {
            return Err(icydb::Error::from_kind(
                ErrorKind::Query(QueryErrorKind::Validate),
                ErrorOrigin::Executor,
            ));
        }

        let maximum_batch = (0..IDENTITY_MAX_BATCH_ROWS)
            .map(|ordinal| identity_payload_patch(u64::from(ordinal) + 3))
            .collect();
        let start = ic_cdk::api::performance_counter(1);
        let maximum = session
            .execute_trusted_structural_insert_batch("SqlTestIdentityBatch", maximum_batch)?;
        let maximum_batch_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        if maximum.affected_rows != IDENTITY_MAX_BATCH_ROWS {
            return Err(icydb::Error::from_kind(
                ErrorKind::Query(QueryErrorKind::Validate),
                ErrorOrigin::Executor,
            ));
        }

        Ok(IdentityCloseoutPerfResult {
            caller_nat64_instructions,
            generated_nat64_instructions,
            generated_nat128_instructions,
            one_row_batch_instructions,
            maximum_batch_instructions,
            maximum_batch_rows: IDENTITY_MAX_BATCH_ROWS,
            sequential_three_entity_instructions,
            atomic_three_entity_instructions,
            maximum_entity_context_instructions,
            maximum_entity_context_count,
            sequential_typed_enrollment_instructions,
            atomic_typed_enrollment_instructions,
        })
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
