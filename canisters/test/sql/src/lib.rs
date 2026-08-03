//!
//! Small SQL canister used for lightweight SQL fixture smoke tests.
//!

use candid::CandidType;
use ic_cdk::{query, update};
#[cfg(feature = "test-admin-api")]
use icydb::types::{Decimal, Float32, Float64};
use icydb::{
    ErrorKind, ErrorOrigin, QueryErrorKind,
    db::{StructuralMutation, StructuralPatch, WriteCell},
    value::InputValue,
};
#[cfg(feature = "sql")]
use icydb::{
    db::{DynamicQuery, query::asc},
    prelude::FieldRef,
    value::OutputValue,
};
use icydb_model::base::types::web::MimeType;
use icydb_model::{Inner as _, NormalizeAndValidate as _, normalize, validate};

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
const IDENTITY_MAX_BATCH_ROWS: u32 = 16 * 1024 - 1;
const APPLICATION_BEHAVIOR_PERF_ITERATIONS: u32 = 256;

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct IdentityCloseoutPerfResult {
    caller_nat64_instructions: u64,
    generated_nat64_instructions: u64,
    generated_nat128_instructions: u64,
    one_row_batch_instructions: u64,
    maximum_batch_instructions: u64,
    maximum_batch_rows: u32,
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
    db()?.execute_trusted_structural_insert_batch("SqlTestUser", sql_user_patches())?;
    db()?.execute_trusted_structural_insert_batch(
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
        .field("name", WriteCell::Value(InputValue::Text(name.to_string())))
        .field("age", WriteCell::Value(InputValue::from(i32::from(age))))
        .field("rank", WriteCell::Value(InputValue::from(rank)));

    #[cfg(feature = "schema-migration-v2")]
    let patch = StructuralPatch::new()
        .field("name", WriteCell::Value(InputValue::Text(name.to_string())))
        .field("age", WriteCell::Value(InputValue::from(age)))
        .field("score", WriteCell::Value(InputValue::from(rank)));

    patch
}

/// Seed one runtime-built oversized unindexed payload for generated endpoint
/// response-budget tests without embedding a megabyte literal in the wasm.
#[cfg(feature = "sql")]
#[update]
fn seed_oversized_sql_group_name() -> Result<(), icydb::Error> {
    let session = db()?;
    let query = DynamicQuery::new("SqlTestNumericTypes")
        .filter(FieldRef::new("label").eq("alpha"))
        .order_by(asc("id"))
        .select(["id"])
        .limit(1);
    let result = session.execute_trusted_dynamic_query(&query)?;
    let id = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| match value {
            OutputValue::Ulid(id) => Some(*id),
            _ => None,
        })
        .ok_or_else(|| {
            icydb::Error::from_kind(
                ErrorKind::Query(QueryErrorKind::NotFound),
                ErrorOrigin::Response,
            )
        })?;
    let group_name = "x".repeat(OVERSIZED_SQL_GROUP_NAME_LEN);
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
            "alpha", "mage", -1, -2, 35, -500, 14, 3, 120, 1_000, 15, 0.75, 0.50,
        ),
        sql_numeric_type_patch(
            "beta", "fighter", 2, 5, 58, 9_000, 16, 7, 300, 9_000, 25, 0.25, 0.25,
        ),
    ]
}

#[cfg(feature = "test-admin-api")]
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
    float32_value: f32,
    float64_value: f64,
) -> StructuralPatch {
    StructuralPatch::new()
        .field(
            "label",
            WriteCell::Value(InputValue::Text(label.to_string())),
        )
        .field(
            "group_name",
            WriteCell::Value(InputValue::Text(group_name.to_string())),
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
            WriteCell::Value(InputValue::from(
                Float32::try_new(float32_value).expect("finite float32 fixture value"),
            )),
        )
        .field(
            "float64_value",
            WriteCell::Value(InputValue::from(
                Float64::try_new(float64_value).expect("finite float64 fixture value"),
            )),
        )
}

fn identity_payload_patch(payload: u64) -> StructuralPatch {
    StructuralPatch::new().field("payload", WriteCell::Value(InputValue::Nat64(payload)))
}

fn caller_nat64_patch(id: u64, payload: u64) -> StructuralPatch {
    identity_payload_patch(payload).field("id", WriteCell::Value(InputValue::Nat64(id)))
}

/// Measure the final Identity write matrix on one fresh test canister.
#[update]
fn measure_identity_closeout_perf() -> Result<IdentityCloseoutPerfResult, icydb::Error> {
    let session = db()?;

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
    let generated_nat64_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

    let start = ic_cdk::api::performance_counter(1);
    session.execute_trusted_structural_mutation(StructuralMutation::Insert {
        entity: "SqlTestIdentityNat128".to_string(),
        patch: identity_payload_patch(2),
    })?;
    let generated_nat128_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

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
    let maximum =
        session.execute_trusted_structural_insert_batch("SqlTestIdentityBatch", maximum_batch)?;
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
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
