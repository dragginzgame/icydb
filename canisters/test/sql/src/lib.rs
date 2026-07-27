//!
//! Small SQL canister used for lightweight SQL fixture smoke tests.
//!

use ic_cdk::update;
use icydb::types::{Decimal, Float32, Float64};
use icydb::{
    ErrorKind, ErrorOrigin, QueryErrorKind,
    db::{DynamicQuery, StructuralMutation, StructuralPatch, WriteCell},
    prelude::FieldRef,
    value::{InputValue, OutputValue},
};

icydb::start!();

const OVERSIZED_SQL_GROUP_NAME_LEN: usize = 1_050_000;

/// Load one deterministic baseline fixture dataset for SQL smoke tests.
#[allow(
    dead_code,
    reason = "fixture load hook is invoked by generated canister endpoint glue"
)]
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    db()?.execute_trusted_structural_insert_batch("SqlTestUser", sql_user_patches())?;
    db()?.execute_trusted_structural_insert_batch(
        "SqlTestNumericTypes",
        sql_numeric_type_patches(),
    )?;

    Ok(())
}

/// Build one deterministic baseline SQL user fixture batch.
#[allow(
    dead_code,
    reason = "fixture rows are consumed through the generated fixture load hook"
)]
fn sql_user_patches() -> Vec<StructuralPatch> {
    vec![
        sql_user_patch("alice", 31, 28),
        sql_user_patch("bob", 24, 25),
        sql_user_patch("charlie", 43, 43),
    ]
}

fn sql_user_patch(name: &str, age: i32, rank: i32) -> StructuralPatch {
    StructuralPatch::new()
        .field("name", WriteCell::Value(InputValue::Text(name.to_string())))
        .field("age", WriteCell::Value(InputValue::from(age)))
        .field("rank", WriteCell::Value(InputValue::from(rank)))
}

/// Seed one runtime-built oversized unindexed payload for generated endpoint
/// response-budget tests without embedding a megabyte literal in the wasm.
#[update]
fn seed_oversized_sql_group_name() -> Result<(), icydb::Error> {
    let session = db()?;
    let query = DynamicQuery::new("SqlTestNumericTypes")
        .filter(FieldRef::new("label").eq("alpha"))
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
#[allow(
    dead_code,
    reason = "fixture rows are consumed through the generated fixture load hook"
)]
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

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
