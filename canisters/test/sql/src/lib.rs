//!
//! Small SQL canister used for lightweight SQL fixture smoke tests.
//!

use ic_cdk::update;
use icydb::types::{Decimal, Float32, Float64, Timestamp, Ulid};
use icydb::{db::MutationMode, value::InputValue};
use icydb_testing_test_sql_fixtures::sql::{SqlTestNumericTypes, SqlTestUser};

icydb::start!();

const OVERSIZED_SQL_GROUP_NAME_LEN: usize = 1_050_000;

/// Load one deterministic baseline fixture dataset for SQL smoke tests.
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    db().insert_many_atomic(sql_users())?;
    db().insert_many_atomic(sql_numeric_type_rows())?;

    Ok(())
}

/// Build one deterministic baseline SQL user fixture batch.
fn sql_users() -> Vec<SqlTestUser> {
    vec![
        SqlTestUser {
            id: Ulid::generate(),
            name: "alice".to_string(),
            age: 31,
            rank: 28,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        SqlTestUser {
            id: Ulid::generate(),
            name: "bob".to_string(),
            age: 24,
            rank: 25,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        SqlTestUser {
            id: Ulid::generate(),
            name: "charlie".to_string(),
            age: 43,
            rank: 43,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
    ]
}

/// Seed one runtime-built oversized unindexed payload for generated endpoint
/// response-budget tests without embedding a megabyte literal in the wasm.
#[update]
fn seed_oversized_sql_group_name() -> Result<(), icydb::Error> {
    let alpha = db()
        .load::<SqlTestNumericTypes>()
        .filter_eq("label", "alpha")
        .entity()?;
    let group_name = "x".repeat(OVERSIZED_SQL_GROUP_NAME_LEN);
    let patch = db().structural_patch::<SqlTestNumericTypes, _, _>([(
        "group_name",
        InputValue::from(group_name),
    )])?;

    db().mutate_structural::<SqlTestNumericTypes>(alpha.id, patch, MutationMode::Update)?;

    Ok(())
}

/// Build one deterministic mixed numeric fixture batch for SQL type coverage.
fn sql_numeric_type_rows() -> Vec<SqlTestNumericTypes> {
    vec![
        SqlTestNumericTypes {
            id: Ulid::generate(),
            label: "alpha".to_string(),
            group_name: "mage".to_string(),
            int8_value: -1,
            int16_value: -2,
            int32_value: 35,
            int64_value: -500,
            nat8_value: 14,
            nat16_value: 3,
            nat32_value: 120,
            nat64_value: 1_000,
            decimal_value: Decimal::new(15, 2),
            float32_value: Float32::try_new(0.75).expect("finite float32 fixture value"),
            float64_value: Float64::try_new(0.50).expect("finite float64 fixture value"),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
        SqlTestNumericTypes {
            id: Ulid::generate(),
            label: "beta".to_string(),
            group_name: "fighter".to_string(),
            int8_value: 2,
            int16_value: 5,
            int32_value: 58,
            int64_value: 9_000,
            nat8_value: 16,
            nat16_value: 7,
            nat32_value: 300,
            nat64_value: 9_000,
            decimal_value: Decimal::new(25, 2),
            float32_value: Float32::try_new(0.25).expect("finite float32 fixture value"),
            float64_value: Float64::try_new(0.25).expect("finite float64 fixture value"),
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        },
    ]
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
