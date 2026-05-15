//!
//! Small SQL canister used for lightweight SQL fixture smoke tests.
//!

extern crate canic_cdk as ic_cdk;

use icydb::types::{Decimal, Float32, Float64};
use icydb_testing_test_sql_fixtures::sql::{SqlTestNumericTypes, SqlTestUser};

icydb::start!();
icydb::admin_sql_query!();

/// Load one deterministic baseline fixture dataset for SQL smoke tests.
fn icydb_admin_sql_load_default() -> Result<(), icydb::Error> {
    db().insert_many_atomic(sql_users())?;
    db().insert_many_atomic(sql_numeric_type_rows())?;

    Ok(())
}

/// Build one deterministic baseline SQL user fixture batch.
fn sql_users() -> Vec<SqlTestUser> {
    vec![
        SqlTestUser {
            name: "alice".to_string(),
            age: 31,
            rank: 28,
            ..Default::default()
        },
        SqlTestUser {
            name: "bob".to_string(),
            age: 24,
            rank: 25,
            ..Default::default()
        },
        SqlTestUser {
            name: "charlie".to_string(),
            age: 43,
            rank: 43,
            ..Default::default()
        },
    ]
}

/// Build one deterministic mixed numeric fixture batch for SQL type coverage.
fn sql_numeric_type_rows() -> Vec<SqlTestNumericTypes> {
    vec![
        SqlTestNumericTypes {
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
            ..Default::default()
        },
        SqlTestNumericTypes {
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
            ..Default::default()
        },
    ]
}

canic_cdk::export_candid!();
