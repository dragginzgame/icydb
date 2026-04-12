//!
//! Small SQL canister used for lightweight SQL fixture smoke tests.
//!

extern crate canic_cdk as ic_cdk;

use canic_cdk::update;
use icydb_testing_test_sql_fixtures::sql::SqlTestUser;

icydb::start!();

/// Clear all lightweight SQL smoke-test fixture rows from this canister.
#[update]
fn fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<SqlTestUser>().execute()?;

    Ok(())
}

/// Load one deterministic baseline fixture dataset for SQL smoke tests.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;
    db().insert_many_atomic(sql_users())?;

    Ok(())
}

/// Build one deterministic baseline SQL user fixture batch.
fn sql_users() -> Vec<SqlTestUser> {
    vec![
        SqlTestUser {
            name: "alice".to_string(),
            age: 31,
            ..Default::default()
        },
        SqlTestUser {
            name: "bob".to_string(),
            age: 24,
            ..Default::default()
        },
        SqlTestUser {
            name: "charlie".to_string(),
            age: 43,
            ..Default::default()
        },
    ]
}

canic_cdk::export_candid!();
