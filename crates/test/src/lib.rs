mod delete_unique;
mod ops;
mod view_into;

use canic_cdk::{export_candid, update};
use icydb::design::prelude::*;
use test_design::schema::{TestCanister, TestDataStore, TestIndexStore};

//
// INIT
//

icydb::start!();

///
/// ENDPOINTS
///

/// Clear all test stores between suite runs.
pub(crate) fn clear_test_data_store() {
    // clear before each test
    crate::DATA_REGISTRY.with(|reg| {
        let _ = reg.with_store_mut(TestDataStore::PATH, |s| s.clear());
    });
    crate::INDEX_REGISTRY.with(|reg| {
        let _ = reg.with_store_mut(TestIndexStore::PATH, |s| s.clear());
    });
}

///
/// Core session access
/// Internal-only helper for executor-focused tests.
/// Bypasses the public facade boundary.
///

pub(crate) const fn core_db() -> icydb::__internal::core::db::DbSession<TestCanister> {
    icydb::__internal::core::db::DbSession::new(crate::DB)
}

/// test
/// Entrypoint that runs the full end-to-end test suite in canister mode.
#[update]
pub fn test() {
    let tests: Vec<(&str, fn())> = vec![
        ("ops", ops::OpsSuite::test),
        ("view_into", view_into::ViewIntoSuite::test),
        ("delete_unique", delete_unique::DeleteUniqueSuite::test),
    ];

    // run tests
    for (name, test_fn) in tests {
        clear_test_data_store();

        println!("Running test: {name}");
        test_fn();
    }

    println!("test: all tests passed successfully");
}

export_candid!();
