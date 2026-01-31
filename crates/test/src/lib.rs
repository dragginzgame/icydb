mod ops;
mod view_into;

use canic_cdk::{export_candid, update};
use icydb::design::prelude::*;

//
// INIT
//

icydb::start!();

///
/// ENDPOINTS
///

/// Clear all test stores between suite runs.
pub(crate) fn clear_test_data_store() {
    crate::db().clear_stores_for_tests();
}

/// test
/// Entrypoint that runs the full end-to-end test suite in canister mode.
#[update]
pub fn test() {
    let tests: Vec<(&str, fn())> = vec![
        ("ops", ops::OpsSuite::test),
        ("view_into", view_into::ViewIntoSuite::test),
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
