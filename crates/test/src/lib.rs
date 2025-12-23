mod db;
mod delete_unique;
mod filter;
mod index;
mod load_executor;
mod merge;
mod metrics;
mod ops;
mod query_planner;
mod upsert;
mod view_into;

use canic_cdk::{export_candid, query, update};
use icydb::core::db::response::ResponseExt as _;
use icydb::{Error, design::prelude::*};
use test_design::{
    e2e::filter::{Filterable, FilterableView},
    schema::{TestDataStore, TestIndexStore},
};

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

/// test
/// Entrypoint that runs the full end-to-end test suite in canister mode.
#[update]
pub fn test() {
    let tests: Vec<(&str, fn())> = vec![
        ("db", db::DbSuite::test),
        ("index", index::IndexSuite::test),
        ("ops", ops::OpsSuite::test),
        ("metrics", metrics::MetricsSuite::test),
        ("merge", merge::MergeSuite::test),
        ("view_into", view_into::ViewIntoSuite::test),
        ("upsert", upsert::UpsertSuite::test),
        ("delete_unique", delete_unique::DeleteUniqueSuite::test),
        ("query_planner", query_planner::QueryPlannerSuite::test),
        ("load_executor", load_executor::LoadExecutorSuite::test),
        // filter
        ("delete_filter", filter::delete::DeleteFilterSuite::test),
        ("index_filter", filter::index::IndexFilterSuite::test),
        ("load_filter", filter::load::LoadFilterSuite::test),
    ];

    // run tests
    for (name, test_fn) in tests {
        clear_test_data_store();

        println!("Running test: {name}");
        test_fn();
    }

    println!("test: all tests passed successfully");
}

//
// ENDPOINTS
//

/// filterable
/// Return all `Filterable` entities mapped into the `FilterableView`.
#[query]
pub fn filterable() -> Result<Vec<FilterableView>, Error> {
    let res = db!()
        .debug()
        .load::<Filterable>()
        .all()
        .entities()?
        .to_view();

    Ok(res)
}

export_candid!();
