mod db;
mod filter;
mod index;
mod merge;
mod metrics;
mod ops;
mod view_into;

use canic::{cdk::export_candid, prelude::*};
use icydb::{Error, design::prelude::*};
use test_design::{
    e2e::filter::{Filterable, FilterableView},
    schema::{TestDataStore, TestIndexStore},
};

//
// INIT
//

icydb::start!();

pub static WASMS: &[(CanisterType, &[u8])] = &[];

///
/// ENDPOINTS
///

pub fn clear_test_data_store() {
    // clear before each test
    crate::DATA_REGISTRY.with(|reg| {
        let _ = reg.with_store_mut(TestDataStore::PATH, |s| s.clear());
    });
    crate::INDEX_REGISTRY.with(|reg| {
        let _ = reg.with_store_mut(TestIndexStore::PATH, |s| s.clear());
    });
}

// test
#[update]
pub fn test() {
    let tests: Vec<(&str, fn())> = vec![
        ("db", db::DbSuite::test),
        ("index", index::IndexSuite::test),
        ("ops", ops::OpsSuite::test),
        ("metrics", metrics::MetricsSuite::test),
        ("merge", merge::MergeSuite::test),
        ("view_into", view_into::ViewIntoSuite::test),
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

    perf_start!();

    log!(Ok, "test: all tests passed successfully");
}

//
// ENDPOINTS
//

// filterable
#[query]
pub fn filterable() -> Result<Vec<FilterableView>, Error> {
    let res = db!(debug).load::<Filterable>().all()?.entities().to_view();

    Ok(res)
}

export_candid!();
