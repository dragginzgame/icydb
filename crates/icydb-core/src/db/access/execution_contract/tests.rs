//! Module: db::access::execution_contract::tests
//! Responsibility: module-local ownership and contracts for db::access::execution_contract::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::access::{AccessPlan, AccessStrategy},
    model::index::IndexModel,
    value::Value,
};

const INDEX_MULTI_LOOKUP_TEST_FIELDS: [&str; 1] = ["group"];

#[test]
fn access_strategy_debug_summary_reports_scalar_path_shape() {
    let plan = AccessPlan::by_key(7u64);
    let strategy = AccessStrategy::from_plan(&plan);

    assert_eq!(
        strategy.debug_summary(),
        "IndexLookup(pk=7)",
        "single-key strategies should render concise path summaries",
    );
}

#[test]
fn access_strategy_debug_summary_reports_composite_shape() {
    let plan = AccessPlan::union(vec![AccessPlan::by_key(1u64), AccessPlan::by_key(2u64)]);
    let strategy = AccessStrategy::from_plan(&plan);
    let summary = strategy.debug_summary();

    assert!(
        summary.starts_with("Union("),
        "composite strategies should render union summary headings",
    );
    assert!(
        summary.contains("IndexLookup(pk=1)") && summary.contains("IndexLookup(pk=2)"),
        "composite strategy summaries should include child path summaries",
    );
    assert!(
        format!("{strategy:?}").contains("summary"),
        "debug output should include the summarized route label",
    );
}

#[test]
fn access_strategy_debug_summary_reports_index_multi_lookup_shape() {
    let index = IndexModel::new(
        "tests::idx_group",
        "tests::store",
        &INDEX_MULTI_LOOKUP_TEST_FIELDS,
        false,
    );
    let plan: AccessPlan<u64> =
        AccessPlan::index_multi_lookup(index, vec![Value::Uint(7), Value::Uint(9)]);
    let strategy = AccessStrategy::from_plan(&plan);

    assert!(
        strategy.debug_summary().contains("IndexMultiLookup"),
        "index multi-lookup strategies should render dedicated path summaries",
    );
}
