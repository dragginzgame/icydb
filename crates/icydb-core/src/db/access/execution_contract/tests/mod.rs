//! Module: db::access::execution_contract::tests
//! Covers execution-contract summaries and pushdown decisions derived from
//! lowered executable access plans.

use crate::{
    db::access::{AccessPlan, SemanticIndexAccessContract, summarize_executable_access_plan},
    model::index::IndexModel,
    value::Value,
};

const INDEX_MULTI_LOOKUP_TEST_FIELDS: [&str; 1] = ["group"];

#[test]
fn executable_access_summary_reports_scalar_path_shape() {
    let plan = AccessPlan::by_key(7u64);
    let executable = plan.executable_contract();

    assert_eq!(
        summarize_executable_access_plan(&executable),
        "IndexLookup(pk=7)",
        "single-key executable access should render concise path summaries",
    );
}

#[test]
fn executable_access_summary_reports_composite_shape() {
    let plan = AccessPlan::union(vec![AccessPlan::by_key(1u64), AccessPlan::by_key(2u64)]);
    let executable = plan.executable_contract();
    let summary = summarize_executable_access_plan(&executable);

    assert!(
        summary.starts_with("Union("),
        "composite executable access should render union summary headings",
    );
    assert!(
        summary.contains("IndexLookup(pk=1)") && summary.contains("IndexLookup(pk=2)"),
        "composite executable access summaries should include child path summaries",
    );
}

#[test]
fn executable_access_summary_reports_index_multi_lookup_shape() {
    let index = IndexModel::generated(
        "tests::idx_group",
        "tests::store",
        &INDEX_MULTI_LOOKUP_TEST_FIELDS,
        false,
    );
    let plan: AccessPlan<u64> = AccessPlan::index_multi_lookup_from_contract(
        SemanticIndexAccessContract::model_only_from_generated_index(index),
        vec![Value::Nat(7), Value::Nat(9)],
    );
    let executable = plan.executable_contract();

    assert!(
        summarize_executable_access_plan(&executable).contains("IndexMultiLookup"),
        "index multi-lookup executable access should render dedicated path summaries",
    );
}
