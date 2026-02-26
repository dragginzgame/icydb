use super::*;

#[test]
fn route_feature_budget_no_eligibility_helpers_outside_route_module() {
    let aggregate_source = include_str!("../../load/aggregate/mod.rs");
    let aggregate_fast_path_source = include_str!("../../kernel/aggregate.rs");
    let execute_source = include_str!("../../load/execute.rs");
    let index_range_limit_source = include_str!("../../load/index_range_limit.rs");
    let page_source = include_str!("../../load/page.rs");
    let pk_stream_source = include_str!("../../load/pk_stream.rs");
    let secondary_index_source = include_str!("../../load/secondary_index.rs");
    let mod_source = include_str!("mod.rs");

    assert_no_eligibility_helper_defs("aggregate/mod.rs", aggregate_source);
    assert_no_eligibility_helper_defs("kernel/aggregate.rs", aggregate_fast_path_source);
    assert_no_eligibility_helper_defs("execute.rs", execute_source);
    assert_no_eligibility_helper_defs("index_range_limit.rs", index_range_limit_source);
    assert_no_eligibility_helper_defs("page.rs", page_source);
    assert_no_eligibility_helper_defs("pk_stream.rs", pk_stream_source);
    assert_no_eligibility_helper_defs("secondary_index.rs", secondary_index_source);
    assert_no_eligibility_helper_defs("mod.rs", mod_source);
}
