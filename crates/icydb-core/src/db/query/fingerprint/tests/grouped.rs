use super::*;

#[test]
fn explain_fingerprint_grouped_strategy_only_change_does_not_invalidate() {
    let mut hash_strategy = grouped_explain_with_fixed_shape();
    let mut ordered_strategy = hash_strategy.clone();

    let ExplainGrouping::Grouped {
        strategy: hash_value,
        ..
    } = &mut hash_strategy.grouping
    else {
        panic!("grouped explain fixture must produce grouped explain shape");
    };
    *hash_value = "hash_group";
    let ExplainGrouping::Grouped {
        strategy: ordered_value,
        ..
    } = &mut ordered_strategy.grouping
    else {
        panic!("grouped explain fixture must produce grouped explain shape");
    };
    *ordered_value = "ordered_group";

    assert_eq!(
        hash_strategy.fingerprint(),
        ordered_strategy.fingerprint(),
        "execution strategy hints are explain/runtime metadata and must not affect semantic fingerprint identity",
    );
}

#[test]
fn grouped_fingerprint_identity_projection_remains_stable() {
    let plan = grouped_query_with_fixed_shape();
    let identity_projection = plan.projection_spec_for_identity();

    assert_eq!(
        plan.fingerprint().as_hex(),
        encode_cursor(&fingerprint_with_projection(&plan, &identity_projection)),
        "grouped fingerprint identity must stay stable across plan-owned and explain-owned grouped projection seams",
    );
}
