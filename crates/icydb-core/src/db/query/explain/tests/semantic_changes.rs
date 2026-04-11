use super::*;

#[test]
fn explain_differs_for_semantic_changes() {
    let plan_a: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::ByKey(Value::Ulid(Ulid::from_u128(1))),
        MissingRowPolicy::Ignore,
    );
    let plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    assert_ne!(plan_a.explain(), plan_b.explain());
}
