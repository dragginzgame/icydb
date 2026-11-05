use super::IndexPrefixChildExpansionBudget;

#[test]
fn prefix_child_count_accepts_maximum_and_rejects_next() {
    assert!(
        IndexPrefixChildExpansionBudget::validate_child_count(
            IndexPrefixChildExpansionBudget::MAX_PREFIXES,
        )
        .is_ok()
    );
    assert!(
        IndexPrefixChildExpansionBudget::validate_child_count(
            IndexPrefixChildExpansionBudget::MAX_PREFIXES + 1,
        )
        .is_err()
    );
}
