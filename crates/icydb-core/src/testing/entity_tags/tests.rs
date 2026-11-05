use crate::testing::entity_tags::ALL_TEST_ENTITY_TAGS;
use std::collections::BTreeSet;

#[test]
fn test_entity_tags_are_unique() {
    let unique = ALL_TEST_ENTITY_TAGS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    assert_eq!(unique.len(), ALL_TEST_ENTITY_TAGS.len());
}
