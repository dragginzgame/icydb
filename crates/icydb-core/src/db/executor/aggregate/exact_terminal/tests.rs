use crate::{
    db::index::{IndexId, UserIndexPrefixCardinalityKey},
    types::EntityTag,
};

use super::common_prefix_cardinality_index_id;
#[cfg(feature = "sql")]
use super::exact_distinct_success_budget;

#[test]
fn exact_count_rejects_prefix_keys_from_mixed_index_generations() {
    let entity_tag = EntityTag::new(0xCA7D);
    let current_index = IndexId::new_with_generation(entity_tag, 2, 7);
    let next_index = IndexId::new_with_generation(entity_tag, 2, 8);
    let current = UserIndexPrefixCardinalityKey::new(current_index, vec![b"collection-a".to_vec()]);
    let same_generation =
        UserIndexPrefixCardinalityKey::new(current_index, vec![b"collection-b".to_vec()]);
    let next_generation =
        UserIndexPrefixCardinalityKey::new(next_index, vec![b"collection-c".to_vec()]);

    assert_eq!(
        common_prefix_cardinality_index_id(&[current.clone(), same_generation]),
        Some(current_index),
    );
    assert_eq!(
        common_prefix_cardinality_index_id(&[current, next_generation]),
        None,
    );
}

#[test]
#[cfg(feature = "sql")]
fn exact_distinct_budget_overflow_is_an_invariant_failure() {
    assert!(exact_distinct_success_budget(u64::MAX).is_err());
}
