use super::*;
use crate::db::{
    index::{IndexId, IndexKeyKind},
    key_taxonomy::PrimaryKeyComponent,
};

fn test_index_key(component: u8, primary_key: &PrimaryKeyValue) -> IndexKey {
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(EntityTag::new(73), 1),
        IndexKeyKind::User,
        &[vec![component]],
        primary_key,
    )
    .expect("test index key should encode")
}

#[test]
fn unchanged_index_membership_emits_no_commit_delta_group() {
    let primary_key = PrimaryKeyValue::from(PrimaryKeyComponent::Int64(41));
    let key = test_index_key(7, &primary_key);
    let mut groups = Vec::new();

    push_index_delta_group(
        &mut groups,
        "store",
        Some(key.clone()),
        Some(key),
        Some(&primary_key),
        Some(&primary_key),
    )
    .expect("unchanged membership should remain valid");

    assert!(groups.is_empty());
}

#[test]
fn changed_index_membership_retains_remove_and_insert_deltas() {
    let primary_key = PrimaryKeyValue::from(PrimaryKeyComponent::Int64(41));
    let mut groups = Vec::new();

    push_index_delta_group(
        &mut groups,
        "store",
        Some(test_index_key(7, &primary_key)),
        Some(test_index_key(8, &primary_key)),
        Some(&primary_key),
        Some(&primary_key),
    )
    .expect("changed membership should remain valid");

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].deltas.len(), 2);
}
