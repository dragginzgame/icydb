//! Complete-domain proof retains unchanged entries while emitting exact effects.

use super::*;

fn qualify_delta(
    before_indexes: Vec<PersistedIndexSnapshot>,
    after_indexes: Vec<PersistedIndexSnapshot>,
    expected_effects: (usize, usize),
) {
    let before = snapshot_with_indexes(&base_snapshot(), before_indexes.clone());
    let after = snapshot_with_indexes(&before, after_indexes.clone());
    let contract = accepted_row_contract(&before);
    let slots = ["a", "Ada", "UPPER", "z"].map(|text| RebuildSlotReader {
        values: vec![None, Some(Value::Text(text.into()))],
    });
    let rows = slots
        .iter()
        .enumerate()
        .map(|(index, row)| (u64::try_from(index).unwrap() + 1, row))
        .collect::<Vec<_>>();
    let mut actual = IndexStore::init_heap();
    let mut expected = IndexStore::init_heap();
    insert_projection_entries(&mut actual, &before_indexes, &rows, EntityTag::new(7));
    // The oracle constructs the complete accepted-after domain, independent of
    // how staging chooses the physical delta from its complete before proof.
    insert_projection_entries(&mut expected, &after_indexes, &rows, EntityTag::new(7));
    for key in [
        unrelated_key(EntityTag::new(7), IndexKeyKind::System, 9, 91),
        unrelated_key(EntityTag::new(8), IndexKeyKind::User, 1, 92),
    ] {
        actual
            .insert(key.clone(), IndexEntryValue::presence())
            .expect("fixture storage operation should succeed");
        expected
            .insert(key, IndexEntryValue::presence())
            .expect("fixture storage operation should succeed");
    }
    let original = index_store_entries(&actual);
    let staged = stage_domain(
        accepted_identity(&before),
        &before,
        &after,
        Some(&contract),
        rows.iter().map(|(key, row)| domain_row(*key, row)),
        &actual,
    )
    .map_err(super::super::StagedUserIndexDomainError::into_internal_error)
    .unwrap();
    assert_eq!(index_store_entries(&actual), original);
    assert_eq!(
        (
            staged.deletion_keys().len(),
            staged.insertion_entries().len()
        ),
        expected_effects
    );
    assert_eq!(staged.usage().source_rows(), 4);
    assert_eq!(
        staged.usage().accepted_before_entries(),
        before_indexes.len() * 4
    );
    assert_eq!(
        staged.usage().accepted_after_entries(),
        after_indexes.len() * 4
    );
    let (deletions, insertions) = staged.into_apply_parts();
    assert!(deletions.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        insertions
            .windows(2)
            .all(|pair| pair[0].key() < pair[1].key())
    );
    for key in deletions {
        assert_eq!(
            (actual.remove(&key)).expect("storage operation should succeed"),
            Some(IndexEntryValue::presence())
        );
    }
    for entry in insertions {
        let (key, value) = entry.into_parts();
        assert_eq!(
            (actual.insert(key, value)).expect("storage operation should succeed"),
            None
        );
    }
    assert_eq!(index_store_entries(&actual), index_store_entries(&expected));
}

#[test]
fn unchanged_field_expression_and_unique_domains_emit_no_index_effects() {
    for unique in [false, true] {
        let indexes = vec![
            domain_field_index(1, "by_name", unique),
            domain_expression_index(2, "by_lower_name", unique, None),
        ];
        qualify_delta(indexes.clone(), indexes, (0, 0));
    }
}

#[test]
fn create_and_drop_leave_the_unmodified_index_in_place() {
    let field = domain_field_index(1, "by_name", false);
    let expression = domain_expression_index(2, "by_lower_name", false, None);
    qualify_delta(
        vec![field.clone()],
        vec![field.clone(), expression.clone()],
        (0, 4),
    );
    qualify_delta(vec![field.clone(), expression], vec![field], (4, 0));
}

#[test]
fn changed_keys_and_common_keys_merge_in_both_directions() {
    let field = domain_field_index(1, "by_name", false);
    let lower = domain_expression_index(1, "by_lower_name", false, None);
    qualify_delta(vec![field.clone()], vec![lower.clone()], (2, 2));
    qualify_delta(vec![lower], vec![field], (2, 2));
}

#[test]
fn wholly_added_or_removed_domains_keep_the_full_required_effects() {
    let field = domain_field_index(1, "by_name", false);
    qualify_delta(Vec::new(), vec![field.clone()], (0, 4));
    qualify_delta(vec![field], Vec::new(), (4, 0));
}

#[test]
fn unchanged_domain_still_refuses_a_missing_physical_entry() {
    let index = domain_field_index(1, "by_name", false);
    let snapshot = snapshot_with_indexes(&base_snapshot(), vec![index.clone()]);
    let row = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".into()))],
    };
    let mut store = IndexStore::init_heap();
    insert_projection_entries(&mut store, &[index], &[(1, &row)], EntityTag::new(7));
    let key = index_store_entries(&store)[0].0.clone();
    store
        .remove(&key)
        .expect("fixture storage operation should succeed");
    let error = stage_domain(
        accepted_identity(&snapshot),
        &snapshot,
        &snapshot,
        None,
        [domain_row(1, &row)],
        &store,
    )
    .expect_err("an unchanged catalog must still prove its physical domain")
    .into_internal_error();
    assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
    assert!((store.is_empty()).expect("fixture storage read should succeed"));
}
