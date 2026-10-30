//! Admission precedence across dynamic and typed mixed-batch callers.

use super::*;
use crate::{
    db::{DynamicMutation, DynamicStructuralPatch},
    error::InternalError,
};

fn assert_same_diagnostic(actual: &InternalError, expected: &InternalError) {
    assert_eq!(actual.diagnostic(), expected.diagnostic());
    assert_eq!(actual.diagnostic_facts(), expected.diagnostic_facts());
}

#[test]
fn batch_shape_refusal_precedes_invalid_names_and_stale_bindings() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("shape fixture should bind");
    let mutation = typed_insert(&binding, 1, 10);
    let mut stale = binding;
    stale.accepted_revision = stale.accepted_revision.saturating_add(1);
    let invalid_name = DynamicMutation::Delete {
        entity: String::new(),
        key: InputValue::nat64(1),
    };
    let maximum = super::super::MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS;

    for (count, expected) in [
        (0, InternalError::mutation_batch_empty()),
        (
            maximum + 1,
            InternalError::mutation_batch_too_many_items(maximum + 1, maximum),
        ),
    ] {
        let dynamic = session
            .execute_trusted_dynamic_mutation_batch(vec![invalid_name.clone(); count])
            .expect_err("shape refusal must precede dynamic lookup");
        assert_same_diagnostic(&dynamic, &expected);
        let typed = session
            .execute_trusted_typed_mutation_batch(vec![(stale.clone(), mutation.clone()); count])
            .expect_err("shape refusal must precede typed stale-binding resolution");
        assert_same_diagnostic(&typed, &expected);
    }
    DATA_STORE.with(|store| assert_eq!(store.borrow().len(), 0));
}

#[test]
fn store_refusal_precedes_item_lowering_in_both_batch_lanes() {
    let session = initialize_mixed_typed_session(true);
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("first store should bind");
    let other = session
        .issue_typed_entity_binding(&OTHER_ENTITY_DESCRIPTOR)
        .expect("second store should bind");
    let expected =
        InternalError::mutation_batch_store_mismatch(1, binding.entity_tag, other.entity_tag);

    // Both second items also have invalid patches: moving lowering ahead of
    // admission would hide the bounded store-mismatch diagnostic.
    let dynamic = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Delete {
                entity: "Entity".to_string(),
                key: InputValue::nat64(1),
            },
            DynamicMutation::Insert {
                entity: "OtherEntity".to_string(),
                patch: DynamicStructuralPatch::new(vec![(
                    "missing".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(2)),
                )]),
            },
        ])
        .expect_err("dynamic store admission must precede patch lowering");
    assert_same_diagnostic(&dynamic, &expected);
    let typed = session
        .execute_trusted_typed_mutation_batch(vec![
            (binding.clone(), typed_insert(&binding, 1, 10)),
            (other, typed_insert(&binding, 2, 20)),
        ])
        .expect_err("typed store admission must precede mismatched-patch refusal");
    assert_same_diagnostic(&typed, &expected);
    DATA_STORE.with(|store| assert_eq!(store.borrow().len(), 0));
    OTHER_DATA_STORE.with(|store| assert_eq!(store.borrow().len(), 0));
}

#[test]
fn stale_cross_store_binding_declines_before_store_refusal_and_can_retry() {
    let session = initialize_mixed_typed_session(true);
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .expect("first store should bind");
    let mut stale = session
        .issue_typed_entity_binding(&OTHER_ENTITY_DESCRIPTOR)
        .expect("second store should bind");
    stale.accepted_revision = stale.accepted_revision.saturating_add(1);
    let result = session
        .execute_trusted_typed_mutation_batch(vec![
            (binding.clone(), typed_insert(&binding, 1, 10)),
            (stale.clone(), typed_other_insert(&stale, 2)),
        ])
        .expect("stale binding must remain a non-error adapter refusal");
    assert!(result.is_none());
    DATA_STORE.with(|store| assert_eq!(store.borrow().len(), 0));
    OTHER_DATA_STORE.with(|store| assert_eq!(store.borrow().len(), 0));

    let result = session
        .execute_trusted_typed_mutation_batch(vec![(
            binding.clone(),
            typed_insert(&binding, 1, 10),
        )])
        .expect("declined admission must leave a valid retry possible")
        .expect("original same-store binding should remain current");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].affected_rows, 1);
}
