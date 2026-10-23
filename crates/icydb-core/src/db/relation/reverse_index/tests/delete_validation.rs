//! Completed source checks are exact, bounded batch facts, never cached errors.

use super::*;
use crate::db::data::DecodedDataStoreKey;

#[test]
fn delete_source_validation_facts_keep_complete_identity_and_do_not_cache_errors() {
    let mut budget = RelationCommitBudget::default();
    let mut checks = 0;
    let keys = [
        PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7)),
        PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(&[
                PrimaryKeyComponent::Nat64(7),
                PrimaryKeyComponent::Nat64(8),
            ])
            .unwrap(),
        ),
        PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(&[
                PrimaryKeyComponent::Nat64(7),
                PrimaryKeyComponent::Nat64(9),
            ])
            .unwrap(),
        ),
    ];
    for entity in [7, u64::MAX] {
        for relation in [1, u32::MAX] {
            let relation = RelationId::new(relation).unwrap();
            for key in keys {
                let raw = DecodedDataStoreKey::new(EntityTag::new(entity), &key)
                    .to_raw()
                    .unwrap();
                budget
                    .validate_delete_source_once(relation, raw.clone(), |_| {
                        checks += 1;
                        Ok(())
                    })
                    .unwrap();
                budget
                    .validate_delete_source_once(relation, raw, |_| {
                        panic!("completed check must be reused")
                    })
                    .unwrap();
            }
        }
    }
    assert_eq!(checks, 12);
    assert_eq!(budget.validated_delete_sources.len(), 12);
    assert_eq!(budget.reverse_deltas, 0);

    let mut budget = RelationCommitBudget::default();
    let relation = RelationId::new(1).unwrap();
    let key = DecodedDataStoreKey::new(EntityTag::new(7), &keys[0])
        .to_raw()
        .unwrap();
    let error = budget
        .validate_delete_source_once(relation, key.clone(), |_| {
            Err(crate::error::InternalError::index_corruption())
        })
        .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        crate::error::InternalError::index_corruption().diagnostic_code()
    );
    assert!(budget.validated_delete_sources.is_empty());
    budget
        .validate_delete_source_once(relation, key, |_| Ok(()))
        .unwrap();
    assert_eq!(budget.validated_delete_sources.len(), 1);
}

#[test]
fn delete_source_validation_facts_share_the_existing_reverse_effect_ceiling() {
    let mut budget = RelationCommitBudget::default();
    let relation = RelationId::new(1).unwrap();
    for id in 0..MAX_RELATION_BATCH_REVERSE_DELTAS {
        let key = DecodedDataStoreKey::new(
            EntityTag::new(7),
            &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(id)),
        )
        .to_raw()
        .unwrap();
        budget
            .validate_delete_source_once(relation, key, |_| Ok(()))
            .unwrap();
    }
    let duplicate = DecodedDataStoreKey::new(
        EntityTag::new(7),
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(0)),
    )
    .to_raw()
    .unwrap();
    budget
        .validate_delete_source_once(relation, duplicate, |_| {
            panic!("a hit at the cap must not revalidate")
        })
        .unwrap();
    let next = DecodedDataStoreKey::new(
        EntityTag::new(7),
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(
            MAX_RELATION_BATCH_REVERSE_DELTAS,
        )),
    )
    .to_raw()
    .unwrap();
    let error = budget
        .validate_delete_source_once(relation, next, |_| {
            panic!("refuse before validation or growth")
        })
        .unwrap_err();
    let expected = crate::error::InternalError::relation_budget_exceeded(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        MAX_RELATION_BATCH_REVERSE_DELTAS,
        MAX_RELATION_BATCH_REVERSE_DELTAS + 1,
    );
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    assert_eq!(
        budget.validated_delete_sources.len() as u64,
        MAX_RELATION_BATCH_REVERSE_DELTAS
    );
    assert_eq!(budget.reverse_deltas, 0);
}
