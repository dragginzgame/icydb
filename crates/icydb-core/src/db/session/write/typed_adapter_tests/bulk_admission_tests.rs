//! Canonical byte admission and stage-qualified retry authority.

use super::*;
use crate::db::{DynamicMutationResult, DynamicTypedBatchAttempt, data::DecodedDataStoreKey};
use crate::value::Value;
use std::sync::Arc;

fn attempt(
    session: &DbSession<TestCanister>,
    binding: &DynamicTypedEntityBinding,
    staged: usize,
    result: usize,
) -> Result<DynamicTypedBatchAttempt, crate::error::InternalError> {
    session
        .attempt_trusted_typed_mutation_batch_shared(
            vec![(Arc::new(binding.clone()), typed_insert(binding, 1, 10))],
            staged,
            result,
        )
        .map(|outcome| outcome.expect("current binding"))
}

#[test]
fn bulk_admission_uses_exact_canonical_staged_and_result_bytes() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .unwrap();
    let expected: Vec<DynamicMutationResult> = session
        .execute_trusted_typed_mutation_batch(vec![(
            binding.clone(),
            typed_insert(&binding, 1, 10),
        )])
        .unwrap()
        .unwrap();
    let encoded = icydb_schema::wire::encoded_reply_len(&expected).unwrap();
    let key = DecodedDataStoreKey::try_from_structural_key(
        EntityTag::new(binding.entity_tag),
        &Value::Nat64(1),
    )
    .unwrap()
    .to_raw()
    .unwrap();
    let staged =
        DATA_STORE.with(|store| key.as_bytes().len() + store.borrow().get(&key).unwrap().len());

    session
        .execute_trusted_typed_mutation(
            &binding,
            DynamicTypedMutation::Delete {
                key: InputValue::nat64(1),
            },
        )
        .unwrap();

    // Exact inclusive limits commit. One byte below either canonical size
    // refuses without publishing even the first row.
    for (staged_limit, result_limit, admitted) in [
        (staged, encoded, true),
        (staged - 1, encoded, false),
        (staged, encoded - 1, false),
    ] {
        let before = crate::db::commit::database_control_proof_identity().unwrap();
        let outcome = attempt(&session, &binding, staged_limit, result_limit).unwrap();
        match outcome {
            DynamicTypedBatchAttempt::Committed(results) => {
                assert!(admitted);
                assert_eq!(results, expected);
            }
            DynamicTypedBatchAttempt::PrecommitByteRefusal(_) => {
                assert!(!admitted);
                assert_eq!(
                    crate::db::commit::database_control_proof_identity().unwrap(),
                    before
                );
            }
        }
        DATA_STORE.with(|store| assert_eq!(store.borrow().len(), u64::from(admitted)));
        if admitted {
            session
                .execute_trusted_typed_mutation(
                    &binding,
                    DynamicTypedMutation::Delete {
                        key: InputValue::nat64(1),
                    },
                )
                .unwrap();
        }
    }
}

#[test]
fn bulk_admission_does_not_turn_constraint_failures_into_retry_authority() {
    let session = initialize_typed_session();
    let binding = session
        .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
        .unwrap();
    assert!(matches!(
        attempt(&session, &binding, usize::MAX, usize::MAX).unwrap(),
        DynamicTypedBatchAttempt::Committed(_)
    ));
    // Duplicate-key refusal is an ordinary database error, even with stricter
    // byte limits. The adapter must not try a smaller batch for it.
    assert!(attempt(&session, &binding, 1, 1).is_err());
    DATA_STORE.with(|store| assert_eq!(store.borrow().len(), 1));
}
