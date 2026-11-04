//! Generated submission identity, separate from entity meaning and plan identity.

use sha2::{Digest, Sha256};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Derive one generated submission key from sealed declaration bytes and an
/// explicit operator-selected attempt. Equal inputs always yield exact replay.
///
/// This is build-time proposal identity, never runtime schema authority. A new
/// attempt permits retry after abort without changing source or plan semantics.
#[doc(hidden)]
#[must_use]
pub fn generated_schema_submission_key(
    fragment: &[u8],
    plan: Option<&[u8]>,
    attempt: u64,
    assignments: &[(&str, &str, Option<&str>)],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"icydb.generated-schema-submission.attempt.v1");
    hasher.update((fragment.len() as u64).to_be_bytes());
    hasher.update(fragment);
    let plan = plan.unwrap_or_default();
    hasher.update((plan.len() as u64).to_be_bytes());
    hasher.update(plan);
    hasher.update(attempt.to_be_bytes());
    // Store assignment and storage mode are source proposal facts too. Runtime
    // target identities remain independently bound by the composed proposal.
    let mut assignments = assignments.to_vec();
    assignments.sort_unstable();
    hasher.update((assignments.len() as u64).to_be_bytes());
    for (entity, store, journal_key) in assignments {
        for value in [entity, store] {
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        hasher.update([u8::from(journal_key.is_some())]);
        if let Some(key) = journal_key {
            hasher.update((key.len() as u64).to_be_bytes());
            hasher.update(key.as_bytes());
        }
    }

    let digest = hasher.finalize();
    let mut key = String::with_capacity(74);
    key.push_str("generated/");
    for byte in digest {
        key.push(char::from(HEX[usize::from(byte >> 4)]));
        key.push(char::from(HEX[usize::from(byte & 15)]));
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempts_are_distinct_bounded_and_repeatable() {
        let key = generated_schema_submission_key(b"source", Some(b"plan"), 0, &[]);
        assert_eq!(
            key,
            generated_schema_submission_key(b"source", Some(b"plan"), 0, &[])
        );
        for different in [
            generated_schema_submission_key(b"source", Some(b"plan"), 1, &[]),
            generated_schema_submission_key(b"sources", Some(b"plan"), 0, &[]),
            generated_schema_submission_key(b"source", Some(b"plans"), 0, &[]),
            generated_schema_submission_key(b"source", None, 0, &[]),
            generated_schema_submission_key(b"source", Some(b"plan"), u64::MAX, &[]),
        ] {
            assert_ne!(key, different);
            assert!(crate::SchemaSubmissionKey::try_new(different).is_ok());
        }
    }
    #[test]
    fn assignments_and_modes_bind_readiness_but_input_order_does_not() {
        let bindings = [("E", "StoreA", Some("key_a")), ("F", "StoreB", None)];
        let key = generated_schema_submission_key(b"source", None, 0, &bindings);
        assert_eq!(
            key,
            generated_schema_submission_key(b"source", None, 0, &[bindings[1], bindings[0]])
        );
        for changed in [
            [("E", "StoreB", Some("key_a")), bindings[1]],
            [("E", "StoreA", None), bindings[1]],
            [("E", "StoreA", Some("key_b")), bindings[1]],
        ] {
            assert_ne!(
                key,
                generated_schema_submission_key(b"source", None, 0, &changed)
            );
        }
    }
}
