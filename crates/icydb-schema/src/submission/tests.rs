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
