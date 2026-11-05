use super::*;

#[test]
fn automatic_name_encoding_has_a_stable_domain_and_framing() {
    assert_eq!(
        canonical_index_name("User", &["email"], false, None),
        "idx_user__email_52542a8111bf3e129f572bf5b395d78a"
    );
}

#[test]
fn names_distinguish_exact_terms_scope_uniqueness_and_predicates() {
    let definitions = [
        ("User", vec!["a_b"], false, None),
        ("User", vec!["a", "b"], false, None),
        ("User", vec!["fooBar"], false, None),
        ("User", vec!["foo_bar"], false, None),
        ("User", vec!["lower_email"], false, None),
        ("User", vec!["LOWER(email)"], false, None),
        ("User", vec!["a", "b"], true, None),
        ("User", vec!["b", "a"], false, None),
        ("user", vec!["a", "b"], false, None),
        ("User", vec!["a", "b"], false, Some(b"true".as_slice())),
        ("User", vec!["a", "b"], false, Some(b"false".as_slice())),
        ("User", vec!["a", "b"], false, Some(b"".as_slice())),
    ];
    let names = definitions
        .iter()
        .map(|(entity, terms, unique, predicate)| {
            canonical_index_name(entity, terms, *unique, *predicate)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        names.iter().collect::<std::collections::HashSet<_>>().len(),
        names.len()
    );
    for name in names {
        assert!(crate::SchemaName::try_new(name).is_ok());
    }
}

#[test]
fn readable_truncation_preserves_full_definition_suffix() {
    let first = "x".repeat(64);
    let second = format!("{}y", "x".repeat(63));
    let left = canonical_index_name(&first, &[&first, &first, &first, &first], false, None);
    let right = canonical_index_name(&first, &[&first, &first, &first, &second], false, None);
    assert_eq!(left.len(), crate::MAX_SCHEMA_NAME_BYTES);
    assert_eq!(right.len(), crate::MAX_SCHEMA_NAME_BYTES);
    assert_eq!(&left[..left.len() - 32], &right[..right.len() - 32]);
    assert_ne!(left, right);
}

#[test]
fn canonical_index_slug_preserves_the_shared_identity_contract() {
    let cases = [
        ("User", "user"),
        ("UserAccount", "user_account"),
        ("HTTPServer", "http_server"),
        ("LOWER(email)", "lower_email"),
        ("profile.nickname", "profile_nickname"),
        ("Shape2D", "shape2_d"),
    ];

    for (input, expected) in cases {
        assert_eq!(canonical_index_name_slug(input), expected);
    }
}
