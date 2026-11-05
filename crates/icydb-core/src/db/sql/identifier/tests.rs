use crate::db::sql::identifier::{identifiers_tail_match, normalize_identifier_to_scope};

#[test]
fn qualifier_reduction_preserves_owned_backing_and_matching_policy() {
    for (identifier, scopes, expected) in [
        ("u.profile.rank", vec!["u"], "profile.rank"),
        ("U.profile.rank", vec!["u"], "profile.rank"),
        ("other.Users.name", vec!["public.Users"], "other.Users.name"),
        ("Users.名", vec!["Users"], "名"),
        ("é.名", vec!["é"], "名"),
        ("é.name", vec!["x"], "é.name"),
        ("u.", vec!["u"], ""),
        (".name", vec!["users"], ".name"),
        ("name", vec!["users"], "name"),
        // Retain the public utility's supplied candidate order.
        ("a.b.name", vec!["a", "a.b"], "b.name"),
    ] {
        let input = identifier.to_string();
        let pointer = input.as_ptr();
        let capacity = input.capacity();
        let scopes: Vec<_> = scopes.into_iter().map(str::to_string).collect();
        let output = normalize_identifier_to_scope(input, &scopes);
        assert_eq!(output, expected);
        assert_eq!(output.as_ptr(), pointer);
        assert_eq!(output.capacity(), capacity);
    }
}

#[test]
fn scope_reduction_preserves_inner_entity_and_alias_segments() {
    let scopes = vec!["app.Account".into(), "Account".into(), "s".into()];
    for path in ["owner.account.id", "owner.s.id", "other.Account.id"] {
        assert_eq!(normalize_identifier_to_scope(path.into(), &scopes), path);
    }
}

#[test]
fn identifiers_tail_match_accepts_schema_qualified_forms() {
    assert!(identifiers_tail_match("public.FixtureUser", "FixtureUser"));
    assert!(identifiers_tail_match("fixtureorder", "FixtureOrder"));
    assert!(!identifiers_tail_match("FixtureUser", "FixtureOrder"));
}

#[test]
fn normalize_identifier_to_scope_strips_matching_qualifier() {
    let scope = vec!["public.FixtureUser".to_string(), "FixtureUser".to_string()];
    assert_eq!(
        normalize_identifier_to_scope("FixtureUser.email".to_string(), scope.as_slice()),
        "email".to_string()
    );
    assert_eq!(
        normalize_identifier_to_scope("public.FixtureUser.email".to_string(), scope.as_slice()),
        "email".to_string()
    );
}

#[test]
fn normalize_identifier_to_scope_strips_scope_before_record_path() {
    let scope = vec![
        "public.FixtureUser".to_string(),
        "FixtureUser".to_string(),
        "u".to_string(),
    ];

    for identifier in [
        "public.FixtureUser.profile.rank",
        "FixtureUser.profile.rank",
        "u.profile.rank",
    ] {
        assert_eq!(
            normalize_identifier_to_scope(identifier.to_string(), scope.as_slice()),
            "profile.rank",
        );
    }
}

#[test]
fn normalize_identifier_to_scope_preserves_non_matching_qualifier() {
    let scope = vec!["FixtureUser".to_string()];
    assert_eq!(
        normalize_identifier_to_scope("FixtureOrder.email".to_string(), scope.as_slice()),
        "FixtureOrder.email".to_string()
    );
}
