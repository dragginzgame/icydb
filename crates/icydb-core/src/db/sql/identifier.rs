//! Module: db::sql::identifier
//! Responsibility: canonical reduced-SQL identifier normalization helpers.
//! Does not own: SQL parsing/tokenization, planner policy, or execution.
//! Boundary: shared identifier matching/qualifier-reduction semantics used by
//! SQL lowering and external SQL dispatch boundaries.

///
/// Normalize one possibly-qualified identifier against one SQL entity scope.
///
/// Remove a complete entity/alias scope at the start of a field or record path,
/// trying candidates in their supplied order with ASCII case-insensitive matching.
/// Inner path segments never qualify the path; preserve the input when no
/// root-anchored scope matches.
///

#[must_use]
pub fn normalize_identifier_to_scope(mut identifier: String, entity_scope: &[String]) -> String {
    for candidate in entity_scope {
        let Some(count) = matching_scope_prefix_len(identifier.split('.'), candidate) else {
            continue;
        };
        // Whole segments plus their ASCII separators establish a UTF-8 boundary.
        // Reuse owned backing rather than allocating another suffix string.
        let prefix_len = identifier
            .split('.')
            .take(count)
            .map(|part| part.len() + 1)
            .sum::<usize>();
        identifier.drain(..prefix_len);
        break;
    }
    identifier
}

// Shared scope authority for string identifiers and parser-owned path segments.
// Require a remaining field and compare from the root; a matching tail alone
// cannot establish that an arbitrary record prefix is an entity qualifier.
pub(in crate::db) fn matching_scope_prefix_len<'a>(
    mut path: impl Iterator<Item = &'a str>,
    candidate: &str,
) -> Option<usize> {
    let mut count = 0;
    for part in candidate.split('.') {
        if !path
            .next()
            .is_some_and(|segment| segment.eq_ignore_ascii_case(part))
        {
            return None;
        }
        count += 1;
    }
    path.next().map(|_| count)
}

/// Split one qualified identifier into `(qualifier, leaf)` on the last `.`.
#[must_use]
pub fn split_qualified_identifier(identifier: &str) -> Option<(&str, &str)> {
    let (qualifier, leaf) = identifier.rsplit_once('.')?;
    if qualifier.is_empty() || leaf.is_empty() {
        return None;
    }

    Some((qualifier, leaf))
}

/// Return one final dotted identifier segment.
#[must_use]
pub fn identifier_last_segment(identifier: &str) -> Option<&str> {
    identifier.rsplit('.').next()
}

/// Return whether two SQL identifiers resolve to the same entity tail segment.
#[must_use]
pub(in crate::db) fn identifiers_tail_match(left: &str, right: &str) -> bool {
    if left.eq_ignore_ascii_case(right) {
        return true;
    }

    let left_last = identifier_last_segment(left);
    let right_last = identifier_last_segment(right);
    match (left_last, right_last) {
        (Some(l), Some(r)) => l.eq_ignore_ascii_case(r),
        _ => false,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
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
}
