//! Module: db::sql::identifier
//! Responsibility: canonical reduced-SQL identifier normalization helpers.
//! Does not own: SQL parsing/tokenization, planner policy, or execution.
//! Boundary: shared identifier matching/qualifier-reduction semantics used by
//! SQL lowering and external SQL dispatch boundaries.

///
/// Normalize one possibly-qualified identifier against one SQL entity scope.
///
/// If `identifier` starts with an entity/alias scope followed by one direct
/// field or record path, this removes only that scope prefix. Otherwise it
/// falls back to direct qualifier matching and preserves the original input
/// when no scope candidate matches.
///

#[must_use]
pub fn normalize_identifier_to_scope(identifier: String, entity_scope: &[String]) -> String {
    for candidate in entity_scope {
        let prefix_len = candidate.len();
        if identifier.len() > prefix_len
            && identifier
                .get(..prefix_len)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(candidate))
            && identifier.as_bytes().get(prefix_len) == Some(&b'.')
        {
            return identifier[prefix_len.saturating_add(1)..].to_string();
        }
    }

    let Some((qualifier, leaf)) = split_qualified_identifier(identifier.as_str()) else {
        return identifier;
    };
    if !entity_scope
        .iter()
        .any(|candidate| identifiers_tail_match(candidate.as_str(), qualifier))
    {
        return identifier;
    }

    leaf.to_string()
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
