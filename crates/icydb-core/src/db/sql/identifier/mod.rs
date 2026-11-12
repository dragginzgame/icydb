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
mod tests;
