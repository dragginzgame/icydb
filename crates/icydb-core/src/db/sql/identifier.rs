//! Module: db::sql::identifier
//! Responsibility: canonical reduced-SQL identifier normalization helpers.
//! Does not own: SQL parsing/tokenization, planner policy, or execution.
//! Boundary: shared identifier matching/qualifier-reduction semantics used by
//! SQL lowering and external SQL dispatch boundaries.

pub(in crate::db::sql) use crate::db::predicate::rewrite_field_identifiers;

///
/// Normalize one possibly-qualified identifier against one SQL entity scope.
///
/// If `identifier` is `qualifier.field` and `qualifier` matches any scope
/// candidate by tail-equivalence, this returns `field`. Otherwise returns the
/// original identifier unchanged.
///
#[must_use]
pub fn normalize_identifier_to_scope(identifier: String, entity_scope: &[String]) -> String {
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
pub fn identifiers_tail_match(left: &str, right: &str) -> bool {
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
    use crate::{
        db::{
            predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
            sql::identifier::{identifiers_tail_match, normalize_identifier_to_scope},
        },
        value::Value,
    };

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
    fn normalize_identifier_to_scope_preserves_non_matching_qualifier() {
        let scope = vec!["FixtureUser".to_string()];
        assert_eq!(
            normalize_identifier_to_scope("FixtureOrder.email".to_string(), scope.as_slice()),
            "FixtureOrder.email".to_string()
        );
    }

    #[test]
    fn rewrite_field_identifiers_updates_nested_predicate_fields() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq(
                "users.age".to_string(),
                Value::Int64(21),
            )),
            Predicate::Or(vec![
                Predicate::IsNull {
                    field: "users.deleted_at".to_string(),
                },
                Predicate::Not(Box::new(Predicate::TextContainsCi {
                    field: "users.email".to_string(),
                    value: Value::Text("EXAMPLE".to_string()),
                })),
            ]),
        ]);

        let rewritten = super::rewrite_field_identifiers(predicate, strip_users_prefix);

        let expected = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("age".to_string(), Value::Int64(21))),
            Predicate::Or(vec![
                Predicate::IsNull {
                    field: "deleted_at".to_string(),
                },
                Predicate::Not(Box::new(Predicate::TextContainsCi {
                    field: "email".to_string(),
                    value: Value::Text("EXAMPLE".to_string()),
                })),
            ]),
        ]);

        assert_eq!(rewritten, expected);
    }

    #[test]
    fn rewrite_field_identifiers_preserves_compare_semantics() {
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "users.email",
            CompareOp::StartsWith,
            Value::Text("Ada".to_string()),
            CoercionId::TextCasefold,
        ));

        let rewritten = super::rewrite_field_identifiers(predicate, strip_users_prefix);
        let Predicate::Compare(compare) = rewritten else {
            panic!("rewritten predicate should remain compare");
        };

        assert_eq!(compare.field, "email".to_string());
        assert_eq!(compare.op, CompareOp::StartsWith);
        assert_eq!(compare.value, Value::Text("Ada".to_string()));
        assert_eq!(compare.coercion.id, CoercionId::TextCasefold);
    }

    fn strip_users_prefix(identifier: String) -> String {
        if let Some(field) = identifier.strip_prefix("users.") {
            return field.to_string();
        }

        identifier
    }
}
