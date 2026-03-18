//! Module: db::sql::identifier
//! Responsibility: canonical reduced-SQL identifier normalization helpers.
//! Does not own: SQL parsing/tokenization, planner policy, or execution.
//! Boundary: shared identifier matching/qualifier-reduction semantics used by
//! SQL lowering and external SQL dispatch boundaries.

use crate::db::predicate::{ComparePredicate, Predicate};

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

/// Rewrite all field identifiers in one predicate tree using one adapter callback.
///
/// This helper is strictly structural:
/// - predicate shape is preserved
/// - compare operators/literals/coercions are preserved
/// - only field identifier strings are transformed
pub(crate) fn rewrite_field_identifiers<F>(predicate: Predicate, map_field: F) -> Predicate
where
    F: FnMut(String) -> String,
{
    let mut map_field = map_field;

    rewrite_field_identifiers_inner(predicate, &mut map_field)
}

// Recursively walk the predicate tree and apply one field-string adapter.
fn rewrite_field_identifiers_inner<F>(predicate: Predicate, map_field: &mut F) -> Predicate
where
    F: FnMut(String) -> String,
{
    match predicate {
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,
        Predicate::And(children) => Predicate::And(
            children
                .into_iter()
                .map(|child| rewrite_field_identifiers_inner(child, map_field))
                .collect(),
        ),
        Predicate::Or(children) => Predicate::Or(
            children
                .into_iter()
                .map(|child| rewrite_field_identifiers_inner(child, map_field))
                .collect(),
        ),
        Predicate::Not(inner) => {
            Predicate::Not(Box::new(rewrite_field_identifiers_inner(*inner, map_field)))
        }
        Predicate::Compare(compare) => {
            Predicate::Compare(rewrite_compare_field(compare, map_field))
        }
        Predicate::IsNull { field } => Predicate::IsNull {
            field: map_field(field),
        },
        Predicate::IsNotNull { field } => Predicate::IsNotNull {
            field: map_field(field),
        },
        Predicate::IsMissing { field } => Predicate::IsMissing {
            field: map_field(field),
        },
        Predicate::IsEmpty { field } => Predicate::IsEmpty {
            field: map_field(field),
        },
        Predicate::IsNotEmpty { field } => Predicate::IsNotEmpty {
            field: map_field(field),
        },
        Predicate::TextContains { field, value } => Predicate::TextContains {
            field: map_field(field),
            value,
        },
        Predicate::TextContainsCi { field, value } => Predicate::TextContainsCi {
            field: map_field(field),
            value,
        },
    }
}

// Rewrite only the compare field while preserving the compare semantic payload.
fn rewrite_compare_field<F>(compare: ComparePredicate, map_field: &mut F) -> ComparePredicate
where
    F: FnMut(String) -> String,
{
    ComparePredicate {
        field: map_field(compare.field),
        op: compare.op,
        value: compare.value,
        coercion: compare.coercion,
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
                Value::Int(21),
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
            Predicate::Compare(ComparePredicate::eq("age".to_string(), Value::Int(21))),
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
