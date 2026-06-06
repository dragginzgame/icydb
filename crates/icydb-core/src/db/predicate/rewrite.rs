use crate::db::predicate::{CompareFieldsPredicate, ComparePredicate, Predicate};

/// Rewrite all field identifiers in one predicate tree using one adapter callback.
///
/// This helper is strictly structural:
/// - predicate shape is preserved
/// - compare operators/literals/coercions are preserved
/// - only field identifier strings are transformed
pub(in crate::db) fn rewrite_field_identifiers<F>(predicate: Predicate, map_field: F) -> Predicate
where
    F: FnMut(String) -> String,
{
    let mut map_field = map_field;

    rewrite_field_identifiers_inner(predicate, &mut map_field)
}

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
        Predicate::CompareFields(compare) => {
            Predicate::CompareFields(rewrite_compare_fields(compare, map_field))
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

fn rewrite_compare_fields<F>(
    compare: CompareFieldsPredicate,
    map_field: &mut F,
) -> CompareFieldsPredicate
where
    F: FnMut(String) -> String,
{
    CompareFieldsPredicate::with_coercion(
        map_field(compare.left_field().to_string()),
        compare.op(),
        map_field(compare.right_field().to_string()),
        compare.coercion().id,
    )
}
