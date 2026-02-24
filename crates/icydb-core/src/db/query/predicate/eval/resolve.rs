use crate::{
    db::query::predicate::{ComparePredicate, Predicate},
    model::entity::resolve_field_slot,
    traits::EntityKind,
};
use std::collections::BTreeSet;

use crate::db::query::predicate::eval::{ResolvedComparePredicate, ResolvedPredicate};

// Collect every resolved field slot referenced by one compiled predicate tree.
pub(super) fn collect_required_slots(predicate: &ResolvedPredicate) -> Vec<usize> {
    let mut slots = BTreeSet::new();
    collect_required_slots_into(predicate, &mut slots);

    slots.into_iter().collect()
}

// Recursively gather field-slot references from one compiled predicate node.
fn collect_required_slots_into(predicate: &ResolvedPredicate, slots: &mut BTreeSet<usize>) {
    match predicate {
        ResolvedPredicate::True | ResolvedPredicate::False => {}
        ResolvedPredicate::And(children) | ResolvedPredicate::Or(children) => {
            for child in children {
                collect_required_slots_into(child, slots);
            }
        }
        ResolvedPredicate::Not(inner) => collect_required_slots_into(inner, slots),
        ResolvedPredicate::Compare(cmp) => {
            if let Some(field_slot) = cmp.field_slot {
                slots.insert(field_slot);
            }
        }
        ResolvedPredicate::IsNull { field_slot }
        | ResolvedPredicate::IsMissing { field_slot }
        | ResolvedPredicate::IsEmpty { field_slot }
        | ResolvedPredicate::IsNotEmpty { field_slot }
        | ResolvedPredicate::TextContains { field_slot, .. }
        | ResolvedPredicate::TextContainsCi { field_slot, .. } => {
            if let Some(field_slot) = field_slot {
                slots.insert(*field_slot);
            }
        }
    }
}

// Compile field-name predicates to stable field-slot predicates once per query.
pub(super) fn resolve_predicate_slots<E: EntityKind>(predicate: &Predicate) -> ResolvedPredicate {
    fn resolve_field<E: EntityKind>(field_name: &str) -> Option<usize> {
        resolve_field_slot(E::MODEL, field_name)
    }

    match predicate {
        Predicate::True => ResolvedPredicate::True,
        Predicate::False => ResolvedPredicate::False,
        Predicate::And(children) => ResolvedPredicate::And(
            children
                .iter()
                .map(resolve_predicate_slots::<E>)
                .collect::<Vec<_>>(),
        ),
        Predicate::Or(children) => ResolvedPredicate::Or(
            children
                .iter()
                .map(resolve_predicate_slots::<E>)
                .collect::<Vec<_>>(),
        ),
        Predicate::Not(inner) => {
            ResolvedPredicate::Not(Box::new(resolve_predicate_slots::<E>(inner)))
        }
        Predicate::Compare(ComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: resolve_field::<E>(field),
            op: *op,
            value: value.clone(),
            coercion: coercion.clone(),
        }),
        Predicate::IsNull { field } => ResolvedPredicate::IsNull {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsMissing { field } => ResolvedPredicate::IsMissing {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsEmpty { field } => ResolvedPredicate::IsEmpty {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsNotEmpty { field } => ResolvedPredicate::IsNotEmpty {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::TextContains { field, value } => ResolvedPredicate::TextContains {
            field_slot: resolve_field::<E>(field),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => ResolvedPredicate::TextContainsCi {
            field_slot: resolve_field::<E>(field),
            value: value.clone(),
        },
    }
}
