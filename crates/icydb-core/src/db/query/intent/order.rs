use crate::{
    db::query::plan::{OrderDirection, OrderSpec},
    model::entity::EntityModel,
};

/// Helper to append an ordering field while preserving existing order spec.
pub(in crate::db::query::intent) fn push_order(
    order: Option<OrderSpec>,
    field: &str,
    direction: OrderDirection,
) -> OrderSpec {
    match order {
        Some(mut spec) => {
            spec.fields.push((field.to_string(), direction));
            spec
        }
        None => OrderSpec {
            fields: vec![(field.to_string(), direction)],
        },
    }
}

// Normalize ORDER BY into a canonical, deterministic shape:
// - preserve user field order
// - remove explicit primary-key references from the user segment
// - append exactly one primary-key field as the terminal tie-break
pub(in crate::db::query::intent) fn canonicalize_order_spec(
    model: &EntityModel,
    order: Option<OrderSpec>,
) -> Option<OrderSpec> {
    let mut order = order?;
    let pk = model.primary_key.name;

    let mut pk_direction = None;

    order.fields.retain(|(field, dir)| {
        if field == pk {
            pk_direction.get_or_insert(*dir);
            false
        } else {
            true
        }
    });

    order
        .fields
        .push((pk.to_string(), pk_direction.unwrap_or(OrderDirection::Asc)));

    Some(order)
}
