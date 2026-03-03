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
    if order.fields.is_empty() {
        return Some(order);
    }

    let pk_field = model.primary_key.name;
    let mut pk_direction = None;
    order.fields.retain(|(field, direction)| {
        if field == pk_field {
            if pk_direction.is_none() {
                pk_direction = Some(*direction);
            }
            false
        } else {
            true
        }
    });

    let pk_direction = pk_direction.unwrap_or(OrderDirection::Asc);
    order.fields.push((pk_field.to_string(), pk_direction));

    Some(order)
}
