//! Module: query::plan::covering
//! Responsibility: planner covering-projection eligibility and order-contract derivation.
//! Does not own: runtime projection materialization or executor ordering enforcement.
//! Boundary: exposes planner-only covering contracts for index-backed paths.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::db::{
    access::AccessPlan,
    direction::Direction,
    query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec},
};
use crate::value::Value;

///
/// CoveringProjectionOrder
///
/// Planner-owned covering projection order contract.
/// Index order means projected component order is preserved from index traversal.
/// Primary-key order means runtime must reorder by primary key after projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CoveringProjectionOrder {
    IndexOrder(Direction),
    PrimaryKeyOrder(Direction),
}

///
/// CoveringProjectionContext
///
/// Planner-owned covering projection context contract.
/// Captures projection component position, bound-prefix arity, and output-order
/// interpretation for one index-backed access shape.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CoveringProjectionContext {
    pub(in crate::db) component_index: usize,
    pub(in crate::db) prefix_len: usize,
    pub(in crate::db) order_contract: CoveringProjectionOrder,
}

/// Return whether one scalar aggregate terminal can remain index-only using
/// existing-row semantics under the current planner + predicate-compile
/// contracts.
#[must_use]
pub(in crate::db) fn index_covering_existing_rows_terminal_eligible(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> bool {
    if plan.scalar_plan().order.is_some() {
        return false;
    }

    let index_shape_supported =
        plan.access.as_index_prefix_path().is_some() || plan.access.as_index_range_path().is_some();
    if !index_shape_supported {
        return false;
    }
    if plan.scalar_plan().predicate.is_none() {
        return true;
    }

    strict_predicate_compatible
}

/// Derive one covering projection context from one access shape + scalar order
/// contract and target field.
#[must_use]
pub(in crate::db) fn covering_index_projection_context<K>(
    access: &AccessPlan<K>,
    order: Option<&OrderSpec>,
    target_field: &str,
    primary_key_name: &'static str,
) -> Option<CoveringProjectionContext> {
    let (index_fields, prefix_len, path_kind_is_range) =
        if let Some((index, values)) = access.as_index_prefix_path() {
            (index.fields(), values.len(), false)
        } else if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
            (index.fields(), prefix_values.len(), true)
        } else {
            return None;
        };
    let component_index = index_fields
        .iter()
        .position(|field| *field == target_field)?;
    let order_contract = covering_projection_order_contract(
        order,
        index_fields,
        prefix_len,
        primary_key_name,
        path_kind_is_range,
    )?;

    Some(CoveringProjectionContext {
        component_index,
        prefix_len,
        order_contract,
    })
}

/// Resolve one constant projection value when access shape binds the target
/// field through index-prefix equality components.
#[must_use]
pub(in crate::db) fn constant_covering_projection_value_from_access<K>(
    access: &AccessPlan<K>,
    target_field: &str,
) -> Option<Value> {
    if let Some((index, values)) = access.as_index_prefix_path() {
        return constant_covering_projection_value_from_prefix(
            index.fields(),
            values,
            target_field,
        );
    }
    if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
        return constant_covering_projection_value_from_prefix(
            index.fields(),
            prefix_values,
            target_field,
        );
    }

    None
}

/// Return whether adjacent dedupe is safe for one covering projection context.
///
/// Safety contract:
/// - output order remains index traversal order (no primary-key reorder),
/// - target field is the first unbound index component.
#[must_use]
pub(in crate::db) const fn covering_index_adjacent_distinct_eligible(
    context: CoveringProjectionContext,
) -> bool {
    matches!(
        context.order_contract,
        CoveringProjectionOrder::IndexOrder(_)
    ) && context.component_index == context.prefix_len
}

// Resolve one covering projection order contract from scalar ORDER BY shape.
fn covering_projection_order_contract(
    order: Option<&OrderSpec>,
    index_fields: &[&'static str],
    prefix_len: usize,
    primary_key_name: &'static str,
    path_kind_is_range: bool,
) -> Option<CoveringProjectionOrder> {
    let Some(order) = order else {
        return Some(CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc));
    };
    if let Some(direction) = order.primary_key_only_direction(primary_key_name) {
        let direction = match direction {
            OrderDirection::Asc => Direction::Asc,
            OrderDirection::Desc => Direction::Desc,
        };

        return Some(CoveringProjectionOrder::PrimaryKeyOrder(direction));
    }

    let direction = match order.deterministic_secondary_order_direction(primary_key_name)? {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    };
    if order.matches_index_suffix_plus_primary_key(index_fields, prefix_len, primary_key_name) {
        return Some(CoveringProjectionOrder::IndexOrder(direction));
    }

    if path_kind_is_range {
        return None;
    }

    order
        .matches_index_full_plus_primary_key(index_fields, primary_key_name)
        .then_some(CoveringProjectionOrder::IndexOrder(direction))
}

// Resolve one constant projection value from index-prefix component bindings.
fn constant_covering_projection_value_from_prefix(
    index_fields: &[&'static str],
    prefix_values: &[Value],
    target_field: &str,
) -> Option<Value> {
    index_fields
        .iter()
        .zip(prefix_values.iter())
        .find_map(|(field, value)| (*field == target_field).then(|| value.clone()))
}
