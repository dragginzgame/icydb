//! Module: db::session::sql::projection::labels
//! Responsibility: derive stable outward SQL projection column labels from
//! structural plans, prepared projection specs, and computed SQL surfaces.
//! Does not own: projection execution or projection payload storage.
//! Boundary: keeps SQL projection naming policy at the session boundary.

#[cfg(feature = "sql-explain")]
use crate::db::{
    query::plan::expr::ProjectionSpec, session::query::projection_labels_from_projection_spec,
};
#[cfg(feature = "sql-explain")]
use crate::{
    db::query::{
        explain::{ExplainExecutionNodeDescriptor, property_keys, property_values},
        plan::AccessPlannedQuery,
    },
    value::Value,
};

// Attach SQL-facing projection labels and shell-facing projection runtime hints
// only at the session SQL boundary so executor-owned EXPLAIN assembly stays
// structural.
#[cfg(feature = "sql-explain")]
pub(in crate::db::session::sql) fn annotate_sql_projection_debug_on_execution_descriptor(
    descriptor: &mut ExplainExecutionNodeDescriptor,
    plan: &AccessPlannedQuery,
    projection: &ProjectionSpec,
) {
    let labels = projection_labels_from_projection_spec(projection)
        .into_iter()
        .map(Value::from)
        .collect();
    descriptor
        .node_properties
        .insert(property_keys::PROJECTION_FIELDS, Value::List(labels));

    // Classify the materialization mode from the frozen planner contract
    // directly here so SQL EXPLAIN does not round-trip through a single-use
    // wrapper just to attach one stable debug label.
    let materialization = if !plan.scalar_plan().mode.is_load()
        || plan.grouped_plan().is_some()
        || plan.scalar_projection_plan().is_none()
    {
        None
    } else if descriptor.covering_scan() == Some(true) {
        Some(property_values::COVERING_READ)
    } else {
        // Recognize the retained-slot direct projection shape directly from
        // the planner-frozen projection metadata instead of routing through a
        // single-use predicate helper.
        let direct_slot_projection =
            plan.frozen_direct_projection_slots()
                .is_some_and(|direct_projection_slots| {
                    projection.len() == direct_projection_slots.len()
                        && projection
                            .fields()
                            .all(|field| field.direct_field_name().is_some())
                });

        if direct_slot_projection {
            Some(property_values::DIRECT_SLOT_ROW)
        } else {
            Some(property_values::SCALAR_PROJECTION)
        }
    };

    if let Some(materialization) = materialization {
        descriptor.node_properties.insert(
            property_keys::PROJECTION_MATERIALIZATION,
            Value::from(materialization),
        );
    }
}
