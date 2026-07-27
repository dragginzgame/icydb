//! Module: db::query::plan::access_planner
//! Responsibility: derive the canonical access plan from normalized query
//! intent, predicate, ordering, and planner-visible index metadata.
//! Does not own: executor runtime behavior or final access-choice scoring policy outside planning.
//! Boundary: turns validated logical query intent into planner-owned access plans.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{Predicate, normalize, normalize_enum_literals},
        query::plan::{
            OrderSpec, PlannedAccessSelection, PlannerError, VisibleIndexes,
            canonicalize_order_spec_for_grouping,
            plan_access_selection_with_order_and_accepted_semantic_indexes,
        },
        schema::{SchemaInfo, ValidateError},
    },
    value::Value,
};

///
/// AccessPlanningInputs
///
/// Access-planning input contract projected from query intent.
/// Carries the optional predicate and raw order shape.
/// Access planning consumes this contract before logical plan assembly and
/// normalizes order independently of the later logical-plan pass.
///

#[derive(Debug)]
pub(in crate::db::query) struct AccessPlanningInputs<'a> {
    predicate: Option<&'a Predicate>,
    order: Option<&'a OrderSpec>,
}

impl<'a> AccessPlanningInputs<'a> {
    /// Build access-planning inputs from intent-projected values.
    #[must_use]
    pub(in crate::db::query) const fn new(
        predicate: Option<&'a Predicate>,
        order: Option<&'a OrderSpec>,
    ) -> Self {
        Self { predicate, order }
    }

    /// Borrow predicate input for normalization and planner analysis.
    #[must_use]
    pub(in crate::db::query) const fn predicate(&self) -> Option<&'a Predicate> {
        self.predicate
    }

    /// Borrow raw ORDER BY input for planner-side canonicalization.
    #[must_use]
    pub(in crate::db::query) const fn order(&self) -> Option<&'a OrderSpec> {
        self.order
    }
}

// Normalize one optional predicate into canonical planner form.
pub(in crate::db::query) fn normalize_query_predicate(
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<Option<Predicate>, ValidateError> {
    predicate
        .map(|predicate| {
            let predicate = normalize_enum_literals(schema_info, predicate)?;

            Ok::<Predicate, ValidateError>(normalize(&predicate))
        })
        .transpose()
}

/// Select one access plan from accepted schema and accepted semantic indexes.
///
/// Standalone SQL and dynamic reads enter here after accepted catalog
/// selection; generated model metadata is neither required nor consulted.
pub(in crate::db::query) fn plan_query_access_with_accepted_schema(
    visible_indexes: &VisibleIndexes,
    schema_info: &SchemaInfo,
    normalized_predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
    key_access_override: Option<AccessPlan<Value>>,
) -> Result<PlannedAccessSelection, PlannerError> {
    if let Some(plan) = key_access_override {
        return Ok(PlannedAccessSelection::new(
            plan,
            Some(crate::db::query::plan::PlannedNonIndexAccessReason::IntentKeyAccessOverride),
        ));
    }

    let canonical_order =
        canonicalize_order_spec_for_grouping(schema_info, order.cloned(), grouped);
    plan_access_selection_with_order_and_accepted_semantic_indexes(
        visible_indexes.accepted_semantic_index_contracts(),
        visible_indexes.accepted_field_path_indexes(),
        schema_info,
        normalized_predicate,
        canonical_order.as_ref(),
        grouped,
    )
}
