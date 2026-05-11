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
            canonicalize_order_spec_for_grouping, plan_access_selection_with_order,
            plan_access_selection_with_order_and_accepted_indexes,
        },
        query::predicate::reject_unsupported_query_features,
        schema::{SchemaInfo, ValidateError},
    },
    model::entity::EntityModel,
    value::Value,
};

///
/// AccessPlanningInputs
///
/// Access-planning input contract projected from query intent.
/// Carries optional predicate, raw order shape, and explicit key-access override hints.
/// Access planning consumes this contract before logical plan assembly and
/// normalizes order independently of the later logical-plan pass.
///

#[derive(Debug)]
pub(in crate::db::query) struct AccessPlanningInputs<'a> {
    predicate: Option<&'a Predicate>,
    order: Option<&'a OrderSpec>,
    key_access_override: Option<AccessPlan<Value>>,
}

impl<'a> AccessPlanningInputs<'a> {
    /// Build access-planning inputs from intent-projected values.
    #[must_use]
    pub(in crate::db::query) const fn new(
        predicate: Option<&'a Predicate>,
        order: Option<&'a OrderSpec>,
        key_access_override: Option<AccessPlan<Value>>,
    ) -> Self {
        Self {
            predicate,
            order,
            key_access_override,
        }
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

    /// Consume and return explicit key-access override if present.
    #[must_use]
    pub(in crate::db::query) fn into_key_access_override(self) -> Option<AccessPlan<Value>> {
        self.key_access_override
    }
}

// Normalize one optional predicate into canonical planner form.
pub(in crate::db::query) fn normalize_query_predicate(
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<Option<Predicate>, ValidateError> {
    predicate
        .map(|predicate| {
            reject_unsupported_query_features(predicate).map_err(ValidateError::from)?;
            let predicate = normalize_enum_literals(schema_info, predicate)?;

            Ok::<Predicate, ValidateError>(normalize(&predicate))
        })
        .transpose()
}

// Select one access plan for a normalized query, honoring explicit key-access
// overrides before falling back to predicate-derived access planning.
pub(in crate::db::query) fn plan_query_access(
    model: &EntityModel,
    visible_indexes: &VisibleIndexes<'_>,
    schema_info: &SchemaInfo,
    normalized_predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
    key_access_override: Option<AccessPlan<Value>>,
) -> Result<PlannedAccessSelection, PlannerError> {
    if let Some(plan) = key_access_override {
        Ok(PlannedAccessSelection::new(
            plan,
            Some(crate::db::query::plan::PlannedNonIndexAccessReason::IntentKeyAccessOverride),
        ))
    } else {
        let canonical_order = canonicalize_order_spec_for_grouping(model, order.cloned(), grouped);

        if visible_indexes.accepted_field_path_index_count().is_some() {
            plan_access_selection_with_order_and_accepted_indexes(
                model,
                visible_indexes.accepted_planner_indexes(),
                schema_info,
                normalized_predicate,
                canonical_order.as_ref(),
                grouped,
            )
        } else {
            plan_access_selection_with_order(
                model,
                visible_indexes.generated_model_only_indexes(),
                schema_info,
                normalized_predicate,
                canonical_order.as_ref(),
                grouped,
            )
        }
    }
}
