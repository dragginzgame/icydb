//! Module: db::query::plan::access_planner
//! Responsibility: derive the canonical access plan from normalized query
//! intent, predicate, ordering, and planner-visible index metadata.
//! Does not own: executor runtime behavior or final access-choice scoring policy outside planning.
//! Boundary: turns validated logical query intent into planner-owned access plans.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{Predicate, normalize, normalize_enum_literals},
        query::plan::{OrderSpec, PlannerError, canonicalize_order_spec, plan_access_with_order},
        schema::{SchemaInfo, ValidateError, reject_unsupported_query_features},
    },
    model::{entity::EntityModel, index::IndexModel},
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
    visible_indexes: &[&'static IndexModel],
    schema_info: &SchemaInfo,
    normalized_predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    key_access_override: Option<AccessPlan<Value>>,
) -> Result<AccessPlan<Value>, PlannerError> {
    match key_access_override {
        Some(plan) => Ok(plan),
        None => {
            let canonical_order = canonicalize_order_spec(model, order.cloned());

            plan_access_with_order(
                model,
                visible_indexes,
                schema_info,
                normalized_predicate,
                canonical_order.as_ref(),
            )
        }
    }
}
