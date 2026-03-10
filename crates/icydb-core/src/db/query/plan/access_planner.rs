//! Module: db::query::plan::access_planner
//! Responsibility: module-local ownership and contracts for db::query::plan::access_planner.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{Predicate, normalize, normalize_enum_literals},
        query::plan::{PlannerError, plan_access},
        schema::{SchemaInfo, ValidateError, reject_unsupported_query_features},
    },
    model::entity::EntityModel,
    value::Value,
};

///
/// AccessPlanningInputs
///
/// Access-planning input contract projected from query intent.
/// Carries optional predicate and explicit key-access override hints.
/// Access planning consumes this contract before logical plan assembly.
///

#[derive(Debug)]
pub(in crate::db::query) struct AccessPlanningInputs<'a> {
    predicate: Option<&'a Predicate>,
    key_access_override: Option<AccessPlan<Value>>,
}

impl<'a> AccessPlanningInputs<'a> {
    /// Build access-planning inputs from intent-projected values.
    #[must_use]
    pub(in crate::db::query) const fn new(
        predicate: Option<&'a Predicate>,
        key_access_override: Option<AccessPlan<Value>>,
    ) -> Self {
        Self {
            predicate,
            key_access_override,
        }
    }

    /// Borrow predicate input for normalization and planner analysis.
    #[must_use]
    pub(in crate::db::query) const fn predicate(&self) -> Option<&'a Predicate> {
        self.predicate
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

// Select one access plan from a normalized predicate.
pub(in crate::db::query) fn plan_access_from_normalized_predicate(
    model: &EntityModel,
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<Value>, PlannerError> {
    plan_access(model, schema_info, predicate)
}

// Select one access plan for a normalized query, honoring explicit key-access
// overrides before falling back to predicate-derived access planning.
pub(in crate::db::query) fn plan_query_access(
    model: &EntityModel,
    schema_info: &SchemaInfo,
    normalized_predicate: Option<&Predicate>,
    key_access_override: Option<AccessPlan<Value>>,
) -> Result<AccessPlan<Value>, PlannerError> {
    match key_access_override {
        Some(plan) => Ok(plan),
        None => plan_access_from_normalized_predicate(model, schema_info, normalized_predicate),
    }
}
