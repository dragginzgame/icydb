//! Module: db::query::plan::planner
//! Responsibility: module-local ownership and contracts for db::query::plan::planner.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Semantic planning from predicates to access strategies; must not assert invariants.
//!
//! Determinism: the planner canonicalizes output so the same model and
//! predicate shape always produce identical access plans.

mod compare;
mod index_select;
mod predicate;
mod prefix;
mod range;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        access::AccessPlan,
        predicate::{Predicate, normalize as normalize_predicate},
        query::plan::{PlanError, stability::normalize_planned_access_plan_for_stability},
        schema::SchemaInfo,
    },
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};
use thiserror::Error as ThisError;

pub(in crate::db::query::plan::planner) use index_select::{
    index_literal_matches_schema, sorted_indexes,
};

///
/// PlannerError
///

#[derive(Debug, ThisError)]
pub enum PlannerError {
    #[error("{0}")]
    Plan(Box<PlanError>),

    #[error("{0}")]
    Internal(Box<InternalError>),
}

impl From<PlanError> for PlannerError {
    fn from(err: PlanError) -> Self {
        Self::Plan(Box::new(err))
    }
}

impl From<InternalError> for PlannerError {
    fn from(err: InternalError) -> Self {
        Self::Internal(Box::new(err))
    }
}

/// Planner entrypoint that operates on a prebuilt schema surface.
///
/// CONTRACT: the caller is responsible for predicate validation.
pub(crate) fn plan_access(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<Value>, PlannerError> {
    let Some(predicate) = predicate else {
        return Ok(AccessPlan::full_scan());
    };

    // Planner determinism guarantee:
    // Given a validated EntityModel and normalized predicate, planning is pure and deterministic.
    //
    // Planner determinism rules:
    // - Predicate normalization sorts AND/OR children by (field, operator, value, coercion).
    // - Index candidates are considered in lexicographic IndexModel.name order.
    // - Access paths are ranked: primary key lookups, exact index matches, prefix matches, full scans.
    // - Order specs preserve user order after validation (planner does not reorder).
    // - Field resolution uses SchemaInfo's name map (sorted by field name).
    let normalized = normalize_predicate(predicate);
    let plan = normalize_planned_access_plan_for_stability(predicate::plan_predicate(
        model,
        schema,
        &normalized,
    )?);

    Ok(plan)
}
