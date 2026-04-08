//! Module: db::query::plan::planner
//! Responsibility: module-local ownership and contracts for db::query::plan::planner.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Semantic planning from predicates to access strategies; must not assert invariants.
//!
//! Determinism: canonicalization is delegated to predicate/access ownership
//! boundaries so the same model and predicate shape produce identical access plans.

mod compare;
mod index_select;
mod order_select;
mod predicate;
mod prefix;
mod range;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        access::{AccessPlan, normalize_access_plan_value},
        predicate::Predicate,
        query::plan::{OrderSpec, PlanError},
        schema::SchemaInfo,
    },
    error::InternalError,
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};
use thiserror::Error as ThisError;

pub(in crate::db::query::plan) use index_select::{
    index_literal_matches_schema, sorted_indexes, sorted_model_indexes,
};
pub(in crate::db) use index_select::{
    residual_query_predicate_after_access_path_bounds,
    residual_query_predicate_after_filtered_access,
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
/// CONTRACT: the caller is responsible for predicate validation and
/// predicate canonicalization before planner entry.
#[cfg(test)]
pub(crate) fn plan_access(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<Value>, PlannerError> {
    plan_access_with_order(model, visible_indexes, schema, predicate, None)
}

/// Planner entrypoint that also considers a pre-canonicalized ORDER BY
/// fallback when predicate planning alone would full-scan.
pub(crate) fn plan_access_with_order(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
) -> Result<AccessPlan<Value>, PlannerError> {
    let Some(predicate) = predicate else {
        return Ok(
            order_select::index_range_from_order(model, visible_indexes, order, None)
                .unwrap_or_else(AccessPlan::full_scan),
        );
    };

    // Planner determinism guarantee:
    // Given a validated EntityModel and canonical predicate, planning is pure and deterministic.
    //
    // Planner determinism rules:
    // - Predicate canonicalization is owned by `db::predicate`.
    // - Index candidates are considered in lexicographic IndexModel.name order.
    // - Access paths are ranked: primary key lookups, exact index matches, prefix matches, full scans.
    // - Order specs preserve user order after validation (planner does not reorder).
    // - Field resolution uses SchemaInfo's name map (sorted by field name).
    let plan = normalize_access_plan_value(predicate::plan_predicate(
        model,
        visible_indexes,
        schema,
        predicate,
        predicate,
        order,
    )?);
    if !plan.is_single_full_scan() {
        return Ok(plan);
    }

    if let Some(order_plan) =
        order_select::index_range_from_order(model, visible_indexes, order, Some(predicate))
    {
        return Ok(order_plan);
    }

    Ok(plan)
}
