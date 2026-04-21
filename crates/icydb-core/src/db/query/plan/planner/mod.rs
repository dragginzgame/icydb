//! Module: db::query::plan::planner
//! Owns semantic access planning from query predicates and ordering contracts
//! to canonical access strategies.

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
mod ranking;
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
    index_literal_matches_schema, index_predicate_guarantees_compare, sorted_indexes,
    sorted_model_indexes,
};
pub(in crate::db) use index_select::{
    residual_query_predicate_after_access_path_bounds,
    residual_query_predicate_after_filtered_access,
};
pub(in crate::db::query::plan) use ranking::{
    AccessCandidateScore, access_candidate_score_outranks, candidate_satisfies_secondary_order,
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
    plan_access_with_order(model, visible_indexes, schema, predicate, None, false)
}

/// Planner entrypoint that also considers a pre-canonicalized ORDER BY
/// fallback when predicate planning alone would full-scan.
pub(crate) fn plan_access_with_order(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<AccessPlan<Value>, PlannerError> {
    let Some(predicate) = predicate else {
        let true_predicate = Predicate::True;
        let eligible_indexes = sorted_indexes(visible_indexes, &true_predicate);

        return Ok(order_fallback_plan(
            model,
            eligible_indexes.as_slice(),
            order,
            grouped,
        ));
    };

    let eligible_indexes = sorted_indexes(visible_indexes, predicate);

    // Planner determinism guarantee:
    // Given a validated EntityModel and canonical predicate, planning is pure and deterministic.
    //
    // Planner determinism rules:
    // - Predicate canonicalization is owned by `db::predicate`.
    // - Index candidates are considered in lexicographic IndexModel.name order.
    // - Competing index candidates are ranked by one canonical planner score:
    //   prefix match, family-specific exact-match preference, then secondary-order compatibility.
    // - Structural ties still break on lexicographic IndexModel.name order.
    // - Access paths are ranked: primary key lookups, exact index matches, prefix matches, full scans.
    // - Order specs preserve user order after validation (planner does not reorder).
    // - Field resolution uses SchemaInfo's name map (sorted by field name).
    let plan = normalize_access_plan_value(predicate::plan_predicate(
        model,
        eligible_indexes.as_slice(),
        schema,
        predicate,
        order,
        grouped,
    )?);
    if !plan.is_single_full_scan() {
        return Ok(plan);
    }

    Ok(
        order_select::index_range_from_order(model, eligible_indexes.as_slice(), order, grouped)
            .unwrap_or(plan),
    )
}

// Order-only planning is the final planner-owned fallback once predicate
// access either does not exist or degenerates to a full scan.
fn order_fallback_plan(
    model: &EntityModel,
    eligible_indexes: &[&'static IndexModel],
    order: Option<&OrderSpec>,
    grouped: bool,
) -> AccessPlan<Value> {
    order_select::index_range_from_order(model, eligible_indexes, order, grouped)
        .unwrap_or_else(AccessPlan::full_scan)
}
