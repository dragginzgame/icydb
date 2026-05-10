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
        query::plan::{
            AcceptedPlannerFieldPathIndex, OrderSpec, PlanError, PlannedNonIndexAccessReason,
        },
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
    residual_query_predicate_after_filtered_access_contract,
};
pub(in crate::db::query::plan) use ranking::{
    AccessCandidateScore, AndFamilyCandidateScore, AndFamilyPriorityClass,
    access_candidate_score_outranks, and_family_candidate_score_outranks,
    candidate_satisfies_secondary_order, range_bound_count,
    selected_index_contract_satisfies_secondary_order,
};

///
/// PlannedAccessSelection
///
/// PlannedAccessSelection freezes the planner-selected access path together
/// with any concrete non-index winner reason known at planning time.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::query) struct PlannedAccessSelection {
    access: AccessPlan<Value>,
    planned_non_index_reason: Option<PlannedNonIndexAccessReason>,
}

impl PlannedAccessSelection {
    /// Construct one planner-owned access selection bundle.
    #[must_use]
    pub(in crate::db::query) const fn new(
        access: AccessPlan<Value>,
        planned_non_index_reason: Option<PlannedNonIndexAccessReason>,
    ) -> Self {
        Self {
            access,
            planned_non_index_reason,
        }
    }

    /// Consume the selection into its access plan and optional non-index reason.
    #[must_use]
    pub(in crate::db::query) fn into_parts(
        self,
    ) -> (AccessPlan<Value>, Option<PlannedNonIndexAccessReason>) {
        (self.access, self.planned_non_index_reason)
    }

    /// Consume the selection into the chosen access plan only.
    #[must_use]
    pub(in crate::db::query::plan) fn into_access(self) -> AccessPlan<Value> {
        self.access
    }
}

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
pub(in crate::db) fn plan_access(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<Value>, PlannerError> {
    plan_access_with_order(model, visible_indexes, schema, predicate, None, false)
}

/// Planner entrypoint that also considers a pre-canonicalized ORDER BY
/// fallback when predicate planning alone would full-scan.
pub(in crate::db::query) fn plan_access_with_order(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<AccessPlan<Value>, PlannerError> {
    Ok(
        plan_access_selection_with_order(
            model,
            visible_indexes,
            schema,
            predicate,
            order,
            grouped,
        )?
        .into_access(),
    )
}

// Planner entrypoint that preserves planner-owned non-index winner reasons for
// higher layers that need to freeze explain metadata from the selected route.
pub(in crate::db::query) fn plan_access_selection_with_order(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<PlannedAccessSelection, PlannerError> {
    plan_access_selection_with_order_from_authority(
        model,
        visible_indexes,
        schema,
        predicate,
        order,
        grouped,
        OrderFallbackIndexAuthority::GeneratedModelOnly,
    )
}

// Runtime planner entrypoint that preserves accepted field-path index
// contracts for order-only fallback while predicate access planning still uses
// the generated `IndexModel` bridge.
pub(in crate::db::query) fn plan_access_selection_with_order_and_accepted_indexes(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<PlannedAccessSelection, PlannerError> {
    plan_access_selection_with_order_from_authority(
        model,
        visible_indexes,
        schema,
        predicate,
        order,
        grouped,
        OrderFallbackIndexAuthority::AcceptedFieldPathIndexes(accepted_field_path_indexes),
    )
}

#[derive(Clone, Copy)]
enum OrderFallbackIndexAuthority<'a> {
    GeneratedModelOnly,
    AcceptedFieldPathIndexes(&'a [AcceptedPlannerFieldPathIndex]),
}

fn plan_access_selection_with_order_from_authority(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
    order_fallback_authority: OrderFallbackIndexAuthority<'_>,
) -> Result<PlannedAccessSelection, PlannerError> {
    let Some(predicate) = predicate else {
        let true_predicate = Predicate::True;
        let eligible_indexes = sorted_indexes(visible_indexes, &true_predicate);

        return Ok(order_fallback_selection(
            model,
            eligible_indexes.as_slice(),
            order,
            grouped,
            order_fallback_authority,
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
    let selection = predicate::plan_predicate(
        model,
        eligible_indexes.as_slice(),
        schema,
        predicate,
        order,
        grouped,
    )?;
    let (access, planned_non_index_reason) = selection.into_parts();
    let plan = normalize_access_plan_value(access);
    if !plan.is_single_full_scan() {
        return Ok(PlannedAccessSelection::new(plan, planned_non_index_reason));
    }

    Ok(index_range_from_order_with_authority(
        model,
        eligible_indexes.as_slice(),
        order,
        grouped,
        order_fallback_authority,
    )
    .map_or_else(
        || {
            PlannedAccessSelection::new(
                plan,
                Some(PlannedNonIndexAccessReason::PlannerFullScanFallback),
            )
        },
        |access| PlannedAccessSelection::new(access, None),
    ))
}

// Order-only planning is the final planner-owned fallback once predicate
// access either does not exist or degenerates to a full scan.
fn order_fallback_selection(
    model: &EntityModel,
    eligible_indexes: &[&'static IndexModel],
    order: Option<&OrderSpec>,
    grouped: bool,
    order_fallback_authority: OrderFallbackIndexAuthority<'_>,
) -> PlannedAccessSelection {
    index_range_from_order_with_authority(
        model,
        eligible_indexes,
        order,
        grouped,
        order_fallback_authority,
    )
    .map_or_else(
        || {
            PlannedAccessSelection::new(
                AccessPlan::full_scan(),
                Some(PlannedNonIndexAccessReason::PlannerFullScanFallback),
            )
        },
        |access| PlannedAccessSelection::new(access, None),
    )
}

fn index_range_from_order_with_authority(
    model: &EntityModel,
    eligible_indexes: &[&'static IndexModel],
    order: Option<&OrderSpec>,
    grouped: bool,
    order_fallback_authority: OrderFallbackIndexAuthority<'_>,
) -> Option<AccessPlan<Value>> {
    match order_fallback_authority {
        OrderFallbackIndexAuthority::GeneratedModelOnly => {
            order_select::index_range_from_order_for_generated_model_only(
                model,
                eligible_indexes,
                order,
                grouped,
            )
        }
        OrderFallbackIndexAuthority::AcceptedFieldPathIndexes(accepted_field_path_indexes) => {
            order_select::index_range_from_order_with_accepted_indexes(
                model,
                eligible_indexes,
                accepted_field_path_indexes,
                order,
                grouped,
            )
        }
    }
}
