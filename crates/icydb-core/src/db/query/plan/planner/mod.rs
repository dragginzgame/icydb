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
use crate::{
    db::{
        access::{AccessPlan, SemanticIndexAccessContract, normalize_access_plan_value},
        predicate::Predicate,
        query::plan::{OrderSpec, PlanError, PlannedNonIndexAccessReason},
        schema::SchemaInfo,
    },
    error::InternalError,
    value::Value,
};

pub(in crate::db::query::plan) use crate::db::access::MAX_INDEX_BRANCH_SET_VALUES;
pub(in crate::db::query) use index_select::{
    eligible_sorted_index_contracts, index_field_literal_matcher, index_literal_matches_schema,
};
pub(in crate::db) use index_select::{
    residual_query_predicate_after_access_path_bounds,
    residual_query_predicate_after_filtered_access_contract,
};
pub(in crate::db::query) use prefix::count_cardinality_index_branch_set_from_and;
pub(in crate::db::query::plan) use ranking::{
    AccessCandidateScore, AndFamilyCandidateScore, AndFamilyPriorityClass,
    access_candidate_score_from_index_contract, access_candidate_score_outranks,
    and_family_candidate_score_outranks, range_bound_count,
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
    pub(in crate::db::query) fn into_access_and_non_index_reason(
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

#[derive(Debug)]
pub enum PlannerError {
    Plan(Box<PlanError>),

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

// Access planning, access-choice projection, and reranking consume the same
// reduced semantic index contracts without reopening generated declarations.
pub(in crate::db::query) fn plan_access_selection_with_order_and_semantic_indexes(
    semantic_candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<PlannedAccessSelection, PlannerError> {
    plan_access_selection_with_order(
        semantic_candidate_indexes,
        schema,
        predicate,
        order,
        grouped,
    )
}

fn plan_access_selection_with_order(
    visible_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<PlannedAccessSelection, PlannerError> {
    let Some(predicate) = predicate else {
        let true_predicate = Predicate::True;
        let eligible_indexes = eligible_sorted_index_contracts(visible_indexes, &true_predicate);

        return Ok(order_fallback_selection(
            eligible_indexes.as_slice(),
            schema,
            &true_predicate,
            order,
            grouped,
        ));
    };

    let mut eligible_indexes = eligible_sorted_index_contracts(visible_indexes, predicate);
    if grouped {
        eligible_indexes.retain(|index| {
            order_select::index_stream_is_complete_for_query(schema, index, predicate)
        });
    }

    // Planner determinism guarantee:
    // Given accepted schema and a canonical predicate, planning is pure and deterministic.
    //
    // Planner determinism rules:
    // - Predicate canonicalization is owned by `db::predicate`.
    // - Index candidates are considered in lexicographic accepted-name order.
    // - Competing index candidates are ranked by one canonical planner score:
    //   prefix match, family-specific exact-match preference, then secondary-order compatibility.
    // - Structural ties still break on lexicographic accepted-name order.
    // - Access paths are ranked: primary key lookups, exact index matches, prefix matches, full scans.
    // - Order specs preserve user order after validation (planner does not reorder).
    // - Field resolution uses SchemaInfo's name map (sorted by field name).
    let selection = predicate::plan_predicate(
        eligible_indexes.as_slice(),
        schema,
        predicate,
        order,
        grouped,
    )?;
    let (access, planned_non_index_reason) = selection.into_access_and_non_index_reason();
    let plan = normalize_access_plan_value(access);
    if !plan.is_single_full_scan() {
        return Ok(PlannedAccessSelection::new(plan, planned_non_index_reason));
    }

    Ok(order_select::index_range_from_order_with_semantic_indexes(
        eligible_indexes.as_slice(),
        schema,
        predicate,
        order,
        grouped,
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
    eligible_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    query_predicate: &Predicate,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> PlannedAccessSelection {
    order_select::index_range_from_order_with_semantic_indexes(
        eligible_indexes,
        schema,
        query_predicate,
        order,
        grouped,
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
