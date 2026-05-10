//! Module: query::plan
//! Responsibility: logical query-plan module wiring and boundary re-exports.
//! Does not own: plan-model construction or semantic helper implementation details.
//! Boundary: intent/explain/planner/validator consumers import from this root only.

mod access_choice;
mod access_plan;
mod access_planner;
mod continuation;
mod covering;
pub(in crate::db) mod expr;
mod group;
mod grouped_layout;
mod key_item_match;
mod logical_builder;
mod model;
mod model_builder;
mod order_contract;
mod order_term;
mod pipeline;
mod planner;
mod projection;
mod semantics;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

use crate::{
    db::{Predicate, access::SemanticIndexAccessContract, schema::SchemaInfo},
    model::index::IndexModel,
};
use std::borrow::Cow;

pub(in crate::db) use crate::model::{
    canonicalize_filter_literal_for_kind,
    canonicalize_grouped_having_numeric_literal_for_field_kind,
};
pub(in crate::db) use access_choice::{
    AccessChoiceCandidateExplainSummary, AccessChoiceExplainSnapshot, AccessChoiceResidualBurden,
    AccessChoiceSelectedReason,
};
pub(in crate::db::query) use access_choice::{
    rerank_access_plan_by_residual_burden_with_accepted_indexes,
    rerank_access_plan_by_residual_burden_with_indexes,
};
pub(in crate::db) use access_plan::AccessPlannedQuery;
pub(in crate::db) use access_plan::{
    EffectiveRuntimeFilterProgram, PlannedNonIndexAccessReason, ResolvedOrder, ResolvedOrderField,
    ResolvedOrderValueSource, StaticPlanningShape,
};
pub(in crate::db::query) use access_planner::{
    AccessPlanningInputs, normalize_query_predicate, plan_query_access,
};
pub(in crate::db) use continuation::{
    PlannedContinuationContract, ScalarAccessWindowPlan, effective_offset_for_cursor_window,
};
pub(in crate::db) use covering::{
    CoveringExistingRowMode, CoveringProjectionContext, CoveringProjectionOrder,
    CoveringReadExecutionPlan, CoveringReadField, CoveringReadFieldSource, CoveringReadPlan,
    constant_covering_projection_value_from_access,
    covering_hybrid_projection_plan_with_schema_info, covering_index_adjacent_distinct_eligible,
    covering_index_projection_context, covering_read_execution_plan_from_fields,
    covering_read_execution_plan_with_schema_info, covering_read_reason_code_for_load_plan,
    covering_strict_predicate_compatible, index_covering_existing_rows_terminal_eligible,
};
pub(in crate::db) use group::{
    GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, GroupedExecutionRoute,
    PlannedProjectionLayout, grouped_aggregate_execution_specs,
    grouped_aggregate_specs_from_projection_spec, grouped_executor_handoff,
    resolved_grouped_distinct_execution_strategy_with_schema_info,
};
pub(in crate::db) use grouped_layout::validate_grouped_projection_layout;
pub(in crate::db::query) use logical_builder::{
    LogicalPlanningInputs, build_logical_plan, canonicalize_order_spec_for_grouping,
    logical_query_from_logical_inputs,
};
pub use model::OrderDirection;
pub(in crate::db) use model::OrderTerm;
pub(in crate::db) use model::render_scalar_filter_expr_plan_label;
pub(in crate::db) use model::{AggregateKind, DistinctExecutionStrategy};
pub(in crate::db) use model::{ContinuationPolicy, ExecutionShapeSignature, PlannerRouteProfile};
pub(in crate::db) use model::{
    DeleteLimitSpec, FieldSlot, GlobalDistinctAggregateKind, GroupAggregateSpec, GroupPlan,
    GroupSpec, GroupedExecutionConfig, GroupedPlanAggregateFamily, LogicalPlan, OrderSpec,
    PageSpec, ScalarPlan,
};
pub use model::{DeleteSpec, LoadSpec, QueryMode};
pub(in crate::db) use order_contract::{
    DeterministicSecondaryIndexOrderMatch, DeterministicSecondaryOrderContract,
    ExecutionOrderContract, ExecutionOrdering,
    access_satisfies_deterministic_secondary_order_contract,
    deterministic_secondary_index_key_items_order_compatibility,
    deterministic_secondary_index_order_terms_satisfied, grouped_index_order_terms_satisfied,
};
#[cfg(test)]
pub(in crate::db) use order_contract::{
    GroupedIndexOrderMatch, deterministic_secondary_index_order_compatibility,
    deterministic_secondary_index_order_satisfied, grouped_index_order_match,
};
pub(in crate::db) use order_term::index_key_item_order_terms;
#[cfg(test)]
pub(in crate::db) use order_term::index_order_terms;
pub(in crate::db) use pipeline::PreparedScalarPlanningState;
#[cfg(test)]
pub(in crate::db::query) use pipeline::prepare_query_model_scalar_planning_state_for_model_only;
#[cfg(test)]
pub(in crate::db::query) use pipeline::try_build_trivial_scalar_load_plan_for_model_only;
pub(in crate::db::query) use pipeline::{
    build_query_model_plan_for_model_only, build_query_model_plan_with_indexes_for_model_only,
    build_query_model_plan_with_indexes_from_scalar_planning_state,
    prepare_query_model_scalar_planning_state_with_schema_info,
    try_build_trivial_scalar_load_plan_with_schema_info,
};
pub(in crate::db::query) use planner::PlannerError;
#[cfg(test)]
pub(in crate::db) use planner::plan_access;
pub(in crate::db::query) use planner::{
    PlannedAccessSelection, plan_access_selection_with_order,
    plan_access_selection_with_order_and_accepted_indexes,
    plan_access_selection_with_order_and_semantic_indexes,
};
pub(in crate::db) use planner::{
    residual_query_predicate_after_access_path_bounds,
    residual_query_predicate_after_filtered_access_contract,
};
pub(in crate::db) use projection::lower_global_aggregate_projection;
pub(in crate::db::query) use projection::{
    lower_data_row_direct_projection_slots_with_schema, lower_direct_projection_slots_with_schema,
    lower_projection_identity, lower_projection_intent,
};
pub(in crate::db) use semantics::global_distinct_group_spec_for_aggregate_identity;
pub(in crate::db) use semantics::group_aggregate_spec_expr;
pub(in crate::db) use semantics::{
    AccessPlanProjection, AggregateIdentity, AggregateSemanticKey, GroupDistinctAdmissibility,
    GroupDistinctPolicyReason, GroupedCursorPolicyViolation, GroupedPlanFallbackReason,
    GroupedPlanStrategy, access_plan_label, explain_access_kind_label,
    explain_access_strategy_label, grouped_distinct_admissibility,
    grouped_having_binary_compare_op, grouped_having_compare_op_supported, project_access_plan,
    project_explain_access_path, resolve_global_distinct_field_aggregate,
};
pub(in crate::db) use semantics::{
    LogicalPushdownEligibility, derive_logical_pushdown_eligibility,
    grouped_cursor_policy_violation, grouped_having_compare_expr, grouped_plan_strategy,
};
#[cfg(test)]
pub(in crate::db) use semantics::{
    global_distinct_field_aggregate_admissibility, is_global_distinct_field_aggregate_candidate,
};
#[cfg(test)]
pub(crate) use validate::GroupPlanError;
pub use validate::PlanError;
pub(crate) use validate::PolicyPlanError;
pub(in crate::db::query) use validate::{
    CursorPagingPolicyError, FluentLoadPolicyViolation, IntentKeyAccessKind,
    IntentKeyAccessPolicyViolation, has_explicit_order, validate_fluent_non_paged_mode,
    validate_fluent_paged_mode, validate_group_query_semantics, validate_intent_key_access_policy,
    validate_intent_plan_shape, validate_query_semantics,
};
pub(in crate::db) use validate::{
    resolve_aggregate_target_field_slot_with_schema, resolve_group_field_slot,
    resolve_group_field_slot_with_schema, validate_cursor_order_plan_shape,
};

/// Return true when a query mode declares an explicit load `LIMIT 0` window.
#[must_use]
pub(in crate::db::query) fn is_limit_zero_load_window(mode: QueryMode) -> bool {
    matches!(mode, QueryMode::Load(spec) if spec.limit() == Some(0))
}

/// Fold canonical constant predicates before access routing.
///
/// Contract:
/// - `Some(Predicate::True)` is elided to `None`
/// - `Some(Predicate::False)` is preserved so explain semantics remain explicit
/// - all other predicates are passed through unchanged
#[must_use]
pub(in crate::db::query) fn fold_constant_predicate(
    predicate: Option<Predicate>,
) -> Option<Predicate> {
    match predicate {
        Some(Predicate::True) => None,
        other => other,
    }
}

/// Return true when the normalized predicate is a canonical constant false.
#[must_use]
pub(in crate::db::query) const fn predicate_is_constant_false(
    predicate: Option<&Predicate>,
) -> bool {
    matches!(predicate, Some(Predicate::False))
}
#[cfg(test)]
pub(crate) use validate::{PlanPolicyError, PlanUserError};

///
/// VisibleIndexes
///
/// Planner-bound index slice that has already passed runtime visibility
/// gating at the session boundary, or one schema-owned detached slice for
/// tooling/tests that intentionally do not carry runtime store context.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db) enum VisibleIndexAuthority {
    StoreNotReady,
    GeneratedModelOnly,
    AcceptedSchema { field_path_indexes: usize },
}

#[derive(Clone, Debug)]
pub(in crate::db) struct VisibleIndexes<'a> {
    // Generated candidate bridges remain only for expression-index planning
    // and filtered accepted-index predicate lookup until accepted expression
    // and predicate contracts exist.
    generated_candidate_bridge_indexes: Cow<'a, [&'static IndexModel]>,
    accepted_field_path_indexes: Vec<AcceptedPlannerFieldPathIndex>,
    accepted_schema_info: Option<SchemaInfo>,
    authority: VisibleIndexAuthority,
}

///
/// AcceptedPlannerFieldPathIndex
///
/// Planner-facing accepted field-path index contract.
/// This owns the accepted schema metadata needed by field-path planner
/// decisions plus a reduced semantic access contract for selected-path
/// construction without retaining the full generated `IndexModel`.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedPlannerFieldPathIndex {
    name: String,
    store: String,
    unique: bool,
    fields: Vec<AcceptedPlannerFieldPathIndexField>,
    semantic_access_contract: SemanticIndexAccessContract,
}

impl AcceptedPlannerFieldPathIndex {
    fn from_schema_index(
        accepted: &crate::db::schema::SchemaIndexInfo,
        generated_predicate_bridge: Option<&'static IndexModel>,
    ) -> Self {
        Self {
            name: accepted.name().to_string(),
            store: accepted.store().to_string(),
            unique: accepted.unique(),
            fields: accepted
                .fields()
                .iter()
                .map(AcceptedPlannerFieldPathIndexField::from_schema_field)
                .collect(),
            semantic_access_contract: SemanticIndexAccessContract::from_accepted_field_path_index(
                accepted,
                generated_predicate_bridge,
            ),
        }
    }

    /// Borrow the accepted stable index name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow the accepted backing index store path.
    #[must_use]
    pub(in crate::db) const fn store(&self) -> &str {
        self.store.as_str()
    }

    /// Return whether this accepted index enforces uniqueness.
    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    /// Borrow accepted field-path key components.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[AcceptedPlannerFieldPathIndexField] {
        self.fields.as_slice()
    }

    /// Return the reduced semantic access contract used by selected access
    /// paths after planner candidate discovery.
    #[must_use]
    pub(in crate::db) fn semantic_access_contract(&self) -> SemanticIndexAccessContract {
        self.semantic_access_contract.clone()
    }

    /// Return accepted order terms for this field-path index.
    #[must_use]
    pub(in crate::db) fn order_terms(&self) -> Vec<String> {
        self.fields
            .iter()
            .map(AcceptedPlannerFieldPathIndexField::term)
            .collect()
    }

    fn debug_contract_consistent(&self) -> bool {
        !self.name().is_empty()
            && !self.store().is_empty()
            && self.semantic_access_contract().name() == self.name()
            && self.semantic_access_contract().store_path() == self.store()
            && self.semantic_access_contract().is_unique() == self.unique()
            && self.semantic_access_contract().key_arity() == self.fields().len()
            && !self.fields().is_empty()
            && self.order_terms().len() == self.fields().len()
            && self
                .fields()
                .iter()
                .all(AcceptedPlannerFieldPathIndexField::debug_contract_consistent)
    }
}

fn generated_predicate_bridge_for_accepted_field_path_index(
    indexes: &[&'static IndexModel],
    accepted: &crate::db::schema::SchemaIndexInfo,
) -> Option<&'static IndexModel> {
    indexes
        .iter()
        .copied()
        .find(|index| !index.has_expression_key_items() && index.name() == accepted.name())
}

///
/// AcceptedPlannerFieldPathIndexField
///
/// Planner-facing accepted field-path index key component.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedPlannerFieldPathIndexField {
    field_name: String,
    slot: usize,
    path: Vec<String>,
}

impl AcceptedPlannerFieldPathIndexField {
    fn from_schema_field(field: &crate::db::schema::SchemaIndexFieldPathInfo) -> Self {
        Self {
            field_name: field.field_name().to_string(),
            slot: field.slot(),
            path: field.path().to_vec(),
        }
    }

    /// Borrow the accepted top-level field name.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field_name.as_str()
    }

    /// Return the accepted row-layout slot for this key component.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> usize {
        self.slot
    }

    /// Borrow the accepted field path for this key component.
    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    fn term(&self) -> String {
        if self.path.len() <= 1 {
            return self.field_name.clone();
        }

        self.path.join(".")
    }

    fn debug_contract_consistent(&self) -> bool {
        !self.field_name().is_empty()
            && !self.path().is_empty()
            && self
                .path()
                .first()
                .is_some_and(|root| root == self.field_name())
            && self.slot() < usize::MAX
    }
}

impl<'a> VisibleIndexes<'a> {
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            generated_candidate_bridge_indexes: Cow::Borrowed(&[]),
            accepted_field_path_indexes: Vec::new(),
            accepted_schema_info: None,
            authority: VisibleIndexAuthority::StoreNotReady,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn planner_visible(indexes: &'a [&'static IndexModel]) -> Self {
        Self {
            generated_candidate_bridge_indexes: Cow::Borrowed(indexes),
            accepted_field_path_indexes: Vec::new(),
            accepted_schema_info: None,
            authority: VisibleIndexAuthority::GeneratedModelOnly,
        }
    }

    #[must_use]
    pub(in crate::db) const fn schema_owned(indexes: &'a [&'static IndexModel]) -> Self {
        Self {
            generated_candidate_bridge_indexes: Cow::Borrowed(indexes),
            accepted_field_path_indexes: Vec::new(),
            accepted_schema_info: None,
            authority: VisibleIndexAuthority::GeneratedModelOnly,
        }
    }

    #[must_use]
    pub(in crate::db) fn accepted_schema_visible(
        indexes: &'a [&'static IndexModel],
        schema_info: &SchemaInfo,
    ) -> Self {
        let accepted_field_path_indexes = schema_info
            .field_path_indexes()
            .iter()
            .map(|accepted| {
                AcceptedPlannerFieldPathIndex::from_schema_index(
                    accepted,
                    generated_predicate_bridge_for_accepted_field_path_index(indexes, accepted),
                )
            })
            .collect::<Vec<_>>();
        let accepted_indexes = indexes
            .iter()
            .copied()
            .filter(|index| index.has_expression_key_items())
            .collect();
        let accepted_field_path_index_count = accepted_field_path_indexes.len();

        Self {
            generated_candidate_bridge_indexes: Cow::Owned(accepted_indexes),
            accepted_field_path_indexes,
            accepted_schema_info: Some(schema_info.clone()),
            authority: VisibleIndexAuthority::AcceptedSchema {
                field_path_indexes: accepted_field_path_index_count,
            },
        }
    }

    #[must_use]
    pub(in crate::db) fn generated_candidate_bridge_indexes(&self) -> &[&'static IndexModel] {
        self.generated_candidate_bridge_indexes.as_ref()
    }

    /// Borrow accepted planner-facing field-path index contracts.
    #[must_use]
    pub(in crate::db) const fn accepted_field_path_indexes(
        &self,
    ) -> &[AcceptedPlannerFieldPathIndex] {
        self.accepted_field_path_indexes.as_slice()
    }

    /// Borrow the accepted schema info that authorized this visible-index view.
    #[must_use]
    pub(in crate::db) const fn accepted_schema_info(&self) -> Option<&SchemaInfo> {
        self.accepted_schema_info.as_ref()
    }

    /// Return whether accepted field-path planner contracts are internally
    /// consistent with their temporary generated index bridge.
    #[must_use]
    pub(in crate::db) fn accepted_field_path_contracts_are_consistent(&self) -> bool {
        self.accepted_field_path_indexes()
            .iter()
            .all(AcceptedPlannerFieldPathIndex::debug_contract_consistent)
    }

    #[must_use]
    pub(in crate::db) const fn accepted_field_path_index_count(&self) -> Option<usize> {
        match self.authority {
            VisibleIndexAuthority::AcceptedSchema { field_path_indexes } => {
                Some(field_path_indexes)
            }
            VisibleIndexAuthority::GeneratedModelOnly | VisibleIndexAuthority::StoreNotReady => {
                None
            }
        }
    }
}
