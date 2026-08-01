//! Module: query::plan
//! Responsibility: logical query-plan module wiring and boundary re-exports.
//! Does not own: plan-model construction or semantic helper implementation details.
//! Boundary: intent/explain/planner/validator consumers import from this root only.

mod access_choice;
mod access_plan;
mod access_planner;
mod aggregate_shape;
mod continuation;
mod covering;
pub(in crate::db) mod expr;
mod group;
mod grouped_layout;
mod key_item_match;
mod key_support;
mod logical_builder;
mod model;
mod model_builder;
mod order_contract;
mod order_term;
mod pipeline;
mod planner;
mod primary_key_access_proof;
mod primary_key_input_resource;
mod projection;
mod semantics;
pub(crate) mod validate;

use crate::db::{Predicate, access::SemanticIndexAccessContract, schema::SchemaInfo};

pub(in crate::db::query) use access_choice::rerank_access_plan_by_residual_burden_with_semantic_indexes;
pub(in crate::db) use access_choice::{
    AccessChoiceCandidateExplainSummary, AccessChoiceExplainSnapshot, AccessChoiceRejectedIndex,
    AccessChoiceResidualBurden, AccessChoiceSelectedReason, PrimaryKeyInputResourceSummary,
};
pub(in crate::db) use access_plan::AccessPlannedQuery;
pub(in crate::db) use access_plan::{
    EffectiveRuntimeFilterProgram, PlannedNonIndexAccessReason, PredicatePushdownDiagnostics,
    ResidualFilterContract, ResidualFilterShape, ResolvedOrder, ResolvedOrderField,
    ResolvedOrderValueSource, StaticExecutionPlanningContract,
};
pub(in crate::db::query) use access_planner::{
    AccessPlanningInputs, normalize_query_predicate, plan_query_access_with_accepted_schema,
};
pub(in crate::db) use aggregate_shape::AggregateShape;
pub(in crate::db) use continuation::{
    AcceptedContinuationIdentity, GroupedPaginationWindow, PlannedContinuationContract,
    ScalarAccessWindowPlan, effective_offset_for_cursor_window,
};
#[cfg(feature = "query")]
pub(in crate::db) use covering::CoveringReadField;
#[cfg(feature = "query")]
pub(in crate::db) use covering::CoveringReadFieldSource;
#[cfg(any(test, feature = "query"))]
pub(in crate::db) use covering::covering_hybrid_projection_execution_plan_with_schema_info;
pub(in crate::db) use covering::{
    CoveringExistingRowMode, CoveringHybridReadExecutionPlan, CoveringProjectionOrder,
    CoveringReadExecutionPlan, covering_read_execution_plan_with_schema_info,
    covering_strict_predicate_compatible,
};
#[cfg(feature = "sql-explain")]
pub(in crate::db) use covering::{
    covering_read_reason_code_for_load_plan, index_covering_existing_rows_terminal_eligible,
};
pub(in crate::db::query::plan) use group::extend_unique_grouped_aggregate_specs_from_expr;
#[cfg(feature = "query")]
pub(in crate::db) use group::grouped_executor_handoff;
pub(in crate::db) use group::{
    GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, GroupedExecutionRoute,
    PlannedProjectionLayout, grouped_aggregate_execution_specs,
    grouped_aggregate_specs_from_projection_spec,
    resolved_grouped_distinct_execution_strategy_with_schema_info,
};
pub(in crate::db) use grouped_layout::validate_grouped_projection_layout;
pub(in crate::db::query::plan) use key_support::field_key_contract_supports_operator;
pub(in crate::db::query) use logical_builder::{
    LogicalPlanningInputs, build_logical_plan, canonicalize_order_spec_for_grouping,
    logical_query_from_logical_inputs,
};
pub(in crate::db::query::plan) use model::FieldSlotAuthority;
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
pub(in crate::db) use order_term::index_key_item_order_terms;
pub(in crate::db) use pipeline::PreparedScalarPlanningState;
#[cfg(feature = "query")]
pub(in crate::db::query) use pipeline::try_build_count_cardinality_prefix_access_from_query_model;
#[cfg(feature = "query")]
pub(in crate::db) use pipeline::{CountCardinalityPrefixAccess, CountCardinalityPrefixValues};
pub(in crate::db::query) use pipeline::{
    build_query_model_plan_with_indexes_from_scalar_planning_state,
    prepare_query_model_scalar_planning_state_with_schema_info,
    try_build_trivial_scalar_load_plan_with_schema_info,
};
pub(in crate::db::query) use planner::PlannerError;
pub(in crate::db::query) use planner::{
    PlannedAccessSelection, plan_access_selection_with_order_and_accepted_semantic_indexes,
    plan_access_selection_with_order_and_semantic_indexes,
};
pub(in crate::db) use planner::{
    residual_query_predicate_after_access_path_bounds,
    residual_query_predicate_after_filtered_access_contract,
};
pub(in crate::db::query::plan) use primary_key_access_proof::PrimaryKeyAccessProof;
pub(in crate::db::query) use primary_key_input_resource::primary_key_input_resource_from_value_list;
#[cfg(feature = "sql")]
pub(in crate::db) use projection::lower_global_aggregate_projection;
pub(in crate::db::query) use projection::{
    lower_data_row_direct_projection_slots_with_schema, lower_direct_projection_slots_with_schema,
    lower_projection_identity, lower_projection_intent_with_schema,
};
#[cfg(feature = "sql-explain")]
pub(in crate::db) use semantics::access_plan_label;
#[cfg(feature = "sql")]
pub(in crate::db) use semantics::canonicalize_grouped_having_numeric_literal_for_slot;
pub(in crate::db) use semantics::{
    AccessPlanProjection, AggregateIdentity, AggregateSemanticKey, GroupDistinctAdmissibility,
    GroupDistinctPolicyReason, GroupedCursorPolicyViolation, GroupedPlanFallbackReason,
    GroupedPlanStrategy, explain_access_strategy_label, grouped_distinct_admissibility,
    grouped_having_binary_compare_op, grouped_having_compare_op_supported, project_access_plan,
    project_explain_access_path, resolve_global_distinct_field_aggregate,
};
pub(in crate::db) use semantics::{
    LogicalPushdownEligibility, derive_logical_pushdown_eligibility,
    grouped_cursor_policy_violation, grouped_plan_strategy,
};
pub(crate) use validate::PlanError;
pub(crate) use validate::PolicyPlanError;
#[cfg(feature = "sql")]
pub(in crate::db) use validate::resolve_aggregate_target_field_slot_with_schema;
pub(in crate::db) use validate::{
    resolve_group_field_slot_with_schema, validate_cursor_order_plan_shape,
};
pub(in crate::db::query) use validate::{
    validate_group_query_semantics_with_schema, validate_intent_plan_shape,
    validate_query_semantics_with_schema,
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

///
/// VisibleIndexes
///
/// Planner-bound index slice that has already passed runtime visibility
/// gating at the session boundary, or one schema-owned detached slice for
/// tooling/tests that intentionally do not carry runtime store context.
///

#[derive(Clone, Copy, Debug)]
enum VisibleIndexAuthority {
    StoreNotReady,
    AcceptedSchema {
        field_path_indexes: usize,
        expression_indexes: usize,
    },
}

#[derive(Clone, Debug)]
pub(in crate::db) struct VisibleIndexes {
    accepted_field_path_indexes: Vec<AcceptedPlannerFieldPathIndex>,
    accepted_expression_indexes: Vec<AcceptedPlannerExpressionIndex>,
    accepted_semantic_index_contracts: Vec<SemanticIndexAccessContract>,
    accepted_schema_info: Option<SchemaInfo>,
    authority: VisibleIndexAuthority,
}

///
/// AcceptedPlannerExpressionIndex
///
/// Planner-facing accepted expression index contract. This owns the accepted
/// expression key contract needed to migrate expression-index planning away
/// from generated index declarations.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedPlannerExpressionIndex {
    name: String,
    store: String,
    unique: bool,
    semantic_access_contract: SemanticIndexAccessContract,
}

impl AcceptedPlannerExpressionIndex {
    fn from_schema_index(accepted: &crate::db::schema::SchemaExpressionIndexInfo) -> Self {
        Self {
            name: accepted.name().to_string(),
            store: accepted.store().to_string(),
            unique: accepted.unique(),
            semantic_access_contract: SemanticIndexAccessContract::from_accepted_expression_index(
                accepted,
            ),
        }
    }

    /// Borrow the accepted stable index name.
    #[must_use]
    pub(in crate::db::query::plan) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow the accepted backing index store path.
    #[must_use]
    pub(in crate::db::query::plan) const fn store(&self) -> &str {
        self.store.as_str()
    }

    /// Return whether this accepted index enforces uniqueness.
    #[must_use]
    pub(in crate::db::query::plan) const fn unique(&self) -> bool {
        self.unique
    }

    /// Return the reduced semantic access contract used by selected access
    /// paths after planner candidate discovery.
    #[must_use]
    pub(in crate::db::query::plan) fn semantic_access_contract(
        &self,
    ) -> SemanticIndexAccessContract {
        self.semantic_access_contract.clone()
    }

    fn debug_contract_consistent(&self) -> bool {
        !self.name().is_empty()
            && !self.store().is_empty()
            && self.semantic_access_contract().name() == self.name()
            && self.semantic_access_contract().store_path() == self.store()
            && self.semantic_access_contract().is_unique() == self.unique()
            && self.semantic_access_contract().has_expression_key_items()
    }
}

///
/// AcceptedPlannerFieldPathIndex
///
/// Planner-facing accepted field-path index contract.
/// This owns the accepted schema metadata needed by field-path planner
/// decisions plus a reduced semantic access contract for selected-path
/// construction without retaining a generated index declaration.
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
    fn from_schema_index(accepted: &crate::db::schema::SchemaIndexInfo) -> Self {
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
            ),
        }
    }

    /// Borrow the accepted stable index name.
    #[must_use]
    pub(in crate::db::query::plan) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow the accepted backing index store path.
    #[must_use]
    pub(in crate::db::query::plan) const fn store(&self) -> &str {
        self.store.as_str()
    }

    /// Return whether this accepted index enforces uniqueness.
    #[must_use]
    pub(in crate::db::query::plan) const fn unique(&self) -> bool {
        self.unique
    }

    /// Borrow accepted field-path key components.
    #[must_use]
    pub(in crate::db::query::plan) const fn fields(&self) -> &[AcceptedPlannerFieldPathIndexField] {
        self.fields.as_slice()
    }

    /// Return the reduced semantic access contract used by selected access
    /// paths after planner candidate discovery.
    #[must_use]
    pub(in crate::db::query::plan) fn semantic_access_contract(
        &self,
    ) -> SemanticIndexAccessContract {
        self.semantic_access_contract.clone()
    }

    /// Return accepted order terms for this field-path index.
    #[must_use]
    pub(in crate::db::query::plan) fn order_terms(&self) -> Vec<String> {
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
    pub(in crate::db::query::plan) const fn field_name(&self) -> &str {
        self.field_name.as_str()
    }

    /// Return the accepted row-layout slot for this key component.
    #[must_use]
    pub(in crate::db::query::plan) const fn slot(&self) -> usize {
        self.slot
    }

    /// Borrow the accepted field path for this key component.
    #[must_use]
    pub(in crate::db::query::plan) const fn path(&self) -> &[String] {
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

impl VisibleIndexes {
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            accepted_field_path_indexes: Vec::new(),
            accepted_expression_indexes: Vec::new(),
            accepted_semantic_index_contracts: Vec::new(),
            accepted_schema_info: None,
            authority: VisibleIndexAuthority::StoreNotReady,
        }
    }

    #[must_use]
    pub(in crate::db) fn accepted_schema_visible(schema_info: &SchemaInfo) -> Self {
        let accepted_field_path_indexes = schema_info
            .field_path_indexes()
            .iter()
            .map(AcceptedPlannerFieldPathIndex::from_schema_index)
            .collect::<Vec<_>>();
        let accepted_expression_indexes = schema_info
            .expression_indexes()
            .iter()
            .map(AcceptedPlannerExpressionIndex::from_schema_index)
            .collect::<Vec<_>>();
        let accepted_semantic_index_contracts = sorted_accepted_semantic_index_contracts(
            accepted_field_path_indexes.as_slice(),
            accepted_expression_indexes.as_slice(),
        );
        let accepted_field_path_index_count = accepted_field_path_indexes.len();
        let accepted_expression_index_count = accepted_expression_indexes.len();

        Self {
            accepted_field_path_indexes,
            accepted_expression_indexes,
            accepted_semantic_index_contracts,
            accepted_schema_info: Some(schema_info.clone()),
            authority: VisibleIndexAuthority::AcceptedSchema {
                field_path_indexes: accepted_field_path_index_count,
                expression_indexes: accepted_expression_index_count,
            },
        }
    }

    /// Build one accepted-schema planning view that exposes no secondary indexes.
    ///
    /// Exact mutation selection uses this view to retain accepted field and
    /// codec authority while forcing authoritative primary-store traversal.
    #[must_use]
    #[cfg(feature = "query")]
    pub(in crate::db) fn accepted_schema_primary_only(schema_info: &SchemaInfo) -> Self {
        Self {
            accepted_field_path_indexes: Vec::new(),
            accepted_expression_indexes: Vec::new(),
            accepted_semantic_index_contracts: Vec::new(),
            accepted_schema_info: Some(schema_info.clone()),
            authority: VisibleIndexAuthority::AcceptedSchema {
                field_path_indexes: 0,
                expression_indexes: 0,
            },
        }
    }

    /// Borrow accepted planner-facing field-path index contracts.
    #[must_use]
    pub(in crate::db) const fn accepted_field_path_indexes(
        &self,
    ) -> &[AcceptedPlannerFieldPathIndex] {
        self.accepted_field_path_indexes.as_slice()
    }

    /// Borrow accepted planner-facing expression index contracts.
    #[must_use]
    pub(in crate::db) const fn accepted_expression_indexes(
        &self,
    ) -> &[AcceptedPlannerExpressionIndex] {
        self.accepted_expression_indexes.as_slice()
    }

    /// Borrow sorted reduced semantic contracts for accepted planner-visible
    /// indexes. Predicate planning and access-choice scoring consume this
    /// reduced surface instead of reprojecting it from the richer accepted
    /// field-path/expression contracts for every query.
    #[must_use]
    pub(in crate::db) const fn accepted_semantic_index_contracts(
        &self,
    ) -> &[SemanticIndexAccessContract] {
        self.accepted_semantic_index_contracts.as_slice()
    }

    /// Borrow the accepted schema info that authorized this visible-index view.
    #[must_use]
    pub(in crate::db) const fn accepted_schema_info(&self) -> Option<&SchemaInfo> {
        self.accepted_schema_info.as_ref()
    }

    /// Return whether accepted field-path planner contracts are internally
    /// consistent with their reduced semantic access facts.
    #[must_use]
    pub(in crate::db) fn accepted_field_path_contracts_are_consistent(&self) -> bool {
        self.accepted_field_path_indexes()
            .iter()
            .all(AcceptedPlannerFieldPathIndex::debug_contract_consistent)
    }

    /// Return whether accepted expression planner contracts are internally
    /// consistent with their reduced semantic access facts.
    #[must_use]
    pub(in crate::db) fn accepted_expression_contracts_are_consistent(&self) -> bool {
        self.accepted_expression_indexes()
            .iter()
            .all(AcceptedPlannerExpressionIndex::debug_contract_consistent)
    }

    /// Return whether accepted semantic contracts match the accepted
    /// field-path/expression counts and stay in deterministic planner order.
    #[must_use]
    pub(in crate::db) fn accepted_semantic_contracts_are_consistent(&self) -> bool {
        let expected_count =
            self.accepted_field_path_indexes.len() + self.accepted_expression_indexes.len();
        self.accepted_semantic_index_contracts.len() == expected_count
            && semantic_index_contracts_are_sorted(
                self.accepted_semantic_index_contracts.as_slice(),
            )
    }

    #[must_use]
    pub(in crate::db) const fn accepted_field_path_index_count(&self) -> Option<usize> {
        match self.authority {
            VisibleIndexAuthority::AcceptedSchema {
                field_path_indexes, ..
            } => Some(field_path_indexes),
            VisibleIndexAuthority::StoreNotReady => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn accepted_expression_index_count(&self) -> Option<usize> {
        match self.authority {
            VisibleIndexAuthority::AcceptedSchema {
                expression_indexes, ..
            } => Some(expression_indexes),
            VisibleIndexAuthority::StoreNotReady => None,
        }
    }
}

fn sorted_accepted_semantic_index_contracts(
    field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    expression_indexes: &[AcceptedPlannerExpressionIndex],
) -> Vec<SemanticIndexAccessContract> {
    let mut contracts = field_path_indexes
        .iter()
        .map(AcceptedPlannerFieldPathIndex::semantic_access_contract)
        .collect::<Vec<_>>();
    contracts.extend(
        expression_indexes
            .iter()
            .map(AcceptedPlannerExpressionIndex::semantic_access_contract),
    );
    contracts.sort_unstable_by(|left, right| left.name().cmp(right.name()));

    contracts
}

fn semantic_index_contracts_are_sorted(contracts: &[SemanticIndexAccessContract]) -> bool {
    contracts
        .windows(2)
        .all(|pair| pair[0].name() <= pair[1].name())
}
