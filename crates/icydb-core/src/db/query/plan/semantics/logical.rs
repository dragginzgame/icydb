//! Module: query::plan::semantics::logical
//! Responsibility: logical-plan semantic lowering from planner contracts to access-planned queries.
//! Does not own: access-path index selection internals or runtime execution behavior.
//! Boundary: derives planner-owned execution semantics, shape signatures, and continuation policy.

use crate::db::{QueryError, query::preparation::PreparationWork};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::borrow::Cow;

use crate::db::predicate::MissingRowPolicy;
use crate::{
    db::{
        access::{AccessPlan, SemanticIndexKeyItemRef},
        predicate::{IndexCompileTarget, IndexCompileTargetKind, Predicate, PredicateProgram},
        query::plan::{
            AccessPlannedQuery, ContinuationPolicy, DistinctExecutionStrategy,
            EffectiveRuntimeFilterProgram, ExecutionShapeSignature, GroupPlan,
            GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, GroupedPlanStrategy,
            LogicalPlan, PlannerRouteProfile, PredicatePushdownDiagnostics, QueryMode,
            ResidualFilterContract, ResidualFilterShape, ResolvedOrder, ResolvedOrderField,
            ResolvedOrderValueSource, ScalarPlan, StaticExecutionPlanningContract,
            derive_logical_pushdown_eligibility,
            expr::{
                CompiledExpr, Expr, ProjectionSpec, compile_scalar_projection_expr_with_schema,
                compile_scalar_projection_plan_with_schema,
            },
            extend_unique_grouped_aggregate_specs_from_expr, grouped_aggregate_execution_specs,
            grouped_aggregate_specs_from_projection_spec, grouped_cursor_policy_violation,
            grouped_plan_strategy, lower_direct_projection_layouts_with_schema,
            lower_projection_identity, lower_projection_intent_with_schema,
            residual_query_predicate_after_access_path_bounds,
            residual_query_predicate_after_filtered_access_contract,
            resolved_grouped_distinct_execution_strategy_with_schema_info,
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    value::Value,
};

impl QueryMode {
    /// True if this mode represents a load intent.
    #[must_use]
    pub const fn is_load(&self) -> bool {
        match self {
            Self::Load(_) => true,
            Self::Delete(_) => false,
        }
    }

    /// True if this mode represents a delete intent.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        match self {
            Self::Delete(_) => true,
            Self::Load(_) => false,
        }
    }
}

impl LogicalPlan {
    /// Borrow scalar semantic fields shared by scalar/grouped logical variants.
    #[must_use]
    pub(in crate::db) const fn scalar_semantics(&self) -> &ScalarPlan {
        match self {
            Self::Scalar(plan) => plan,
            Self::Grouped(plan) => &plan.scalar,
        }
    }
}

impl AccessPlannedQuery {
    /// Borrow scalar semantic fields shared by scalar/grouped logical variants.
    #[must_use]
    pub(in crate::db) const fn scalar_plan(&self) -> &ScalarPlan {
        self.logical.scalar_semantics()
    }

    /// Borrow scalar missing-row consistency without exposing the full scalar
    /// plan to executor owners that only need row-presence policy.
    #[must_use]
    pub(in crate::db) fn scalar_consistency(&self) -> MissingRowPolicy {
        if self.access.has_selected_index_access_path() {
            // An accepted secondary index is a persisted claim that every
            // emitted key identifies an authoritative row. Ignoring a missing
            // row would turn accepted-index corruption into an incomplete
            // successful result.
            MissingRowPolicy::Error
        } else {
            self.scalar_plan().consistency
        }
    }

    /// Borrow grouped semantic fields when this plan is grouped.
    #[must_use]
    pub(in crate::db) const fn grouped_plan(&self) -> Option<&GroupPlan> {
        match &self.logical {
            LogicalPlan::Scalar(_) => None,
            LogicalPlan::Grouped(plan) => Some(plan),
        }
    }

    /// Borrow the projection retained by successful accepted-schema finalization.
    /// Unprepared plans cannot reconstruct execution metadata on demand.
    pub(in crate::db) fn projection_spec(&self) -> Result<&ProjectionSpec, QueryError> {
        self.static_execution_planning_contract
            .as_ref()
            .map(|contract| &contract.projection_spec)
            .ok_or_else(QueryError::invariant)
    }

    /// Construct the projection once for semantic validation and finalization.
    pub(in crate::db) fn prepare_projection(
        &self,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<ProjectionSpec, QueryError> {
        lower_projection_intent_with_schema(schema, &self.logical, &self.projection_selection, work)
    }

    /// Lower this plan into one projection semantic shape for identity hashing.
    #[must_use]
    pub(in crate::db::query) fn projection_spec_for_identity(&self) -> ProjectionSpec {
        lower_projection_identity(&self.logical, &self.projection_selection)
    }

    /// Return the executor-facing predicate after removing only filtered-index
    /// guard clauses the chosen access path already proves.
    ///
    /// This conservative form is used by preparation/explain surfaces that
    /// still need to see access-bound equalities as index-predicate input.
    /// Copies are admitted before construction; guard pruning consumes them.
    pub(in crate::db) fn execution_preparation_predicate(
        &self,
        work: &PreparationWork<'_>,
    ) -> Result<Option<Predicate>, QueryError> {
        if let Some(static_contract) = self.static_execution_planning_contract.as_ref() {
            return static_contract
                .execution_preparation_predicate
                .as_ref()
                .map(|predicate| work.copy_predicate(predicate))
                .transpose();
        }

        let predicate = self
            .scalar_plan()
            .predicate
            .as_ref()
            .map(|predicate| work.copy_predicate(predicate))
            .transpose()?;
        Ok(derive_execution_preparation_predicate(
            &self.access,
            predicate,
        ))
    }

    /// Return the executor-facing residual predicate after removing any
    /// filtered-index guard clauses and fixed access-bound equalities already
    /// guaranteed by the chosen path.
    /// Finalized plans lend their frozen predicate; only pre-finalization
    /// derivation owns a temporary. Presence and projection must not copy it.
    #[must_use]
    pub(in crate::db) fn effective_execution_predicate(&self) -> Option<Cow<'_, Predicate>> {
        if let Some(static_contract) = self.static_execution_planning_contract.as_ref() {
            return static_contract
                .residual_filter_contract
                .residual_filter_predicate()
                .map(Cow::Borrowed);
        }

        derive_residual_filter_predicate(self.scalar_plan(), &self.access).map(Cow::Owned)
    }

    /// Return whether one explicit residual predicate survives access
    /// planning and still participates in residual execution.
    #[must_use]
    pub(in crate::db) fn has_residual_filter_predicate(&self) -> bool {
        self.effective_execution_predicate().is_some()
    }

    /// Borrow the planner-owned residual scalar filter expression when one
    /// surviving semantic remainder still requires runtime evaluation.
    #[must_use]
    pub(in crate::db) fn residual_filter_expr(&self) -> Option<&Expr> {
        if let Some(static_contract) = self.static_execution_planning_contract.as_ref() {
            return static_contract
                .residual_filter_contract
                .residual_filter_expr();
        }

        if !derive_has_residual_filter(self) {
            return None;
        }

        self.scalar_plan().filter_expr.as_ref()
    }

    /// Return whether one explicit residual scalar filter expression survives
    /// access planning and still requires runtime evaluation.
    #[must_use]
    pub(in crate::db) fn has_residual_filter_expr(&self) -> bool {
        self.residual_filter_expr().is_some()
    }

    /// Return the planner-owned residual-filter shape used by diagnostics.
    #[must_use]
    pub(in crate::db) fn residual_filter_shape(&self) -> ResidualFilterShape {
        if let Some(static_contract) = self.static_execution_planning_contract.as_ref() {
            return static_contract.residual_filter_contract.shape();
        }

        residual_filter_facts_for_access(self.scalar_plan(), &self.access).0
    }

    /// Return the planner-owned predicate pushdown label consumed by verbose
    /// execution diagnostics.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn predicate_pushdown_label(&self) -> String {
        self.predicate_pushdown_diagnostics().label()
    }

    /// Return planner-owned predicate-pushdown diagnostics.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn predicate_pushdown_diagnostics(&self) -> PredicatePushdownDiagnostics {
        if let Some(static_contract) = self.static_execution_planning_contract.as_ref() {
            return static_contract.predicate_pushdown_diagnostics;
        }

        derive_predicate_pushdown_diagnostics(self, self.residual_filter_shape())
    }

    /// Return the planner-owned predicate-pushdown outcome label.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn predicate_pushdown_outcome_label(&self) -> &'static str {
        self.predicate_pushdown_diagnostics().outcome_label()
    }

    /// Return the planner-owned predicate-pushdown reason label.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn predicate_pushdown_reason_label(&self) -> &'static str {
        self.predicate_pushdown_diagnostics().reason_label()
    }

    /// Borrow the planner-compiled execution-preparation predicate program.
    #[must_use]
    pub(in crate::db) fn execution_preparation_compiled_predicate(
        &self,
    ) -> Option<&PredicateProgram> {
        self.static_execution_planning_contract()?
            .execution_preparation_compiled_predicate
            .as_ref()
    }

    /// Borrow the planner-compiled effective runtime predicate program.
    #[must_use]
    pub(in crate::db) fn effective_runtime_compiled_predicate(&self) -> Option<&PredicateProgram> {
        match self
            .static_execution_planning_contract()?
            .residual_filter_contract
            .effective_runtime_filter_program()
        {
            Some(program) => program.predicate_program(),
            None => None,
        }
    }

    /// Borrow the planner-frozen effective runtime scalar filter program.
    #[must_use]
    pub(in crate::db) fn effective_runtime_filter_program(
        &self,
    ) -> Option<&EffectiveRuntimeFilterProgram> {
        self.static_execution_planning_contract()?
            .residual_filter_contract
            .effective_runtime_filter_program()
    }

    /// Lower scalar DISTINCT semantics into one executor-facing execution strategy.
    #[must_use]
    pub(in crate::db) fn distinct_execution_strategy(&self) -> DistinctExecutionStrategy {
        if !self.scalar_plan().distinct {
            return DistinctExecutionStrategy::None;
        }

        // DISTINCT on duplicate-safe single-path access shapes is a planner
        // no-op for runtime dedup mechanics. Composite shapes can surface
        // duplicate keys and therefore retain explicit dedup execution.
        match distinct_runtime_dedup_strategy(&self.access) {
            Some(strategy) => strategy,
            None => DistinctExecutionStrategy::None,
        }
    }

    /// Freeze one planner-owned route profile from accepted schema authority.
    pub(in crate::db) fn finalize_planner_route_profile_for_model_with_schema(
        &mut self,
        schema_info: &SchemaInfo,
    ) {
        self.set_planner_route_profile(project_planner_route_profile_for_schema(schema_info, self));
    }

    /// Freeze planner-owned executor metadata, consuming the validated projection.
    /// Accepted group keys are already resolved during intent conversion;
    /// rebinding refreshes slot authority without changing projection expressions.
    pub(in crate::db) fn finalize_static_execution_planning_contract_with_schema(
        &mut self,
        schema_info: &SchemaInfo,
        projection: ProjectionSpec,
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        self.bind_group_field_slots_to_schema(schema_info, work)?;
        self.static_execution_planning_contract =
            Some(project_static_execution_planning_contract_with_schema(
                schema_info,
                self,
                projection,
                work,
            )?);

        Ok(())
    }

    // Resolve authoring-time group field names onto accepted slots before the
    // plan becomes executable.
    fn bind_group_field_slots_to_schema(
        &mut self,
        schema_info: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        let LogicalPlan::Grouped(grouped) = &mut self.logical else {
            return Ok(());
        };

        let accepted_fields = grouped
            .group
            .group_fields
            .resolve_with_schema(schema_info, work)?
            .ok_or_else(|| QueryError::execute(InternalError::planner_executor_invariant()))?;
        grouped.group.group_fields = accepted_fields;

        Ok(())
    }

    /// Build one immutable execution-shape signature contract for runtime layers.
    pub(in crate::db) fn execution_shape_signature(
        &self,
        entity_path: &str,
    ) -> Result<ExecutionShapeSignature, InternalError> {
        Ok(ExecutionShapeSignature::new(
            self.continuation_signature(entity_path)?,
        ))
    }

    /// Return whether the chosen access contract fully satisfies the current
    /// scalar query predicate without any additional runtime residual filtering.
    #[must_use]
    pub(in crate::db) fn predicate_fully_satisfied_by_access_contract(&self) -> bool {
        if let Some(static_contract) = self.static_execution_planning_contract.as_ref() {
            return self.scalar_plan().predicate.is_some()
                && !static_contract
                    .residual_filter_contract
                    .has_residual_filter();
        }

        derive_predicate_fully_satisfied_by_access_contract(self)
    }

    /// Borrow the planner-frozen compiled scalar projection program.
    #[must_use]
    pub(in crate::db) fn scalar_projection_plan(&self) -> Option<&[CompiledExpr]> {
        self.static_execution_planning_contract()?
            .scalar_projection_plan
            .as_deref()
    }

    /// Return whether planner-owned static execution metadata has already been frozen.
    #[must_use]
    pub(in crate::db) const fn has_static_execution_planning_contract(&self) -> bool {
        self.static_execution_planning_contract.is_some()
    }

    /// Borrow the planner-frozen ordered primary-key field names.
    pub(in crate::db) fn primary_key_names(&self) -> Result<&[String], InternalError> {
        Ok(&self
            .require_static_execution_planning_contract()?
            .primary_key_names)
    }

    /// Borrow the planner-frozen projection slot reachability set.
    pub(in crate::db) fn projection_referenced_slots(&self) -> Result<&[usize], InternalError> {
        Ok(self
            .require_static_execution_planning_contract()?
            .projection_referenced_slots
            .as_slice())
    }

    /// Return whether projection remains the full model-identity field list.
    pub(in crate::db) fn projection_is_model_identity(&self) -> Result<bool, InternalError> {
        Ok(self
            .require_static_execution_planning_contract()?
            .projection_is_model_identity)
    }

    /// Borrow the planner-frozen ORDER BY slot reachability set, if any.
    #[must_use]
    pub(in crate::db) fn order_referenced_slots(&self) -> Option<&[usize]> {
        self.static_execution_planning_contract()?
            .order_referenced_slots
            .as_deref()
    }

    /// Borrow the planner-frozen resolved ORDER BY program, if one exists.
    #[must_use]
    pub(in crate::db) fn resolved_order(&self) -> Option<&ResolvedOrder> {
        self.static_execution_planning_contract()?
            .resolved_order
            .as_ref()
    }

    /// Borrow the planner-frozen access slot map used by index predicate compilation.
    #[must_use]
    pub(in crate::db) fn slot_map(&self) -> Option<&[usize]> {
        self.static_execution_planning_contract()?
            .slot_map
            .as_deref()
    }

    /// Borrow grouped aggregate execution specs already resolved during static planning.
    #[must_use]
    pub(in crate::db) fn grouped_aggregate_execution_specs(
        &self,
    ) -> Option<&[GroupedAggregateExecutionSpec]> {
        self.static_execution_planning_contract()?
            .grouped_aggregate_execution_specs
            .as_deref()
    }

    /// Borrow the planner-resolved grouped DISTINCT execution strategy when present.
    #[must_use]
    pub(in crate::db) fn grouped_distinct_execution_strategy(
        &self,
    ) -> Option<&GroupedDistinctExecutionStrategy> {
        self.static_execution_planning_contract()?
            .grouped_distinct_execution_strategy
            .as_ref()
    }

    /// Borrow the frozen projection semantic shape without reopening model ownership.
    pub(in crate::db) fn frozen_projection_spec(&self) -> Result<&ProjectionSpec, InternalError> {
        Ok(&self
            .require_static_execution_planning_contract()?
            .projection_spec)
    }

    /// Borrow the frozen direct projection slots without reopening model ownership.
    #[must_use]
    pub(in crate::db) fn frozen_direct_projection_slots(&self) -> Option<&[usize]> {
        self.static_execution_planning_contract()?
            .projection_direct_slots
            .as_deref()
    }

    /// Borrow duplicate-preserving direct projection slots for raw data-row readers.
    #[must_use]
    pub(in crate::db) fn frozen_data_row_direct_projection_slots(&self) -> Option<&[usize]> {
        self.static_execution_planning_contract()?
            .projection_data_row_direct_slots
            .as_deref()
    }

    /// Borrow the planner-frozen key-item-aware compile targets for the chosen access path.
    #[must_use]
    pub(in crate::db) fn index_compile_targets(&self) -> Option<&[IndexCompileTarget]> {
        self.static_execution_planning_contract()?
            .index_compile_targets
            .as_deref()
    }

    const fn static_execution_planning_contract(&self) -> Option<&StaticExecutionPlanningContract> {
        self.static_execution_planning_contract.as_ref()
    }

    fn require_static_execution_planning_contract(
        &self,
    ) -> Result<&StaticExecutionPlanningContract, InternalError> {
        self.static_execution_planning_contract
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)
    }
}

fn distinct_runtime_dedup_strategy<K>(access: &AccessPlan<K>) -> Option<DistinctExecutionStrategy> {
    match access {
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => {
            Some(DistinctExecutionStrategy::PreOrdered)
        }
        AccessPlan::Path(path) if path.as_ref().is_index_multi_lookup() => {
            Some(DistinctExecutionStrategy::HashMaterialize)
        }
        AccessPlan::Path(_) => None,
    }
}

fn derive_continuation_policy_validated(plan: &AccessPlannedQuery) -> ContinuationPolicy {
    let is_grouped_safe = plan
        .grouped_plan()
        .is_none_or(|grouped| grouped_cursor_policy_violation(grouped, true).is_none());

    ContinuationPolicy::new(
        true, // Continuation resume windows require anchor semantics for pushdown-safe replay.
        true, // Continuation resumes must advance strictly to prevent replay/regression loops.
        is_grouped_safe,
    )
}

/// Project one planner-owned route profile from accepted schema authority.
#[must_use]
pub(in crate::db) fn project_planner_route_profile_for_schema(
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> PlannerRouteProfile {
    let secondary_order_contract = plan.scalar_plan().order.as_ref().and_then(|order| {
        order.deterministic_secondary_order_contract_fields(schema_info.shared_primary_key_names())
    });

    PlannerRouteProfile::new(
        derive_continuation_policy_validated(plan),
        derive_logical_pushdown_eligibility(plan, secondary_order_contract.as_ref()),
        secondary_order_contract,
    )
}

fn project_static_execution_planning_contract_with_schema(
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    projection_spec: ProjectionSpec,
    work: &PreparationWork<'_>,
) -> Result<StaticExecutionPlanningContract, QueryError> {
    let execution_preparation_predicate = plan.execution_preparation_predicate(work)?;
    // Preparation retains its predicate independently. Admit the residual's
    // owned copy before pruning it in place; pruning creates no new predicate backing.
    let residual_input = execution_preparation_predicate
        .as_ref()
        .map(|predicate| work.copy_predicate(predicate))
        .transpose()?;
    let residual_filter_predicate = derive_residual_filter_predicate_from_preparation(
        plan.scalar_plan(),
        &plan.access,
        residual_input,
    );
    let residual_filter_expr = derive_residual_filter_expr(plan);
    let effective_runtime_filter_program = compile_effective_runtime_filter_program(
        schema_info,
        residual_filter_expr.as_ref(),
        residual_filter_predicate.as_ref(),
        work,
    )
    .map_err(QueryError::execute)?;
    let residual_filter_contract = ResidualFilterContract::new(
        residual_filter_expr,
        residual_filter_predicate,
        effective_runtime_filter_program,
    );
    let residual_filter_shape = residual_filter_contract.shape();
    let execution_preparation_compiled_predicate =
        (should_compile_execution_preparation_predicate(residual_filter_shape)
            && !planner_predicate_requires_expression_runtime(plan.scalar_plan()))
        .then(|| compile_optional_predicate(schema_info, execution_preparation_predicate.as_ref()))
        .flatten();
    let predicate_pushdown_diagnostics =
        derive_predicate_pushdown_diagnostics(plan, residual_filter_shape);
    let scalar_projection_plan = if plan.grouped_plan().is_none() {
        Some(
            compile_scalar_projection_plan_with_schema(schema_info, &projection_spec, work)
                .map_err(QueryError::execute)?
                .ok_or_else(|| QueryError::execute(InternalError::query_executor_invariant()))?,
        )
    } else {
        None
    };
    let (grouped_aggregate_execution_specs, grouped_distinct_execution_strategy) =
        resolve_grouped_static_planning_semantics(schema_info, plan, &projection_spec, work)
            .map_err(QueryError::execute)?;
    let (projection_direct_slots, projection_data_row_direct_slots) =
        lower_direct_projection_layouts_with_schema(
            schema_info,
            &plan.logical,
            &projection_spec,
            work,
        )?;
    let projection_referenced_slots =
        projection_spec.referenced_slots_for_schema(schema_info, work)?;
    let projection_is_model_identity = projection_spec.is_schema_identity_for(schema_info, work)?;
    let resolved_order = resolved_order_for_plan(schema_info, plan, work)?;
    let order_referenced_slots = resolved_order
        .as_ref()
        .map(|order| order.referenced_slots(work))
        .transpose()?;
    let (slot_map, index_compile_targets) =
        index_execution_metadata_for_schema_plan(schema_info, plan, work)?
            .map_or((None, None), |(slots, targets)| {
                (Some(slots), Some(targets))
            });

    Ok(StaticExecutionPlanningContract {
        primary_key_names: schema_info.shared_primary_key_names(),
        projection_spec,
        execution_preparation_predicate,
        execution_preparation_compiled_predicate,
        residual_filter_contract,
        predicate_pushdown_diagnostics,
        scalar_projection_plan,
        grouped_aggregate_execution_specs,
        grouped_distinct_execution_strategy,
        projection_direct_slots,
        projection_data_row_direct_slots,
        projection_referenced_slots,
        projection_is_model_identity,
        resolved_order,
        order_referenced_slots,
        slot_map,
        index_compile_targets,
    })
}

// Compile the executor-owned residual scalar filter contract once from the
// planner-derived residual artifacts so runtime never has to rediscover
// residual presence or shape from semantic/filter/pushdown state.
fn compile_effective_runtime_filter_program(
    schema_info: &SchemaInfo,
    residual_filter_expr: Option<&Expr>,
    residual_filter_predicate: Option<&Predicate>,
    work: &PreparationWork<'_>,
) -> Result<Option<EffectiveRuntimeFilterProgram>, InternalError> {
    // Keep the existing predicate fast path when the residual semantics still
    // fit the derived predicate contract. The expression-owned lane is only
    // needed once pushdown loses semantic coverage and a residual predicate no
    // longer exists.
    if let Some(predicate) = residual_filter_predicate {
        return Ok(Some(EffectiveRuntimeFilterProgram::predicate(
            PredicateProgram::compile_with_schema_info(schema_info, predicate),
        )));
    }

    if let Some(filter_expr) = residual_filter_expr {
        let compiled = compile_scalar_projection_expr_with_schema(schema_info, filter_expr, work)?
            .ok_or_else(InternalError::query_invalid_logical_plan)?;

        return Ok(Some(EffectiveRuntimeFilterProgram::expression(compiled)));
    }

    Ok(None)
}

// Derive the executor-preparation predicate once from the selected access path.
// This strips only filtered-index guard clauses while preserving access-bound
// equalities that still matter to preparation/explain consumers.
fn derive_execution_preparation_predicate(
    access: &AccessPlan<Value>,
    query_predicate: Option<Predicate>,
) -> Option<Predicate> {
    let query_predicate = query_predicate?;

    match access.selected_index_contract() {
        Some(index) => {
            residual_query_predicate_after_filtered_access_contract(index, query_predicate)
        }
        None => Some(query_predicate),
    }
}

// Derive the final residual predicate once from the already-filtered
// preparation predicate plus any equality bounds guaranteed by the concrete
// access path.
fn derive_residual_filter_predicate(
    scalar: &ScalarPlan,
    access: &AccessPlan<Value>,
) -> Option<Predicate> {
    let filtered_residual =
        derive_execution_preparation_predicate(access, scalar.predicate.clone());

    derive_residual_filter_predicate_from_preparation(scalar, access, filtered_residual)
}

fn derive_residual_filter_predicate_from_preparation(
    scalar: &ScalarPlan,
    access: &AccessPlan<Value>,
    execution_preparation_predicate: Option<Predicate>,
) -> Option<Predicate> {
    let execution_preparation_predicate = execution_preparation_predicate?;

    let residual = residual_query_predicate_after_access_path_bounds(
        access.as_path(),
        execution_preparation_predicate,
    );
    if residual.is_some() && planner_predicate_requires_expression_runtime(scalar) {
        return None;
    }

    residual
}

// Derive the explicit residual semantic expression once for finalized plans.
// The residual expression remains the planner-owned semantic filter when any
// runtime filtering still survives access satisfaction.
fn derive_residual_filter_expr(plan: &AccessPlannedQuery) -> Option<Expr> {
    let filter_expr = plan.scalar_plan().filter_expr.as_ref()?;
    if derive_semantic_filter_fully_satisfied_by_access_contract(plan.scalar_plan())
        && (!planner_predicate_requires_expression_runtime(plan.scalar_plan())
            || planner_predicate_is_fully_satisfied_by_access_contract(plan))
    {
        return None;
    }

    Some(filter_expr.clone())
}

// Nested-path predicate shells are planner facts only: the predicate runtime
// addresses top-level row slots. Keep the already-compiled expression as the
// execution authority unless the selected access path proves the predicate in
// full and no runtime filter remains.
fn planner_predicate_requires_expression_runtime(scalar: &ScalarPlan) -> bool {
    scalar.predicate_covers_filter_expr
        && scalar
            .filter_expr
            .as_ref()
            .is_some_and(Expr::contains_field_path)
}

fn planner_predicate_is_fully_satisfied_by_access_contract(plan: &AccessPlannedQuery) -> bool {
    let Some(predicate) =
        derive_execution_preparation_predicate(&plan.access, plan.scalar_plan().predicate.clone())
    else {
        return false;
    };

    residual_query_predicate_after_access_path_bounds(plan.access.as_path(), predicate).is_none()
}

// Return whether any residual filtering survives after access planning. This
// helper exists only for pre-finalization assembly; finalized plans must read
// the explicit residual artifacts frozen in `StaticExecutionPlanningContract`.
fn derive_has_residual_filter(plan: &AccessPlannedQuery) -> bool {
    match (
        plan.scalar_plan().filter_expr.as_ref(),
        plan.scalar_plan().predicate.as_ref(),
    ) {
        (None, None) => false,
        (Some(_), None) => true,
        (Some(_) | None, Some(_)) => !plan.predicate_fully_satisfied_by_access_contract(),
    }
}

// Freeze predicate-pushdown diagnostics from one logical plan shape. This keeps
// lazy plan accessors and finalized static planning on the same argument
// contract while leaving route selection and residual filtering unchanged.
fn derive_predicate_pushdown_diagnostics(
    plan: &AccessPlannedQuery,
    residual_filter_shape: ResidualFilterShape,
) -> PredicatePushdownDiagnostics {
    PredicatePushdownDiagnostics::from_plan(
        plan.scalar_plan().filter_expr.is_some(),
        plan.scalar_plan().predicate_covers_filter_expr,
        plan.scalar_plan().predicate.as_ref(),
        &plan.access,
        residual_filter_shape,
    )
}

// Return true when the planner-owned predicate contract is fully satisfied by
// access planning and no semantic residual filter expression survives.
fn derive_predicate_fully_satisfied_by_access_contract(plan: &AccessPlannedQuery) -> bool {
    plan.scalar_plan().predicate.is_some()
        && derive_residual_filter_predicate(plan.scalar_plan(), &plan.access).is_none()
        && derive_residual_filter_expr(plan).is_none()
}

// Return true when the semantic filter expression is entirely represented by
// the planner-owned predicate contract and the chosen access path satisfies
// that predicate without any runtime remainder.
const fn derive_semantic_filter_fully_satisfied_by_access_contract(scalar: &ScalarPlan) -> bool {
    scalar.filter_expr.is_some()
        && scalar.predicate.is_some()
        && scalar.predicate_covers_filter_expr
}

/// Derive pre-finalization residual facts from borrowed candidate inputs.
/// Finalized plans must continue reading their frozen residual contract instead.
#[must_use]
pub(in crate::db::query) fn residual_filter_facts_for_access(
    scalar: &ScalarPlan,
    access: &AccessPlan<Value>,
) -> (ResidualFilterShape, Option<Predicate>) {
    let predicate = derive_residual_filter_predicate(scalar, access);
    // Preserve the pre-finalization shape policy: a fully predicate-represented
    // filter does not add a second expression category to candidate ranking.
    let expression_required = scalar.filter_expr.is_some()
        && !derive_semantic_filter_fully_satisfied_by_access_contract(scalar);

    (
        ResidualFilterShape::from_presence(expression_required, predicate.is_some()),
        predicate,
    )
}

// Compile one optional planner-frozen predicate program while keeping the
// static planning assembly path free of repeated `Option` mapping boilerplate.
fn compile_optional_predicate(
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Option<PredicateProgram> {
    predicate.map(|predicate| PredicateProgram::compile_with_schema_info(schema_info, predicate))
}

// Avoid compiling large access-proven predicates into executor preparation.
// When no residual filter survives, the chosen access route already enforces
// the predicate and route/explain consumers can use the explicit residual
// contract instead of recompiling access-bound literals.
const fn should_compile_execution_preparation_predicate(
    residual_filter_shape: ResidualFilterShape,
) -> bool {
    !residual_filter_shape.is_absent()
}

// Resolve the grouped-only static planning semantics bundle once so grouped
// aggregate execution specs and grouped DISTINCT strategy stay derived under
// one shared grouped-plan branch.
fn resolve_grouped_static_planning_semantics(
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    projection_spec: &ProjectionSpec,
    work: &PreparationWork<'_>,
) -> Result<
    (
        Option<Vec<GroupedAggregateExecutionSpec>>,
        Option<GroupedDistinctExecutionStrategy>,
    ),
    InternalError,
> {
    let Some(grouped) = plan.grouped_plan() else {
        return Ok((None, None));
    };

    let mut aggregate_specs = grouped_aggregate_specs_from_projection_spec(
        projection_spec,
        &grouped.group.group_fields,
        grouped.group.aggregates.as_slice(),
    )?;
    extend_grouped_having_aggregate_specs(&mut aggregate_specs, grouped)?;

    let grouped_aggregate_execution_specs = Some(grouped_aggregate_execution_specs(
        schema_info,
        aggregate_specs,
        work,
    )?);
    let grouped_distinct_execution_strategy = Some(
        resolved_grouped_distinct_execution_strategy_with_schema_info(
            schema_info,
            &grouped.group.group_fields,
            grouped.group.aggregates.as_slice(),
            grouped.having_expr.as_ref(),
        )?,
    );

    Ok((
        grouped_aggregate_execution_specs,
        grouped_distinct_execution_strategy,
    ))
}

fn extend_grouped_having_aggregate_specs(
    aggregate_specs: &mut Vec<GroupedAggregateExecutionSpec>,
    grouped: &GroupPlan,
) -> Result<(), InternalError> {
    if let Some(having_expr) = grouped.having_expr.as_ref() {
        extend_unique_grouped_aggregate_specs_from_expr(aggregate_specs, having_expr)?;
    }

    Ok(())
}

fn resolved_order_for_plan(
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    work: &PreparationWork<'_>,
) -> Result<Option<ResolvedOrder>, QueryError> {
    if grouped_plan_strategy(plan).is_some_and(GroupedPlanStrategy::is_top_k_group) {
        return Ok(None);
    }

    let Some(order) = plan.scalar_plan().order.as_ref() else {
        return Ok(None);
    };

    let mut fields = work.vec_with_capacity(order.fields.len())?;
    for term in &order.fields {
        fields.push(ResolvedOrderField::new(
            resolved_order_value_source_for_term(schema_info, term, work)?,
            term.direction(),
        ));
    }

    Ok(Some(ResolvedOrder::new(fields)))
}

fn resolved_order_value_source_for_term(
    schema_info: &SchemaInfo,
    term: &crate::db::query::plan::OrderTerm,
    work: &PreparationWork<'_>,
) -> Result<ResolvedOrderValueSource, QueryError> {
    if let Some(field) = term.direct_field() {
        work.charge(Resource::PredicateExpressionSteps, 1 + field.len() as u64)?;
        let slot = schema_info
            .field_slot_index(field)
            .ok_or_else(|| QueryError::execute(InternalError::query_invalid_logical_plan()))?;

        return Ok(ResolvedOrderValueSource::direct_field(slot));
    }

    validate_resolved_order_scalar_seam(term.expr(), work)?;
    // Scalar compilation owns accepted field resolution. Do not resolve every
    // field again in the seam check or render a label for payload-free errors.
    let compiled = compile_scalar_projection_expr_with_schema(schema_info, term.expr(), work)
        .map_err(QueryError::execute)?
        .ok_or_else(|| QueryError::execute(InternalError::query_invalid_logical_plan()))?;

    Ok(ResolvedOrderValueSource::expression(compiled))
}

// The input-admitted tree has bounded depth. Charge each visited node before
// descending; compilation and referenced-slot construction remain separate owners.
fn validate_resolved_order_scalar_seam(
    expr: &Expr,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    expr.try_for_each_tree_expr(&mut |node| {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        match node {
            Expr::Aggregate(_) | Expr::Unary { .. } => Err(QueryError::execute(
                InternalError::query_invalid_logical_plan(),
            )),
            #[cfg(test)]
            Expr::Alias { .. } => Err(QueryError::execute(
                InternalError::query_invalid_logical_plan(),
            )),
            _ => Ok(()),
        }
    })
}

type IndexExecutionMetadata = (Vec<usize>, Vec<IndexCompileTarget>);

// Both executor slot maps and predicate compile targets describe the same
// ordered key items. Resolve each root once and admit both destination arrays
// before filling them. Missing schema resolution stays distinct from exhaustion.
fn index_execution_metadata_for_schema_plan(
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    work: &PreparationWork<'_>,
) -> Result<Option<IndexExecutionMetadata>, QueryError> {
    let executable = plan.access.executable_contract();
    let Some(path) = executable.as_path() else {
        return Ok(None);
    };
    let Some(key_items) = path.shape_facts().index_key_items_for_slot_map() else {
        return Ok(None);
    };
    let mut slots = work.vec_with_capacity(key_items.key_arity())?;
    let mut targets = work.vec_with_capacity(key_items.key_arity())?;

    for (component_index, key_item) in key_items.key_items().iter().enumerate() {
        let key_item = key_item.as_ref();
        let field = key_item.field();
        work.charge(Resource::PredicateExpressionSteps, 1 + field.len() as u64)?;
        let root = field.split_once('.').map_or(field, |(root, _)| root);
        let Some(field_slot) = schema_info.field_slot_index(root) else {
            return Ok(None);
        };
        slots.push(field_slot);
        targets.push(IndexCompileTarget {
            component_index,
            field_slot,
            kind: match key_item {
                SemanticIndexKeyItemRef::Field(_) => IndexCompileTargetKind::Field,
                SemanticIndexKeyItemRef::AcceptedExpression(expression) => {
                    IndexCompileTargetKind::Expression(expression.op())
                }
            },
        });
    }

    Ok(Some((slots, targets)))
}
