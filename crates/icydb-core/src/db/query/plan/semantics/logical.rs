//! Module: query::plan::semantics::logical
//! Responsibility: logical-plan semantic lowering from planner contracts to access-planned queries.
//! Does not own: access-path index selection internals or runtime execution behavior.
//! Boundary: derives planner-owned execution semantics, shape signatures, and continuation policy.

use crate::{
    db::{
        access::{AccessPlan, ExecutableAccessPlan, lower_executable_access_plan},
        predicate::{IndexCompileTarget, Predicate, PredicateProgram, normalize_enum_literals},
        query::plan::{
            AccessPlannedQuery, ContinuationPolicy, DistinctExecutionStrategy,
            EffectiveRuntimeFilterProgram, ExecutionShapeSignature, GroupPlan,
            GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, GroupedPlanStrategy,
            LogicalPlan, PlannerRouteProfile, QueryMode, ResolvedOrder, ResolvedOrderField,
            ResolvedOrderValueSource, ScalarPlan, StaticPlanningShape,
            derive_logical_pushdown_eligibility,
            expr::{
                Expr, ProjectionSpec, ScalarProjectionExpr,
                canonicalize_runtime_predicate_via_bool_expr, compile_scalar_projection_expr,
                compile_scalar_projection_plan, derive_normalized_bool_expr_predicate_subset,
                normalize_bool_expr,
            },
            grouped_aggregate_execution_specs, grouped_aggregate_specs_from_projection_spec,
            grouped_cursor_policy_violation, grouped_plan_strategy, lower_direct_projection_slots,
            lower_projection_identity, lower_projection_intent,
            residual_query_predicate_after_access_path_bounds,
            residual_query_predicate_after_filtered_access,
            resolved_grouped_distinct_execution_strategy_for_model,
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    model::{entity::EntityModel, index::IndexKeyItemsRef},
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

    /// Borrow scalar semantic fields mutably across logical variants for tests.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_semantics_mut(&mut self) -> &mut ScalarPlan {
        match self {
            Self::Scalar(plan) => plan,
            Self::Grouped(plan) => &mut plan.scalar,
        }
    }

    /// Test-only shorthand for explicit scalar semantic borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar(&self) -> &ScalarPlan {
        self.scalar_semantics()
    }

    /// Test-only shorthand for explicit mutable scalar semantic borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_mut(&mut self) -> &mut ScalarPlan {
        self.scalar_semantics_mut()
    }
}

impl AccessPlannedQuery {
    /// Borrow scalar semantic fields shared by scalar/grouped logical variants.
    #[must_use]
    pub(in crate::db) const fn scalar_plan(&self) -> &ScalarPlan {
        self.logical.scalar_semantics()
    }

    /// Borrow scalar semantic fields mutably across logical variants for tests.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_plan_mut(&mut self) -> &mut ScalarPlan {
        self.logical.scalar_semantics_mut()
    }

    /// Test-only shorthand for explicit scalar plan borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar(&self) -> &ScalarPlan {
        self.scalar_plan()
    }

    /// Test-only shorthand for explicit mutable scalar plan borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_mut(&mut self) -> &mut ScalarPlan {
        self.scalar_plan_mut()
    }

    /// Borrow grouped semantic fields when this plan is grouped.
    #[must_use]
    pub(in crate::db) const fn grouped_plan(&self) -> Option<&GroupPlan> {
        match &self.logical {
            LogicalPlan::Scalar(_) => None,
            LogicalPlan::Grouped(plan) => Some(plan),
        }
    }

    /// Lower this plan into one canonical planner-owned projection semantic spec.
    #[must_use]
    pub(in crate::db) fn projection_spec(&self, model: &EntityModel) -> ProjectionSpec {
        if let Some(static_shape) = &self.static_planning_shape {
            return static_shape.projection_spec.clone();
        }

        lower_projection_intent(model, &self.logical, &self.projection_selection)
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
    #[must_use]
    pub(in crate::db) fn execution_preparation_predicate(&self) -> Option<Predicate> {
        if let Some(static_shape) = self.static_planning_shape.as_ref() {
            return static_shape.execution_preparation_predicate.clone();
        }

        derive_execution_preparation_predicate(self)
    }

    /// Return the executor-facing residual predicate after removing any
    /// filtered-index guard clauses and fixed access-bound equalities already
    /// guaranteed by the chosen path.
    #[must_use]
    pub(in crate::db) fn effective_execution_predicate(&self) -> Option<Predicate> {
        if let Some(static_shape) = self.static_planning_shape.as_ref() {
            return static_shape.residual_filter_predicate.clone();
        }

        derive_residual_filter_predicate(self)
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
        if let Some(static_shape) = self.static_planning_shape.as_ref() {
            return static_shape.residual_filter_expr.as_ref();
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

    /// Borrow the planner-compiled execution-preparation predicate program.
    #[must_use]
    pub(in crate::db) const fn execution_preparation_compiled_predicate(
        &self,
    ) -> Option<&PredicateProgram> {
        self.static_planning_shape()
            .execution_preparation_compiled_predicate
            .as_ref()
    }

    /// Borrow the planner-compiled effective runtime predicate program.
    #[must_use]
    pub(in crate::db) const fn effective_runtime_compiled_predicate(
        &self,
    ) -> Option<&PredicateProgram> {
        match self
            .static_planning_shape()
            .effective_runtime_filter_program
            .as_ref()
        {
            Some(EffectiveRuntimeFilterProgram::Predicate(program)) => Some(program),
            Some(EffectiveRuntimeFilterProgram::Expr(_)) | None => None,
        }
    }

    /// Borrow the planner-compiled effective runtime scalar filter expression.
    #[must_use]
    pub(in crate::db) const fn effective_runtime_compiled_filter_expr(
        &self,
    ) -> Option<&ScalarProjectionExpr> {
        match self
            .static_planning_shape()
            .effective_runtime_filter_program
            .as_ref()
        {
            Some(EffectiveRuntimeFilterProgram::Expr(expr)) => Some(expr),
            Some(EffectiveRuntimeFilterProgram::Predicate(_)) | None => None,
        }
    }

    /// Borrow the planner-frozen effective runtime scalar filter program.
    #[must_use]
    pub(in crate::db) const fn effective_runtime_filter_program(
        &self,
    ) -> Option<&EffectiveRuntimeFilterProgram> {
        self.static_planning_shape()
            .effective_runtime_filter_program
            .as_ref()
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

    /// Freeze one planner-owned route profile after model validation completes.
    pub(in crate::db) fn finalize_planner_route_profile_for_model(&mut self, model: &EntityModel) {
        self.set_planner_route_profile(project_planner_route_profile_for_model(model, self));
    }

    /// Freeze planner-owned executor metadata after logical/access planning completes.
    pub(in crate::db) fn finalize_static_planning_shape_for_model(
        &mut self,
        model: &EntityModel,
    ) -> Result<(), InternalError> {
        self.static_planning_shape = Some(project_static_planning_shape_for_model(model, self)?);

        Ok(())
    }

    /// Build one immutable execution-shape signature contract for runtime layers.
    #[must_use]
    pub(in crate::db) fn execution_shape_signature(
        &self,
        entity_path: &'static str,
    ) -> ExecutionShapeSignature {
        ExecutionShapeSignature::new(self.continuation_signature(entity_path))
    }

    /// Return whether the chosen access contract fully satisfies the current
    /// scalar query predicate without any additional post-access filtering.
    #[must_use]
    pub(in crate::db) fn predicate_fully_satisfied_by_access_contract(&self) -> bool {
        if let Some(static_shape) = self.static_planning_shape.as_ref() {
            return self.scalar_plan().predicate.is_some()
                && static_shape.residual_filter_predicate.is_none()
                && static_shape.residual_filter_expr.is_none();
        }

        derive_predicate_fully_satisfied_by_access_contract(self)
    }

    /// Borrow the planner-frozen compiled scalar projection program.
    #[must_use]
    pub(in crate::db) fn scalar_projection_plan(&self) -> Option<&[ScalarProjectionExpr]> {
        self.static_planning_shape()
            .scalar_projection_plan
            .as_deref()
    }

    /// Borrow the planner-frozen primary-key field name.
    #[must_use]
    pub(in crate::db) const fn primary_key_name(&self) -> &'static str {
        self.static_planning_shape().primary_key_name
    }

    /// Borrow the planner-frozen projection slot reachability set.
    #[must_use]
    pub(in crate::db) const fn projection_referenced_slots(&self) -> &[usize] {
        self.static_planning_shape()
            .projection_referenced_slots
            .as_slice()
    }

    /// Borrow the planner-frozen mask for direct projected output slots.
    #[must_use]
    #[cfg(any(test, feature = "diagnostics"))]
    pub(in crate::db) const fn projected_slot_mask(&self) -> &[bool] {
        self.static_planning_shape().projected_slot_mask.as_slice()
    }

    /// Return whether projection remains the full model-identity field list.
    #[must_use]
    pub(in crate::db) const fn projection_is_model_identity(&self) -> bool {
        self.static_planning_shape().projection_is_model_identity
    }

    /// Borrow the planner-frozen ORDER BY slot reachability set, if any.
    #[must_use]
    pub(in crate::db) fn order_referenced_slots(&self) -> Option<&[usize]> {
        self.static_planning_shape()
            .order_referenced_slots
            .as_deref()
    }

    /// Borrow the planner-frozen resolved ORDER BY program, if one exists.
    #[must_use]
    pub(in crate::db) const fn resolved_order(&self) -> Option<&ResolvedOrder> {
        self.static_planning_shape().resolved_order.as_ref()
    }

    /// Borrow the planner-frozen access slot map used by index predicate compilation.
    #[must_use]
    pub(in crate::db) fn slot_map(&self) -> Option<&[usize]> {
        self.static_planning_shape().slot_map.as_deref()
    }

    /// Borrow grouped aggregate execution specs already resolved during static planning.
    #[must_use]
    pub(in crate::db) fn grouped_aggregate_execution_specs(
        &self,
    ) -> Option<&[GroupedAggregateExecutionSpec]> {
        self.static_planning_shape()
            .grouped_aggregate_execution_specs
            .as_deref()
    }

    /// Borrow the planner-resolved grouped DISTINCT execution strategy when present.
    #[must_use]
    pub(in crate::db) const fn grouped_distinct_execution_strategy(
        &self,
    ) -> Option<&GroupedDistinctExecutionStrategy> {
        self.static_planning_shape()
            .grouped_distinct_execution_strategy
            .as_ref()
    }

    /// Borrow the frozen projection semantic shape without reopening model ownership.
    #[must_use]
    pub(in crate::db) const fn frozen_projection_spec(&self) -> &ProjectionSpec {
        &self.static_planning_shape().projection_spec
    }

    /// Borrow the frozen direct projection slots without reopening model ownership.
    #[must_use]
    pub(in crate::db) fn frozen_direct_projection_slots(&self) -> Option<&[usize]> {
        self.static_planning_shape()
            .projection_direct_slots
            .as_deref()
    }

    /// Borrow the planner-frozen key-item-aware compile targets for the chosen access path.
    #[must_use]
    pub(in crate::db) fn index_compile_targets(&self) -> Option<&[IndexCompileTarget]> {
        self.static_planning_shape()
            .index_compile_targets
            .as_deref()
    }

    const fn static_planning_shape(&self) -> &StaticPlanningShape {
        self.static_planning_shape
            .as_ref()
            .expect("access-planned queries must freeze static planning shape before execution")
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

/// Project one planner-owned route profile from the finalized logical+access plan.
#[must_use]
pub(in crate::db) fn project_planner_route_profile_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> PlannerRouteProfile {
    let secondary_order_contract = plan
        .scalar_plan()
        .order
        .as_ref()
        .and_then(|order| order.deterministic_secondary_order_contract(model.primary_key.name));

    PlannerRouteProfile::new(
        derive_continuation_policy_validated(plan),
        derive_logical_pushdown_eligibility(plan, secondary_order_contract.as_ref()),
        secondary_order_contract,
    )
}

fn project_static_planning_shape_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<StaticPlanningShape, InternalError> {
    let projection_spec = lower_projection_intent(model, &plan.logical, &plan.projection_selection);
    let execution_preparation_predicate = plan.execution_preparation_predicate();
    let residual_filter_predicate = derive_residual_filter_predicate(plan);
    let residual_filter_expr = derive_residual_filter_expr_for_model(model, plan);
    let execution_preparation_compiled_predicate =
        compile_optional_predicate(model, execution_preparation_predicate.as_ref());
    let effective_runtime_filter_program = compile_effective_runtime_filter_program(
        model,
        residual_filter_expr.as_ref(),
        residual_filter_predicate.as_ref(),
    )?;
    let scalar_projection_plan =
        if plan.grouped_plan().is_none() {
            Some(compile_scalar_projection_plan(model, &projection_spec).ok_or_else(|| {
            InternalError::query_executor_invariant(
                "scalar projection program must compile during static planning finalization",
            )
        })?)
        } else {
            None
        };
    let (grouped_aggregate_execution_specs, grouped_distinct_execution_strategy) =
        resolve_grouped_static_planning_semantics(model, plan, &projection_spec)?;
    let projection_direct_slots =
        lower_direct_projection_slots(model, &plan.logical, &plan.projection_selection);
    let projection_referenced_slots = projection_spec.referenced_slots_for(model)?;
    let projected_slot_mask =
        projected_slot_mask_for_spec(model, projection_direct_slots.as_deref());
    let projection_is_model_identity = projection_spec.is_model_identity_for(model);
    let resolved_order = resolved_order_for_plan(model, plan)?;
    let order_referenced_slots = order_referenced_slots_for_resolved_order(resolved_order.as_ref());
    let slot_map = slot_map_for_model_plan(model, plan);
    let index_compile_targets = index_compile_targets_for_model_plan(model, plan);

    Ok(StaticPlanningShape {
        primary_key_name: model.primary_key.name,
        projection_spec,
        execution_preparation_predicate,
        residual_filter_expr,
        residual_filter_predicate,
        execution_preparation_compiled_predicate,
        effective_runtime_filter_program,
        scalar_projection_plan,
        grouped_aggregate_execution_specs,
        grouped_distinct_execution_strategy,
        projection_direct_slots,
        projection_referenced_slots,
        projected_slot_mask,
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
    model: &EntityModel,
    residual_filter_expr: Option<&Expr>,
    residual_filter_predicate: Option<&Predicate>,
) -> Result<Option<EffectiveRuntimeFilterProgram>, InternalError> {
    // Keep the existing predicate fast path when the residual semantics still
    // fit the derived predicate contract. The expression-owned lane is only
    // needed once pushdown loses semantic coverage and a residual predicate no
    // longer exists.
    if let Some(predicate) = residual_filter_predicate {
        return Ok(Some(EffectiveRuntimeFilterProgram::Predicate(
            PredicateProgram::compile(model, predicate),
        )));
    }

    if let Some(filter_expr) = residual_filter_expr {
        let compiled = compile_scalar_projection_expr(model, filter_expr).ok_or_else(|| {
            InternalError::query_invalid_logical_plan(
                "effective runtime scalar filter expression must compile during static planning finalization",
            )
        })?;

        return Ok(Some(EffectiveRuntimeFilterProgram::Expr(compiled)));
    }

    Ok(None)
}

// Derive the executor-preparation predicate once from the selected access path.
// This strips only filtered-index guard clauses while preserving access-bound
// equalities that still matter to preparation/explain consumers.
fn derive_execution_preparation_predicate(plan: &AccessPlannedQuery) -> Option<Predicate> {
    let query_predicate = plan.scalar_plan().predicate.as_ref()?;

    match plan.access.selected_index_model() {
        Some(index) => residual_query_predicate_after_filtered_access(index, query_predicate),
        None => Some(query_predicate.clone()),
    }
}

// Derive the final residual predicate once from the already-filtered
// preparation predicate plus any equality bounds guaranteed by the concrete
// access path.
fn derive_residual_filter_predicate(plan: &AccessPlannedQuery) -> Option<Predicate> {
    let filtered_residual = derive_execution_preparation_predicate(plan);
    let filtered_residual = filtered_residual.as_ref()?;

    residual_query_predicate_after_access_path_bounds(plan.access.as_path(), filtered_residual)
}

// Derive the explicit residual semantic expression once for finalized plans.
// The residual expression remains the planner-owned semantic filter when any
// runtime filtering still survives access satisfaction.
fn derive_residual_filter_expr(plan: &AccessPlannedQuery) -> Option<Expr> {
    let filter_expr = plan.scalar_plan().filter_expr.as_ref()?;
    if derive_semantic_filter_fully_satisfied_by_access_contract(plan) {
        return None;
    }

    Some(filter_expr.clone())
}

// Derive the explicit residual semantic expression during finalization using
// the trusted entity schema so compare-family literal normalization matches the
// planner-owned predicate contract before residual ownership is decided.
fn derive_residual_filter_expr_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Option<Expr> {
    let filter_expr = plan.scalar_plan().filter_expr.as_ref()?;
    if derive_semantic_filter_fully_satisfied_by_access_contract_for_model(model, plan) {
        return None;
    }

    Some(filter_expr.clone())
}

// Return whether any residual filtering survives after access planning. This
// helper exists only for pre-finalization assembly; finalized plans must read
// the explicit residual artifacts frozen in `StaticPlanningShape`.
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

// Return true when the planner-owned predicate contract is fully satisfied by
// access planning and no semantic residual filter expression survives.
fn derive_predicate_fully_satisfied_by_access_contract(plan: &AccessPlannedQuery) -> bool {
    plan.scalar_plan().predicate.is_some()
        && derive_residual_filter_predicate(plan).is_none()
        && derive_residual_filter_expr(plan).is_none()
}

// Return true when the semantic filter expression is entirely represented by
// the planner-owned predicate contract and the chosen access path satisfies
// that predicate without any runtime remainder.
fn derive_semantic_filter_fully_satisfied_by_access_contract(plan: &AccessPlannedQuery) -> bool {
    let Some(filter_expr) = plan.scalar_plan().filter_expr.as_ref() else {
        return false;
    };
    let normalized_filter_expr = normalize_bool_expr(filter_expr.clone());
    let Some(filter_predicate) =
        derive_normalized_bool_expr_predicate_subset(&normalized_filter_expr)
    else {
        return false;
    };
    let Some(query_predicate) = plan.scalar_plan().predicate.as_ref() else {
        return false;
    };

    canonicalize_runtime_predicate_via_bool_expr(filter_predicate)
        == canonicalize_runtime_predicate_via_bool_expr(query_predicate.clone())
}

// Return true when finalized planning can prove that the semantic filter
// expression is completely represented by the planner-owned predicate contract
// after aligning compare literals through the trusted entity schema.
fn derive_semantic_filter_fully_satisfied_by_access_contract_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> bool {
    let Some(filter_expr) = plan.scalar_plan().filter_expr.as_ref() else {
        return false;
    };
    let normalized_filter_expr = normalize_bool_expr(filter_expr.clone());
    let Some(filter_predicate) =
        derive_normalized_bool_expr_predicate_subset(&normalized_filter_expr)
    else {
        return false;
    };
    let Some(query_predicate) = plan.scalar_plan().predicate.as_ref() else {
        return false;
    };
    let schema = SchemaInfo::cached_for_entity_model(model);
    let Ok(filter_predicate) = normalize_enum_literals(schema, &filter_predicate) else {
        return false;
    };
    let Ok(query_predicate) = normalize_enum_literals(schema, query_predicate) else {
        return false;
    };

    canonicalize_runtime_predicate_via_bool_expr(filter_predicate)
        == canonicalize_runtime_predicate_via_bool_expr(query_predicate)
}

// Compile one optional planner-frozen predicate program while keeping the
// static planning assembly path free of repeated `Option` mapping boilerplate.
fn compile_optional_predicate(
    model: &EntityModel,
    predicate: Option<&Predicate>,
) -> Option<PredicateProgram> {
    predicate.map(|predicate| PredicateProgram::compile(model, predicate))
}

// Resolve the grouped-only static planning semantics bundle once so grouped
// aggregate execution specs and grouped DISTINCT strategy stay derived under
// one shared grouped-plan branch.
fn resolve_grouped_static_planning_semantics(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    projection_spec: &ProjectionSpec,
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
        grouped.group.group_fields.as_slice(),
        grouped.group.aggregates.as_slice(),
    )?;
    extend_grouped_having_aggregate_specs(&mut aggregate_specs, grouped)?;

    let grouped_aggregate_execution_specs = Some(grouped_aggregate_execution_specs(
        model,
        aggregate_specs.as_slice(),
    )?);
    let grouped_distinct_execution_strategy =
        Some(resolved_grouped_distinct_execution_strategy_for_model(
            model,
            grouped.group.group_fields.as_slice(),
            grouped.group.aggregates.as_slice(),
            grouped.having_expr.as_ref(),
        )?);

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
        collect_grouped_having_expr_aggregate_specs(aggregate_specs, having_expr)?;
    }

    Ok(())
}

fn collect_grouped_having_expr_aggregate_specs(
    aggregate_specs: &mut Vec<GroupedAggregateExecutionSpec>,
    expr: &Expr,
) -> Result<(), InternalError> {
    if !expr.contains_aggregate() {
        return Ok(());
    }

    expr.try_for_each_tree_aggregate(&mut |aggregate_expr| {
        let aggregate_spec = GroupedAggregateExecutionSpec::from_aggregate_expr(aggregate_expr);

        if aggregate_specs
            .iter()
            .all(|current| current != &aggregate_spec)
        {
            aggregate_specs.push(aggregate_spec);
        }

        Ok(())
    })
}

fn projected_slot_mask_for_spec(
    model: &EntityModel,
    direct_projection_slots: Option<&[usize]>,
) -> Vec<bool> {
    let mut projected_slots = vec![false; model.fields().len()];

    let Some(direct_projection_slots) = direct_projection_slots else {
        return projected_slots;
    };

    for slot in direct_projection_slots.iter().copied() {
        if let Some(projected) = projected_slots.get_mut(slot) {
            *projected = true;
        }
    }

    projected_slots
}

fn resolved_order_for_plan(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<Option<ResolvedOrder>, InternalError> {
    if grouped_plan_strategy(plan).is_some_and(GroupedPlanStrategy::is_top_k_group) {
        return Ok(None);
    }

    let Some(order) = plan.scalar_plan().order.as_ref() else {
        return Ok(None);
    };

    let mut fields = Vec::with_capacity(order.fields.len());
    for term in &order.fields {
        fields.push(ResolvedOrderField::new(
            resolved_order_value_source_for_term(model, term)?,
            term.direction(),
        ));
    }

    Ok(Some(ResolvedOrder::new(fields)))
}

fn resolved_order_value_source_for_term(
    model: &EntityModel,
    term: &crate::db::query::plan::OrderTerm,
) -> Result<ResolvedOrderValueSource, InternalError> {
    if term.direct_field().is_none() {
        let rendered = term.rendered_label();
        validate_resolved_order_expr_fields(model, term.expr(), rendered.as_str())?;
        let compiled = compile_scalar_projection_expr(model, term.expr())
            .ok_or_else(|| order_expression_scalar_seam_error(rendered.as_str()))?;

        return Ok(ResolvedOrderValueSource::expression(compiled));
    }

    let field = term
        .direct_field()
        .expect("direct-field order branch should only execute for field-backed terms");
    let slot = resolve_required_field_slot(model, field, || {
        InternalError::query_invalid_logical_plan(format!(
            "order expression references unknown field '{field}'",
        ))
    })?;

    Ok(ResolvedOrderValueSource::direct_field(slot))
}

fn validate_resolved_order_expr_fields(
    model: &EntityModel,
    expr: &Expr,
    rendered: &str,
) -> Result<(), InternalError> {
    expr.try_for_each_tree_expr(&mut |node| match node {
        Expr::Field(field_id) => resolve_required_field_slot(model, field_id.as_str(), || {
            InternalError::query_invalid_logical_plan(format!(
                "order expression references unknown field '{rendered}'",
            ))
        })
        .map(|_| ()),
        Expr::Aggregate(_) => Err(order_expression_scalar_seam_error(rendered)),
        #[cfg(test)]
        Expr::Alias { .. } => Err(order_expression_scalar_seam_error(rendered)),
        Expr::Unary { .. } => Err(order_expression_scalar_seam_error(rendered)),
        _ => Ok(()),
    })
}

// Resolve one model field slot while keeping planner invalid-logical-plan
// error construction at the callsite that owns the diagnostic wording.
fn resolve_required_field_slot<F>(
    model: &EntityModel,
    field: &str,
    invalid_plan_error: F,
) -> Result<usize, InternalError>
where
    F: FnOnce() -> InternalError,
{
    model
        .resolve_field_slot(field)
        .ok_or_else(invalid_plan_error)
}

// Keep the scalar-order expression seam violation text under one helper so the
// parse validation and compile validation paths do not drift.
fn order_expression_scalar_seam_error(rendered: &str) -> InternalError {
    InternalError::query_invalid_logical_plan(format!(
        "order expression '{rendered}' did not stay on the scalar expression seam",
    ))
}

// Keep one stable executor-facing slot list for grouped order terms after the
// planner has frozen the structural `ResolvedOrder`. The grouped Top-K route
// now consumes this same referenced-slot contract instead of re-deriving order
// sources from planner strategy at runtime.
fn order_referenced_slots_for_resolved_order(
    resolved_order: Option<&ResolvedOrder>,
) -> Option<Vec<usize>> {
    Some(resolved_order?.referenced_slots())
}

fn slot_map_for_model_plan(model: &EntityModel, plan: &AccessPlannedQuery) -> Option<Vec<usize>> {
    let executable = lower_executable_access_plan(&plan.access);

    resolved_index_slots_for_access_path(model, &executable)
}

fn resolved_index_slots_for_access_path(
    model: &EntityModel,
    access: &ExecutableAccessPlan<'_, crate::value::Value>,
) -> Option<Vec<usize>> {
    let path = access.as_path()?;
    let path_capabilities = path.capabilities();
    let index_fields = path_capabilities.index_fields_for_slot_map()?;
    let mut slots = Vec::with_capacity(index_fields.len());

    for field_name in index_fields {
        let slot = model.resolve_field_slot(field_name)?;
        slots.push(slot);
    }

    Some(slots)
}

fn index_compile_targets_for_model_plan(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Option<Vec<IndexCompileTarget>> {
    let index = plan.access.as_path()?.selected_index_model()?;
    let mut targets = Vec::new();

    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            for (component_index, &field_name) in fields.iter().enumerate() {
                let field_slot = model.resolve_field_slot(field_name)?;
                targets.push(IndexCompileTarget {
                    component_index,
                    field_slot,
                    key_item: crate::model::index::IndexKeyItem::Field(field_name),
                });
            }
        }
        IndexKeyItemsRef::Items(items) => {
            for (component_index, &key_item) in items.iter().enumerate() {
                let field_slot = model.resolve_field_slot(key_item.field())?;
                targets.push(IndexCompileTarget {
                    component_index,
                    field_slot,
                    key_item,
                });
            }
        }
    }

    Some(targets)
}
