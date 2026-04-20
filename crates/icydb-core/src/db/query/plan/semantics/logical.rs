//! Module: query::plan::semantics::logical
//! Responsibility: logical-plan semantic lowering from planner contracts to access-planned queries.
//! Does not own: access-path index selection internals or runtime execution behavior.
//! Boundary: derives planner-owned execution semantics, shape signatures, and continuation policy.

use crate::{
    db::{
        access::{AccessPlan, ExecutableAccessPlan},
        predicate::IndexCompileTarget,
        predicate::{Predicate, PredicateProgram},
        query::plan::{
            AccessPlannedQuery, ContinuationPolicy, DistinctExecutionStrategy,
            EffectiveRuntimeFilterProgram, ExecutionShapeSignature, GroupPlan,
            GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, GroupedPlanStrategy,
            LogicalPlan, PlannerRouteProfile, QueryMode, ResolvedOrder, ResolvedOrderField,
            ResolvedOrderValueSource, ScalarPlan, StaticPlanningShape,
            derive_logical_pushdown_eligibility,
            expr::{
                Expr, ProjectionField, ProjectionSpec, ScalarProjectionExpr,
                compile_scalar_projection_expr, compile_scalar_projection_plan,
                projection_field_expr,
            },
            grouped_aggregate_execution_specs, grouped_aggregate_specs_from_projection_spec,
            grouped_cursor_policy_violation, grouped_plan_strategy, lower_direct_projection_slots,
            lower_projection_identity, lower_projection_intent,
            residual_query_predicate_after_access_path_bounds,
            residual_query_predicate_after_filtered_access,
            resolved_grouped_distinct_execution_strategy_for_model,
        },
    },
    error::InternalError,
    model::{
        entity::{EntityModel, resolve_field_slot},
        index::IndexKeyItemsRef,
    },
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
        lower_projection_identity(&self.logical)
    }

    /// Return the executor-facing predicate after removing only filtered-index
    /// guard clauses the chosen access path already proves.
    ///
    /// This conservative form is used by preparation/explain surfaces that
    /// still need to see access-bound equalities as index-predicate input.
    #[must_use]
    pub(in crate::db) fn execution_preparation_predicate(&self) -> Option<Predicate> {
        let query_predicate = self.scalar_plan().predicate.as_ref()?;

        match self.access.selected_index_model() {
            Some(index) => residual_query_predicate_after_filtered_access(index, query_predicate),
            None => Some(query_predicate.clone()),
        }
    }

    /// Return the executor-facing residual predicate after removing any
    /// filtered-index guard clauses and fixed access-bound equalities already
    /// guaranteed by the chosen path.
    #[must_use]
    pub(in crate::db) fn effective_execution_predicate(&self) -> Option<Predicate> {
        // Phase 1: strip only filtered-index guard clauses the chosen access
        // path already proves.
        let filtered_residual = self.execution_preparation_predicate();
        let filtered_residual = filtered_residual.as_ref()?;

        // Phase 2: strip any additional equality clauses already guaranteed by
        // the concrete access-path bounds, such as `tier = 'gold'` on one
        // selected `IndexPrefix(tier='gold', ...)` route.
        residual_query_predicate_after_access_path_bounds(self.access.as_path(), filtered_residual)
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
        self.scalar_plan().predicate.is_some() && self.effective_execution_predicate().is_none()
    }

    /// Return whether scalar filter semantics still require post-access
    /// filtering after accounting for any derived pushdown predicate and
    /// access-path equality bounds.
    #[must_use]
    pub(in crate::db) fn has_residual_filter(&self) -> bool {
        match (
            self.scalar_plan().filter_expr.as_ref(),
            self.scalar_plan().predicate.as_ref(),
        ) {
            (None, None) => false,
            (Some(_), None) => true,
            (Some(_) | None, Some(_)) => !self.predicate_fully_satisfied_by_access_contract(),
        }
    }

    /// Transitional alias for existing residual-filter call sites while scalar
    /// WHERE ownership is moving from predicate-only to expression-first.
    #[must_use]
    pub(in crate::db) fn has_residual_predicate(&self) -> bool {
        self.has_residual_filter()
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
    let execution_preparation_compiled_predicate =
        compile_optional_predicate(model, plan.execution_preparation_predicate().as_ref());
    let effective_runtime_filter_program = compile_effective_runtime_filter_program(model, plan)?;
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
    let projection_referenced_slots =
        projection_referenced_slots_for_spec(model, &projection_spec)?;
    let projected_slot_mask =
        projected_slot_mask_for_spec(model, projection_direct_slots.as_deref());
    let projection_is_model_identity =
        projection_is_model_identity_for_spec(model, &projection_spec);
    let resolved_order = resolved_order_for_plan(model, plan)?;
    let order_referenced_slots = order_referenced_slots_for_resolved_order(resolved_order.as_ref());
    let slot_map = slot_map_for_model_plan(model, plan);
    let index_compile_targets = index_compile_targets_for_model_plan(model, plan);

    Ok(StaticPlanningShape {
        primary_key_name: model.primary_key.name,
        projection_spec,
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

// Compile the executor-owned residual scalar filter contract once so runtime
// can consume either the predicate fast path or the expression-first filter
// path without rediscovering which boundary applies.
fn compile_effective_runtime_filter_program(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<Option<EffectiveRuntimeFilterProgram>, InternalError> {
    if !plan.has_residual_filter() {
        return Ok(None);
    }

    if let Some(filter_expr) = plan.scalar_plan().filter_expr.as_ref() {
        let compiled = compile_scalar_projection_expr(model, filter_expr).ok_or_else(|| {
            InternalError::query_invalid_logical_plan(
                "effective runtime scalar filter expression must compile during static planning finalization",
            )
        })?;

        return Ok(Some(EffectiveRuntimeFilterProgram::Expr(compiled)));
    }

    Ok(plan
        .effective_execution_predicate()
        .as_ref()
        .map(|predicate| {
            EffectiveRuntimeFilterProgram::Predicate(PredicateProgram::compile(model, predicate))
        }))
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
    match expr {
        Expr::Aggregate(aggregate_expr) => {
            let aggregate_spec = GroupedAggregateExecutionSpec::from_aggregate_expr(aggregate_expr);

            if aggregate_specs
                .iter()
                .all(|current| current != &aggregate_spec)
            {
                aggregate_specs.push(aggregate_spec);
            }
        }
        Expr::Field(_) | Expr::Literal(_) => {}
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_grouped_having_expr_aggregate_specs(aggregate_specs, arg)?;
            }
        }
        Expr::Unary { expr, .. } => {
            collect_grouped_having_expr_aggregate_specs(aggregate_specs, expr)?;
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                collect_grouped_having_expr_aggregate_specs(aggregate_specs, arm.condition())?;
                collect_grouped_having_expr_aggregate_specs(aggregate_specs, arm.result())?;
            }

            collect_grouped_having_expr_aggregate_specs(aggregate_specs, else_expr)?;
        }
        Expr::Binary { left, right, .. } => {
            collect_grouped_having_expr_aggregate_specs(aggregate_specs, left)?;
            collect_grouped_having_expr_aggregate_specs(aggregate_specs, right)?;
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            collect_grouped_having_expr_aggregate_specs(aggregate_specs, expr)?;
        }
    }

    Ok(())
}

fn projection_referenced_slots_for_spec(
    model: &EntityModel,
    projection: &ProjectionSpec,
) -> Result<Vec<usize>, InternalError> {
    let mut referenced = vec![false; model.fields().len()];

    for field in projection.fields() {
        mark_projection_expr_slots(
            model,
            projection_field_expr(field),
            referenced.as_mut_slice(),
        )?;
    }

    Ok(referenced
        .into_iter()
        .enumerate()
        .filter_map(|(slot, required)| required.then_some(slot))
        .collect())
}

fn mark_projection_expr_slots(
    model: &EntityModel,
    expr: &Expr,
    referenced: &mut [bool],
) -> Result<(), InternalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let slot = resolve_required_field_slot(model, field_name, || {
                InternalError::query_invalid_logical_plan(format!(
                    "projection expression references unknown field '{field_name}'",
                ))
            })?;
            referenced[slot] = true;
        }
        Expr::Literal(_) => {}
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                mark_projection_expr_slots(model, arg, referenced)?;
            }
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                mark_projection_expr_slots(model, arm.condition(), referenced)?;
                mark_projection_expr_slots(model, arm.result(), referenced)?;
            }
            mark_projection_expr_slots(model, else_expr.as_ref(), referenced)?;
        }
        Expr::Aggregate(_) => {}
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            mark_projection_expr_slots(model, expr.as_ref(), referenced)?;
        }
        Expr::Unary { expr, .. } => {
            mark_projection_expr_slots(model, expr.as_ref(), referenced)?;
        }
        Expr::Binary { left, right, .. } => {
            mark_projection_expr_slots(model, left.as_ref(), referenced)?;
            mark_projection_expr_slots(model, right.as_ref(), referenced)?;
        }
    }

    Ok(())
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

fn projection_is_model_identity_for_spec(model: &EntityModel, projection: &ProjectionSpec) -> bool {
    if projection.len() != model.fields().len() {
        return false;
    }

    for (field_model, projected_field) in model.fields().iter().zip(projection.fields()) {
        match projected_field {
            ProjectionField::Scalar {
                expr: Expr::Field(field_id),
                alias: None,
            } if field_id.as_str() == field_model.name() => {}
            ProjectionField::Scalar { .. } => return false,
        }
    }

    true
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
    match expr {
        Expr::Field(field_id) => {
            resolve_required_field_slot(model, field_id.as_str(), || {
                InternalError::query_invalid_logical_plan(format!(
                    "order expression references unknown field '{rendered}'",
                ))
            })?;
        }
        Expr::Literal(_) => {}
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                validate_resolved_order_expr_fields(model, arg, rendered)?;
            }
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                validate_resolved_order_expr_fields(model, arm.condition(), rendered)?;
                validate_resolved_order_expr_fields(model, arm.result(), rendered)?;
            }
            validate_resolved_order_expr_fields(model, else_expr.as_ref(), rendered)?;
        }
        Expr::Binary { left, right, .. } => {
            validate_resolved_order_expr_fields(model, left.as_ref(), rendered)?;
            validate_resolved_order_expr_fields(model, right.as_ref(), rendered)?;
        }
        Expr::Aggregate(_) => {
            return Err(order_expression_scalar_seam_error(rendered));
        }
        #[cfg(test)]
        Expr::Alias { .. } => {
            return Err(order_expression_scalar_seam_error(rendered));
        }
        Expr::Unary { .. } => {
            return Err(order_expression_scalar_seam_error(rendered));
        }
    }

    Ok(())
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
    resolve_field_slot(model, field).ok_or_else(invalid_plan_error)
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
    let resolved_order = resolved_order?;
    let mut referenced = Vec::new();

    // Keep one stable slot list without re-parsing order expressions after the
    // planner has already frozen structural ORDER BY sources.
    for field in resolved_order.fields() {
        field.source().extend_referenced_slots(&mut referenced);
    }

    Some(referenced)
}

fn slot_map_for_model_plan(model: &EntityModel, plan: &AccessPlannedQuery) -> Option<Vec<usize>> {
    let access_strategy = plan.access.resolve_strategy();
    let executable = access_strategy.executable();

    resolved_index_slots_for_access_path(model, executable)
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
        let slot = resolve_field_slot(model, field_name)?;
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
                let field_slot = resolve_field_slot(model, field_name)?;
                targets.push(IndexCompileTarget {
                    component_index,
                    field_slot,
                    key_item: crate::model::index::IndexKeyItem::Field(field_name),
                });
            }
        }
        IndexKeyItemsRef::Items(items) => {
            for (component_index, &key_item) in items.iter().enumerate() {
                let field_slot = resolve_field_slot(model, key_item.field())?;
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
