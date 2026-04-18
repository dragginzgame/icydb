//! Module: query::plan::group
//! Responsibility: grouped-plan handoff contract between query planning and executor.
//! Does not own: grouped runtime execution logic.
//! Boundary: explicit grouped query-to-executor transfer surface.

use crate::{
    db::query::{
        builder::AggregateExpr,
        plan::{
            AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec,
            GroupDistinctAdmissibility, GroupDistinctPolicyReason, GroupedExecutionConfig,
            GroupedPlanStrategy,
            expr::{
                Expr, ProjectionSpec, ScalarProjectionExpr, compile_scalar_projection_expr,
                expr_references_only_fields, projection_field_expr,
            },
            grouped_distinct_admissibility, grouped_plan_strategy,
            resolve_aggregate_target_field_slot, resolve_global_distinct_field_aggregate,
            validate_grouped_projection_layout,
        },
    },
    error::InternalError,
    model::entity::EntityModel,
};

///
/// PlannedProjectionLayout
///
/// Planner-owned grouped projection position layout transferred to executor.
/// Positions are derived only from `ProjectionSpec` semantic shape and preserve
/// projection declaration order for grouped fields and aggregates.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PlannedProjectionLayout {
    pub(in crate::db) group_field_positions: Vec<usize>,
    pub(in crate::db) aggregate_positions: Vec<usize>,
}

///
/// GroupedAggregateExecutionSpec
///
/// GroupedAggregateExecutionSpec is the canonical grouped aggregate carrier
/// shared across planner projection analysis and grouped runtime handoff.
/// Semantic identity stays present for projection/HAVING matching, while
/// planner-owned slot resolution and compiled input preparation are attached
/// once under model ownership before execution begins.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedAggregateExecutionSpec {
    kind: AggregateKind,
    target_slot: Option<FieldSlot>,
    input_expr: Option<Expr>,
    compiled_input_expr: Option<ScalarProjectionExpr>,
    distinct: bool,
}

///
/// GroupedProjectionAggregateScan
///
/// Planner-local grouped projection aggregate scan result.
/// This keeps aggregate-bearing expression classification and first-seen
/// grouped aggregate slot introduction under one helper so grouped projection
/// handoff does not walk the same expression tree twice.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GroupedProjectionAggregateScan {
    contains_aggregate: bool,
    introduced_aggregate_count: usize,
}

impl GroupedProjectionAggregateScan {
    /// Build one empty grouped aggregate scan result.
    #[must_use]
    const fn none() -> Self {
        Self {
            contains_aggregate: false,
            introduced_aggregate_count: 0,
        }
    }

    /// Build one grouped aggregate scan result for a discovered aggregate leaf.
    #[must_use]
    const fn found_aggregate(introduced_aggregate_count: usize) -> Self {
        Self {
            contains_aggregate: true,
            introduced_aggregate_count,
        }
    }

    /// Merge two grouped aggregate scan results while preserving first-seen
    /// introduction counts across one projection expression walk.
    #[must_use]
    const fn combine(self, other: Self) -> Self {
        Self {
            contains_aggregate: self.contains_aggregate || other.contains_aggregate,
            introduced_aggregate_count: self
                .introduced_aggregate_count
                .saturating_add(other.introduced_aggregate_count),
        }
    }

    /// Return whether the scanned expression references at least one grouped aggregate leaf.
    #[must_use]
    const fn contains_aggregate(self) -> bool {
        self.contains_aggregate
    }

    /// Return how many new grouped aggregate slots this expression introduced.
    #[must_use]
    const fn introduced_aggregate_count(self) -> usize {
        self.introduced_aggregate_count
    }
}

impl GroupedAggregateExecutionSpec {
    /// Build one grouped aggregate spec from one semantic aggregate expression.
    #[must_use]
    pub(in crate::db) fn from_aggregate_expr(aggregate_expr: &AggregateExpr) -> Self {
        Self {
            kind: aggregate_expr.kind(),
            target_slot: None,
            input_expr: aggregate_expr.input_expr().cloned(),
            compiled_input_expr: None,
            distinct: aggregate_expr.is_distinct(),
        }
    }

    /// Resolve planner-owned grouped aggregate execution attachments for one model.
    pub(in crate::db) fn resolve_for_model(
        &self,
        model: &EntityModel,
    ) -> Result<Self, InternalError> {
        let compiled_input_expr = self
            .input_expr()
            .map(|expr| {
                compile_scalar_projection_expr(model, expr).ok_or_else(|| {
                    InternalError::planner_executor_invariant(format!(
                        "grouped aggregate execution input expression must stay on the scalar seam: kind={:?} input_expr={expr:?}",
                        self.kind(),
                    ))
                })
            })
            .transpose()?;
        let target_slot = self
            .target_field()
            .map(|field| {
                resolve_aggregate_target_field_slot(model, field).map_err(|err| {
                    InternalError::planner_executor_invariant(format!(
                        "grouped aggregate execution target slot resolution failed: field='{field}', error={err}",
                    ))
                })
            })
            .transpose()?;

        Ok(Self {
            kind: self.kind(),
            target_slot,
            input_expr: self.input_expr().cloned(),
            compiled_input_expr,
            distinct: self.distinct(),
        })
    }

    /// Return the grouped aggregate kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow the optional grouped aggregate target field label.
    #[must_use]
    pub(in crate::db) const fn target_field(&self) -> Option<&str> {
        match self.input_expr() {
            Some(Expr::Field(field_id)) => Some(field_id.as_str()),
            _ => None,
        }
    }

    /// Borrow the optional planner-resolved grouped aggregate target slot.
    #[must_use]
    pub(in crate::db) const fn target_slot(&self) -> Option<&FieldSlot> {
        self.target_slot.as_ref()
    }

    /// Borrow the canonical grouped aggregate input expression, if any.
    #[must_use]
    pub(in crate::db) const fn input_expr(&self) -> Option<&Expr> {
        self.input_expr.as_ref()
    }

    /// Return whether the grouped aggregate uses DISTINCT semantics.
    #[must_use]
    pub(in crate::db) const fn distinct(&self) -> bool {
        self.distinct
    }

    /// Return whether this grouped aggregate spec matches one planner-owned grouped aggregate.
    #[must_use]
    pub(in crate::db) fn matches_semantic_aggregate(&self, aggregate: &GroupAggregateSpec) -> bool {
        self.kind == aggregate.kind()
            && self.input_expr() == aggregate.semantic_input_expr_owned().as_ref()
            && self.distinct == aggregate.distinct()
    }

    /// Borrow the compiled grouped aggregate input expression used by runtime, if any.
    #[must_use]
    pub(in crate::db) const fn compiled_input_expr(&self) -> Option<&ScalarProjectionExpr> {
        self.compiled_input_expr.as_ref()
    }

    /// Return whether one aggregate expression matches this grouped execution spec semantically.
    #[must_use]
    pub(in crate::db) fn matches_aggregate_expr(&self, aggregate_expr: &AggregateExpr) -> bool {
        self.kind == aggregate_expr.kind()
            && self.input_expr() == aggregate_expr.input_expr()
            && self.distinct == aggregate_expr.is_distinct()
    }

    /// Build one grouped aggregate execution spec directly for tests that do
    /// not carry a model-owned grouped lowering context.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn from_parts_for_test(
        kind: AggregateKind,
        target_slot: Option<FieldSlot>,
        target_field: Option<&str>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            target_slot,
            input_expr: target_field
                .map(|field| Expr::Field(crate::db::query::plan::expr::FieldId::new(field))),
            compiled_input_expr: None,
            distinct,
        }
    }
}

impl PlannedProjectionLayout {
    /// Borrow grouped field positions in projection declaration order.
    #[must_use]
    pub(in crate::db) const fn group_field_positions(&self) -> &[usize] {
        self.group_field_positions.as_slice()
    }

    /// Borrow aggregate positions in projection declaration order.
    #[must_use]
    pub(in crate::db) const fn aggregate_positions(&self) -> &[usize] {
        self.aggregate_positions.as_slice()
    }

    /// Construct one grouped layout invariant for non-monotonic grouped-field positions.
    pub(in crate::db) fn group_field_positions_not_strictly_increasing() -> InternalError {
        InternalError::planner_executor_invariant(
            "grouped projection layout group-field positions must be strictly increasing",
        )
    }

    /// Construct one grouped layout invariant for non-monotonic aggregate positions.
    pub(in crate::db) fn aggregate_positions_not_strictly_increasing() -> InternalError {
        InternalError::planner_executor_invariant(
            "grouped projection layout aggregate positions must be strictly increasing",
        )
    }

    /// Construct one grouped layout invariant for mixed field/aggregate ordering.
    pub(in crate::db) fn group_fields_must_precede_aggregates() -> InternalError {
        InternalError::planner_executor_invariant(
            "grouped projection layout must keep group fields before aggregate terminals",
        )
    }

    /// Construct one grouped layout invariant for runtime projection splits
    /// that reference a layout position outside the projected value buffer.
    pub(in crate::db) fn projected_position_out_of_bounds(
        position_kind: &str,
        position: usize,
        projected_len: usize,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped projection layout {position_kind} position out of bounds: position={position}, projected_len={projected_len}",
        ))
    }
}

///
/// GroupedExecutorHandoff
///
/// Borrowed grouped planning handoff consumed at the query->executor boundary.
/// This contract keeps grouped execution routing input explicit while grouped
/// runtime entry remains explicit at query->executor boundaries.
///

///
/// GroupedFoldPath
///
/// Planner-carried grouped fold-path contract for executor runtime.
/// This is execution-mechanical only: it tells grouped runtime whether the
/// planner admitted the dedicated grouped `COUNT(*)` fold path or the
/// canonical generic grouped reducer path. Runtime must consume this contract
/// instead of branching on grouped planner strategy directly.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedFoldPath {
    CountRowsDedicated,
    GenericReducers,
}

impl GroupedFoldPath {
    /// Project one grouped fold path from the planner-owned grouped strategy.
    #[must_use]
    pub(in crate::db) const fn from_plan_strategy(strategy: GroupedPlanStrategy) -> Self {
        if strategy.is_single_count_rows() {
            Self::CountRowsDedicated
        } else {
            Self::GenericReducers
        }
    }

    /// Return whether grouped runtime may use the dedicated grouped `COUNT(*)`
    /// fold/finalize path for this planner-carried fold contract.
    #[must_use]
    pub(in crate::db) const fn uses_count_rows_dedicated_fold(self) -> bool {
        matches!(self, Self::CountRowsDedicated)
    }
}

///
/// GroupedExecutorHandoff
///
/// Borrowed grouped planning handoff consumed at the query->executor boundary.
/// This contract keeps grouped execution routing input explicit while grouped
/// runtime entry remains explicit at query->executor boundaries.
///

#[derive(Clone)]
pub(in crate::db) struct GroupedExecutorHandoff<'a> {
    base: &'a AccessPlannedQuery,
    group_fields: &'a [FieldSlot],
    #[cfg(test)]
    aggregate_specs: Vec<GroupedAggregateExecutionSpec>,
    grouped_aggregate_execution_specs: Vec<GroupedAggregateExecutionSpec>,
    projection_layout: PlannedProjectionLayout,
    projection_is_identity: bool,
    grouped_plan_strategy: GroupedPlanStrategy,
    grouped_fold_path: GroupedFoldPath,
    grouped_distinct_policy_contract: GroupedDistinctPolicyContract,
    having_expr: Option<&'a crate::db::query::plan::expr::Expr>,
    execution: GroupedExecutionConfig,
}

impl<'a> GroupedExecutorHandoff<'a> {
    /// Borrow the grouped query base plan.
    #[must_use]
    pub(in crate::db) const fn base(&self) -> &'a AccessPlannedQuery {
        self.base
    }

    /// Borrow declared grouped key fields.
    #[must_use]
    pub(in crate::db) const fn group_fields(&self) -> &'a [FieldSlot] {
        self.group_fields
    }

    /// Borrow grouped aggregate specs derived from planner projection semantics.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn aggregate_specs(&self) -> &[GroupedAggregateExecutionSpec] {
        self.aggregate_specs.as_slice()
    }

    /// Borrow grouped aggregate execution specs resolved during static planning.
    #[must_use]
    pub(in crate::db) const fn grouped_aggregate_execution_specs(
        &self,
    ) -> &[GroupedAggregateExecutionSpec] {
        self.grouped_aggregate_execution_specs.as_slice()
    }

    /// Borrow grouped projection position layout derived by planner.
    #[must_use]
    pub(in crate::db) const fn projection_layout(&self) -> &PlannedProjectionLayout {
        &self.projection_layout
    }

    /// Return whether planner already proved the grouped projection is row-identical.
    #[must_use]
    pub(in crate::db) const fn projection_is_identity(&self) -> bool {
        self.projection_is_identity
    }

    /// Borrow grouped execution strategy projected by planner semantics.
    #[must_use]
    pub(in crate::db) const fn grouped_plan_strategy(&self) -> GroupedPlanStrategy {
        self.grouped_plan_strategy
    }

    /// Borrow planner-carried grouped fold-path selection.
    #[must_use]
    pub(in crate::db) const fn grouped_fold_path(&self) -> GroupedFoldPath {
        self.grouped_fold_path
    }

    /// Borrow grouped DISTINCT execution strategy lowered by planner.
    #[must_use]
    pub(in crate::db) const fn distinct_execution_strategy(
        &self,
    ) -> &GroupedDistinctExecutionStrategy {
        self.grouped_distinct_policy_contract.execution_strategy()
    }

    /// Borrow grouped DISTINCT policy violation reason for executor boundaries.
    #[must_use]
    pub(in crate::db) const fn distinct_policy_violation_for_executor(
        &self,
    ) -> Option<GroupDistinctPolicyReason> {
        self.grouped_distinct_policy_contract
            .violation_for_executor()
    }

    /// Borrow grouped HAVING expression when present.
    #[must_use]
    pub(in crate::db) const fn having_expr(
        &self,
    ) -> Option<&'a crate::db::query::plan::expr::Expr> {
        self.having_expr
    }

    /// Borrow grouped execution hard-limit policy selected by planning.
    #[must_use]
    pub(in crate::db) const fn execution(&self) -> GroupedExecutionConfig {
        self.execution
    }
}

/// Build one grouped executor handoff from one grouped logical plan.
pub(in crate::db) fn grouped_executor_handoff(
    plan: &AccessPlannedQuery,
) -> Result<GroupedExecutorHandoff<'_>, InternalError> {
    // Grouped handoff is valid only for plans with grouped execution payload.
    let Some(grouped) = plan.grouped_plan() else {
        return Err(InternalError::planner_executor_invariant(
            "grouped executor handoff requires grouped logical plans",
        ));
    };
    let projection_spec = plan.frozen_projection_spec();
    let (projection_layout, aggregate_specs, projection_is_identity) =
        planned_projection_layout_and_aggregate_specs_from_spec(
            projection_spec,
            grouped.group.group_fields.as_slice(),
            grouped.group.aggregates.as_slice(),
        )?;
    #[cfg(not(test))]
    let _ = &aggregate_specs;
    validate_grouped_projection_layout(&projection_layout)?;
    let grouped_plan_strategy = grouped_plan_strategy(plan).ok_or_else(|| {
        InternalError::planner_executor_invariant(
            "grouped executor handoff must carry grouped strategy for grouped plans",
        )
    })?;
    let grouped_aggregate_execution_specs = plan
        .grouped_aggregate_execution_specs()
        .ok_or_else(|| {
            InternalError::planner_executor_invariant(
                "grouped executor handoff requires frozen grouped aggregate execution specs",
            )
        })?
        .to_vec();
    let grouped_fold_path = GroupedFoldPath::from_plan_strategy(grouped_plan_strategy);
    let grouped_distinct_policy_contract = GroupedDistinctPolicyContract::new(
        match grouped_distinct_admissibility(grouped.scalar.distinct, grouped.having_expr.is_some())
        {
            GroupDistinctAdmissibility::Allowed => None,
            GroupDistinctAdmissibility::Disallowed(reason) => Some(reason),
        },
        plan.grouped_distinct_execution_strategy()
            .ok_or_else(|| {
                InternalError::planner_executor_invariant(
                    "grouped executor handoff requires frozen grouped DISTINCT strategy",
                )
            })?
            .clone(),
    );

    Ok(GroupedExecutorHandoff {
        base: plan,
        group_fields: grouped.group.group_fields.as_slice(),
        #[cfg(test)]
        aggregate_specs,
        grouped_aggregate_execution_specs,
        projection_layout,
        projection_is_identity,
        grouped_plan_strategy,
        grouped_fold_path,
        grouped_distinct_policy_contract,
        having_expr: grouped.having_expr.as_ref(),
        execution: grouped.group.execution,
    })
}

/// Build grouped aggregate execution specs from planner-owned aggregate
/// projection specs and one structural model context.
pub(in crate::db) fn grouped_aggregate_execution_specs(
    model: &EntityModel,
    aggregate_specs: &[GroupedAggregateExecutionSpec],
) -> Result<Vec<GroupedAggregateExecutionSpec>, InternalError> {
    aggregate_specs
        .iter()
        .map(|aggregate_spec| aggregate_spec.resolve_for_model(model))
        .collect()
}

/// Lower grouped aggregate specs directly from canonical grouped
/// projection semantics without requiring a frozen grouped executor handoff.
pub(in crate::db) fn grouped_aggregate_specs_from_projection_spec(
    projection_spec: &ProjectionSpec,
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) -> Result<Vec<GroupedAggregateExecutionSpec>, InternalError> {
    let (_, aggregate_specs, _) = planned_projection_layout_and_aggregate_specs_from_spec(
        projection_spec,
        group_fields,
        aggregates,
    )?;

    Ok(aggregate_specs)
}

///
/// GroupedDistinctPolicyContract
///
/// Planner-projected grouped DISTINCT policy contract for executor boundaries.
/// Route/load layers consume this contract directly instead of re-deriving
/// grouped DISTINCT policy from logical plan internals.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedDistinctPolicyContract {
    violation_for_executor: Option<GroupDistinctPolicyReason>,
    execution_strategy: GroupedDistinctExecutionStrategy,
}

impl GroupedDistinctPolicyContract {
    /// Construct one grouped DISTINCT policy contract.
    #[must_use]
    const fn new(
        violation_for_executor: Option<GroupDistinctPolicyReason>,
        execution_strategy: GroupedDistinctExecutionStrategy,
    ) -> Self {
        Self {
            violation_for_executor,
            execution_strategy,
        }
    }

    /// Borrow grouped DISTINCT execution strategy lowered by planner.
    #[must_use]
    pub(in crate::db) const fn execution_strategy(&self) -> &GroupedDistinctExecutionStrategy {
        &self.execution_strategy
    }

    /// Borrow grouped DISTINCT policy violation reason for executor boundaries.
    #[must_use]
    pub(in crate::db) const fn violation_for_executor(&self) -> Option<GroupDistinctPolicyReason> {
        self.violation_for_executor
    }
}

///
/// GroupedDistinctExecutionStrategy
///
/// Planner-owned grouped DISTINCT execution strategy lowered for executor consumption.
/// This strategy is mechanical-only and must not be re-derived by executor policy checks.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedDistinctExecutionStrategy {
    None,
    GlobalDistinctFieldCount {
        target_field: String,
        target_slot: FieldSlot,
    },
    GlobalDistinctFieldSum {
        target_field: String,
        target_slot: FieldSlot,
    },
    GlobalDistinctFieldAvg {
        target_field: String,
        target_slot: FieldSlot,
    },
}

impl GroupedDistinctExecutionStrategy {
    /// Borrow the planner-resolved target field slot used by grouped DISTINCT runtime.
    #[must_use]
    pub(in crate::db) const fn global_distinct_target_slot(&self) -> Option<&FieldSlot> {
        match self {
            Self::None => None,
            Self::GlobalDistinctFieldCount { target_slot, .. }
            | Self::GlobalDistinctFieldSum { target_slot, .. }
            | Self::GlobalDistinctFieldAvg { target_slot, .. } => Some(target_slot),
        }
    }

    /// Return the canonical aggregate kind used by the dedicated global
    /// DISTINCT field-target runtime path.
    #[must_use]
    pub(in crate::db) const fn global_distinct_aggregate_kind(&self) -> Option<AggregateKind> {
        match self {
            Self::None => None,
            Self::GlobalDistinctFieldCount { .. } => Some(AggregateKind::Count),
            Self::GlobalDistinctFieldSum { .. } => Some(AggregateKind::Sum),
            Self::GlobalDistinctFieldAvg { .. } => Some(AggregateKind::Avg),
        }
    }
}

// Lower grouped DISTINCT execution strategy from validated grouped planner semantics
// while freezing the field-target slot under planner ownership.
pub(in crate::db) fn resolved_grouped_distinct_execution_strategy_for_model(
    model: &EntityModel,
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
    having_expr: Option<&crate::db::query::plan::expr::Expr>,
) -> Result<GroupedDistinctExecutionStrategy, InternalError> {
    match resolve_global_distinct_field_aggregate(group_fields, aggregates, having_expr) {
        Ok(Some(aggregate)) => {
            let target_field = aggregate.target_field().to_string();
            let target_slot =
                resolve_aggregate_target_field_slot(model, aggregate.target_field()).map_err(
                    |err| {
                        InternalError::planner_executor_invariant(format!(
                            "grouped DISTINCT strategy target slot resolution failed: field='{}', error={err}",
                            aggregate.target_field(),
                        ))
                    },
                )?;

            match aggregate.kind() {
                AggregateKind::Count => {
                    Ok(GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount {
                        target_field,
                        target_slot,
                    })
                }
                AggregateKind::Sum => {
                    Ok(GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum {
                        target_field,
                        target_slot,
                    })
                }
                AggregateKind::Avg => {
                    Ok(GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg {
                        target_field,
                        target_slot,
                    })
                }
                AggregateKind::Exists
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::First
                | AggregateKind::Last => Err(InternalError::planner_executor_invariant(
                    "planner grouped DISTINCT strategy handoff must lower only COUNT/SUM/AVG field-target aggregates",
                )),
            }
        }
        Ok(None) => Ok(GroupedDistinctExecutionStrategy::None),
        Err(reason) => Err(reason.into_planner_handoff_internal_error()),
    }
}

// Derive grouped field/aggregate projection slots and grouped aggregate
// projection specs from canonical projection semantics.
fn planned_projection_layout_and_aggregate_specs_core(
    projection_spec: &ProjectionSpec,
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) -> (
    PlannedProjectionLayout,
    Vec<GroupedAggregateExecutionSpec>,
    bool,
) {
    let grouped_field_names = group_fields
        .iter()
        .map(FieldSlot::field)
        .collect::<Vec<_>>();
    let mut group_field_positions = Vec::new();
    let mut aggregate_positions = Vec::new();
    let mut aggregate_specs = Vec::new();
    let mut projection_is_identity =
        projection_spec.len() == group_fields.len().saturating_add(aggregates.len());
    let mut next_group_field_index = 0usize;
    let mut next_aggregate_index = 0usize;

    for (index, field) in projection_spec.fields().enumerate() {
        let root_expr = expression_without_alias(projection_field_expr(field));
        let aggregate_scan =
            collect_grouped_projection_aggregate_scan(root_expr, &mut aggregate_specs);

        match root_expr {
            Expr::Field(field_id) => {
                group_field_positions.push(index);
                projection_is_identity &= next_aggregate_index == 0
                    && group_fields
                        .get(next_group_field_index)
                        .is_some_and(|group_field| field_id.as_str() == group_field.field.as_str());
                next_group_field_index = next_group_field_index.saturating_add(1);
            }
            Expr::Aggregate(aggregate_expr) => {
                aggregate_positions.push(index);
                let aggregate_spec =
                    GroupedAggregateExecutionSpec::from_aggregate_expr(aggregate_expr);
                projection_is_identity &= next_group_field_index == group_fields.len()
                    && aggregates
                        .get(next_aggregate_index)
                        .is_some_and(|aggregate| {
                            aggregate_spec.matches_semantic_aggregate(aggregate)
                        });
                next_aggregate_index = next_aggregate_index
                    .saturating_add(aggregate_scan.introduced_aggregate_count());
            }
            _ if aggregate_scan.contains_aggregate() => {
                aggregate_positions.push(index);
                projection_is_identity = false;
                next_aggregate_index = next_aggregate_index
                    .saturating_add(aggregate_scan.introduced_aggregate_count());
            }
            _ if expr_references_only_fields(root_expr, grouped_field_names.as_slice()) => {
                group_field_positions.push(index);
                projection_is_identity &= grouped_projection_expression_preserves_identity(
                    root_expr,
                    group_fields,
                    next_group_field_index,
                    next_aggregate_index,
                );
                next_group_field_index = next_group_field_index.saturating_add(1);
            }
            _ => {
                group_field_positions.push(index);
                projection_is_identity = false;
                next_group_field_index = next_group_field_index.saturating_add(1);
            }
        }
    }
    projection_is_identity &=
        next_group_field_index == group_fields.len() && next_aggregate_index == aggregates.len();

    (
        PlannedProjectionLayout {
            group_field_positions,
            aggregate_positions,
        },
        aggregate_specs,
        projection_is_identity,
    )
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "test builds keep one extra grouped projection strictness pass while non-test builds stay on the planner core path"
)]
fn planned_projection_layout_and_aggregate_specs_from_spec(
    projection_spec: &ProjectionSpec,
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) -> Result<
    (
        PlannedProjectionLayout,
        Vec<GroupedAggregateExecutionSpec>,
        bool,
    ),
    InternalError,
> {
    // Test builds keep one extra strictness pass so grouped layout regressions
    // fail at the planner boundary instead of only in downstream assertions.
    #[cfg(test)]
    {
        let grouped_field_names = group_fields
            .iter()
            .map(FieldSlot::field)
            .collect::<Vec<_>>();

        for (index, field) in projection_spec.fields().enumerate() {
            let root_expr = expression_without_alias(projection_field_expr(field));
            if !expr_references_only_fields(root_expr, grouped_field_names.as_slice()) {
                return Err(InternalError::planner_executor_invariant(format!(
                    "grouped projection layout expects only field/aggregate expressions; found non-grouped projection expression at index={index}",
                )));
            }
        }
    }

    Ok(planned_projection_layout_and_aggregate_specs_core(
        projection_spec,
        group_fields,
        aggregates,
    ))
}

// Keep grouped layout identity checks local to the planner-owned layout core
// so computed grouped-key expressions do not pretend to preserve field-order
// identity.
fn grouped_projection_expression_preserves_identity(
    root_expr: &Expr,
    group_fields: &[FieldSlot],
    next_group_field_index: usize,
    next_aggregate_index: usize,
) -> bool {
    next_aggregate_index == 0
        && matches!(
            root_expr,
            Expr::Field(field_id)
                if group_fields.get(next_group_field_index).is_some_and(
                    |group_field| field_id.as_str() == group_field.field.as_str(),
                )
        )
}

fn collect_grouped_projection_aggregate_scan(
    expr: &Expr,
    aggregate_specs: &mut Vec<GroupedAggregateExecutionSpec>,
) -> GroupedProjectionAggregateScan {
    match expr {
        Expr::Aggregate(aggregate_expr) => {
            GroupedProjectionAggregateScan::found_aggregate(push_unique_grouped_aggregate_spec(
                aggregate_specs,
                GroupedAggregateExecutionSpec::from_aggregate_expr(aggregate_expr),
            ))
        }
        Expr::Field(_) | Expr::Literal(_) => GroupedProjectionAggregateScan::none(),
        Expr::FunctionCall { args, .. } => {
            args.iter()
                .fold(GroupedProjectionAggregateScan::none(), |scan, arg| {
                    scan.combine(collect_grouped_projection_aggregate_scan(
                        arg,
                        aggregate_specs,
                    ))
                })
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => when_then_arms.iter().fold(
            collect_grouped_projection_aggregate_scan(else_expr.as_ref(), aggregate_specs),
            |scan, arm| {
                scan.combine(collect_grouped_projection_aggregate_scan(
                    arm.condition(),
                    aggregate_specs,
                ))
                .combine(collect_grouped_projection_aggregate_scan(
                    arm.result(),
                    aggregate_specs,
                ))
            },
        ),
        Expr::Binary { left, right, .. } => {
            collect_grouped_projection_aggregate_scan(left.as_ref(), aggregate_specs).combine(
                collect_grouped_projection_aggregate_scan(right.as_ref(), aggregate_specs),
            )
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            collect_grouped_projection_aggregate_scan(expr.as_ref(), aggregate_specs)
        }
        Expr::Unary { expr, .. } => {
            collect_grouped_projection_aggregate_scan(expr.as_ref(), aggregate_specs)
        }
    }
}

// Keep grouped aggregate specs on one stable first-seen unique
// order so repeated aggregate leaves reuse the same grouped execution slot.
fn push_unique_grouped_aggregate_spec(
    aggregate_specs: &mut Vec<GroupedAggregateExecutionSpec>,
    aggregate_spec: GroupedAggregateExecutionSpec,
) -> usize {
    if aggregate_specs
        .iter()
        .all(|current| current != &aggregate_spec)
    {
        aggregate_specs.push(aggregate_spec);
        return 1;
    }

    0
}

// Strip alias wrappers so layout classification uses semantic expression roots.
#[allow(
    clippy::missing_const_for_fn,
    reason = "alias stripping traverses boxed expression refs that are not const-callable on stable"
)]
fn expression_without_alias(expr: &Expr) -> &Expr {
    #[cfg(test)]
    {
        let mut current = expr;
        while let Expr::Alias { expr: inner, .. } = current {
            current = inner.as_ref();
        }

        current
    }

    #[cfg(not(test))]
    expr
}
