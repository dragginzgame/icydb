//! Module: db::executor::executable_plan
//! Responsibility: bind validated access-planned queries to executor-ready contracts.
//! Does not own: logical plan semantics or route policy decisions.
//! Boundary: shared plan container for load/delete/aggregate runtime entrypoints.

#[cfg(test)]
use crate::db::codec::cursor::encode_cursor;
use crate::{
    db::{
        access::AccessPlan,
        cursor::{ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor},
        direction::Direction,
        executor::{
            ExecutionPreparation, ExecutorPlanError, GroupedPaginationWindow,
            LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            explain::{
                assemble_aggregate_terminal_execution_descriptor,
                assemble_load_execution_node_descriptor,
                assemble_load_execution_verbose_diagnostics,
            },
            lower_index_prefix_specs, lower_index_range_specs,
            traversal::row_read_consistency_for_plan,
            validate_executor_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, ContinuationContract, ExecutionOrdering, GroupedExecutorHandoff,
            OrderDirection, OrderSpec, PageSpec, QueryMode,
            constant_covering_projection_value_from_access, covering_index_projection_context,
            grouped_executor_handoff, index_covering_existing_rows_terminal_eligible,
        },
        query::{
            builder::AggregateExpr,
            explain::{ExplainExecutionDescriptor, ExplainExecutionNodeDescriptor},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
#[cfg(test)]
use std::ops::Bound;

///
/// ExecutionStrategy
///
/// Executor-facing execution shape contract derived from planner ordering.
/// Session and runtime entrypoints consume this strategy and must not
/// re-derive grouped/scalar routing shape from boolean flags.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionStrategy {
    PrimaryKey,
    Ordered,
    Grouped,
}

///
/// BytesByProjectionMode
///
/// Canonical `bytes_by(field)` projection mode classification used by execution
/// and explain surfaces.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum BytesByProjectionMode {
    Materialized,
    CoveringIndex,
    CoveringConstant,
}

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Debug)]
pub(in crate::db) struct ExecutablePlan<E: EntityKind> {
    plan: AccessPlannedQuery<E::Key>,
    continuation: Option<ContinuationContract<E::Key>>,
    index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Vec<LoweredIndexRangeSpec>,
    index_range_spec_invalid: bool,
}

impl<E: EntityKind> ExecutablePlan<E> {
    #[cfg(test)]
    pub(in crate::db) fn new(plan: AccessPlannedQuery<E::Key>) -> Self {
        Self::build(plan)
    }

    #[cfg(not(test))]
    pub(in crate::db) fn new(plan: AccessPlannedQuery<E::Key>) -> Self {
        Self::build(plan)
    }

    fn build(plan: AccessPlannedQuery<E::Key>) -> Self {
        // Phase 0: derive immutable continuation contract once from planner semantics.
        let continuation = plan.continuation_contract(E::PATH);

        // Phase 1: Lower index-prefix specs once and retain invariant state.
        let (index_prefix_specs, index_prefix_spec_invalid) =
            match lower_index_prefix_specs::<E>(&plan.access) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };

        // Phase 2: Lower index-range specs once and retain invariant state.
        let (index_range_specs, index_range_spec_invalid) =
            match lower_index_range_specs::<E>(&plan.access) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };

        Self {
            plan,
            continuation,
            index_prefix_specs,
            index_prefix_spec_invalid,
            index_range_specs,
            index_range_spec_invalid,
        }
    }

    /// Explain one scalar aggregate execution descriptor without executing it.
    #[must_use]
    pub(in crate::db) fn explain_aggregate_terminal_execution_descriptor(
        &self,
        aggregate: AggregateExpr,
    ) -> ExplainExecutionDescriptor
    where
        E: EntityValue,
    {
        assemble_aggregate_terminal_execution_descriptor::<E>(&self.plan, aggregate)
    }

    /// Explain scalar load execution shape as one canonical execution-node descriptor tree.
    pub(in crate::db) fn explain_load_execution_node_descriptor(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, InternalError>
    where
        E: EntityValue,
    {
        if !self.mode().is_load() {
            return Err(crate::db::error::query_executor_invariant(
                "load execution descriptor requires load-mode executable plans",
            ));
        }

        assemble_load_execution_node_descriptor::<E>(&self.plan)
    }

    /// Explain scalar load execution route diagnostics for verbose surfaces.
    pub(in crate::db) fn explain_load_execution_verbose_diagnostics(
        &self,
    ) -> Result<Vec<String>, InternalError>
    where
        E: EntityValue,
    {
        if !self.mode().is_load() {
            return Err(crate::db::error::query_executor_invariant(
                "load execution verbose diagnostics require load-mode executable plans",
            ));
        }

        assemble_load_execution_verbose_diagnostics::<E>(&self.plan)
    }

    /// Render one canonical executable-plan snapshot payload for regression tests.
    ///
    /// The format is line-oriented and deterministic by construction:
    /// fixed key order, stable planner fingerprints, and canonical explain fields.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn render_snapshot_canonical(&self) -> String
    where
        E: EntityValue,
    {
        // Phase 1: capture stable planner/executor contracts that define executable shape.
        let execution_strategy = self.execution_strategy().map_or_else(
            |err| format!("error:{err:?}"),
            |strategy| format!("{strategy:?}"),
        );
        let ordering_direction = self.continuation_contract().map_or_else(
            |err| format!("error:{err:?}"),
            |contract| format!("{:?}", contract.order_contract().direction()),
        );
        let continuation_signature = self.continuation_signature_for_runtime().map_or_else(
            |err| format!("error:{err:?}"),
            |signature| signature.to_string(),
        );
        let projection_coverage_flag = self.index_covering_existing_rows_terminal_eligible();
        let explain = self.plan.explain_with_model(E::MODEL);

        // Phase 2: emit one deterministic, append-only snapshot payload.
        let mut lines = Vec::new();
        lines.push("snapshot_version=1".to_string());
        lines.push(format!("plan_hash={}", self.plan.fingerprint()));
        lines.push(format!("mode={:?}", self.mode()));
        lines.push(format!("is_grouped={}", self.is_grouped()));
        lines.push(format!("execution_strategy={execution_strategy}"));
        lines.push(format!("ordering_direction={ordering_direction}"));
        lines.push(format!(
            "distinct_execution_strategy={:?}",
            self.plan.distinct_execution_strategy()
        ));
        lines.push(format!(
            "projection_selection={:?}",
            self.plan.projection_selection
        ));
        lines.push(format!(
            "projection_spec={:?}",
            self.plan.projection_spec(E::MODEL)
        ));
        lines.push(format!("order_spec={:?}", self.order_spec()));
        lines.push(format!("page_spec={:?}", self.page_spec()));
        lines.push(format!(
            "projection_coverage_flag={projection_coverage_flag}"
        ));
        lines.push(format!("continuation_signature={continuation_signature}"));
        lines.push(format!(
            "index_prefix_specs={}",
            snapshot_index_prefix_specs(self.index_prefix_specs.as_slice())
        ));
        lines.push(format!(
            "index_range_specs={}",
            snapshot_index_range_specs(self.index_range_specs.as_slice())
        ));
        lines.push(format!("explain_plan={explain:?}"));

        lines.join("\n")
    }

    /// Compute a stable continuation signature for cursor compatibility checks.
    ///
    /// Unlike `fingerprint()`, this excludes window state such as `limit`/`offset`.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) const fn continuation_signature(&self) -> ContinuationSignature {
        match self.continuation {
            Some(ref contract) => contract.continuation_signature(),
            None => {
                panic!("continuation signature requires load-mode continuation contract")
            }
        }
    }

    /// Validate and decode a continuation cursor into executor-ready cursor state.
    pub(in crate::db) fn prepare_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(cursor_plan_error(
                "continuation cursors are only supported for load plans",
            ));
        };

        contract
            .prepare_scalar_cursor::<E>(cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> QueryMode {
        self.plan.scalar_plan().mode
    }

    /// Return whether this executable plan carries grouped logical shape.
    #[must_use]
    pub(in crate::db) const fn is_grouped(&self) -> bool {
        match self.continuation {
            Some(ref contract) => contract.is_grouped(),
            None => false,
        }
    }

    /// Return planner-projected execution ordering used by runtime dispatch.
    pub(in crate::db) fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.order_contract().ordering().clone())
    }

    /// Return planner-projected execution strategy for entrypoint dispatch.
    pub(in crate::db) fn execution_strategy(&self) -> Result<ExecutionStrategy, InternalError> {
        let ordering = self.execution_ordering()?;

        Ok(match ordering {
            ExecutionOrdering::PrimaryKey => ExecutionStrategy::PrimaryKey,
            ExecutionOrdering::Explicit(_) => ExecutionStrategy::Ordered,
            ExecutionOrdering::Grouped(_) => ExecutionStrategy::Grouped,
        })
    }

    pub(in crate::db) const fn access(&self) -> &AccessPlan<E::Key> {
        &self.plan.access
    }

    /// Borrow scalar row-consistency policy for runtime row reads.
    #[must_use]
    pub(in crate::db) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.plan)
    }

    /// Classify canonical `bytes_by(field)` execution mode for this plan/field.
    #[must_use]
    pub(in crate::db) fn bytes_by_projection_mode(
        &self,
        target_field: &str,
    ) -> BytesByProjectionMode {
        if !matches!(self.consistency(), MissingRowPolicy::Ignore) {
            return BytesByProjectionMode::Materialized;
        }

        if constant_covering_projection_value_from_access(self.access(), target_field).is_some() {
            return BytesByProjectionMode::CoveringConstant;
        }

        if self.has_predicate() {
            return BytesByProjectionMode::Materialized;
        }

        if covering_index_projection_context(
            self.access(),
            self.order_spec(),
            target_field,
            E::MODEL.primary_key.name,
        )
        .is_some()
        {
            return BytesByProjectionMode::CoveringIndex;
        }

        BytesByProjectionMode::Materialized
    }

    /// Return a stable explain/diagnostic label for one bytes-by mode.
    #[must_use]
    pub(in crate::db) const fn bytes_by_projection_mode_label(
        mode: BytesByProjectionMode,
    ) -> &'static str {
        match mode {
            BytesByProjectionMode::Materialized => "field_materialized",
            BytesByProjectionMode::CoveringIndex => "field_covering_index",
            BytesByProjectionMode::CoveringConstant => "field_covering_constant",
        }
    }

    /// Borrow scalar ORDER BY contract for this executable plan, if any.
    #[must_use]
    pub(in crate::db::executor) const fn order_spec(&self) -> Option<&OrderSpec> {
        self.plan.scalar_plan().order.as_ref()
    }

    /// Borrow scalar pagination contract for this executable plan, if any.
    #[must_use]
    pub(in crate::db::executor) const fn page_spec(&self) -> Option<&PageSpec> {
        self.plan.scalar_plan().page.as_ref()
    }

    /// Return whether this executable plan has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) const fn has_predicate(&self) -> bool {
        self.plan.scalar_plan().predicate.is_some()
    }

    /// Return whether this executable plan enables scalar DISTINCT semantics.
    #[must_use]
    pub(in crate::db::executor) const fn is_distinct(&self) -> bool {
        self.plan.scalar_plan().distinct
    }

    /// Return whether this plan clears both residual-predicate and DISTINCT
    /// gates required by route-owned scalar fast-path contracts.
    #[must_use]
    pub(in crate::db::executor) const fn has_no_predicate_or_distinct(&self) -> bool {
        !self.has_predicate() && !self.is_distinct()
    }

    /// Return primary-key order direction when ORDER BY is explicitly
    /// primary-key-only; otherwise return `None`.
    #[must_use]
    pub(in crate::db::executor) fn explicit_primary_key_order_direction(
        &self,
    ) -> Option<Direction> {
        let order = self.order_spec()?;
        order
            .primary_key_only_direction(E::MODEL.primary_key.name)
            .map(|direction| match direction {
                OrderDirection::Asc => Direction::Asc,
                OrderDirection::Desc => Direction::Desc,
            })
    }

    /// Return one canonical scan direction for unordered plans (`Asc`) or
    /// explicit primary-key-only ordering; return `None` for non-PK ordering.
    #[must_use]
    pub(in crate::db::executor) fn unordered_or_primary_key_order_direction(
        &self,
    ) -> Option<Direction> {
        let Some(order) = self.order_spec() else {
            return Some(Direction::Asc);
        };

        order
            .primary_key_only_direction(E::MODEL.primary_key.name)
            .map(|direction| match direction {
                OrderDirection::Asc => Direction::Asc,
                OrderDirection::Desc => Direction::Desc,
            })
    }

    /// Build canonical execution preparation for this executable plan.
    #[must_use]
    pub(in crate::db::executor) fn execution_preparation(&self) -> ExecutionPreparation
    where
        E: EntityValue,
    {
        ExecutionPreparation::for_plan::<E>(&self.plan)
    }

    /// Return whether COUNT/EXISTS can keep one index-covering existing-row terminal path.
    #[must_use]
    pub(in crate::db::executor) fn index_covering_existing_rows_terminal_eligible(&self) -> bool
    where
        E: EntityValue,
    {
        let strict_predicate_compatible = self.execution_preparation().strict_mode().is_some();

        index_covering_existing_rows_terminal_eligible(&self.plan, strict_predicate_compatible)
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn as_inner(&self) -> &AccessPlannedQuery<E::Key> {
        &self.plan
    }

    pub(in crate::db) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        if self.index_prefix_spec_invalid {
            return Err(crate::db::error::query_executor_invariant(
                LOWERED_INDEX_PREFIX_SPEC_INVALID,
            ));
        }

        Ok(self.index_prefix_specs.as_slice())
    }

    pub(in crate::db) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.index_range_spec_invalid {
            return Err(crate::db::error::query_executor_invariant(
                LOWERED_INDEX_RANGE_SPEC_INVALID,
            ));
        }

        Ok(self.index_range_specs.as_slice())
    }

    pub(in crate::db) fn into_inner(self) -> AccessPlannedQuery<E::Key> {
        self.plan
    }

    /// Build grouped executor handoff from this executable plan using one
    /// canonical executor-boundary validation pass.
    pub(in crate::db) fn grouped_handoff(
        &self,
    ) -> Result<GroupedExecutorHandoff<'_, E::Key>, InternalError> {
        validate_executor_plan::<E>(&self.plan)?;
        grouped_executor_handoff(&self.plan)
    }

    /// Revalidate executor-provided cursor state through the canonical cursor spine.
    pub(in crate::db) fn revalidate_cursor(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(crate::db::error::query_executor_invariant(
                "continuation cursors are only supported for load plans",
            ));
        };

        contract
            .revalidate_scalar_cursor::<E>(cursor)
            .map_err(crate::db::error::from_cursor_plan_error)
    }

    /// Validate and decode grouped continuation cursor state for grouped plans.
    pub(in crate::db) fn prepare_grouped_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(cursor_plan_error(
                "grouped cursor preparation requires grouped logical plans",
            ));
        };

        contract
            .prepare_grouped_cursor::<E>(cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Revalidate grouped cursor state before grouped executor entry.
    pub(in crate::db) fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(crate::db::error::query_executor_invariant(
                "grouped cursor revalidation requires grouped logical plans",
            ));
        };

        contract
            .revalidate_grouped_cursor(cursor)
            .map_err(crate::db::error::from_cursor_plan_error)
    }

    /// Borrow continuation signature from immutable continuation contract.
    pub(in crate::db) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.continuation_signature())
    }

    /// Borrow grouped cursor boundary arity from immutable continuation contract.
    pub(in crate::db) fn grouped_cursor_boundary_arity(&self) -> Result<usize, InternalError> {
        let contract = self.continuation_contract()?;
        if !contract.is_grouped() {
            return Err(crate::db::error::query_executor_invariant(
                "grouped cursor boundary arity requires grouped logical plans",
            ));
        }

        Ok(contract.boundary_arity())
    }

    /// Derive grouped paging window from immutable continuation contract.
    pub(in crate::db::executor) fn grouped_pagination_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        let contract = self.continuation_contract()?;
        let window = contract
            .grouped_paging_window(cursor)
            .map_err(crate::db::error::from_cursor_plan_error)?;
        let (
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ) = window.into_parts();

        Ok(GroupedPaginationWindow::new(
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ))
    }

    // Borrow immutable continuation contract for load-mode plans.
    fn continuation_contract(&self) -> Result<&ContinuationContract<E::Key>, InternalError> {
        self.continuation.as_ref().ok_or_else(|| {
            crate::db::error::query_executor_invariant(
                "continuation contracts are only supported for load plans",
            )
        })
    }
}

fn cursor_plan_error(message: impl Into<String>) -> ExecutorPlanError {
    ExecutorPlanError::from(CursorPlanError::continuation_cursor_invariant(
        crate::db::error::executor_invariant_message(message),
    ))
}

#[cfg(test)]
fn snapshot_index_prefix_specs(specs: &[LoweredIndexPrefixSpec]) -> String {
    if specs.is_empty() {
        return "[]".to_string();
    }

    let rendered = specs
        .iter()
        .map(|spec| {
            format!(
                "{{index:{},bound_type:equality,lower:{},upper:{}}}",
                spec.index().name(),
                snapshot_lowered_bound(spec.lower()),
                snapshot_lowered_bound(spec.upper()),
            )
        })
        .collect::<Vec<_>>();

    format!("[{}]", rendered.join(","))
}

#[cfg(test)]
fn snapshot_index_range_specs(specs: &[LoweredIndexRangeSpec]) -> String {
    if specs.is_empty() {
        return "[]".to_string();
    }

    let rendered = specs
        .iter()
        .map(|spec| {
            let bound_type = snapshot_range_bound_type(spec);
            format!(
                "{{index:{},bound_type:{bound_type},lower:{},upper:{}}}",
                spec.index().name(),
                snapshot_lowered_bound(spec.lower()),
                snapshot_lowered_bound(spec.upper()),
            )
        })
        .collect::<Vec<_>>();

    format!("[{}]", rendered.join(","))
}

#[cfg(test)]
fn snapshot_range_bound_type(spec: &LoweredIndexRangeSpec) -> &'static str {
    match (spec.lower(), spec.upper()) {
        (Bound::Included(lower), Bound::Included(upper)) if lower == upper => "equality",
        _ => "range",
    }
}

#[cfg(test)]
fn snapshot_lowered_bound(bound: &Bound<crate::db::access::LoweredKey>) -> String {
    match bound {
        Bound::Unbounded => "unbounded".to_string(),
        Bound::Included(key) => format!("included({})", snapshot_lowered_key(key)),
        Bound::Excluded(key) => format!("excluded({})", snapshot_lowered_key(key)),
    }
}

#[cfg(test)]
fn snapshot_lowered_key(key: &crate::db::access::LoweredKey) -> String {
    let bytes = key.as_bytes();
    let preview_len = bytes.len().min(8);
    let head = encode_cursor(&bytes[..preview_len]);
    let tail = encode_cursor(&bytes[bytes.len().saturating_sub(preview_len)..]);

    format!("len:{}:head:{}:tail:{}", bytes.len(), head, tail)
}
