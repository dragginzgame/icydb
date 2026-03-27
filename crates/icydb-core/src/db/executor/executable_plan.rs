//! Module: db::executor::executable_plan
//! Responsibility: bind validated access-planned queries to executor-ready contracts.
//! Does not own: logical plan semantics or route policy decisions.
//! Boundary: shared plan container for load/delete/aggregate runtime entrypoints.

use crate::{
    db::{
        cursor::{ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor},
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutorPlanError, GroupedPaginationWindow,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            explain::assemble_load_execution_node_descriptor_with_model, lower_index_prefix_specs,
            lower_index_range_specs, preparation::slot_map_for_model_plan,
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::explain::ExplainExecutionNodeDescriptor,
        query::plan::{
            AccessPlannedQuery, ContinuationContract, ExecutionOrdering, GroupSpec, OrderSpec,
            QueryMode, constant_covering_projection_value_from_access,
            covering_index_projection_context,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::marker::PhantomData;
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

/// ExecutablePlanCore
///
/// Generic-free executable-plan payload shared by typed `ExecutablePlan<E>`
/// wrappers. This keeps cursor, ordering, and lowered structural plan state
/// monomorphic while typed access and model-driven behavior remain at the
/// outer executor boundary.
///

#[derive(Debug)]
struct ExecutablePlanCore {
    plan: AccessPlannedQuery,
    continuation: Option<ContinuationContract>,
    index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Vec<LoweredIndexRangeSpec>,
    index_range_spec_invalid: bool,
}

impl ExecutablePlanCore {
    #[must_use]
    const fn new(
        plan: AccessPlannedQuery,
        continuation: Option<ContinuationContract>,
        index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
        index_prefix_spec_invalid: bool,
        index_range_specs: Vec<LoweredIndexRangeSpec>,
        index_range_spec_invalid: bool,
    ) -> Self {
        Self {
            plan,
            continuation,
            index_prefix_specs,
            index_prefix_spec_invalid,
            index_range_specs,
            index_range_spec_invalid,
        }
    }

    #[must_use]
    const fn plan(&self) -> &AccessPlannedQuery {
        &self.plan
    }

    #[must_use]
    const fn mode(&self) -> QueryMode {
        self.plan.scalar_plan().mode
    }

    #[must_use]
    const fn is_grouped(&self) -> bool {
        match self.continuation {
            Some(ref contract) => contract.is_grouped(),
            None => false,
        }
    }

    fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.order_contract().ordering().clone())
    }

    fn execution_strategy(&self) -> Result<ExecutionStrategy, InternalError> {
        let ordering = self.execution_ordering()?;

        Ok(match ordering {
            ExecutionOrdering::PrimaryKey => ExecutionStrategy::PrimaryKey,
            ExecutionOrdering::Explicit(_) => ExecutionStrategy::Ordered,
            ExecutionOrdering::Grouped(_) => ExecutionStrategy::Grouped,
        })
    }

    #[must_use]
    const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.plan)
    }

    #[must_use]
    const fn order_spec(&self) -> Option<&OrderSpec> {
        self.plan.scalar_plan().order.as_ref()
    }

    #[must_use]
    const fn has_predicate(&self) -> bool {
        self.plan.scalar_plan().predicate.is_some()
    }

    fn index_prefix_specs(&self) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        if self.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }

        Ok(self.index_prefix_specs.as_slice())
    }

    fn index_range_specs(&self) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(self.index_range_specs.as_slice())
    }

    #[must_use]
    fn into_inner(self) -> AccessPlannedQuery {
        self.plan
    }

    fn prepare_cursor(
        &self,
        authority: EntityAuthority,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(ExecutorPlanError::continuation_cursor_requires_load_plan());
        };

        contract
            .prepare_scalar_cursor(
                authority.entity_path(),
                authority.entity_tag(),
                authority.model(),
                cursor,
            )
            .map_err(ExecutorPlanError::from)
    }

    fn revalidate_cursor(
        &self,
        authority: EntityAuthority,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(
                ExecutorPlanError::continuation_cursor_requires_load_plan().into_internal_error()
            );
        };

        contract
            .revalidate_scalar_cursor(authority.entity_tag(), authority.model(), cursor)
            .map_err(CursorPlanError::into_internal_error)
    }

    fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(
                ExecutorPlanError::grouped_cursor_revalidation_requires_grouped_plan()
                    .into_internal_error(),
            );
        };

        contract
            .revalidate_grouped_cursor(cursor)
            .map_err(CursorPlanError::into_internal_error)
    }

    fn continuation_signature_for_runtime(&self) -> Result<ContinuationSignature, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.continuation_signature())
    }

    fn grouped_cursor_boundary_arity(&self) -> Result<usize, InternalError> {
        let contract = self.continuation_contract()?;
        if !contract.is_grouped() {
            return Err(
                ExecutorPlanError::grouped_cursor_boundary_arity_requires_grouped_plan()
                    .into_internal_error(),
            );
        }

        Ok(contract.boundary_arity())
    }

    fn grouped_pagination_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        let contract = self.continuation_contract()?;
        let window = contract
            .grouped_paging_window(cursor)
            .map_err(CursorPlanError::into_internal_error)?;
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
    fn continuation_contract(&self) -> Result<&ContinuationContract, InternalError> {
        self.continuation.as_ref().ok_or_else(|| {
            ExecutorPlanError::continuation_contract_requires_load_plan().into_internal_error()
        })
    }
}

// Build one canonical lowered executable-plan core from resolved authority
// plus one logical plan, regardless of whether the caller started from a typed
// `ExecutablePlan<E>` shell or a structural follow-on rewrite.
fn build_executable_plan_core(
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> ExecutablePlanCore {
    // Phase 0: derive immutable continuation contract once from planner semantics.
    let continuation = plan.continuation_contract(authority.entity_path());

    // Phase 1: lower index-prefix specs once and retain invariant state.
    let (index_prefix_specs, index_prefix_spec_invalid) =
        match lower_index_prefix_specs(authority.entity_tag(), &plan.access) {
            Ok(specs) => (specs, false),
            Err(_) => (Vec::new(), true),
        };

    // Phase 2: lower index-range specs once and retain invariant state.
    let (index_range_specs, index_range_spec_invalid) =
        match lower_index_range_specs(authority.entity_tag(), &plan.access) {
            Ok(specs) => (specs, false),
            Err(_) => (Vec::new(), true),
        };

    ExecutablePlanCore::new(
        plan,
        continuation,
        index_prefix_specs,
        index_prefix_spec_invalid,
        index_range_specs,
        index_range_spec_invalid,
    )
}

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Debug)]
pub(in crate::db) struct ExecutablePlan<E: EntityKind> {
    core: ExecutablePlanCore,
    marker: PhantomData<fn() -> E>,
}

///
/// PreparedLoadPlan
///
/// Generic-free load-plan boundary consumed by continuation resolution and
/// load pipeline preparation after the typed `ExecutablePlan<E>` shell is no
/// longer needed.
///

#[derive(Debug)]
pub(in crate::db::executor) struct PreparedLoadPlan {
    authority: EntityAuthority,
    core: ExecutablePlanCore,
}

impl PreparedLoadPlan {
    #[must_use]
    pub(in crate::db::executor) fn from_plan(
        authority: EntityAuthority,
        plan: AccessPlannedQuery,
    ) -> Self {
        Self {
            authority,
            core: build_executable_plan_core(authority, plan),
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn authority(&self) -> EntityAuthority {
        self.authority
    }

    #[must_use]
    pub(in crate::db::executor) const fn mode(&self) -> QueryMode {
        self.core.mode()
    }

    #[must_use]
    pub(in crate::db::executor) const fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    pub(in crate::db::executor) fn execution_ordering(
        &self,
    ) -> Result<ExecutionOrdering, InternalError> {
        self.core.execution_ordering()
    }

    pub(in crate::db::executor) fn revalidate_cursor(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        self.core.revalidate_cursor(self.authority, cursor)
    }

    pub(in crate::db::executor) fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        self.core.revalidate_grouped_cursor(cursor)
    }

    pub(in crate::db::executor) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        self.core.continuation_signature_for_runtime()
    }

    pub(in crate::db::executor) fn grouped_cursor_boundary_arity(
        &self,
    ) -> Result<usize, InternalError> {
        self.core.grouped_cursor_boundary_arity()
    }

    pub(in crate::db::executor) fn grouped_pagination_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        self.core.grouped_pagination_window(cursor)
    }

    pub(in crate::db::executor) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        self.core.index_prefix_specs()
    }

    pub(in crate::db::executor) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        self.core.index_range_specs()
    }

    #[must_use]
    pub(in crate::db::executor) fn into_plan(self) -> AccessPlannedQuery {
        self.core.into_inner()
    }
}

///
/// PreparedAggregatePlan
///
/// Generic-free aggregate-plan boundary consumed by aggregate terminal and
/// runtime preparation after the typed `ExecutablePlan<E>` shell is no longer
/// needed.
///

#[derive(Debug)]
pub(in crate::db::executor) struct PreparedAggregatePlan {
    authority: EntityAuthority,
    core: ExecutablePlanCore,
}

impl PreparedAggregatePlan {
    #[must_use]
    pub(in crate::db::executor) const fn authority(&self) -> EntityAuthority {
        self.authority
    }

    #[must_use]
    pub(in crate::db::executor) const fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    #[must_use]
    pub(in crate::db::executor) fn execution_preparation(&self) -> ExecutionPreparation {
        ExecutionPreparation::from_plan(
            self.authority.model(),
            self.core.plan(),
            slot_map_for_model_plan(self.authority.model(), self.core.plan()),
        )
    }

    pub(in crate::db::executor) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        self.core.index_prefix_specs()
    }

    pub(in crate::db::executor) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        self.core.index_range_specs()
    }

    /// Re-shape one prepared aggregate plan into one grouped prepared load plan
    /// without reconstructing a typed `ExecutablePlan<E>` shell.
    #[must_use]
    pub(in crate::db::executor) fn into_grouped_load_plan(
        self,
        group: GroupSpec,
    ) -> PreparedLoadPlan {
        PreparedLoadPlan::from_plan(self.authority, self.core.into_inner().into_grouped(group))
    }

    #[must_use]
    pub(in crate::db::executor) fn into_plan(self) -> AccessPlannedQuery {
        self.core.into_inner()
    }
}

impl<E: EntityKind> ExecutablePlan<E> {
    pub(in crate::db) fn new(plan: AccessPlannedQuery) -> Self {
        Self::build(plan)
    }

    fn build(plan: AccessPlannedQuery) -> Self {
        let authority = EntityAuthority::for_type::<E>();

        Self {
            core: build_executable_plan_core(authority, plan),
            marker: PhantomData,
        }
    }

    /// Explain scalar load execution shape as one canonical execution-node descriptor tree.
    pub(in crate::db) fn explain_load_execution_node_descriptor(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, InternalError>
    where
        E: EntityValue,
    {
        if !self.mode().is_load() {
            return Err(
                ExecutorPlanError::load_execution_descriptor_requires_load_plan()
                    .into_internal_error(),
            );
        }

        assemble_load_execution_node_descriptor_with_model(E::MODEL, self.core.plan())
    }

    /// Validate and decode a continuation cursor into executor-ready cursor state.
    pub(in crate::db) fn prepare_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError> {
        self.core
            .prepare_cursor(EntityAuthority::for_type::<E>(), cursor)
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> QueryMode {
        self.core.mode()
    }

    /// Return whether this executable plan carries grouped logical shape.
    #[must_use]
    pub(in crate::db) const fn is_grouped(&self) -> bool {
        self.core.is_grouped()
    }

    /// Return planner-projected execution strategy for entrypoint dispatch.
    pub(in crate::db) fn execution_strategy(&self) -> Result<ExecutionStrategy, InternalError> {
        self.core.execution_strategy()
    }

    /// Borrow the structural logical plan for executor-owned tests.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    /// Expose planner-projected execution ordering for executor/lowering tests.
    #[cfg(test)]
    pub(in crate::db) fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        self.core.execution_ordering()
    }

    pub(in crate::db) const fn access(
        &self,
    ) -> &crate::db::access::AccessPlan<crate::value::Value> {
        &self.core.plan().access
    }

    /// Borrow scalar row-consistency policy for runtime row reads.
    #[must_use]
    pub(in crate::db) const fn consistency(&self) -> MissingRowPolicy {
        self.core.consistency()
    }

    /// Classify canonical `bytes_by(field)` execution mode for this plan/field.
    #[must_use]
    pub(in crate::db) fn bytes_by_projection_mode(
        &self,
        target_field: &str,
    ) -> BytesByProjectionMode {
        let authority = EntityAuthority::for_type::<E>();

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
            authority.model().primary_key.name,
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
        self.core.order_spec()
    }

    /// Return whether this executable plan has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) const fn has_predicate(&self) -> bool {
        self.core.has_predicate()
    }

    pub(in crate::db) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        self.core.index_prefix_specs()
    }

    pub(in crate::db) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        self.core.index_range_specs()
    }

    /// Render one canonical executor snapshot for test-only planner/executor
    /// contract checks.
    #[cfg(test)]
    pub(in crate::db) fn render_snapshot_canonical(&self) -> Result<String, InternalError>
    where
        E: EntityValue,
    {
        // Phase 1: project all executor-owned summary fields from the logical plan.
        let plan = self.core.plan();
        let projection_spec = plan.projection_spec(E::MODEL);
        let projection_selection =
            if plan.grouped_plan().is_some() || projection_spec.len() != E::MODEL.fields.len() {
                "Declared"
            } else {
                "All"
            };
        let projection_coverage_flag = plan.grouped_plan().is_some();
        let continuation_signature = self.core.continuation_signature_for_runtime()?;
        let ordering_direction = self
            .core
            .continuation_contract()?
            .order_contract()
            .direction();

        // Phase 2: lower index-bound summaries into stable compact text.
        let index_prefix_specs = render_index_prefix_specs(self.core.index_prefix_specs()?);
        let index_range_specs = render_index_range_specs(self.core.index_range_specs()?);
        let explain_plan = plan.explain_with_model(E::MODEL);

        // Phase 3: join the canonical snapshot payload in one stable line order.
        Ok([
            "snapshot_version=1".to_string(),
            format!("plan_hash={}", plan.fingerprint()),
            format!("mode={:?}", self.core.mode()),
            format!("is_grouped={}", self.core.is_grouped()),
            format!("execution_strategy={:?}", self.core.execution_strategy()?),
            format!("ordering_direction={ordering_direction:?}"),
            format!(
                "distinct_execution_strategy={:?}",
                plan.distinct_execution_strategy()
            ),
            format!("projection_selection={projection_selection}"),
            format!("projection_spec={projection_spec:?}"),
            format!("order_spec={:?}", plan.scalar_plan().order),
            format!("page_spec={:?}", plan.scalar_plan().page),
            format!("projection_coverage_flag={projection_coverage_flag}"),
            format!("continuation_signature={continuation_signature}"),
            format!("index_prefix_specs={index_prefix_specs}"),
            format!("index_range_specs={index_range_specs}"),
            format!("explain_plan={explain_plan:?}"),
        ]
        .join("\n"))
    }

    /// Split the executable plan into its canonical structural logical plan.
    ///
    /// Aggregate/scalar prepared boundaries should prefer this helper when they
    /// no longer need the typed `ExecutablePlan<E>` shell after entering
    /// structural execution preparation.
    pub(in crate::db) fn into_plan(self) -> AccessPlannedQuery {
        self.core.into_inner()
    }

    /// Validate and decode grouped continuation cursor state for grouped plans.
    pub(in crate::db) fn prepare_grouped_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.core.continuation.as_ref() else {
            return Err(ExecutorPlanError::grouped_cursor_preparation_requires_grouped_plan());
        };

        contract
            .prepare_grouped_cursor(EntityAuthority::for_type::<E>().entity_path(), cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Consume one typed load executable plan into one generic-free boundary
    /// payload for continuation and load-pipeline preparation.
    #[must_use]
    pub(in crate::db::executor) fn into_prepared_load_plan(self) -> PreparedLoadPlan {
        PreparedLoadPlan {
            authority: EntityAuthority::for_type::<E>(),
            core: self.core,
        }
    }

    /// Consume one typed aggregate executable plan into one generic-free
    /// boundary payload for aggregate terminal and runtime preparation.
    #[must_use]
    pub(in crate::db::executor) fn into_prepared_aggregate_plan(self) -> PreparedAggregatePlan {
        PreparedAggregatePlan {
            authority: EntityAuthority::for_type::<E>(),
            core: self.core,
        }
    }
}

#[cfg(test)]
fn render_index_prefix_specs(specs: &[LoweredIndexPrefixSpec]) -> String {
    let rendered = specs
        .iter()
        .map(|spec| {
            format!(
                "{{index:{},bound_type:equality,lower:{},upper:{}}}",
                spec.index().name(),
                render_lowered_bound(spec.lower()),
                render_lowered_bound(spec.upper()),
            )
        })
        .collect::<Vec<_>>();

    format!("[{}]", rendered.join(","))
}

#[cfg(test)]
fn render_index_range_specs(specs: &[LoweredIndexRangeSpec]) -> String {
    let rendered = specs
        .iter()
        .map(|spec| {
            format!(
                "{{index:{},lower:{},upper:{}}}",
                spec.index().name(),
                render_lowered_bound(spec.lower()),
                render_lowered_bound(spec.upper()),
            )
        })
        .collect::<Vec<_>>();

    format!("[{}]", rendered.join(","))
}

#[cfg(test)]
fn render_lowered_bound(bound: &Bound<crate::db::access::LoweredKey>) -> String {
    match bound {
        Bound::Included(key) => format!("included({})", render_lowered_key_summary(key)),
        Bound::Excluded(key) => format!("excluded({})", render_lowered_key_summary(key)),
        Bound::Unbounded => "unbounded".to_string(),
    }
}

#[cfg(test)]
fn render_lowered_key_summary(key: &crate::db::access::LoweredKey) -> String {
    let bytes = key.as_bytes();
    let head_len = bytes.len().min(8);
    let tail_len = bytes.len().min(8);
    let head = crate::db::codec::cursor::encode_cursor(&bytes[..head_len]);
    let tail = crate::db::codec::cursor::encode_cursor(&bytes[bytes.len() - tail_len..]);

    format!("len:{}:head:{head}:tail:{tail}", bytes.len())
}
