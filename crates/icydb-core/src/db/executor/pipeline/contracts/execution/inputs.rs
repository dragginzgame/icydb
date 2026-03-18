//! Module: db::executor::pipeline::contracts::inputs
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::inputs.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::CursorBoundary,
        direction::Direction,
        executor::pipeline::contracts::FastPathKeyResult,
        executor::{
            AccessStreamBindings, Context, ExecutableAccess, ExecutionKernel, ExecutionPreparation,
            LoadExecutor, OrderedKeyStream, OrderedKeyStreamBox, ScalarContinuationBindings,
            preparation::resolved_index_slots_for_access_path, route::RoutedKeyStreamRequest,
            terminal::page::PageMaterializationRequest, traversal::row_read_consistency_for_plan,
        },
        index::predicate::IndexPredicateExecution,
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue},
};
use std::any::Any;

///
/// RuntimePageMaterializationRequest
///
/// Generic-free page materialization envelope consumed through the executor
/// runtime adapter boundary.
///

pub(in crate::db::executor) struct RuntimePageMaterializationRequest<'a> {
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) stream_order_contract_safe: bool,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
}

///
/// ExecutionRuntime
///
/// Executor-bound runtime adapter resolved once at the typed boundary.
/// All typed access-path and context authority must flow through this trait so
/// shared execution code can remain monomorphic over plan shape.
///

pub(in crate::db::executor) trait ExecutionRuntime {
    /// Resolve one primary-key fast path when the route is already verified.
    fn try_execute_pk_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>;

    /// Resolve one verified secondary-prefix fast path.
    fn try_execute_secondary_index_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>;

    /// Resolve one verified index-range limit-pushdown fast path.
    fn try_execute_index_range_limit_pushdown_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
        continuation: crate::db::executor::AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>;

    /// Resolve the canonical fallback routed key stream for this execution attempt.
    fn resolve_fallback_execution_key_stream(
        &self,
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<OrderedKeyStreamBox, InternalError>;

    /// Attempt the cursorless row-collector short path and erase the typed page result.
    fn try_materialize_load_via_row_collector(
        &self,
        plan: &AccessPlannedQuery,
        cursor_boundary: Option<&CursorBoundary>,
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<Option<(Box<dyn Any>, usize, usize)>, InternalError>;

    /// Materialize one ordered key stream into one erased typed page payload.
    fn materialize_key_stream_into_page(
        &self,
        request: RuntimePageMaterializationRequest<'_>,
    ) -> Result<(Box<dyn Any>, usize, usize), InternalError>;
}

///
/// ExecutionRuntimeAdapter
///
/// Typed runtime adapter that captures recovered context plus typed access
/// helpers once at the execution boundary and exposes one monomorphic runtime
/// trait surface to shared executor code.
///

pub(in crate::db::executor) struct ExecutionRuntimeAdapter<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    ctx: &'a Context<'ctx, E>,
    access: &'a crate::db::access::AccessPlan<E::Key>,
    slot_map: Option<Vec<usize>>,
}

impl<'ctx, 'a, E> ExecutionRuntimeAdapter<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    /// Build one typed runtime adapter from recovered context plus typed access sidecar.
    #[must_use]
    pub(in crate::db::executor) fn new(
        ctx: &'a Context<'ctx, E>,
        access: &'a crate::db::access::AccessPlan<E::Key>,
    ) -> Self {
        let slot_map =
            resolved_index_slots_for_access_path(E::MODEL, access.resolve_strategy().executable());

        Self {
            ctx,
            access,
            slot_map,
        }
    }

    /// Borrow the structural entity model used by this typed adapter.
    #[must_use]
    pub(in crate::db::executor) fn model(&self) -> &'static EntityModel {
        E::MODEL
    }

    /// Borrow the precomputed slot map for this typed adapter.
    #[must_use]
    pub(in crate::db::executor) fn slot_map(&self) -> Option<&[usize]> {
        self.slot_map.as_deref()
    }
}

impl<E> ExecutionRuntime for ExecutionRuntimeAdapter<'_, '_, E>
where
    E: EntityKind + EntityValue,
{
    fn try_execute_pk_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        LoadExecutor::<E>::try_execute_pk_order_stream(
            self.ctx,
            plan,
            direction,
            physical_fetch_hint,
        )
    }

    fn try_execute_secondary_index_order_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        LoadExecutor::<E>::try_execute_secondary_index_order_stream(
            self.ctx,
            plan,
            index_prefix_spec,
            direction,
            physical_fetch_hint,
            index_predicate_execution,
        )
    }

    fn try_execute_index_range_limit_pushdown_stream(
        &self,
        plan: &AccessPlannedQuery,
        index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
        continuation: crate::db::executor::AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        LoadExecutor::<E>::try_execute_index_range_limit_pushdown_stream(
            self.ctx,
            plan,
            index_range_spec,
            continuation,
            fetch,
            index_predicate_execution,
        )
    }

    fn resolve_fallback_execution_key_stream(
        &self,
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let access = ExecutableAccess::new(
            self.access,
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
        );

        LoadExecutor::<E>::resolve_routed_key_stream(
            self.ctx,
            RoutedKeyStreamRequest::ExecutableAccess(access),
        )
    }

    fn try_materialize_load_via_row_collector(
        &self,
        plan: &AccessPlannedQuery,
        cursor_boundary: Option<&CursorBoundary>,
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<Option<(Box<dyn Any>, usize, usize)>, InternalError> {
        Ok(
            ExecutionKernel::try_materialize_load_via_row_collector::<E>(
                self.ctx,
                plan,
                cursor_boundary,
                key_stream,
            )?
            .map(|(page, keys_scanned, post_access_rows)| {
                (
                    Box::new(page) as Box<dyn Any>,
                    keys_scanned,
                    post_access_rows,
                )
            }),
        )
    }

    fn materialize_key_stream_into_page(
        &self,
        request: RuntimePageMaterializationRequest<'_>,
    ) -> Result<(Box<dyn Any>, usize, usize), InternalError> {
        let (page, keys_scanned, post_access_rows) =
            LoadExecutor::<E>::materialize_key_stream_into_page(PageMaterializationRequest {
                ctx: self.ctx,
                plan: request.plan,
                predicate_slots: request.predicate_slots,
                key_stream: request.key_stream,
                scan_budget_hint: request.scan_budget_hint,
                stream_order_contract_safe: request.stream_order_contract_safe,
                consistency: request.consistency,
                continuation: request.continuation,
            })?;

        Ok((
            Box::new(page) as Box<dyn Any>,
            keys_scanned,
            post_access_rows,
        ))
    }
}

///
/// ExecutionInputs
///
/// Shared immutable execution inputs for one load execution attempt.
/// Keeps shared execution code monomorphic by carrying plan shape, runtime
/// bindings, and the pre-resolved runtime adapter instead of typed entity params.
///

pub(in crate::db::executor) struct ExecutionInputs<'a> {
    runtime: &'a dyn ExecutionRuntime,
    plan: &'a AccessPlannedQuery,
    stream_bindings: AccessStreamBindings<'a>,
    execution_preparation: &'a ExecutionPreparation,
}

impl<'a> ExecutionInputs<'a> {
    /// Construct one scalar execution-input projection payload.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        runtime: &'a dyn ExecutionRuntime,
        plan: &'a AccessPlannedQuery,
        stream_bindings: AccessStreamBindings<'a>,
        execution_preparation: &'a ExecutionPreparation,
    ) -> Self {
        Self {
            runtime,
            plan,
            stream_bindings,
            execution_preparation,
        }
    }

    /// Borrow the resolved runtime adapter for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn runtime(&self) -> &dyn ExecutionRuntime {
        self.runtime
    }

    /// Borrow logical access plan payload for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn plan(&self) -> &AccessPlannedQuery {
        self.plan
    }

    /// Borrow lowered access stream bindings for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn stream_bindings(&self) -> &AccessStreamBindings<'_> {
        &self.stream_bindings
    }

    /// Borrow precomputed execution-preparation payloads.
    #[must_use]
    pub(in crate::db::executor) const fn execution_preparation(&self) -> &ExecutionPreparation {
        self.execution_preparation
    }

    /// Return row-read missing-row policy for this execution attempt.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.plan)
    }
}
