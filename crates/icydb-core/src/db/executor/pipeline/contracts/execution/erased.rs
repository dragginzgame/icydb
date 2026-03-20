//! Module: db::executor::pipeline::contracts::execution::erased
//! Responsibility: structural execution-runtime bindings for fast-path and fallback stream resolution.
//! Does not own: structural page materialization or execution-attempt orchestration.
//! Boundary: carries one structural traversal runtime so hot-path stream execution stays free of typed context recovery.

use crate::{
    db::{
        access::AccessPlan,
        direction::Direction,
        executor::{
            AccessStreamBindings, ExecutableAccess, OrderedKeyStreamBox,
            pipeline::contracts::FastPathKeyResult,
            scan::{FastStreamRouteKind, FastStreamRouteRequest, execute_fast_stream_route},
            stream::access::StructuralTraversalRuntime,
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    value::Value,
};

///
/// ErasedRuntimeBindings
///
/// ErasedRuntimeBindings keeps the structural stream-resolution authority
/// needed by the execution runtime adapter after the typed boundary computes
/// model-specific preparation inputs.
/// It deliberately stores one non-generic structural traversal runtime so
/// fast-path key-stream execution no longer recovers `Context<'_, E>` through
/// a typed vtable leaf.
///

pub(in crate::db::executor::pipeline::contracts::execution) struct ErasedRuntimeBindings {
    runtime: StructuralTraversalRuntime,
}

impl ErasedRuntimeBindings {
    /// Construct one structural runtime binding bundle from one structural traversal runtime.
    #[must_use]
    pub(in crate::db::executor::pipeline::contracts::execution) const fn from_runtime(
        runtime: StructuralTraversalRuntime,
    ) -> Self {
        Self { runtime }
    }

    /// Delegate primary-key fast-path execution through the structural runtime leaf.
    pub(in crate::db::executor::pipeline::contracts::execution) fn pk_order(
        &self,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        execute_fast_stream_route(
            &self.runtime,
            FastStreamRouteKind::PrimaryKey,
            FastStreamRouteRequest::PrimaryKey {
                plan,
                stream_direction: direction,
                probe_fetch_hint: physical_fetch_hint,
            },
        )
    }

    /// Delegate secondary-index fast-path execution through the structural runtime leaf.
    pub(in crate::db::executor::pipeline::contracts::execution) fn secondary_index_order(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&crate::db::executor::LoweredIndexPrefixSpec>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        execute_fast_stream_route(
            &self.runtime,
            FastStreamRouteKind::SecondaryIndex,
            FastStreamRouteRequest::SecondaryIndex {
                plan,
                index_prefix_spec,
                stream_direction: direction,
                probe_fetch_hint: physical_fetch_hint,
                index_predicate_execution,
            },
        )
    }

    /// Delegate index-range limit-pushdown execution through the structural runtime leaf.
    pub(in crate::db::executor::pipeline::contracts::execution) fn index_range_limit_pushdown(
        &self,
        plan: &AccessPlannedQuery,
        index_range_spec: Option<&crate::db::executor::LoweredIndexRangeSpec>,
        continuation: crate::db::executor::AccessScanContinuationInput<'_>,
        fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        execute_fast_stream_route(
            &self.runtime,
            FastStreamRouteKind::IndexRangeLimitPushdown,
            FastStreamRouteRequest::IndexRangeLimitPushdown {
                plan,
                index_range_spec,
                continuation,
                effective_fetch: fetch,
                index_predicate_execution,
            },
        )
    }

    /// Delegate fallback key-stream resolution through the structural runtime leaf.
    pub(in crate::db::executor::pipeline::contracts::execution) fn fallback_execution_keys(
        &self,
        access: &AccessPlan<Value>,
        bindings: AccessStreamBindings<'_>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let access = ExecutableAccess::from_executable_plan(
            access.resolve_strategy().into_executable(),
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
        );

        self.runtime
            .ordered_key_stream_from_structural_runtime_access(access)
    }
}
