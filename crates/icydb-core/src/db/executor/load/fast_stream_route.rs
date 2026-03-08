//! Module: executor::load::fast_stream_route
//! Responsibility: shared fast-stream route execution across PK/secondary/range shapes.
//! Does not own: fast-path eligibility policy selection.
//! Boundary: route-specific spec binding and fast-stream execution dispatch.

use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessScanContinuationInput, AccessStreamBindings,
            ExecutionOptimization, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            load::{FastPathKeyResult, LoadExecutor},
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// FastStreamRouteKind
///
/// Canonical fast-stream route discriminator used by shared load adapters.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor::load) enum FastStreamRouteKind {
    PrimaryKey,
    SecondaryIndex,
    IndexRangeLimitPushdown,
}

///
/// FastStreamRouteRequest
///
/// Route-specific stream binding payload consumed by shared fast-stream dispatch.
///

pub(in crate::db::executor::load) enum FastStreamRouteRequest<'a, K> {
    PrimaryKey {
        plan: &'a AccessPlannedQuery<K>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
    },
    SecondaryIndex {
        plan: &'a AccessPlannedQuery<K>,
        index_prefix_spec: Option<&'a LoweredIndexPrefixSpec>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    },
    IndexRangeLimitPushdown {
        plan: &'a AccessPlannedQuery<K>,
        index_range_spec: Option<&'a LoweredIndexRangeSpec>,
        continuation: AccessScanContinuationInput<'a>,
        effective_fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    },
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one verified fast-stream route through the shared load dispatch boundary.
    pub(in crate::db::executor::load) fn execute_fast_stream_route(
        ctx: &Context<'_, E>,
        route_kind: FastStreamRouteKind,
        request: FastStreamRouteRequest<'_, E::Key>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        match (route_kind, request) {
            (
                FastStreamRouteKind::PrimaryKey,
                FastStreamRouteRequest::PrimaryKey {
                    plan,
                    stream_direction,
                    probe_fetch_hint,
                },
            ) => Self::execute_primary_key_fast_stream_route(
                ctx,
                plan,
                stream_direction,
                probe_fetch_hint,
            ),
            (
                FastStreamRouteKind::SecondaryIndex,
                FastStreamRouteRequest::SecondaryIndex {
                    plan,
                    index_prefix_spec,
                    stream_direction,
                    probe_fetch_hint,
                    index_predicate_execution,
                },
            ) => Self::execute_secondary_index_fast_stream_route(
                ctx,
                plan,
                index_prefix_spec,
                stream_direction,
                probe_fetch_hint,
                index_predicate_execution,
            ),
            (
                FastStreamRouteKind::IndexRangeLimitPushdown,
                FastStreamRouteRequest::IndexRangeLimitPushdown {
                    plan,
                    index_range_spec,
                    continuation,
                    effective_fetch,
                    index_predicate_execution,
                },
            ) => Self::execute_index_range_fast_stream_route(
                ctx,
                plan,
                index_range_spec,
                continuation,
                effective_fetch,
                index_predicate_execution,
            ),
            _ => Err(crate::db::executor::load::invariant(
                "fast-stream route kind/request mismatch",
            )),
        }
    }

    fn execute_primary_key_fast_stream_route(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // Phase 1: validate that the routed access shape is PK-stream compatible.
        Self::verify_pk_stream_fast_path_access(plan)?;

        // Phase 2: lower through the canonical access-stream resolver boundary.
        let descriptor = AccessExecutionDescriptor::from_bindings(
            &plan.access,
            AccessStreamBindings::no_index(stream_direction),
            probe_fetch_hint,
            None,
        );
        Ok(Some(Self::execute_fast_stream_request(
            ctx,
            descriptor,
            ExecutionOptimization::PrimaryKey,
        )?))
    }

    fn execute_secondary_index_fast_stream_route(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // Phase 1: verify access-path/spec invariants for index-prefix execution.
        let access_strategy = plan.access_strategy();
        let Some(executable_path) = access_strategy.as_path() else {
            return Ok(None);
        };
        let path_capabilities = executable_path.capabilities();
        let Some(index) = path_capabilities.index_prefix_model() else {
            return Ok(None);
        };
        let Some(index_prefix_spec) = index_prefix_spec else {
            return Err(crate::db::executor::load::invariant(
                "index-prefix executable spec must be materialized for index-prefix plans",
            ));
        };
        debug_assert_eq!(
            index_prefix_spec.index(),
            &index,
            "secondary fast-path spec/index alignment must be validated by resolver",
        );

        // Phase 2: bind execution inputs and run the shared fast-stream boundary.
        let descriptor = AccessExecutionDescriptor::from_executable_bindings(
            access_strategy.into_executable(),
            AccessStreamBindings::with_index_prefix(index_prefix_spec, stream_direction),
            probe_fetch_hint,
            index_predicate_execution,
        );
        let fast = Self::execute_fast_stream_request(
            ctx,
            descriptor,
            ExecutionOptimization::SecondaryOrderPushdown,
        )?;
        if let Some(fetch) = probe_fetch_hint {
            debug_assert!(
                fast.rows_scanned <= fetch,
                "secondary fast-path rows_scanned must not exceed bounded fetch",
            );
        }

        Ok(Some(fast))
    }

    fn execute_index_range_fast_stream_route(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: AccessScanContinuationInput<'_>,
        effective_fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // Phase 1: verify access-path and executable spec materialization invariants.
        let access_strategy = plan.access_strategy();
        let Some(executable_path) = access_strategy.as_path() else {
            return Ok(None);
        };
        let path_capabilities = executable_path.capabilities();
        let Some(index) = path_capabilities.index_range_model() else {
            return Ok(None);
        };
        let Some(index_range_spec) = index_range_spec else {
            return Err(crate::db::executor::load::invariant(
                "index-range executable spec must be materialized for index-range plans",
            ));
        };
        debug_assert_eq!(
            index_range_spec.index(),
            &index,
            "index-range fast-path spec/index alignment must be validated by resolver",
        );

        // Phase 2: bind range/anchor inputs and execute through shared fast-stream helper.
        let descriptor = AccessExecutionDescriptor::from_executable_bindings(
            access_strategy.into_executable(),
            AccessStreamBindings::with_index_range_continuation(index_range_spec, continuation),
            Some(effective_fetch),
            index_predicate_execution,
        );

        Ok(Some(Self::execute_fast_stream_request(
            ctx,
            descriptor,
            ExecutionOptimization::IndexRangeLimitPushdown,
        )?))
    }
}
