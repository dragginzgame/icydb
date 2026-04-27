//! Module: db::executor::prepared_execution_plan
//! Responsibility: bind validated access-planned queries to executor-ready contracts.
//! Does not own: logical plan semantics or route policy decisions.
//! Boundary: shared plan container for load/delete/aggregate runtime entrypoints.

mod aggregate_plan;
mod bytes_projection;
mod core;
mod load_plan;
mod parts;
mod shared_plan;
#[cfg(test)]
mod snapshot;

#[cfg(test)]
use crate::db::{executor::LoweredIndexPrefixSpec, query::plan::ExecutionOrdering};
use crate::{
    db::{
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::{
            EntityAuthority, ExecutorPlanError, explain::assemble_load_execution_node_descriptor,
        },
        predicate::MissingRowPolicy,
        query::{
            explain::ExplainExecutionNodeDescriptor,
            plan::{AccessPlannedQuery, OrderSpec, QueryMode},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::marker::PhantomData;

pub(in crate::db::executor) use aggregate_plan::PreparedAggregatePlan;
pub(in crate::db) use bytes_projection::BytesByProjectionMode;
pub(in crate::db::executor) use bytes_projection::classify_bytes_by_projection_mode;
pub use core::ExecutionFamily;
pub(in crate::db::executor) use core::PreparedScalarPlanCore;
pub(in crate::db::executor::prepared_execution_plan) use core::{
    PreparedExecutionPlanCore, build_prepared_execution_plan_core,
    build_prepared_execution_plan_core_with_lowered_access,
    build_prepared_execution_plan_core_with_shared_lowered_access,
};
pub(in crate::db::executor) use load_plan::PreparedLoadPlan;
pub(in crate::db) use parts::SharedPreparedProjectionRuntimeParts;
pub(in crate::db::executor) use parts::{
    PreparedAccessPlanParts, PreparedAggregateStreamingPlanParts, PreparedGroupedRuntimeParts,
    PreparedScalarRuntimeParts,
};
pub(in crate::db) use shared_plan::SharedPreparedExecutionPlan;

///
/// PreparedExecutionPlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Debug)]
pub(in crate::db) struct PreparedExecutionPlan<E: EntityKind> {
    core: PreparedExecutionPlanCore,
    marker: PhantomData<fn() -> E>,
}

impl<E: EntityKind> PreparedExecutionPlan<E> {
    pub(in crate::db) fn new(plan: AccessPlannedQuery) -> Self {
        Self::build(plan)
    }

    fn build(mut plan: AccessPlannedQuery) -> Self {
        let authority = EntityAuthority::for_type::<E>();
        authority.finalize_planner_route_profile(&mut plan);

        Self {
            core: build_prepared_execution_plan_core(authority, plan),
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

        let authority = EntityAuthority::for_type::<E>();

        assemble_load_execution_node_descriptor(
            authority.fields(),
            authority.primary_key_name(),
            self.core.plan(),
        )
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
    pub(in crate::db) fn mode(&self) -> QueryMode {
        self.core.mode()
    }

    /// Return whether this prepared execution plan carries grouped logical shape.
    #[must_use]
    pub(in crate::db) fn is_grouped(&self) -> bool {
        self.core.is_grouped()
    }

    /// Return planner-projected execution strategy for entrypoint dispatch.
    pub(in crate::db) fn execution_family(&self) -> Result<ExecutionFamily, InternalError> {
        self.core.execution_family()
    }

    /// Borrow the structural logical plan behind this prepared execution plan.
    #[must_use]
    pub(in crate::db) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    /// Expose planner-projected execution ordering for executor/lowering tests.
    #[cfg(test)]
    pub(in crate::db) fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        self.core.execution_ordering()
    }

    pub(in crate::db) fn access(&self) -> &crate::db::access::AccessPlan<crate::value::Value> {
        &self.core.plan().access
    }

    /// Borrow scalar row-consistency policy for runtime row reads.
    #[must_use]
    pub(in crate::db) fn consistency(&self) -> MissingRowPolicy {
        self.core.consistency()
    }

    /// Classify canonical `bytes_by(field)` execution mode for this plan/field.
    #[must_use]
    pub(in crate::db) fn bytes_by_projection_mode(
        &self,
        target_field: &str,
    ) -> BytesByProjectionMode {
        let authority = EntityAuthority::for_type::<E>();

        classify_bytes_by_projection_mode(
            self.access(),
            self.order_spec(),
            self.consistency(),
            self.has_predicate(),
            target_field,
            authority.primary_key_name(),
        )
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

    /// Borrow scalar ORDER BY contract for this prepared execution plan, if any.
    #[must_use]
    pub(in crate::db::executor) fn order_spec(&self) -> Option<&OrderSpec> {
        self.core.order_spec()
    }

    /// Borrow lowered index-prefix specs for test-only executor contracts.
    #[cfg(test)]
    pub(in crate::db) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        self.core.index_prefix_specs()
    }

    /// Return whether this prepared execution plan has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) fn has_predicate(&self) -> bool {
        self.core.has_predicate()
    }

    // Collapse the typed prepared shell into the structural logical plan plus
    // lowered access specs together so structural consumers do not peel those
    // three prepared artifacts back out through separate wrappers.
    pub(in crate::db) fn into_access_plan_parts(
        self,
    ) -> Result<PreparedAccessPlanParts, InternalError> {
        let shared = self.core.into_shared();

        if shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(PreparedAccessPlanParts {
            plan: shared.plan,
            index_prefix_specs: shared.index_prefix_specs,
            index_range_specs: shared.index_range_specs,
        })
    }

    /// Validate and decode grouped continuation cursor state for grouped plans.
    #[cfg(test)]
    pub(in crate::db) fn prepare_grouped_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.core.shared.continuation.as_ref() else {
            return Err(ExecutorPlanError::grouped_cursor_preparation_requires_grouped_plan());
        };

        contract
            .prepare_grouped_cursor(EntityAuthority::for_type::<E>().entity_path(), cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Validate one already-decoded grouped continuation token for grouped plans.
    pub(in crate::db) fn prepare_grouped_cursor_token(
        &self,
        cursor: Option<crate::db::cursor::GroupedContinuationToken>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.core.shared.continuation.as_ref() else {
            return Err(ExecutorPlanError::grouped_cursor_preparation_requires_grouped_plan());
        };

        contract
            .prepare_grouped_cursor_token(EntityAuthority::for_type::<E>().entity_path(), cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Consume one typed prepared execution plan into one generic-free boundary
    /// payload for continuation and load-pipeline preparation.
    #[must_use]
    pub(in crate::db::executor) fn into_prepared_load_plan(self) -> PreparedLoadPlan {
        PreparedLoadPlan {
            authority: EntityAuthority::for_type::<E>(),
            core: self.core,
        }
    }

    /// Consume one typed prepared execution plan into one generic-free
    /// boundary payload for aggregate terminal and runtime preparation.
    #[must_use]
    pub(in crate::db::executor) fn into_prepared_aggregate_plan(self) -> PreparedAggregatePlan {
        PreparedAggregatePlan {
            authority: EntityAuthority::for_type::<E>(),
            core: self.core,
        }
    }
}
