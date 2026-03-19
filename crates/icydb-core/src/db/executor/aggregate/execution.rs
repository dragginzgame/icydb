//! Module: executor::aggregate::execution
//! Responsibility: aggregate execution descriptor/input payload contracts.
//! Does not own: aggregate execution branching logic.
//! Boundary: shared immutable payloads between aggregate orchestration helpers.

use crate::{
    db::{
        Context,
        access::AccessPlan,
        direction::Direction,
        executor::{
            ExecutionPlan, ExecutionPreparation, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            aggregate::{
                AggregateKind, field::FieldSlot, projection::ScalarProjectionBoundaryRequest,
            },
            pipeline::contracts::GroupedRouteStage,
            traversal::row_read_consistency_for_plan,
        },
        index::IndexPredicateProgram,
        predicate::MissingRowPolicy,
        query::{
            builder::AggregateExpr,
            plan::{
                AccessPlannedQuery, CoveringProjectionContext, OrderDirection, OrderSpec, PageSpec,
            },
        },
    },
    traits::{EntityKind, EntityValue},
    value::Value,
};

///
/// AggregateFastPathInputs
///
/// Aggregate fast-path execution inputs bundled for one dispatch entry.
/// Keeps branch routing parameters aligned between aggregate path helpers.
///

pub(in crate::db::executor) struct AggregateFastPathInputs<'exec, 'ctx, E: EntityKind + EntityValue>
{
    pub(in crate::db::executor) ctx: &'exec Context<'ctx, E>,
    pub(in crate::db::executor) logical_plan: &'exec AccessPlannedQuery,
    pub(in crate::db::executor) route_plan: &'exec ExecutionPlan,
    pub(in crate::db::executor) index_prefix_specs: &'exec [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'exec [LoweredIndexRangeSpec],
    pub(in crate::db::executor) index_predicate_program: Option<&'exec IndexPredicateProgram>,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) kind: super::AggregateKind,
    pub(in crate::db::executor) fold_mode: super::AggregateFoldMode,
}

impl<E> AggregateFastPathInputs<'_, '_, E>
where
    E: EntityKind + EntityValue,
{
    /// Return row-read missing-row policy for this aggregate fast-path attempt.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.logical_plan)
    }
}

///
/// AggregateExecutionDescriptor
///
/// Canonical aggregate execution descriptor constructed once from a terminal
/// aggregate spec and validated plan shape before execution branching.
///

#[derive(Clone)]
pub(in crate::db::executor) struct AggregateExecutionDescriptor {
    pub(in crate::db::executor) aggregate: AggregateExpr,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) route_plan: ExecutionPlan,
    pub(in crate::db::executor) execution_preparation: ExecutionPreparation,
}

///
/// PreparedAggregateExecutionState
///
/// PreparedAggregateExecutionState is the canonical scalar aggregate execution
/// payload after the typed boundary has consumed `ExecutablePlan<E>`.
/// It keeps aggregate descriptor state together with prepared logical/runtime
/// inputs so downstream execution no longer reconstructs typed plan shells.
///

pub(in crate::db::executor) struct PreparedAggregateExecutionState<
    'ctx,
    E: EntityKind + EntityValue,
> {
    pub(in crate::db::executor) descriptor: AggregateExecutionDescriptor,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx, E>,
}

///
/// PreparedAggregateStreamingInputs
///
/// PreparedAggregateStreamingInputs owns canonical aggregate streaming setup
/// state after `ExecutablePlan` is consumed into logical plan form.
///

pub(in crate::db::executor) struct PreparedAggregateStreamingInputs<
    'ctx,
    E: EntityKind + EntityValue,
> {
    pub(in crate::db::executor) ctx: Context<'ctx, E>,
    pub(in crate::db::executor) logical_plan: AccessPlannedQuery,
    pub(in crate::db::executor) typed_access: AccessPlan<E::Key>,
    pub(in crate::db::executor) index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    pub(in crate::db::executor) index_range_specs: Vec<LoweredIndexRangeSpec>,
}

impl<E> PreparedAggregateStreamingInputs<'_, E>
where
    E: EntityKind + EntityValue,
{
    /// Return whether normalized plan semantics prove the aggregate window is empty.
    #[must_use]
    pub(in crate::db::executor) fn window_is_provably_empty(&self) -> bool {
        self.page_spec().is_some_and(|page| page.limit == Some(0))
            || self
                .logical_plan
                .access
                .resolve_strategy()
                .as_path()
                .is_some_and(|path| path.capabilities().is_by_keys_empty())
    }

    /// Borrow scalar ORDER BY semantics for prepared aggregate execution.
    #[must_use]
    pub(in crate::db::executor) const fn order_spec(&self) -> Option<&OrderSpec> {
        self.logical_plan.scalar_plan().order.as_ref()
    }

    /// Borrow scalar page-window semantics for prepared aggregate execution.
    #[must_use]
    pub(in crate::db::executor) const fn page_spec(&self) -> Option<&PageSpec> {
        self.logical_plan.scalar_plan().page.as_ref()
    }

    /// Return whether prepared aggregate execution still has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) const fn has_predicate(&self) -> bool {
        self.logical_plan.scalar_plan().predicate.is_some()
    }

    /// Return whether prepared aggregate execution still has scalar DISTINCT enabled.
    #[must_use]
    pub(in crate::db::executor) const fn is_distinct(&self) -> bool {
        self.logical_plan.scalar_plan().distinct
    }

    /// Return whether the prepared aggregate shape clears predicate and DISTINCT gates.
    #[must_use]
    pub(in crate::db::executor) const fn has_no_predicate_or_distinct(&self) -> bool {
        !self.has_predicate() && !self.is_distinct()
    }

    /// Return primary-key order direction when prepared execution uses only PK ordering.
    #[must_use]
    pub(in crate::db::executor) fn explicit_primary_key_order_direction(
        &self,
        primary_key_name: &'static str,
    ) -> Option<Direction> {
        let order = self.order_spec()?;

        order
            .primary_key_only_direction(primary_key_name)
            .map(|direction| match direction {
                OrderDirection::Asc => Direction::Asc,
                OrderDirection::Desc => Direction::Desc,
            })
    }

    /// Return row-read missing-row policy for prepared aggregate streaming.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.logical_plan)
    }
}

///
/// PreparedScalarNumericOp
///
/// Non-generic numeric terminal operation resolved at the typed scalar
/// boundary. Execution branches only on this operation selector plus the
/// prepared strategy and no longer inspect `ExecutablePlan<E>`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum PreparedScalarNumericOp {
    Sum,
    Avg,
}

impl PreparedScalarNumericOp {
    /// Return the aggregate kind represented by this numeric terminal.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_kind(self) -> AggregateKind {
        match self {
            Self::Sum => AggregateKind::Sum,
            Self::Avg => AggregateKind::Avg,
        }
    }

    /// Return the stable terminal name used in DISTINCT decode mismatch text.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_name(self) -> &'static str {
        match self {
            Self::Sum => "SUM",
            Self::Avg => "AVG",
        }
    }
}

///
/// PreparedScalarNumericStrategy
///
/// Non-generic numeric execution strategy resolved during typed boundary
/// preparation. Runtime execution matches this enum instead of re-reading
/// access-path and pagination policy from the original plan.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum PreparedScalarNumericStrategy {
    Streaming,
    Materialized,
    GlobalDistinctGrouped,
}

///
/// PreparedScalarNumericBoundary
///
/// PreparedScalarNumericBoundary is the non-generic numeric scalar contract
/// derived once from a typed plan and request.
/// It contains only resolved field metadata, the numeric operation, and the
/// chosen execution strategy.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct PreparedScalarNumericBoundary {
    pub(in crate::db::executor) target_field_name: String,
    pub(in crate::db::executor) field_slot: FieldSlot,
    pub(in crate::db::executor) op: PreparedScalarNumericOp,
    pub(in crate::db::executor) strategy: PreparedScalarNumericStrategy,
}

///
/// PreparedScalarNumericExecutionState
///
/// PreparedScalarNumericExecutionState pairs one non-generic numeric boundary
/// contract with the runtime payload needed to execute it.
/// The boundary itself is plan-free; only the runtime payload remains typed.
///

pub(in crate::db::executor) enum PreparedScalarNumericExecutionState<
    'ctx,
    E: EntityKind + EntityValue,
> {
    Aggregate {
        boundary: PreparedScalarNumericBoundary,
        prepared: Box<PreparedAggregateStreamingInputs<'ctx, E>>,
    },
    GlobalDistinct {
        boundary: PreparedScalarNumericBoundary,
        route: Box<GroupedRouteStage>,
    },
}

///
/// PreparedCoveringDistinctStrategy
///
/// PreparedCoveringDistinctStrategy records the dedupe policy selected during
/// boundary preparation for covering DISTINCT field projections.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum PreparedCoveringDistinctStrategy {
    Adjacent,
    PreserveFirst,
}

///
/// PreparedScalarProjectionOp
///
/// Non-generic field-projection terminal operation resolved at the typed
/// boundary before runtime execution begins.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum PreparedScalarProjectionOp {
    Values,
    DistinctValues,
    CountDistinct,
    ValuesWithIds,
    TerminalValue { terminal_kind: AggregateKind },
}

///
/// ScalarProjectionWindow
///
/// ScalarProjectionWindow stores effective covering-projection pagination
/// bounds after typed plan normalization.
/// Covering execution uses this structural window directly instead of reading
/// `PageSpec` from the original plan.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarProjectionWindow {
    pub(in crate::db::executor) offset: usize,
    pub(in crate::db::executor) limit: Option<usize>,
}

///
/// PreparedScalarProjectionStrategy
///
/// Non-generic projection execution strategy resolved during typed boundary
/// preparation. Execution branches only on this strategy plus the prepared
/// operation contract.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum PreparedScalarProjectionStrategy {
    Materialized,
    CoveringIndex {
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
        distinct: Option<PreparedCoveringDistinctStrategy>,
    },
    CoveringConstant {
        value: Value,
    },
}

///
/// PreparedScalarProjectionBoundary
///
/// PreparedScalarProjectionBoundary is the plan-free scalar projection
/// contract derived once at the typed boundary.
/// It captures the resolved field slot, operation kind, and prepared
/// execution strategy without retaining `ExecutablePlan<E>`.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct PreparedScalarProjectionBoundary {
    pub(in crate::db::executor) target_field_name: String,
    pub(in crate::db::executor) field_slot: FieldSlot,
    pub(in crate::db::executor) op: PreparedScalarProjectionOp,
    pub(in crate::db::executor) strategy: PreparedScalarProjectionStrategy,
}

///
/// PreparedScalarProjectionExecutionState
///
/// PreparedScalarProjectionExecutionState combines the non-generic prepared
/// projection contract with the runtime payload required to execute it.
/// The executor matches only the boundary contract; typed state remains an
/// opaque runtime dependency rather than a policy source.
///

pub(in crate::db::executor) struct PreparedScalarProjectionExecutionState<
    'ctx,
    E: EntityKind + EntityValue,
> {
    pub(in crate::db::executor) boundary: PreparedScalarProjectionBoundary,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx, E>,
}

///
/// PreparedScalarTerminalOp
///
/// Non-generic scalar terminal operation resolved at the typed boundary before
/// execution begins.
/// This keeps terminal execution keyed to one prepared runtime contract
/// instead of to the original typed executable plan.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum PreparedScalarTerminalOp {
    Count,
    Exists,
    IdTerminal {
        kind: AggregateKind,
    },
    IdBySlot {
        kind: AggregateKind,
        target_field_name: String,
    },
}

impl PreparedScalarTerminalOp {
    /// Return the aggregate kind represented by this prepared scalar terminal.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_kind(&self) -> AggregateKind {
        match self {
            Self::Count => AggregateKind::Count,
            Self::Exists => AggregateKind::Exists,
            Self::IdTerminal { kind } | Self::IdBySlot { kind, .. } => *kind,
        }
    }
}

///
/// PreparedScalarTerminalStrategy
///
/// Non-generic scalar terminal execution strategy selected during typed
/// boundary preparation.
/// Runtime execution matches this enum directly instead of re-reading fast-path
/// eligibility from `ExecutablePlan<E>`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum PreparedScalarTerminalStrategy {
    KernelAggregate,
    CountPrimaryKeyCardinality,
    CountExistingRows {
        direction: Direction,
        covering: bool,
    },
    ExistsExistingRows {
        direction: Direction,
    },
}

///
/// PreparedScalarTerminalBoundary
///
/// PreparedScalarTerminalBoundary is the plan-free scalar terminal contract
/// derived once at the typed boundary for COUNT, EXISTS, and id terminals.
/// It carries the resolved operation and selected execution strategy without
/// retaining `ExecutablePlan<E>`.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct PreparedScalarTerminalBoundary {
    pub(in crate::db::executor) op: PreparedScalarTerminalOp,
    pub(in crate::db::executor) strategy: PreparedScalarTerminalStrategy,
}

///
/// PreparedScalarTerminalExecutionState
///
/// PreparedScalarTerminalExecutionState pairs one prepared scalar terminal
/// boundary with the runtime payload needed to execute it.
/// Terminal execution consumes this prepared state directly and no longer
/// receives plan-owned fast-path policy.
///

pub(in crate::db::executor) struct PreparedScalarTerminalExecutionState<
    'ctx,
    E: EntityKind + EntityValue,
> {
    pub(in crate::db::executor) boundary: PreparedScalarTerminalBoundary,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx, E>,
}

impl PreparedScalarProjectionOp {
    /// Resolve one public projection boundary request into the non-generic
    /// prepared projection operation used by execution.
    pub(in crate::db::executor) const fn from_request(
        request: ScalarProjectionBoundaryRequest,
    ) -> Self {
        match request {
            ScalarProjectionBoundaryRequest::Values => Self::Values,
            ScalarProjectionBoundaryRequest::DistinctValues => Self::DistinctValues,
            ScalarProjectionBoundaryRequest::CountDistinct => Self::CountDistinct,
            ScalarProjectionBoundaryRequest::ValuesWithIds => Self::ValuesWithIds,
            ScalarProjectionBoundaryRequest::TerminalValue { terminal_kind } => {
                Self::TerminalValue { terminal_kind }
            }
        }
    }
}
