//! Module: executor::aggregate::execution
//! Responsibility: aggregate execution descriptor/input payload contracts.
//! Does not own: aggregate execution branching logic.
//! Boundary: shared immutable payloads between aggregate orchestration helpers.

use crate::{
    db::{
        access::{LoweredAccess, LoweredAccessError, lower_access},
        direction::Direction,
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation, ExecutorPlanError,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec, StoreResolver,
            aggregate::{AggregateKind, ScalarTerminalKind, field::FieldSlot},
            pipeline::contracts::GroupedRouteStage,
            route::AggregateRouteShape,
            traversal::row_read_consistency_for_plan,
        },
        index::IndexPredicateProgram,
        predicate::MissingRowPolicy,
        query::{
            builder::aggregate::ScalarProjectionBoundaryRequest,
            plan::{AccessPlannedQuery, CoveringProjectionContext, OrderSpec, PageSpec},
        },
        registry::StoreHandle,
    },
    error::InternalError,
    value::Value,
};
use std::sync::Arc;

///
/// AggregateFastPathInputs
///
/// Aggregate fast-path execution inputs bundled for one dispatch entry.
/// Keeps branch routing parameters aligned between aggregate path helpers.
///

pub(in crate::db::executor) struct AggregateFastPathInputs<'exec> {
    pub(in crate::db::executor) logical_plan: &'exec AccessPlannedQuery,
    pub(in crate::db::executor) executable_access:
        &'exec crate::db::access::ExecutableAccessPlan<'exec, Value>,
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) store: StoreHandle,
    pub(in crate::db::executor) route_plan: &'exec ExecutionPlan,
    pub(in crate::db::executor) index_prefix_specs: &'exec [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'exec [LoweredIndexRangeSpec],
    pub(in crate::db::executor) index_predicate_program: Option<&'exec IndexPredicateProgram>,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) kind: super::ScalarTerminalKind,
    pub(in crate::db::executor) fold_mode: super::AggregateFoldMode,
}

impl AggregateFastPathInputs<'_> {
    /// Return row-read missing-row policy for this aggregate fast-path attempt.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.logical_plan)
    }
}

/// PreparedAggregateTargetField
///
/// PreparedAggregateTargetField freezes one field-target aggregate descriptor.
/// It carries the runtime field label, resolved field slot, and route-facing
/// target-field flags needed by aggregate execution without reopening
/// planner or field-table semantics during execution.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct PreparedAggregateTargetField {
    target_field_name: String,
    field_slot: FieldSlot,
    target_field_known: bool,
    target_field_orderable: bool,
    target_field_is_primary_key: bool,
}

impl PreparedAggregateTargetField {
    /// Construct one field-target aggregate descriptor from prepared metadata.
    #[expect(
        clippy::missing_const_for_fn,
        reason = "constructs one owned String-backed descriptor for runtime handoff"
    )]
    #[must_use]
    pub(in crate::db::executor) fn new(
        target_field_name: String,
        field_slot: FieldSlot,
        target_field_known: bool,
        target_field_orderable: bool,
        target_field_is_primary_key: bool,
    ) -> Self {
        Self {
            target_field_name,
            field_slot,
            target_field_known,
            target_field_orderable,
            target_field_is_primary_key,
        }
    }

    /// Borrow the prepared target-field label.
    #[expect(
        clippy::missing_const_for_fn,
        reason = "String::as_str is kept on the ordinary method boundary for readability"
    )]
    #[must_use]
    pub(in crate::db::executor) fn target_field_name(&self) -> &str {
        self.target_field_name.as_str()
    }

    /// Borrow the prepared runtime field slot.
    #[must_use]
    pub(in crate::db::executor) const fn field_slot(&self) -> FieldSlot {
        self.field_slot
    }
}

///
/// PreparedAggregateSpec
///
/// PreparedAggregateSpec is the immutable aggregate execution request carried
/// through runtime after typed boundary preparation.
/// It freezes kind plus any field-target descriptor so aggregate routing and
/// execution can consume prepared metadata without rebuilding `AggregateExpr`
/// or revalidating target fields against schema authority.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct PreparedAggregateSpec {
    kind: AggregateKind,
    target_field: Option<PreparedAggregateTargetField>,
}

impl PreparedAggregateSpec {
    /// Construct one non-field-target aggregate spec.
    #[must_use]
    pub(in crate::db::executor) const fn terminal(kind: AggregateKind) -> Self {
        Self {
            kind,
            target_field: None,
        }
    }

    /// Construct one field-target aggregate spec from prepared metadata.
    #[must_use]
    pub(in crate::db::executor) const fn field_target(
        kind: AggregateKind,
        target_field: PreparedAggregateTargetField,
    ) -> Self {
        Self {
            kind,
            target_field: Some(target_field),
        }
    }

    /// Borrow the aggregate kind.
    #[must_use]
    pub(in crate::db::executor) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow the prepared target-field descriptor, if any.
    #[must_use]
    pub(in crate::db::executor) const fn target_field(
        &self,
    ) -> Option<&PreparedAggregateTargetField> {
        self.target_field.as_ref()
    }

    /// Lower one route-owned aggregate shape from this prepared spec.
    #[must_use]
    pub(in crate::db::executor) fn route_shape(&self) -> AggregateRouteShape<'_> {
        let Some(target_field) = self.target_field.as_ref() else {
            return AggregateRouteShape::new_resolved(self.kind, None, true, false, false);
        };

        AggregateRouteShape::new_resolved(
            self.kind,
            Some(target_field.target_field_name()),
            target_field.target_field_known,
            target_field.target_field_orderable,
            target_field.target_field_is_primary_key,
        )
    }
}

///
/// AggregateExecutionDescriptor
///
/// Canonical aggregate execution descriptor constructed once from one
/// prepared aggregate spec and validated plan shape before execution branching.
///

#[derive(Clone)]
pub(in crate::db::executor) struct AggregateExecutionDescriptor {
    pub(in crate::db::executor) aggregate: PreparedAggregateSpec,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) route_plan: ExecutionPlan,
}

///
/// PreparedAggregateExecutionState
///
/// PreparedAggregateExecutionState is the canonical scalar aggregate execution
/// payload after the typed boundary has consumed `PreparedExecutionPlan<E>`.
/// It keeps aggregate descriptor state together with prepared logical/runtime
/// inputs so downstream execution no longer reconstructs typed plan shells.
///

pub(in crate::db::executor) struct PreparedAggregateExecutionState<'ctx> {
    pub(in crate::db::executor) descriptor: AggregateExecutionDescriptor,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx>,
}

///
/// PreparedAggregateStreamingInputs
///
/// PreparedAggregateStreamingInputs owns canonical aggregate streaming setup
/// state after `PreparedExecutionPlan` is consumed into logical plan form.
///

pub(in crate::db::executor) struct PreparedAggregateStreamingInputs<'ctx> {
    pub(in crate::db::executor) store_resolver: StoreResolver<'ctx>,
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) store: StoreHandle,
    pub(in crate::db::executor) logical_plan: Arc<AccessPlannedQuery>,
    pub(in crate::db::executor) execution_preparation: ExecutionPreparation,
    pub(in crate::db::executor) index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
    pub(in crate::db::executor) index_range_specs: Arc<[LoweredIndexRangeSpec]>,
}

impl PreparedAggregateStreamingInputs<'_> {
    /// Lower the owned logical access plan for one explicit decision phase.
    pub(in crate::db::executor) fn lowered_access(
        &self,
    ) -> Result<LoweredAccess<'_, Value>, InternalError> {
        lower_access(self.authority.entity_tag(), &self.logical_plan.access).map_err(
            |err| match err {
                LoweredAccessError::IndexPrefix(_) => {
                    ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
                }
                LoweredAccessError::IndexRange(_) => {
                    ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error()
                }
            },
        )
    }

    /// Return whether normalized plan semantics prove the aggregate window is empty.
    #[must_use]
    pub(in crate::db::executor) fn window_is_provably_empty(
        &self,
        lowered_access: &LoweredAccess<'_, Value>,
    ) -> bool {
        self.page_spec().is_some_and(|page| page.limit == Some(0))
            || lowered_access
                .executable()
                .as_path()
                .is_some_and(|path| path.capabilities().is_by_keys_empty())
    }

    /// Borrow scalar ORDER BY semantics for prepared aggregate execution.
    #[must_use]
    pub(in crate::db::executor) fn order_spec(&self) -> Option<&OrderSpec> {
        self.logical_plan.scalar_plan().order.as_ref()
    }

    /// Borrow scalar page-window semantics for prepared aggregate execution.
    #[must_use]
    pub(in crate::db::executor) fn page_spec(&self) -> Option<&PageSpec> {
        self.logical_plan.scalar_plan().page.as_ref()
    }

    /// Return whether prepared aggregate execution still carries one logical
    /// predicate contract.
    ///
    /// Projection/rank terminals keep this broader gate instead of the
    /// narrower residual-only view because their scan-budget and effective
    /// window contracts still follow the filtered query surface, even when the
    /// chosen access path proves the predicate exactly.
    #[must_use]
    pub(in crate::db::executor) fn has_predicate(&self) -> bool {
        self.logical_plan.scalar_plan().predicate.is_some()
    }

    /// Return whether prepared aggregate execution still has scalar DISTINCT enabled.
    #[must_use]
    pub(in crate::db::executor) fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.logical_plan.as_ref())
    }
}

///
/// PreparedScalarNumericOp
///
/// Non-generic numeric terminal operation resolved at the typed scalar
/// boundary. Execution branches only on this operation selector plus the
/// prepared strategy and no longer inspect `PreparedExecutionPlan<E>`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum PreparedScalarNumericOp {
    Sum,
    Avg,
}

impl PreparedScalarNumericOp {
    // Route one numeric-op-specific invariant through the shared query
    // executor error taxonomy without repeating the constructor at each call
    // site in this enum.
    fn invariant(message: impl Into<String>) -> InternalError {
        InternalError::query_executor_invariant(message)
    }

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

    // Build the canonical grouped DISTINCT numeric output mismatch invariant.
    pub(in crate::db::executor) fn grouped_distinct_output_type_mismatch(
        self,
        value: &Value,
    ) -> InternalError {
        Self::invariant(format!(
            "global {}(DISTINCT field) grouped output type mismatch: {value:?}",
            self.aggregate_name(),
        ))
    }
}

///
/// PreparedScalarNumericAggregateStrategy
///
/// Non-generic numeric aggregate-path strategy resolved during typed boundary
/// preparation. This enum covers only the direct aggregate family; grouped
/// global DISTINCT execution is modeled separately in the prepared payload.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum PreparedScalarNumericAggregateStrategy {
    Streaming,
    Materialized,
}

///
/// PreparedScalarNumericPayload
///
/// PreparedScalarNumericPayload selects the runtime family that will execute
/// one numeric scalar boundary. This keeps the direct aggregate path and the
/// grouped global DISTINCT path explicit instead of overloading one strategy
/// enum with both concerns.
///

pub(in crate::db::executor) enum PreparedScalarNumericPayload<'ctx> {
    Aggregate {
        strategy: PreparedScalarNumericAggregateStrategy,
        window_provably_empty: bool,
        prepared: Box<PreparedAggregateStreamingInputs<'ctx>>,
    },
    GlobalDistinct {
        route: Box<GroupedRouteStage>,
    },
}

///
/// PreparedScalarNumericBoundary
///
/// PreparedScalarNumericBoundary is the non-generic numeric scalar contract
/// derived once from a typed plan and request.
/// It contains resolved field metadata, the numeric operation, and the
/// selected runtime family under one prepared boundary object.
///

pub(in crate::db::executor) struct PreparedScalarNumericBoundary<'ctx> {
    pub(in crate::db::executor) target_field_name: String,
    pub(in crate::db::executor) field_slot: FieldSlot,
    pub(in crate::db::executor) op: PreparedScalarNumericOp,
    pub(in crate::db::executor) payload: PreparedScalarNumericPayload<'ctx>,
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

impl PreparedScalarProjectionOp {
    // Route one prepared projection-op invariant through the shared query
    // executor error taxonomy without repeating the constructor in each
    // branch-specific helper below.
    fn invariant(message: impl Into<String>) -> InternalError {
        InternalError::query_executor_invariant(message)
    }

    // Build the canonical prepared-op invariant for missing covering DISTINCT strategy.
    pub(in crate::db::executor) fn covering_distinct_strategy_required(self) -> InternalError {
        let message = match self {
            Self::DistinctValues => {
                "covering DISTINCT projection requires prepared distinct strategy"
            }
            Self::CountDistinct => {
                "covering COUNT DISTINCT projection requires prepared distinct strategy"
            }
            Self::Values | Self::ValuesWithIds | Self::TerminalValue { .. } => {
                "covering DISTINCT strategy requirement is only valid for DISTINCT projection ops"
            }
        };

        Self::invariant(message)
    }

    // Build the canonical prepared-op invariant for unsupported constant covering values-with-ids.
    pub(in crate::db::executor) fn constant_covering_strategy_unsupported(self) -> InternalError {
        let message = match self {
            Self::ValuesWithIds => {
                "values-with-ids projection cannot execute constant covering strategy"
            }
            Self::Values
            | Self::DistinctValues
            | Self::CountDistinct
            | Self::TerminalValue { .. } => {
                "constant covering projection rejection is only valid for values-with-ids"
            }
        };

        Self::invariant(message)
    }

    // Build the canonical prepared-op invariant for terminal-value late materialization.
    pub(in crate::db::executor) fn materialized_branch_unreachable(self) -> InternalError {
        let message = match self {
            Self::TerminalValue { .. } => {
                "terminal value projection materialized branch must execute before row materialization"
            }
            Self::Values | Self::DistinctValues | Self::CountDistinct | Self::ValuesWithIds => {
                "materialized branch terminal-value invariant is only valid for terminal-value projection ops"
            }
        };

        Self::invariant(message)
    }

    // Validate that one terminal-value projection op only carries FIRST/LAST kinds.
    pub(in crate::db::executor) fn validate_terminal_value_kind(self) -> Result<(), InternalError> {
        match self {
            Self::TerminalValue { terminal_kind }
                if !matches!(terminal_kind, AggregateKind::First | AggregateKind::Last) =>
            {
                Err(Self::invariant(
                    "terminal value projection requires FIRST/LAST aggregate kind",
                ))
            }
            Self::Values
            | Self::DistinctValues
            | Self::CountDistinct
            | Self::ValuesWithIds
            | Self::TerminalValue { .. } => Ok(()),
        }
    }
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
/// PreparedScalarProjectionBoundary is the canonical prepared scalar
/// projection contract derived once at the typed boundary.
/// It captures resolved field metadata, the selected projection strategy, and
/// the prepared aggregate streaming inputs under one execution object.
///

pub(in crate::db::executor) struct PreparedScalarProjectionBoundary<'ctx> {
    pub(in crate::db::executor) target_field_name: String,
    pub(in crate::db::executor) field_slot: FieldSlot,
    pub(in crate::db::executor) op: PreparedScalarProjectionOp,
    pub(in crate::db::executor) strategy: PreparedScalarProjectionStrategy,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx>,
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
        field_slot: FieldSlot,
    },
}

impl PreparedScalarTerminalOp {
    // Route one prepared terminal-op invariant through the shared query
    // executor error taxonomy without rebuilding the constructor in each
    // validation branch.
    fn invariant(message: impl Into<String>) -> InternalError {
        InternalError::query_executor_invariant(message)
    }

    /// Return the aggregate kind represented by this prepared scalar terminal.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_kind(&self) -> AggregateKind {
        match self {
            Self::Count => AggregateKind::Count,
            Self::Exists => AggregateKind::Exists,
            Self::IdTerminal { kind } | Self::IdBySlot { kind, .. } => *kind,
        }
    }

    /// Narrow this prepared scalar terminal op onto the supported scalar reducer family.
    pub(in crate::db::executor) fn scalar_terminal_kind(
        &self,
    ) -> Result<ScalarTerminalKind, InternalError> {
        ScalarTerminalKind::try_from_aggregate_kind(self.aggregate_kind())
    }

    // Validate that the prepared terminal op can execute through the shared
    // kernel aggregate request surface.
    pub(in crate::db::executor) fn validate_kernel_request_kind(
        &self,
    ) -> Result<(), InternalError> {
        match self {
            Self::Count
            | Self::Exists
            | Self::IdTerminal {
                kind:
                    AggregateKind::Min | AggregateKind::Max | AggregateKind::First | AggregateKind::Last,
            }
            | Self::IdBySlot {
                kind: AggregateKind::Min | AggregateKind::Max,
                ..
            } => Ok(()),
            Self::IdTerminal { .. } => Err(Self::invariant(
                "id terminal aggregate request requires MIN/MAX/FIRST/LAST kind",
            )),
            Self::IdBySlot { .. } => Err(Self::invariant(
                "id-by-slot aggregate request requires MIN/MAX kind",
            )),
        }
    }
}

///
/// PreparedScalarTerminalStrategy
///
/// Non-generic scalar terminal execution strategy selected during typed
/// boundary preparation.
/// Runtime execution matches this enum directly instead of re-reading fast-path
/// eligibility from `PreparedExecutionPlan<E>`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum PreparedScalarTerminalStrategy {
    KernelAggregate,
    CountPrimaryKeyCardinality,
    ExistingRows { direction: Direction },
}

///
/// PreparedScalarTerminalBoundary
///
/// PreparedScalarTerminalBoundary is the canonical prepared scalar terminal
/// contract derived once at the typed boundary for COUNT, EXISTS, and id terminals.
/// It carries the resolved operation, selected execution strategy, and
/// prepared aggregate streaming inputs under one execution object.
///

pub(in crate::db::executor) struct PreparedScalarTerminalBoundary<'ctx> {
    pub(in crate::db::executor) op: PreparedScalarTerminalOp,
    pub(in crate::db::executor) strategy: PreparedScalarTerminalStrategy,
    pub(in crate::db::executor) window_provably_empty: bool,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx>,
}

///
/// PreparedOrderSensitiveTerminalOp
///
/// PreparedOrderSensitiveTerminalOp carries the structural order-sensitive
/// terminal contract after the typed boundary resolves the request family.
/// It keeps response-order terminals (`first`/`last`) separate from
/// field-ordered terminals (`nth_by`/`median_by`/`min_max_by`) without
/// widening the COUNT/EXISTS/id-terminal boundary again.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum PreparedOrderSensitiveTerminalOp {
    ResponseOrder {
        kind: AggregateKind,
    },
    FieldOrder {
        target_field_name: String,
        field_slot: FieldSlot,
        op: PreparedFieldOrderSensitiveTerminalOp,
    },
}

///
/// PreparedOrderSensitiveTerminalBoundary
///
/// PreparedOrderSensitiveTerminalBoundary is the canonical prepared contract
/// for order-sensitive scalar terminals.
/// It carries the selected order-sensitive operation plus prepared aggregate
/// streaming inputs under one execution object.
///

pub(in crate::db::executor) struct PreparedOrderSensitiveTerminalBoundary<'ctx> {
    pub(in crate::db::executor) op: PreparedOrderSensitiveTerminalOp,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx>,
}

///
/// PreparedFieldOrderSensitiveTerminalOp
///
/// Non-generic field-ordered terminal operation resolved at the typed
/// boundary before execution begins.
/// These terminals require ranking/extrema-specific execution semantics over a
/// planner-resolved orderable field.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum PreparedFieldOrderSensitiveTerminalOp {
    Nth { nth: usize },
    Median,
    MinMax,
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
