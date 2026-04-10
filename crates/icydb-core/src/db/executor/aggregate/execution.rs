//! Module: executor::aggregate::execution
//! Responsibility: aggregate execution descriptor/input payload contracts.
//! Does not own: aggregate execution branching logic.
//! Boundary: shared immutable payloads between aggregate orchestration helpers.

use crate::{
    db::{
        access::AccessPathKind,
        direction::Direction,
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation, LoweredIndexPrefixSpec,
            LoweredIndexRangeSpec, StoreResolver,
            aggregate::{
                AggregateKind, field::FieldSlot, projection::ScalarProjectionBoundaryRequest,
            },
            pipeline::contracts::GroupedRouteStage,
            route::AggregateRouteShape,
            traversal::row_read_consistency_for_plan,
        },
        index::IndexPredicateProgram,
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, CoveringProjectionContext, ExecutionOrderContract, OrderDirection,
            OrderSpec, PageSpec,
        },
        registry::StoreHandle,
    },
    error::InternalError,
    value::Value,
};

///
/// AggregateFastPathInputs
///
/// Aggregate fast-path execution inputs bundled for one dispatch entry.
/// Keeps branch routing parameters aligned between aggregate path helpers.
///

pub(in crate::db::executor) struct AggregateFastPathInputs<'exec> {
    pub(in crate::db::executor) logical_plan: &'exec AccessPlannedQuery,
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) store: StoreHandle,
    pub(in crate::db::executor) route_plan: &'exec ExecutionPlan,
    pub(in crate::db::executor) index_prefix_specs: &'exec [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'exec [LoweredIndexRangeSpec],
    pub(in crate::db::executor) index_predicate_program: Option<&'exec IndexPredicateProgram>,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) kind: super::AggregateKind,
    pub(in crate::db::executor) fold_mode: super::AggregateFoldMode,
}

impl AggregateFastPathInputs<'_> {
    /// Return row-read missing-row policy for this aggregate fast-path attempt.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.logical_plan)
    }
}

///
/// AggregateExecutionDescriptor
///
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
/// payload after the typed boundary has consumed `ExecutablePlan<E>`.
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
/// state after `ExecutablePlan` is consumed into logical plan form.
///

pub(in crate::db::executor) struct PreparedAggregateStreamingInputs<'ctx> {
    pub(in crate::db::executor) store_resolver: StoreResolver<'ctx>,
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) store: StoreHandle,
    pub(in crate::db::executor) logical_plan: AccessPlannedQuery,
    pub(in crate::db::executor) execution_preparation: ExecutionPreparation,
    pub(in crate::db::executor) index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    pub(in crate::db::executor) index_range_specs: Vec<LoweredIndexRangeSpec>,
}

///
/// PreparedAggregateStreamingInputsCore
///
/// PreparedAggregateStreamingInputsCore is the generic-free aggregate runtime
/// payload consumed by structural aggregate execution families.
/// It keeps only model/store/access authority plus normalized planner inputs,
/// so execution kernels no longer need to carry `Context<E>` when they only
/// operate on structural rows, keys, and slots.
///

pub(in crate::db::executor) struct PreparedAggregateStreamingInputsCore {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) store: StoreHandle,
    pub(in crate::db::executor) logical_plan: AccessPlannedQuery,
    pub(in crate::db::executor) execution_preparation: ExecutionPreparation,
    pub(in crate::db::executor) index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    pub(in crate::db::executor) index_range_specs: Vec<LoweredIndexRangeSpec>,
}

impl PreparedAggregateStreamingInputs<'_> {
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

    /// Return whether prepared aggregate execution still carries one logical
    /// predicate contract.
    ///
    /// Projection/rank terminals keep this broader gate instead of the
    /// narrower residual-only view because their scan-budget and effective
    /// window contracts still follow the filtered query surface, even when the
    /// chosen access path proves the predicate exactly.
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

    /// Consume typed aggregate streaming inputs into the generic-free runtime core.
    #[must_use]
    pub(in crate::db::executor) fn into_core(self) -> PreparedAggregateStreamingInputsCore {
        PreparedAggregateStreamingInputsCore {
            authority: self.authority,
            store: self.store,
            logical_plan: self.logical_plan,
            execution_preparation: self.execution_preparation,
            index_prefix_specs: self.index_prefix_specs,
            index_range_specs: self.index_range_specs,
        }
    }
}

impl PreparedAggregateStreamingInputsCore {
    /// Borrow scalar ORDER BY semantics for prepared aggregate execution.
    #[must_use]
    pub(in crate::db::executor) const fn order_spec(&self) -> Option<&OrderSpec> {
        self.logical_plan.scalar_plan().order.as_ref()
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

    // Build the canonical numeric AVG finalization invariant.
    pub(in crate::db::executor) fn avg_divisor_conversion_invariant(self) -> InternalError {
        let message = match self {
            Self::Avg => "numeric field AVG divisor conversion overflowed decimal bounds",
            Self::Sum => "AVG divisor conversion invariant is only valid for AVG numeric ops",
        };

        InternalError::query_executor_invariant(message)
    }

    // Build the canonical grouped DISTINCT numeric output mismatch invariant.
    pub(in crate::db::executor) fn grouped_distinct_output_type_mismatch(
        self,
        value: &Value,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
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
/// PreparedScalarNumericBoundary
///
/// PreparedScalarNumericBoundary is the non-generic numeric scalar contract
/// derived once from a typed plan and request.
/// It contains only resolved field metadata and the numeric operation.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct PreparedScalarNumericBoundary {
    pub(in crate::db::executor) target_field_name: String,
    pub(in crate::db::executor) field_slot: FieldSlot,
    pub(in crate::db::executor) op: PreparedScalarNumericOp,
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
        prepared: Box<PreparedAggregateStreamingInputs<'ctx>>,
    },
    GlobalDistinct {
        route: Box<GroupedRouteStage>,
    },
}

///
/// PreparedScalarNumericExecutionState
///
/// PreparedScalarNumericExecutionState pairs one non-generic numeric boundary
/// contract with the runtime payload needed to execute it.
/// The boundary itself is plan-free; only the payload determines which
/// execution family runs.
///

pub(in crate::db::executor) struct PreparedScalarNumericExecutionState<'ctx> {
    pub(in crate::db::executor) boundary: PreparedScalarNumericBoundary,
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
    CountNonNull,
    CountDistinct,
    ValuesWithIds,
    TerminalValue { terminal_kind: AggregateKind },
}

impl PreparedScalarProjectionOp {
    // Build the canonical prepared-op invariant for missing covering DISTINCT strategy.
    pub(in crate::db::executor) fn covering_distinct_strategy_required(self) -> InternalError {
        let message = match self {
            Self::DistinctValues => {
                "covering DISTINCT projection requires prepared distinct strategy"
            }
            Self::CountDistinct => {
                "covering COUNT DISTINCT projection requires prepared distinct strategy"
            }
            Self::Values
            | Self::CountNonNull
            | Self::ValuesWithIds
            | Self::TerminalValue { .. } => {
                "covering DISTINCT strategy requirement is only valid for DISTINCT projection ops"
            }
        };

        InternalError::query_executor_invariant(message)
    }

    // Build the canonical prepared-op invariant for unsupported constant covering values-with-ids.
    pub(in crate::db::executor) fn constant_covering_strategy_unsupported(self) -> InternalError {
        let message = match self {
            Self::ValuesWithIds => {
                "values-with-ids projection cannot execute constant covering strategy"
            }
            Self::Values
            | Self::DistinctValues
            | Self::CountNonNull
            | Self::CountDistinct
            | Self::TerminalValue { .. } => {
                "constant covering projection rejection is only valid for values-with-ids"
            }
        };

        InternalError::query_executor_invariant(message)
    }

    // Build the canonical prepared-op invariant for terminal-value late materialization.
    pub(in crate::db::executor) fn materialized_branch_unreachable(self) -> InternalError {
        let message = match self {
            Self::TerminalValue { .. } => {
                "terminal value projection materialized branch must execute before row materialization"
            }
            Self::Values
            | Self::DistinctValues
            | Self::CountNonNull
            | Self::CountDistinct
            | Self::ValuesWithIds => {
                "materialized branch terminal-value invariant is only valid for terminal-value projection ops"
            }
        };

        InternalError::query_executor_invariant(message)
    }

    // Validate that one terminal-value projection op only carries FIRST/LAST kinds.
    pub(in crate::db::executor) fn validate_terminal_value_kind(self) -> Result<(), InternalError> {
        match self {
            Self::TerminalValue { terminal_kind }
                if !matches!(terminal_kind, AggregateKind::First | AggregateKind::Last) =>
            {
                Err(InternalError::query_executor_invariant(
                    "terminal value projection requires FIRST/LAST aggregate kind",
                ))
            }
            Self::Values
            | Self::DistinctValues
            | Self::CountNonNull
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
    StreamingCountNonNull {
        direction: Direction,
    },
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
/// It captures the resolved field slot and operation kind without retaining
/// `ExecutablePlan<E>`.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct PreparedScalarProjectionBoundary {
    pub(in crate::db::executor) target_field_name: String,
    pub(in crate::db::executor) field_slot: FieldSlot,
    pub(in crate::db::executor) op: PreparedScalarProjectionOp,
}

///
/// PreparedScalarProjectionExecutionState
///
/// PreparedScalarProjectionExecutionState combines the non-generic prepared
/// projection contract with the runtime payload required to execute it.
/// The executor matches the prepared strategy directly while treating the
/// projection boundary as the stable contract for downstream helpers.
///

pub(in crate::db::executor) struct PreparedScalarProjectionExecutionState<'ctx> {
    pub(in crate::db::executor) boundary: PreparedScalarProjectionBoundary,
    pub(in crate::db::executor) strategy: PreparedScalarProjectionStrategy,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx>,
}

impl PreparedAggregateStreamingInputs<'_> {
    /// Return whether a scalar projection can preserve one direct streaming
    /// existing-row fold without materializing the full response page.
    #[must_use]
    pub(in crate::db::executor) fn supports_streaming_existing_row_field_fold(&self) -> bool {
        if !self.has_no_predicate_or_distinct() {
            return false;
        }

        let access_strategy = self.logical_plan.access.resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return false;
        };
        let path_kind = path.capabilities().kind();
        if !Self::streaming_existing_row_field_path_safe(path_kind) {
            return false;
        }

        self.streaming_existing_row_field_page_window_safe(path_kind)
    }

    /// Return the canonical primary scan direction for one streaming
    /// existing-row field fold.
    #[must_use]
    pub(in crate::db::executor) fn streaming_existing_row_field_direction(&self) -> Direction {
        ExecutionOrderContract::from_plan(false, self.order_spec()).primary_scan_direction()
    }

    /// Return whether the resolved access path can preserve one direct
    /// existing-row field fold without duplication.
    #[must_use]
    const fn streaming_existing_row_field_path_safe(path_kind: AccessPathKind) -> bool {
        path_kind.supports_streaming_numeric_fold()
    }

    /// Return whether the effective page window preserves one direct
    /// existing-row field fold under primary-key order constraints.
    #[must_use]
    fn streaming_existing_row_field_page_window_safe(&self, path_kind: AccessPathKind) -> bool {
        if self.page_spec().is_none() {
            return true;
        }
        let Some(_order) = self.order_spec() else {
            return false;
        };
        if self
            .explicit_primary_key_order_direction(self.authority.primary_key_name())
            .is_none()
        {
            return false;
        }

        path_kind.supports_streaming_numeric_fold_for_paged_primary_key_window()
    }
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
    /// Return the aggregate kind represented by this prepared scalar terminal.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_kind(&self) -> AggregateKind {
        match self {
            Self::Count => AggregateKind::Count,
            Self::Exists => AggregateKind::Exists,
            Self::IdTerminal { kind } | Self::IdBySlot { kind, .. } => *kind,
        }
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
            Self::IdTerminal { .. } => Err(InternalError::query_executor_invariant(
                "id terminal aggregate request requires MIN/MAX/FIRST/LAST kind",
            )),
            Self::IdBySlot { .. } => Err(InternalError::query_executor_invariant(
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
/// eligibility from `ExecutablePlan<E>`.
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

pub(in crate::db::executor) struct PreparedScalarTerminalExecutionState<'ctx> {
    pub(in crate::db::executor) boundary: PreparedScalarTerminalBoundary,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx>,
}

///
/// PreparedOrderSensitiveTerminalBoundary
///
/// PreparedOrderSensitiveTerminalBoundary is the plan-free contract for
/// order-sensitive scalar terminals.
/// It keeps response-order terminals (`first`/`last`) separate from
/// field-ordered terminals (`nth_by`/`median_by`/`min_max_by`) without
/// widening the COUNT/EXISTS/id-terminal boundary again.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum PreparedOrderSensitiveTerminalBoundary {
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

///
/// PreparedOrderSensitiveTerminalExecutionState
///
/// PreparedOrderSensitiveTerminalExecutionState pairs one prepared
/// order-sensitive terminal boundary with the runtime payload needed to
/// execute it.
/// Runtime execution consumes this state directly instead of rebuilding the
/// same slot-resolution and prepared aggregate inputs ad hoc.
///

pub(in crate::db::executor) struct PreparedOrderSensitiveTerminalExecutionState<'ctx> {
    pub(in crate::db::executor) boundary: PreparedOrderSensitiveTerminalBoundary,
    pub(in crate::db::executor) prepared: PreparedAggregateStreamingInputs<'ctx>,
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
            ScalarProjectionBoundaryRequest::CountNonNull => Self::CountNonNull,
            ScalarProjectionBoundaryRequest::CountDistinct => Self::CountDistinct,
            ScalarProjectionBoundaryRequest::ValuesWithIds => Self::ValuesWithIds,
            ScalarProjectionBoundaryRequest::TerminalValue { terminal_kind } => {
                Self::TerminalValue { terminal_kind }
            }
        }
    }
}
