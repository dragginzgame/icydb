//! Module: db::executor::aggregate::contracts::grouped::engine
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::contracts::grouped::engine.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        contracts::canonical_value_compare,
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::contracts::{
                error::GroupError,
                spec::{AggregateKind, AggregateOutput},
                state::{
                    AggregateState, AggregateStateFactory, FoldControl, TerminalAggregateState,
                },
            },
            group::{
                CanonicalKey, GroupKey, KeyCanonicalError, StableHash, canonical_group_key_equals,
            },
        },
        numeric::{add_decimal_terms, average_decimal_terms},
    },
    error::InternalError,
    traits::EntityKind,
    types::Decimal,
    value::Value,
};
use std::collections::BTreeMap;

use crate::db::executor::aggregate::contracts::grouped::context::ExecutionContext;

///
/// GroupedAggregateOutput
///
/// GroupedAggregateOutput carries one finalized grouped terminal row:
/// one canonical group key paired with one aggregate terminal output.
/// Finalized rows are emitted in deterministic canonical order.
///

pub(in crate::db::executor) struct GroupedAggregateOutput<E: EntityKind> {
    group_key: GroupKey,
    output: AggregateOutput<E>,
}

impl<E: EntityKind> GroupedAggregateOutput<E> {
    #[must_use]
    pub(in crate::db::executor) const fn group_key(&self) -> &GroupKey {
        &self.group_key
    }

    #[must_use]
    pub(in crate::db::executor) const fn output(&self) -> &AggregateOutput<E> {
        &self.output
    }
}

///
/// GroupedAggregateStateSlot
///
/// GroupedAggregateStateSlot stores one canonical group key with one
/// group-local terminal aggregate state machine.
/// Slots remain bucket-local and are finalized deterministically.
///

pub(in crate::db::executor::aggregate::contracts::grouped) struct GroupedAggregateStateSlot<
    E: EntityKind,
> {
    group_key: GroupKey,
    state: TerminalAggregateState<E>,
}

impl<E: EntityKind> GroupedAggregateStateSlot<E> {
    #[must_use]
    const fn group_key(&self) -> &GroupKey {
        &self.group_key
    }
}

///
/// GroupedAggregateState
///
/// GroupedAggregateState stores per-group aggregate state machines keyed by
/// canonical group keys and stable-hash buckets.
/// Group-local states are built by `AggregateStateFactory` and finalized in a
/// deterministic order independent of insertion order.
///

pub(in crate::db::executor) struct GroupedAggregateState<E: EntityKind> {
    kind: AggregateKind,
    direction: Direction,
    distinct: bool,
    max_distinct_values_per_group: u64,
    groups: BTreeMap<StableHash, Vec<GroupedAggregateStateSlot<E>>>,
}

impl<E: EntityKind> GroupedAggregateState<E> {
    /// Build one empty grouped aggregate state container.
    #[must_use]
    pub(in crate::db::executor::aggregate) const fn new(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        max_distinct_values_per_group: u64,
    ) -> Self {
        Self {
            kind,
            direction,
            distinct,
            max_distinct_values_per_group,
            groups: BTreeMap::new(),
        }
    }

    /// Apply one `(group_key, data_key)` row into grouped aggregate state.
    #[cfg(test)]
    pub(in crate::db::executor::aggregate) fn apply(
        &mut self,
        group_key: GroupKey,
        data_key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        self.apply_borrowed(&group_key, data_key, execution_context)
    }

    // Apply one `(group_key, data_key)` row into grouped aggregate state using
    // a borrowed grouped key to avoid hot-path clone churn at ingest callsites.
    fn apply_borrowed(
        &mut self,
        group_key: &GroupKey,
        data_key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        // Phase 1: resolve updates for existing buckets/groups.
        let hash = group_key.hash();
        if let Some(bucket) = self.groups.get_mut(&hash) {
            if let Some(slot) = bucket
                .iter_mut()
                .find(|slot| canonical_group_key_equals(slot.group_key(), group_key))
            {
                return slot.state.apply_grouped(data_key, execution_context);
            }

            // New group in an existing bucket.
            let mut state = AggregateStateFactory::create_terminal(
                self.kind,
                self.direction,
                self.distinct,
                self.max_distinct_values_per_group,
            );
            let fold_control = state.apply_grouped(data_key, execution_context)?;
            execution_context.record_new_group::<E>(
                group_key,
                false,
                bucket.len(),
                bucket.capacity(),
            )?;
            bucket.push(GroupedAggregateStateSlot {
                group_key: group_key.clone(),
                state,
            });

            return Ok(fold_control);
        }

        // Phase 2: create a new bucket + group when hash was unseen.
        let mut state = AggregateStateFactory::create_terminal(
            self.kind,
            self.direction,
            self.distinct,
            self.max_distinct_values_per_group,
        );
        let fold_control = state.apply_grouped(data_key, execution_context)?;
        execution_context.record_new_group::<E>(group_key, true, 0, 0)?;
        self.groups.insert(
            hash,
            vec![GroupedAggregateStateSlot {
                group_key: group_key.clone(),
                state,
            }],
        );

        Ok(fold_control)
    }

    /// Return the current number of grouped keys tracked by this state.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) fn group_count(&self) -> usize {
        self.groups
            .values()
            .fold(0usize, |count, bucket| count.saturating_add(bucket.len()))
    }

    /// Finalize all groups into deterministic grouped aggregate outputs.
    #[must_use]
    pub(in crate::db::executor::aggregate) fn finalize(self) -> Vec<GroupedAggregateOutput<E>> {
        let expected_output_count = self
            .groups
            .values()
            .fold(0usize, |count, bucket| count.saturating_add(bucket.len()));
        let mut out = Vec::with_capacity(expected_output_count);

        // Phase 1: walk stable-hash buckets in deterministic key order.
        for (_, mut bucket) in self.groups {
            // Phase 2: break hash-collision ties by canonical group-key value.
            bucket.sort_by(|left, right| {
                canonical_value_compare(
                    left.group_key().canonical_value(),
                    right.group_key().canonical_value(),
                )
            });

            // Phase 3: finalize states in deterministic bucket order.
            for slot in bucket {
                out.push(GroupedAggregateOutput {
                    group_key: slot.group_key,
                    output: slot.state.finalize(),
                });
            }
        }
        debug_assert_eq!(
            out.len(),
            expected_output_count,
            "grouped finalize output cardinality must match tracked grouped state slots",
        );

        out
    }
}

///
/// GlobalDistinctFieldState
///
/// GlobalDistinctFieldState is the canonical reducer state for grouped global
/// DISTINCT field terminals (`COUNT`/`SUM`/`AVG`) after distinct-value admission.
///

pub(in crate::db::executor) struct GlobalDistinctFieldState {
    distinct_count: u64,
    numeric_sum: Decimal,
    saw_numeric_value: bool,
    apply_dispatch: GlobalDistinctApplyDispatch,
    finalize_dispatch: GlobalDistinctFinalizeDispatch,
}

type GlobalDistinctApplyDispatch =
    fn(&mut GlobalDistinctFieldState, Option<Decimal>) -> Result<FoldControl, GroupError>;

type GlobalDistinctFinalizeDispatch =
    fn(&GlobalDistinctFieldState) -> Result<GlobalDistinctFinalizeValue, InternalError>;

enum GlobalDistinctFinalizeValue {
    Count(u32),
    Sum(Option<Decimal>),
}

impl GlobalDistinctFieldState {
    // Build one grouped global DISTINCT reducer state for COUNT/SUM/AVG.
    fn new(kind: AggregateKind) -> Result<Self, GroupError> {
        let (apply_dispatch, finalize_dispatch): (
            GlobalDistinctApplyDispatch,
            GlobalDistinctFinalizeDispatch,
        ) = match kind {
            AggregateKind::Count => (Self::apply_count_dispatch, Self::finalize_count_dispatch),
            AggregateKind::Sum => (Self::apply_numeric_dispatch, Self::finalize_sum_dispatch),
            AggregateKind::Avg => (Self::apply_numeric_dispatch, Self::finalize_avg_dispatch),
            AggregateKind::Exists
            | AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::First
            | AggregateKind::Last => {
                return Err(GroupError::Internal(
                    crate::db::error::query_executor_invariant(
                        "grouped global DISTINCT field reducer requires COUNT/SUM/AVG terminal",
                    ),
                ));
            }
        };

        Ok(Self {
            distinct_count: 0,
            numeric_sum: Decimal::ZERO,
            saw_numeric_value: false,
            apply_dispatch,
            finalize_dispatch,
        })
    }

    // Apply one admitted grouped global DISTINCT field value.
    fn apply_distinct_value(
        &mut self,
        numeric_value: Option<Decimal>,
    ) -> Result<FoldControl, GroupError> {
        (self.apply_dispatch)(self, numeric_value)
    }

    // Finalize grouped global DISTINCT field reducer output into aggregate output.
    fn finalize<E: EntityKind>(self) -> Result<AggregateOutput<E>, InternalError> {
        match (self.finalize_dispatch)(&self)? {
            GlobalDistinctFinalizeValue::Count(count) => Ok(AggregateOutput::Count(count)),
            GlobalDistinctFinalizeValue::Sum(sum) => Ok(AggregateOutput::Sum(sum)),
        }
    }

    const fn apply_count_dispatch(
        state: &mut GlobalDistinctFieldState,
        _numeric_value: Option<Decimal>,
    ) -> Result<FoldControl, GroupError> {
        state.distinct_count = state.distinct_count.saturating_add(1);

        Ok(FoldControl::Continue)
    }

    fn apply_numeric_dispatch(
        state: &mut GlobalDistinctFieldState,
        numeric_value: Option<Decimal>,
    ) -> Result<FoldControl, GroupError> {
        state.distinct_count = state.distinct_count.saturating_add(1);
        let Some(numeric_value) = numeric_value else {
            return Err(GroupError::Internal(
                crate::db::error::query_executor_invariant(
                    "grouped global DISTINCT SUM/AVG reducer requires numeric ingest payload",
                ),
            ));
        };
        state.numeric_sum = add_decimal_terms(state.numeric_sum, numeric_value);
        state.saw_numeric_value = true;

        Ok(FoldControl::Continue)
    }

    fn finalize_count_dispatch(
        state: &GlobalDistinctFieldState,
    ) -> Result<GlobalDistinctFinalizeValue, InternalError> {
        Ok(GlobalDistinctFinalizeValue::Count(
            u32::try_from(state.distinct_count).unwrap_or(u32::MAX),
        ))
    }

    fn finalize_sum_dispatch(
        state: &GlobalDistinctFieldState,
    ) -> Result<GlobalDistinctFinalizeValue, InternalError> {
        Ok(GlobalDistinctFinalizeValue::Sum(
            state.saw_numeric_value.then_some(state.numeric_sum),
        ))
    }

    fn finalize_avg_dispatch(
        state: &GlobalDistinctFieldState,
    ) -> Result<GlobalDistinctFinalizeValue, InternalError> {
        if !state.saw_numeric_value || state.distinct_count == 0 {
            return Ok(GlobalDistinctFinalizeValue::Sum(None));
        }
        let Some(avg) = average_decimal_terms(state.numeric_sum, state.distinct_count) else {
            return Err(crate::db::error::query_executor_invariant(
                "global grouped AVG(DISTINCT field) divisor conversion overflowed decimal bounds",
            ));
        };

        Ok(GlobalDistinctFinalizeValue::Sum(Some(avg)))
    }
}

///
/// AggregateEngine
///
/// Canonical aggregate reducer engine shared by scalar and grouped execution
/// spines. This keeps ingest/finalize semantics centralized across both modes.
///

pub(in crate::db::executor) enum AggregateEngine<E: EntityKind> {
    Scalar(TerminalAggregateState<E>),
    Grouped(GroupedAggregateState<E>),
    GlobalDistinctField(GlobalDistinctFieldState),
}

///
/// AggregateExecutionMode
///
/// AggregateExecutionMode classifies aggregate reducer execution into scalar
/// or grouped ingestion modes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateExecutionMode {
    Scalar,
    Grouped,
    GlobalDistinctField,
}

///
/// AggregateExecutionSpec
///
/// AggregateExecutionSpec captures lane-specific runtime context for one
/// aggregate ingest adapter instance.
/// Mode is selected once at construction and reused across all ingested keys.
///

pub(in crate::db::executor) struct AggregateExecutionSpec<'a> {
    mode: AggregateExecutionMode,
    grouped_execution_context: Option<&'a mut ExecutionContext>,
}

impl<'a> AggregateExecutionSpec<'a> {
    /// Build one scalar aggregate execution spec.
    #[must_use]
    pub(in crate::db::executor) const fn scalar() -> Self {
        Self {
            mode: AggregateExecutionMode::Scalar,
            grouped_execution_context: None,
        }
    }

    /// Build one grouped aggregate execution spec.
    #[must_use]
    pub(in crate::db::executor) const fn grouped(
        execution_context: &'a mut ExecutionContext,
    ) -> Self {
        Self {
            mode: AggregateExecutionMode::Grouped,
            grouped_execution_context: Some(execution_context),
        }
    }

    /// Build one grouped global DISTINCT field aggregate execution spec.
    #[must_use]
    pub(in crate::db::executor) const fn global_distinct_field() -> Self {
        Self {
            mode: AggregateExecutionMode::GlobalDistinctField,
            grouped_execution_context: None,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn mode(&self) -> AggregateExecutionMode {
        self.mode
    }

    fn grouped_execution_context_mut(&mut self) -> Result<&mut ExecutionContext, GroupError> {
        self.grouped_execution_context
            .as_deref_mut()
            .ok_or_else(|| {
                GroupError::Internal(crate::db::error::query_executor_invariant(
                    "grouped aggregate ingest requires grouped execution context in execution spec",
                ))
            })
    }
}

///
/// AggregateIngestAdapter
///
/// AggregateIngestAdapter centralizes scalar/grouped reducer ingestion behind
/// one `ingest` authority so aggregate execution loops share one consumer API.
///

pub(in crate::db::executor) struct AggregateIngestAdapter<'a, E: EntityKind> {
    execution_spec: AggregateExecutionSpec<'a>,
    ingest_dispatch: AggregateIngestDispatch<E>,
    ingest_distinct_value_dispatch: AggregateDistinctValueIngestDispatch<E>,
}

type AggregateIngestDispatch<E> = for<'b> fn(
    &mut AggregateEngine<E>,
    &mut AggregateExecutionSpec<'b>,
    &DataKey,
    Option<&GroupKey>,
) -> Result<FoldControl, GroupError>;

type AggregateDistinctValueIngestDispatch<E> = for<'b> fn(
    &mut AggregateEngine<E>,
    &mut AggregateExecutionSpec<'b>,
    Option<Decimal>,
) -> Result<FoldControl, GroupError>;

impl<E: EntityKind> AggregateEngine<E> {
    fn mode_mismatch_error(mode: AggregateExecutionMode) -> GroupError {
        GroupError::Internal(crate::db::error::query_executor_invariant(match mode {
            AggregateExecutionMode::Scalar => {
                "scalar aggregate ingest reached grouped aggregate engine"
            }
            AggregateExecutionMode::Grouped => {
                "grouped aggregate ingest reached scalar aggregate engine"
            }
            AggregateExecutionMode::GlobalDistinctField => {
                "grouped global DISTINCT aggregate ingest reached non-global-distinct aggregate engine"
            }
        }))
    }
}

impl<'a, E: EntityKind> AggregateIngestAdapter<'a, E> {
    /// Construct one aggregate ingest adapter from one execution descriptor.
    pub(in crate::db::executor) fn from_execution_spec(
        execution_spec: AggregateExecutionSpec<'a>,
    ) -> Self {
        let mode = execution_spec.mode();
        let ingest_dispatch = match mode {
            AggregateExecutionMode::Scalar => Self::ingest_mode_scalar_dispatch,
            AggregateExecutionMode::Grouped => Self::ingest_mode_group_dispatch,
            AggregateExecutionMode::GlobalDistinctField => {
                Self::ingest_key_unsupported_for_global_distinct_dispatch
            }
        };
        let ingest_distinct_value_dispatch = match mode {
            AggregateExecutionMode::Scalar | AggregateExecutionMode::Grouped => {
                Self::ingest_distinct_value_unsupported_dispatch
            }
            AggregateExecutionMode::GlobalDistinctField => {
                Self::ingest_global_distinct_value_dispatch
            }
        };

        Self {
            execution_spec,
            ingest_dispatch,
            ingest_distinct_value_dispatch,
        }
    }

    // Scalar ingest dispatch implementation.
    fn ingest_mode_scalar_dispatch(
        engine: &mut AggregateEngine<E>,
        _execution_spec: &mut AggregateExecutionSpec<'_>,
        data_key: &DataKey,
        group_key: Option<&GroupKey>,
    ) -> Result<FoldControl, GroupError> {
        if group_key.is_some() {
            return Err(GroupError::Internal(
                crate::db::error::query_executor_invariant(
                    "scalar aggregate ingest must not receive grouped group-key payload",
                ),
            ));
        }

        match engine {
            AggregateEngine::Scalar(state) => state.apply(data_key).map_err(GroupError::from),
            AggregateEngine::Grouped(_) | AggregateEngine::GlobalDistinctField(_) => Err(
                AggregateEngine::<E>::mode_mismatch_error(AggregateExecutionMode::Scalar),
            ),
        }
    }

    // Grouped ingest dispatch implementation.
    fn ingest_mode_group_dispatch(
        engine: &mut AggregateEngine<E>,
        execution_spec: &mut AggregateExecutionSpec<'_>,
        data_key: &DataKey,
        group_key: Option<&GroupKey>,
    ) -> Result<FoldControl, GroupError> {
        let Some(group_key) = group_key else {
            return Err(GroupError::Internal(
                crate::db::error::query_executor_invariant(
                    "grouped aggregate ingest requires grouped group-key payload",
                ),
            ));
        };
        let execution_context = execution_spec.grouped_execution_context_mut()?;

        match engine {
            AggregateEngine::Grouped(state) => {
                state.apply_borrowed(group_key, data_key, execution_context)
            }
            AggregateEngine::Scalar(_) | AggregateEngine::GlobalDistinctField(_) => Err(
                AggregateEngine::<E>::mode_mismatch_error(AggregateExecutionMode::Grouped),
            ),
        }
    }

    // Reject key-based ingest for grouped global DISTINCT field reducers.
    fn ingest_key_unsupported_for_global_distinct_dispatch(
        _engine: &mut AggregateEngine<E>,
        _execution_spec: &mut AggregateExecutionSpec<'_>,
        _data_key: &DataKey,
        _group_key: Option<&GroupKey>,
    ) -> Result<FoldControl, GroupError> {
        Err(GroupError::Internal(
            crate::db::error::query_executor_invariant(
                "grouped global DISTINCT field reducers require distinct-value ingest payloads",
            ),
        ))
    }

    // Reject distinct-value ingest for scalar/grouped key reducers.
    fn ingest_distinct_value_unsupported_dispatch(
        _engine: &mut AggregateEngine<E>,
        _execution_spec: &mut AggregateExecutionSpec<'_>,
        _numeric_value: Option<Decimal>,
    ) -> Result<FoldControl, GroupError> {
        Err(GroupError::Internal(
            crate::db::error::query_executor_invariant(
                "scalar/grouped key reducers do not support grouped global DISTINCT distinct-value ingest",
            ),
        ))
    }

    // Grouped global DISTINCT distinct-value ingest dispatch implementation.
    fn ingest_global_distinct_value_dispatch(
        engine: &mut AggregateEngine<E>,
        _execution_spec: &mut AggregateExecutionSpec<'_>,
        numeric_value: Option<Decimal>,
    ) -> Result<FoldControl, GroupError> {
        match engine {
            AggregateEngine::GlobalDistinctField(state) => {
                state.apply_distinct_value(numeric_value)
            }
            AggregateEngine::Scalar(_) | AggregateEngine::Grouped(_) => {
                Err(AggregateEngine::<E>::mode_mismatch_error(
                    AggregateExecutionMode::GlobalDistinctField,
                ))
            }
        }
    }

    /// Ingest one data key through one execution descriptor.
    pub(in crate::db::executor) fn ingest(
        &mut self,
        engine: &mut AggregateEngine<E>,
        data_key: &DataKey,
        group_key: Option<&GroupKey>,
    ) -> Result<FoldControl, GroupError> {
        (self.ingest_dispatch)(engine, &mut self.execution_spec, data_key, group_key)
    }

    /// Ingest one admitted grouped global DISTINCT field value.
    pub(in crate::db::executor) fn ingest_global_distinct_value(
        &mut self,
        engine: &mut AggregateEngine<E>,
        numeric_value: Option<Decimal>,
    ) -> Result<FoldControl, GroupError> {
        (self.ingest_distinct_value_dispatch)(engine, &mut self.execution_spec, numeric_value)
    }
}

///
/// AggregateFinalizeOutput
///
/// AggregateFinalizeOutput is the unified finalize payload emitted by the
/// aggregate finalize adapter for scalar and grouped execution modes.
///

pub(in crate::db::executor) enum AggregateFinalizeOutput<E: EntityKind> {
    Scalar(AggregateOutput<E>),
    Grouped(Vec<GroupedAggregateOutput<E>>),
}

impl<E: EntityKind> AggregateFinalizeOutput<E> {
    /// Project one scalar finalize payload and fail closed for grouped payloads.
    pub(in crate::db::executor) fn into_scalar(self) -> Result<AggregateOutput<E>, InternalError> {
        match self {
            Self::Scalar(output) => Ok(output),
            Self::Grouped(_) => Err(crate::db::error::query_executor_invariant(
                "scalar aggregate finalize expected scalar payload but received grouped payload",
            )),
        }
    }

    /// Project one grouped finalize payload and fail closed for scalar payloads.
    pub(in crate::db::executor) fn into_grouped(
        self,
    ) -> Result<Vec<GroupedAggregateOutput<E>>, InternalError> {
        match self {
            Self::Grouped(output) => Ok(output),
            Self::Scalar(_) => Err(crate::db::error::query_executor_invariant(
                "grouped aggregate finalize expected grouped payload but received scalar payload",
            )),
        }
    }
}

///
/// AggregateFinalizeAdapter
///
/// AggregateFinalizeAdapter centralizes scalar/grouped finalize dispatch
/// behind one adapter-owned `finalize` boundary.
///

pub(in crate::db::executor) struct AggregateFinalizeAdapter<E: EntityKind> {
    finalize_dispatch: AggregateFinalizeDispatch<E>,
}

type AggregateFinalizeDispatch<E> =
    fn(AggregateEngine<E>) -> Result<AggregateFinalizeOutput<E>, InternalError>;

impl<E: EntityKind> AggregateFinalizeAdapter<E> {
    /// Construct one finalize adapter from one execution mode descriptor.
    #[must_use]
    pub(in crate::db::executor) fn from_execution_mode(mode: AggregateExecutionMode) -> Self {
        let finalize_dispatch = match mode {
            AggregateExecutionMode::Scalar => Self::finalize_scalar_dispatch,
            AggregateExecutionMode::Grouped => Self::finalize_grouped_dispatch,
            AggregateExecutionMode::GlobalDistinctField => {
                Self::finalize_global_distinct_field_dispatch
            }
        };

        Self { finalize_dispatch }
    }

    // Finalize dispatch for scalar aggregate execution mode.
    fn finalize_scalar_dispatch(
        engine: AggregateEngine<E>,
    ) -> Result<AggregateFinalizeOutput<E>, InternalError> {
        match engine {
            AggregateEngine::Scalar(state) => Ok(AggregateFinalizeOutput::Scalar(state.finalize())),
            AggregateEngine::Grouped(_) | AggregateEngine::GlobalDistinctField(_) => {
                Err(crate::db::error::query_executor_invariant(
                    "scalar aggregate finalize reached grouped aggregate engine",
                ))
            }
        }
    }

    // Finalize dispatch for grouped aggregate execution mode.
    fn finalize_grouped_dispatch(
        engine: AggregateEngine<E>,
    ) -> Result<AggregateFinalizeOutput<E>, InternalError> {
        match engine {
            AggregateEngine::Grouped(state) => {
                Ok(AggregateFinalizeOutput::Grouped(state.finalize()))
            }
            AggregateEngine::Scalar(_) | AggregateEngine::GlobalDistinctField(_) => {
                Err(crate::db::error::query_executor_invariant(
                    "grouped aggregate finalize reached scalar aggregate engine",
                ))
            }
        }
    }

    // Finalize dispatch for grouped global DISTINCT field execution mode.
    fn finalize_global_distinct_field_dispatch(
        engine: AggregateEngine<E>,
    ) -> Result<AggregateFinalizeOutput<E>, InternalError> {
        match engine {
            AggregateEngine::GlobalDistinctField(state) => {
                let group_key = Value::List(Vec::new())
                    .canonical_key()
                    .map_err(KeyCanonicalError::into_internal_error)?;
                let output = state.finalize::<E>()?;

                Ok(AggregateFinalizeOutput::Grouped(vec![
                    GroupedAggregateOutput { group_key, output },
                ]))
            }
            AggregateEngine::Scalar(_) | AggregateEngine::Grouped(_) => {
                Err(crate::db::error::query_executor_invariant(
                    "grouped global DISTINCT aggregate finalize reached non-global-distinct aggregate engine",
                ))
            }
        }
    }

    /// Finalize one aggregate engine through this adapter.
    pub(in crate::db::executor) fn finalize(
        self,
        engine: AggregateEngine<E>,
    ) -> Result<AggregateFinalizeOutput<E>, InternalError> {
        (self.finalize_dispatch)(engine)
    }
}

// Execute one aggregate engine through one canonical ingest/finalize authority.
// The caller supplies loop/key ingestion behavior while this boundary owns:
// 1) mode selection from the execution spec
// 2) one ingest adapter construction
// 3) one finalize adapter construction
// 4) one finalize projection
pub(in crate::db::executor) fn execute_aggregate<'a, E: EntityKind>(
    mut engine: AggregateEngine<E>,
    execution_spec: AggregateExecutionSpec<'a>,
    ingest_all: &mut dyn FnMut(
        &mut AggregateIngestAdapter<'a, E>,
        &mut AggregateEngine<E>,
    ) -> Result<(), InternalError>,
) -> Result<AggregateFinalizeOutput<E>, InternalError> {
    let execution_mode = execution_spec.mode();
    let mut ingest_adapter = AggregateIngestAdapter::from_execution_spec(execution_spec);
    let finalize_adapter = AggregateFinalizeAdapter::from_execution_mode(execution_mode);
    ingest_all(&mut ingest_adapter, &mut engine)?;

    finalize_adapter.finalize(engine)
}

impl<E: EntityKind> AggregateEngine<E> {
    /// Build one scalar aggregate engine.
    #[must_use]
    pub(in crate::db::executor) const fn new_scalar(
        kind: AggregateKind,
        direction: Direction,
    ) -> Self {
        Self::Scalar(AggregateStateFactory::create_terminal(
            kind,
            direction,
            false,
            u64::MAX,
        ))
    }

    /// Wrap one grouped aggregate state into the shared aggregate engine.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::grouped) const fn from_grouped_state(
        state: GroupedAggregateState<E>,
    ) -> Self {
        Self::Grouped(state)
    }

    /// Build one grouped global DISTINCT field aggregate engine.
    pub(in crate::db::executor) fn new_global_distinct_field(
        kind: AggregateKind,
    ) -> Result<Self, GroupError> {
        Ok(Self::GlobalDistinctField(GlobalDistinctFieldState::new(
            kind,
        )?))
    }
}
