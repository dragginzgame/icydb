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
            group::{GroupKey, StableHash, canonical_group_key_equals},
        },
    },
    error::InternalError,
    traits::EntityKind,
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
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) const fn group_key(&self) -> &GroupKey {
        &self.group_key
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) const fn output(&self) -> &AggregateOutput<E> {
        &self.output
    }

    fn into_value_pair(self) -> (Value, Value) {
        (
            self.group_key.canonical_value().clone(),
            aggregate_output_to_value(self.output),
        )
    }
}

fn aggregate_output_to_value<E: EntityKind>(output: AggregateOutput<E>) -> Value {
    match output {
        AggregateOutput::Count(value) => Value::Uint(u64::from(value)),
        AggregateOutput::Sum(value) => value.map_or(Value::Null, Value::Decimal),
        AggregateOutput::Exists(value) => Value::Bool(value),
        AggregateOutput::Min(value)
        | AggregateOutput::Max(value)
        | AggregateOutput::First(value)
        | AggregateOutput::Last(value) => value.map_or(Value::Null, Value::from),
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
/// AggregateEngine
///
/// Canonical aggregate reducer engine shared by scalar and grouped execution
/// spines. This keeps ingest/finalize semantics centralized across both modes.
///

pub(in crate::db::executor) enum AggregateEngine<E: EntityKind> {
    Scalar(TerminalAggregateState<E>),
    Grouped(GroupedAggregateState<E>),
}

///
/// GroupedAggregateEngine
///
/// GroupedAggregateEngine is the structural grouped reducer boundary used by
/// grouped runtime execution. Grouped fold logic consumes only this trait so
/// grouped runtime no longer needs entity-typed aggregate engine containers.
///

pub(in crate::db::executor) trait GroupedAggregateEngine {
    /// Ingest one grouped row into one grouped aggregate engine.
    fn ingest(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DataKey,
        group_key: &GroupKey,
    ) -> Result<FoldControl, GroupError>;

    /// Finalize one grouped aggregate engine into structural `(group_key, value)` pairs.
    fn finalize(self: Box<Self>) -> Result<Vec<(Value, Value)>, InternalError>;
}

///
/// TypedGroupedAggregateEngine
///
/// TypedGroupedAggregateEngine keeps entity-typed grouped aggregate semantics
/// at the adapter boundary while exposing one structural grouped engine trait
/// to shared grouped fold logic.
///

struct TypedGroupedAggregateEngine<E: EntityKind> {
    engine: AggregateEngine<E>,
}

impl<E: EntityKind> TypedGroupedAggregateEngine<E> {
    const fn new(engine: AggregateEngine<E>) -> Self {
        Self { engine }
    }
}

impl<E: EntityKind> GroupedAggregateEngine for TypedGroupedAggregateEngine<E> {
    fn ingest(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DataKey,
        group_key: &GroupKey,
    ) -> Result<FoldControl, GroupError> {
        self.engine
            .ingest_grouped(execution_context, data_key, group_key)
    }

    fn finalize(self: Box<Self>) -> Result<Vec<(Value, Value)>, InternalError> {
        self.engine.finalize_grouped_values()
    }
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

impl<E: EntityKind> AggregateEngine<E> {
    fn mode_mismatch_error(mode: AggregateExecutionMode) -> GroupError {
        GroupError::Internal(crate::db::error::query_executor_invariant(match mode {
            AggregateExecutionMode::Scalar => {
                "scalar aggregate ingest reached grouped aggregate engine"
            }
            AggregateExecutionMode::Grouped => {
                "grouped aggregate ingest reached scalar aggregate engine"
            }
        }))
    }

    fn ingest_grouped(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DataKey,
        group_key: &GroupKey,
    ) -> Result<FoldControl, GroupError> {
        match self {
            AggregateEngine::Grouped(state) => {
                state.apply_borrowed(group_key, data_key, execution_context)
            }
            AggregateEngine::Scalar(_) => Err(AggregateEngine::<E>::mode_mismatch_error(
                AggregateExecutionMode::Grouped,
            )),
        }
    }

    fn finalize_grouped_values(self) -> Result<Vec<(Value, Value)>, InternalError> {
        match self {
            AggregateEngine::Grouped(state) => Ok(state
                .finalize()
                .into_iter()
                .map(GroupedAggregateOutput::into_value_pair)
                .collect()),
            AggregateEngine::Scalar(_) => Err(crate::db::error::query_executor_invariant(
                "grouped aggregate finalize reached scalar aggregate engine",
            )),
        }
    }

    // Ingest one key through the aggregate execution mode carried by the
    // execution spec so scalar/grouped loops share one reducer entrypoint.
    pub(in crate::db::executor) fn ingest_with_spec(
        &mut self,
        execution_spec: &mut AggregateExecutionSpec<'_>,
        data_key: &DataKey,
        group_key: Option<&GroupKey>,
    ) -> Result<FoldControl, GroupError> {
        match execution_spec.mode() {
            AggregateExecutionMode::Scalar => {
                if group_key.is_some() {
                    return Err(GroupError::Internal(
                        crate::db::error::query_executor_invariant(
                            "scalar aggregate ingest must not receive grouped group-key payload",
                        ),
                    ));
                }

                match self {
                    AggregateEngine::Scalar(state) => {
                        state.apply(data_key).map_err(GroupError::from)
                    }
                    AggregateEngine::Grouped(_) => Err(AggregateEngine::<E>::mode_mismatch_error(
                        AggregateExecutionMode::Scalar,
                    )),
                }
            }
            AggregateExecutionMode::Grouped => {
                let Some(group_key) = group_key else {
                    return Err(GroupError::Internal(
                        crate::db::error::query_executor_invariant(
                            "grouped aggregate ingest requires grouped group-key payload",
                        ),
                    ));
                };
                let execution_context = execution_spec.grouped_execution_context_mut()?;

                match self {
                    AggregateEngine::Grouped(state) => {
                        state.apply_borrowed(group_key, data_key, execution_context)
                    }
                    AggregateEngine::Scalar(_) => Err(AggregateEngine::<E>::mode_mismatch_error(
                        AggregateExecutionMode::Grouped,
                    )),
                }
            }
        }
    }

    // Finalize this engine according to the selected execution mode so scalar
    // and grouped reducer runners share one terminal projection boundary.
    pub(in crate::db::executor) fn finalize_with_mode(
        self,
        mode: AggregateExecutionMode,
    ) -> Result<AggregateFinalizeOutput<E>, InternalError> {
        match mode {
            AggregateExecutionMode::Scalar => match self {
                AggregateEngine::Scalar(state) => {
                    Ok(AggregateFinalizeOutput::Scalar(state.finalize()))
                }
                AggregateEngine::Grouped(_) => Err(crate::db::error::query_executor_invariant(
                    "scalar aggregate finalize reached grouped aggregate engine",
                )),
            },
            AggregateExecutionMode::Grouped => match self {
                AggregateEngine::Grouped(state) => {
                    Ok(AggregateFinalizeOutput::Grouped(state.finalize()))
                }
                AggregateEngine::Scalar(_) => Err(crate::db::error::query_executor_invariant(
                    "grouped aggregate finalize reached scalar aggregate engine",
                )),
            },
        }
    }
}

///
/// AggregateFinalizeOutput
///
/// AggregateFinalizeOutput is the unified finalize payload emitted by the
/// aggregate finalize boundary for scalar and grouped execution modes.
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
            Self::Grouped(output) => Err(crate::db::error::query_executor_invariant(format!(
                "scalar aggregate finalize expected scalar payload but received grouped payload: grouped_len={}",
                output.len()
            ))),
        }
    }
}

// Execute one aggregate engine through one canonical ingest/finalize authority.
// The caller supplies loop/key ingestion behavior while this boundary owns:
// 1) mode selection from the execution spec
// 2) one ingest boundary
// 3) one finalize projection
pub(in crate::db::executor) fn execute_aggregate<'a, E: EntityKind>(
    mut engine: AggregateEngine<E>,
    mut execution_spec: AggregateExecutionSpec<'a>,
    ingest_all: &mut dyn FnMut(
        &mut AggregateExecutionSpec<'a>,
        &mut AggregateEngine<E>,
    ) -> Result<(), InternalError>,
) -> Result<AggregateFinalizeOutput<E>, InternalError> {
    let execution_mode = execution_spec.mode();
    ingest_all(&mut execution_spec, &mut engine)?;

    engine.finalize_with_mode(execution_mode)
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
}

/// Wrap one typed grouped aggregate engine behind the structural grouped runtime trait.
pub(in crate::db::executor) fn box_grouped_engine<E>(
    engine: AggregateEngine<E>,
) -> Box<dyn GroupedAggregateEngine>
where
    E: EntityKind + 'static,
{
    Box::new(TypedGroupedAggregateEngine::new(engine))
}
