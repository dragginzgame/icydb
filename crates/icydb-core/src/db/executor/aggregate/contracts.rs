use crate::{
    db::{
        contracts::{canonical_group_key_equals, canonical_value_compare},
        data::DataKey,
        direction::Direction,
        group_key::GroupKey,
        hash::StableHash,
    },
    error::InternalError,
    traits::EntityKind,
    types::Id,
};
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use thiserror::Error as ThisError;

///
/// AggregateKind
///
/// Internal aggregate terminal selector shared by aggregate routing and fold execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateKind {
    Count,
    Exists,
    Min,
    Max,
    First,
    Last,
}

impl AggregateKind {
    /// Return whether this terminal kind supports explicit field targets.
    #[must_use]
    pub(in crate::db::executor) const fn supports_field_targets(self) -> bool {
        matches!(self, Self::Min | Self::Max)
    }

    /// Return whether this terminal kind belongs to the extrema family.
    #[must_use]
    pub(in crate::db::executor) const fn is_extrema(self) -> bool {
        self.supports_field_targets()
    }

    /// Return whether this terminal kind supports first/last value projection.
    #[must_use]
    pub(in crate::db::executor) const fn supports_terminal_value_projection(self) -> bool {
        matches!(self, Self::First | Self::Last)
    }

    /// Return whether reducer updates for this kind require a decoded id payload.
    #[must_use]
    pub(in crate::db::executor) const fn requires_decoded_id(self) -> bool {
        !matches!(self, Self::Count | Self::Exists)
    }

    /// Return the canonical extrema traversal direction for this terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn extrema_direction(self) -> Option<Direction> {
        match self {
            Self::Min => Some(Direction::Asc),
            Self::Max => Some(Direction::Desc),
            Self::Count | Self::Exists | Self::First | Self::Last => None,
        }
    }

    /// Build the canonical empty-window aggregate output for this terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn zero_output<E: EntityKind>(self) -> AggregateOutput<E> {
        match self {
            Self::Count => AggregateOutput::Count(0),
            Self::Exists => AggregateOutput::Exists(false),
            Self::Min => AggregateOutput::Min(None),
            Self::Max => AggregateOutput::Max(None),
            Self::First => AggregateOutput::First(None),
            Self::Last => AggregateOutput::Last(None),
        }
    }

    /// Build an extrema output payload when this kind is MIN or MAX.
    #[must_use]
    pub(in crate::db::executor) const fn extrema_output<E: EntityKind>(
        self,
        id: Option<Id<E>>,
    ) -> Option<AggregateOutput<E>> {
        match self {
            Self::Min => Some(AggregateOutput::Min(id)),
            Self::Max => Some(AggregateOutput::Max(id)),
            Self::Count | Self::Exists | Self::First | Self::Last => None,
        }
    }

    /// Return true when this kind/output pair is an unresolved extrema result.
    #[must_use]
    pub(in crate::db::executor) const fn is_unresolved_extrema_output<E: EntityKind>(
        self,
        output: &AggregateOutput<E>,
    ) -> bool {
        matches!(
            (self, output),
            (Self::Min, AggregateOutput::Min(None)) | (Self::Max, AggregateOutput::Max(None))
        )
    }
}

///
/// AggregateSpec
///
/// Canonical aggregate execution specification used by route/fold boundaries.
/// `target_field` is reserved for field-scoped aggregates (`min(field)` / `max(field)`).
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateSpec {
    kind: AggregateKind,
    target_field: Option<String>,
}

///
/// GroupAggregateSpec
///
/// Canonical grouped aggregate contract for future GROUP BY execution.
/// Carries grouping keys plus one-or-more aggregate terminal specifications.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) struct GroupAggregateSpec {
    group_keys: Vec<String>,
    aggregate_specs: Vec<AggregateSpec>,
}

///
/// AggregateSpecSupportError
///
/// Canonical unsupported taxonomy for aggregate spec shape validation.
/// Keeps field-target capability errors explicit before runtime execution.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db::executor) enum AggregateSpecSupportError {
    #[error(
        "field-target aggregates are only supported for min/max terminals: {kind:?}({target_field})"
    )]
    FieldTargetRequiresExtrema {
        kind: AggregateKind,
        target_field: String,
    },
}

///
/// GroupAggregateSpecSupportError
///
/// Canonical unsupported taxonomy for grouped aggregate contract validation.
/// Keeps GROUP BY contract shape failures explicit before execution is enabled.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) enum GroupAggregateSpecSupportError {
    #[error("group aggregate spec requires at least one aggregate terminal")]
    MissingAggregateSpecs,

    #[error("group aggregate spec has duplicate group key: {field}")]
    DuplicateGroupKey { field: String },

    #[error("group aggregate spec contains unsupported terminal at index={index}: {source}")]
    AggregateSpecUnsupported {
        index: usize,
        #[source]
        source: AggregateSpecSupportError,
    },
}

///
/// GroupError
///
/// GroupError is the typed grouped-execution error surface.
/// This taxonomy keeps grouped memory-limit failures explicit and prevents
/// grouped resource guardrails from degrading into generic internal errors.
///

#[derive(Debug, ThisError)]
pub(in crate::db::executor) enum GroupError {
    #[error(
        "grouped execution memory limit exceeded ({resource}): attempted={attempted}, limit={limit}"
    )]
    MemoryLimitExceeded {
        resource: &'static str,
        attempted: u64,
        limit: u64,
    },

    #[error("{0}")]
    Internal(#[from] InternalError),
}

///
/// ExecutionBudget
///
/// ExecutionBudget tracks grouped-execution resource usage counters.
/// `groups` and `aggregate_states` are structural counters; `estimated_bytes`
/// is a conservative allocation estimate used for memory guardrails.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ExecutionBudget {
    groups: u64,
    aggregate_states: u64,
    estimated_bytes: u64,
}

///
/// ExecutionConfig
///
/// ExecutionConfig defines hard grouped-execution limits selected by planning.
/// Limits stay policy-owned at executor boundaries instead of inside operator
/// state containers so memory policy remains centralized and composable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ExecutionConfig {
    max_groups: u64,
    max_group_bytes: u64,
}

///
/// ExecutionContext
///
/// ExecutionContext carries grouped execution policy plus mutable budget usage.
/// Planner/executor boundaries own this context and pass it down to grouped
/// operators so accounting is consistent across all future grouped operators.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ExecutionContext {
    config: ExecutionConfig,
    budget: ExecutionBudget,
}

impl ExecutionBudget {
    /// Build one zeroed grouped-execution budget.
    #[must_use]
    pub(in crate::db::executor) const fn new() -> Self {
        Self {
            groups: 0,
            aggregate_states: 0,
            estimated_bytes: 0,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(in crate::db::executor) const fn groups(&self) -> u64 {
        self.groups
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_states(&self) -> u64 {
        self.aggregate_states
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(in crate::db::executor) const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    fn record_new_group<E: EntityKind>(
        &mut self,
        config: &ExecutionConfig,
        created_bucket: bool,
        bucket_len: usize,
        bucket_capacity: usize,
    ) -> Result<(), GroupError> {
        let next_groups = self.groups.saturating_add(1);
        if next_groups > config.max_groups() {
            return Err(GroupError::MemoryLimitExceeded {
                resource: "groups",
                attempted: next_groups,
                limit: config.max_groups(),
            });
        }

        let bytes_delta =
            estimated_new_group_bytes::<E>(created_bucket, bucket_len, bucket_capacity);
        let next_bytes = self.estimated_bytes.saturating_add(bytes_delta);
        if next_bytes > config.max_group_bytes() {
            return Err(GroupError::MemoryLimitExceeded {
                resource: "estimated_bytes",
                attempted: next_bytes,
                limit: config.max_group_bytes(),
            });
        }

        self.groups = next_groups;
        self.aggregate_states = self.aggregate_states.saturating_add(1);
        self.estimated_bytes = next_bytes;

        Ok(())
    }
}

impl Default for ExecutionBudget {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl ExecutionConfig {
    /// Build one grouped hard-limit configuration.
    #[must_use]
    pub(in crate::db::executor) const fn with_hard_limits(
        max_groups: u64,
        max_group_bytes: u64,
    ) -> Self {
        Self {
            max_groups,
            max_group_bytes,
        }
    }

    /// Build one unbounded grouped configuration for scaffold callers/tests.
    #[must_use]
    pub(in crate::db::executor) const fn unbounded() -> Self {
        Self::with_hard_limits(u64::MAX, u64::MAX)
    }

    #[must_use]
    pub(in crate::db::executor) const fn max_groups(&self) -> u64 {
        self.max_groups
    }

    #[must_use]
    pub(in crate::db::executor) const fn max_group_bytes(&self) -> u64 {
        self.max_group_bytes
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl ExecutionContext {
    /// Build one execution context from grouped hard-limit policy.
    #[must_use]
    pub(in crate::db::executor) const fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            budget: ExecutionBudget::new(),
        }
    }

    #[allow(dead_code)]
    #[must_use]
    pub(in crate::db::executor) const fn config(&self) -> &ExecutionConfig {
        &self.config
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(in crate::db::executor) const fn budget(&self) -> &ExecutionBudget {
        &self.budget
    }

    /// Build one grouped aggregate state through the execution-context boundary.
    ///
    /// This keeps grouped state construction policy-owned by executor context
    /// so grouped operators cannot bypass centralized budget/config plumbing.
    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(in crate::db::executor) fn create_grouped_state<E: EntityKind>(
        &self,
        kind: AggregateKind,
        direction: Direction,
    ) -> GroupedAggregateState<E> {
        debug_assert!(
            self.config.max_groups() > 0 || self.config.max_group_bytes() > 0,
            "grouped execution config must expose at least one positive hard limit"
        );
        GroupedAggregateState::new(kind, direction)
    }

    fn record_new_group<E: EntityKind>(
        &mut self,
        created_bucket: bool,
        bucket_len: usize,
        bucket_capacity: usize,
    ) -> Result<(), GroupError> {
        self.budget
            .record_new_group::<E>(&self.config, created_bucket, bucket_len, bucket_capacity)
    }
}

fn estimated_new_group_bytes<E: EntityKind>(
    created_bucket: bool,
    bucket_len: usize,
    bucket_capacity: usize,
) -> u64 {
    let slot_size = size_of::<GroupedAggregateStateSlot<E>>();
    let map_entry_size = if created_bucket {
        size_of::<(StableHash, Vec<GroupedAggregateStateSlot<E>>)>()
    } else {
        0
    };

    let slot_growth = if bucket_len < bucket_capacity {
        slot_size
    } else {
        let projected_capacity = projected_vec_capacity_after_push(bucket_capacity);
        projected_capacity
            .saturating_sub(bucket_capacity)
            .saturating_mul(slot_size)
    };

    saturating_u64_from_usize(map_entry_size.saturating_add(slot_growth))
}

const fn projected_vec_capacity_after_push(current_capacity: usize) -> usize {
    if current_capacity == 0 {
        1
    } else {
        current_capacity.saturating_mul(2)
    }
}

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl AggregateSpec {
    /// Build a terminal aggregate spec with no explicit field target.
    #[must_use]
    pub(in crate::db::executor) const fn for_terminal(kind: AggregateKind) -> Self {
        Self {
            kind,
            target_field: None,
        }
    }

    /// Build a field-targeted aggregate spec for future field aggregates.
    #[must_use]
    pub(in crate::db::executor) fn for_target_field(
        kind: AggregateKind,
        target_field: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            target_field: Some(target_field.into()),
        }
    }

    /// Return the aggregate terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Return the optional aggregate field target.
    #[must_use]
    pub(in crate::db::executor) fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Validate support boundaries for this aggregate spec in the current release line.
    pub(in crate::db::executor) fn ensure_supported_for_execution(
        &self,
    ) -> Result<(), AggregateSpecSupportError> {
        let Some(target_field) = self.target_field() else {
            return Ok(());
        };
        if !self.kind.supports_field_targets() {
            return Err(AggregateSpecSupportError::FieldTargetRequiresExtrema {
                kind: self.kind,
                target_field: target_field.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl GroupAggregateSpec {
    /// Build one grouped aggregate contract from group-key + terminal specs.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        group_keys: Vec<String>,
        aggregate_specs: Vec<AggregateSpec>,
    ) -> Self {
        Self {
            group_keys,
            aggregate_specs,
        }
    }

    /// Build one global aggregate contract from a single terminal aggregate.
    #[must_use]
    pub(in crate::db::executor) fn for_global_terminal(spec: AggregateSpec) -> Self {
        Self {
            group_keys: Vec::new(),
            aggregate_specs: vec![spec],
        }
    }

    /// Borrow grouped key fields in declared order.
    #[must_use]
    pub(in crate::db::executor) const fn group_keys(&self) -> &[String] {
        self.group_keys.as_slice()
    }

    /// Borrow aggregate terminal specifications in declared projection order.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_specs(&self) -> &[AggregateSpec] {
        self.aggregate_specs.as_slice()
    }

    /// Return true when this contract models grouped aggregation.
    #[must_use]
    pub(in crate::db::executor) const fn is_grouped(&self) -> bool {
        !self.group_keys.is_empty()
    }

    /// Validate support boundaries for grouped aggregate contracts.
    pub(in crate::db::executor) fn ensure_supported_for_execution(
        &self,
    ) -> Result<(), GroupAggregateSpecSupportError> {
        if self.aggregate_specs.is_empty() {
            return Err(GroupAggregateSpecSupportError::MissingAggregateSpecs);
        }

        let mut seen_group_keys = BTreeSet::<&str>::new();
        for field in &self.group_keys {
            if !seen_group_keys.insert(field.as_str()) {
                return Err(GroupAggregateSpecSupportError::DuplicateGroupKey {
                    field: field.clone(),
                });
            }
        }

        for (index, spec) in self.aggregate_specs.iter().enumerate() {
            spec.ensure_supported_for_execution().map_err(|source| {
                GroupAggregateSpecSupportError::AggregateSpecUnsupported { index, source }
            })?;
        }

        Ok(())
    }
}

///
/// AggregateOutput
///
/// Internal aggregate terminal result container shared by aggregate routing and fold execution.
///

pub(in crate::db::executor) enum AggregateOutput<E: EntityKind> {
    Count(u32),
    Exists(bool),
    Min(Option<Id<E>>),
    Max(Option<Id<E>>),
    First(Option<Id<E>>),
    Last(Option<Id<E>>),
}

///
/// FoldControl
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) enum FoldControl {
    Continue,
    Break,
}

///
/// AggregateReducerState
///
/// Shared aggregate terminal reducer state used by streaming and fast-path
/// aggregate execution so terminal update semantics stay centralized.
///

pub(in crate::db::executor) enum AggregateReducerState<E: EntityKind> {
    Count(u32),
    Exists(bool),
    Min(Option<Id<E>>),
    Max(Option<Id<E>>),
    First(Option<Id<E>>),
    Last(Option<Id<E>>),
}

impl<E: EntityKind> AggregateReducerState<E> {
    /// Build the initial reducer state for one aggregate terminal.
    #[must_use]
    pub(in crate::db::executor) const fn for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(0),
            AggregateKind::Exists => Self::Exists(false),
            AggregateKind::Min => Self::Min(None),
            AggregateKind::Max => Self::Max(None),
            AggregateKind::First => Self::First(None),
            AggregateKind::Last => Self::Last(None),
        }
    }

    /// Apply one candidate data key to the reducer and return fold control.
    pub(in crate::db::executor) fn update_from_data_key(
        &mut self,
        kind: AggregateKind,
        direction: Direction,
        key: &DataKey,
    ) -> Result<FoldControl, InternalError> {
        let id = if kind.requires_decoded_id() {
            Some(Id::from_key(key.try_key::<E>()?))
        } else {
            None
        };

        self.update_with_optional_id(kind, direction, id)
    }

    /// Apply one reducer update using an optional decoded id payload.
    pub(in crate::db::executor) fn update_with_optional_id(
        &mut self,
        kind: AggregateKind,
        direction: Direction,
        id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        match (kind, self) {
            (AggregateKind::Count, Self::Count(count)) => {
                *count = count.saturating_add(1);
                Ok(FoldControl::Continue)
            }
            (AggregateKind::Exists, Self::Exists(exists)) => {
                *exists = true;
                Ok(FoldControl::Break)
            }
            (AggregateKind::Min, Self::Min(min_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer MIN update requires decoded id",
                    ));
                };
                let replace = match min_id.as_ref() {
                    Some(current) => id < *current,
                    None => true,
                };
                if replace {
                    *min_id = Some(id);
                }
                if direction == Direction::Asc {
                    return Ok(FoldControl::Break);
                }

                Ok(FoldControl::Continue)
            }
            (AggregateKind::Max, Self::Max(max_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer MAX update requires decoded id",
                    ));
                };
                let replace = match max_id.as_ref() {
                    Some(current) => id > *current,
                    None => true,
                };
                if replace {
                    *max_id = Some(id);
                }
                if direction == Direction::Desc {
                    return Ok(FoldControl::Break);
                }

                Ok(FoldControl::Continue)
            }
            (AggregateKind::First, Self::First(first_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer FIRST update requires decoded id",
                    ));
                };
                *first_id = Some(id);
                Ok(FoldControl::Break)
            }
            (AggregateKind::Last, Self::Last(last_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer LAST update requires decoded id",
                    ));
                };
                *last_id = Some(id);
                Ok(FoldControl::Continue)
            }
            _ => Err(InternalError::query_executor_invariant(
                "aggregate reducer state/kind mismatch",
            )),
        }
    }

    /// Convert reducer state into the aggregate terminal output payload.
    #[must_use]
    pub(in crate::db::executor) const fn into_output(self) -> AggregateOutput<E> {
        match self {
            Self::Count(value) => AggregateOutput::Count(value),
            Self::Exists(value) => AggregateOutput::Exists(value),
            Self::Min(value) => AggregateOutput::Min(value),
            Self::Max(value) => AggregateOutput::Max(value),
            Self::First(value) => AggregateOutput::First(value),
            Self::Last(value) => AggregateOutput::Last(value),
        }
    }
}

///
/// AggregateState
///
/// Canonical aggregate state-machine contract consumed by kernel reducer
/// orchestration. Implementations must keep transitions deterministic and
/// emit terminal outputs using the shared aggregate output taxonomy.
///
pub(in crate::db::executor) trait AggregateState<E: EntityKind> {
    /// Apply one candidate data key to this aggregate state machine.
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError>;

    /// Finalize this aggregate state into one terminal output payload.
    fn finalize(self) -> AggregateOutput<E>;
}

///
/// TerminalAggregateState
///
/// TerminalAggregateState binds one aggregate kind + direction to one reducer
/// state machine so key-stream execution can use a single canonical update
/// pipeline across COUNT/EXISTS/MIN/MAX/FIRST/LAST terminals.
///

pub(in crate::db::executor) struct TerminalAggregateState<E: EntityKind> {
    kind: AggregateKind,
    direction: Direction,
    reducer: AggregateReducerState<E>,
}

impl<E: EntityKind> AggregateState<E> for TerminalAggregateState<E> {
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        self.reducer
            .update_from_data_key(self.kind, self.direction, key)
    }

    fn finalize(self) -> AggregateOutput<E> {
        self.reducer.into_output()
    }
}

///
/// AggregateStateFactory
///
/// AggregateStateFactory builds canonical terminal aggregate state machines
/// from route-owned kind/direction decisions.
/// This keeps state initialization centralized at one boundary.
///

pub(in crate::db::executor) struct AggregateStateFactory;

impl AggregateStateFactory {
    /// Build one terminal aggregate state machine for kernel reducers.
    #[must_use]
    pub(in crate::db::executor) const fn create_terminal<E: EntityKind>(
        kind: AggregateKind,
        direction: Direction,
    ) -> TerminalAggregateState<E> {
        TerminalAggregateState {
            kind,
            direction,
            reducer: AggregateReducerState::for_kind(kind),
        }
    }
}

///
/// GroupedAggregateOutput
///
/// GroupedAggregateOutput carries one finalized grouped terminal row:
/// one canonical group key paired with one aggregate terminal output.
/// Finalized rows are emitted in deterministic canonical order.
///

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) struct GroupedAggregateOutput<E: EntityKind> {
    group_key: GroupKey,
    output: AggregateOutput<E>,
}

#[cfg_attr(not(test), allow(dead_code))]
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

#[cfg_attr(not(test), allow(dead_code))]
struct GroupedAggregateStateSlot<E: EntityKind> {
    group_key: GroupKey,
    state: TerminalAggregateState<E>,
}

///
/// GroupedAggregateState
///
/// GroupedAggregateState stores per-group aggregate state machines keyed by
/// canonical group keys and stable-hash buckets.
/// Group-local states are built by `AggregateStateFactory` and finalized in a
/// deterministic order independent of insertion order.
///

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) struct GroupedAggregateState<E: EntityKind> {
    kind: AggregateKind,
    direction: Direction,
    groups: BTreeMap<StableHash, Vec<GroupedAggregateStateSlot<E>>>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl<E: EntityKind> GroupedAggregateState<E> {
    /// Build one empty grouped aggregate state container.
    #[must_use]
    const fn new(kind: AggregateKind, direction: Direction) -> Self {
        Self {
            kind,
            direction,
            groups: BTreeMap::new(),
        }
    }

    /// Apply one `(group_key, data_key)` row into grouped aggregate state.
    pub(in crate::db::executor) fn apply(
        &mut self,
        group_key: GroupKey,
        data_key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        // Phase 1: resolve updates for existing buckets/groups.
        let hash = group_key.hash();
        if let Some(bucket) = self.groups.get_mut(&hash) {
            if let Some(slot) = bucket
                .iter_mut()
                .find(|slot| canonical_group_key_equals(slot.group_key(), &group_key))
            {
                return slot.state.apply(data_key).map_err(GroupError::from);
            }

            // New group in an existing bucket.
            let mut state = AggregateStateFactory::create_terminal(self.kind, self.direction);
            let fold_control = state.apply(data_key).map_err(GroupError::from)?;
            execution_context.record_new_group::<E>(false, bucket.len(), bucket.capacity())?;
            bucket.push(GroupedAggregateStateSlot { group_key, state });

            return Ok(fold_control);
        }

        // Phase 2: create a new bucket + group when hash was unseen.
        let mut state = AggregateStateFactory::create_terminal(self.kind, self.direction);
        let fold_control = state.apply(data_key).map_err(GroupError::from)?;
        execution_context.record_new_group::<E>(true, 0, 0)?;
        self.groups
            .insert(hash, vec![GroupedAggregateStateSlot { group_key, state }]);

        Ok(fold_control)
    }

    /// Return the current number of grouped keys tracked by this state.
    #[must_use]
    pub(in crate::db::executor) fn group_count(&self) -> usize {
        self.groups
            .values()
            .fold(0usize, |count, bucket| count.saturating_add(bucket.len()))
    }

    /// Finalize all groups into deterministic grouped aggregate outputs.
    #[must_use]
    pub(in crate::db::executor) fn finalize(self) -> Vec<GroupedAggregateOutput<E>> {
        let mut out = Vec::new();

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

        out
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl<E: EntityKind> GroupedAggregateStateSlot<E> {
    #[must_use]
    const fn group_key(&self) -> &GroupKey {
        &self.group_key
    }
}

///
/// AggregateFoldMode
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
}
