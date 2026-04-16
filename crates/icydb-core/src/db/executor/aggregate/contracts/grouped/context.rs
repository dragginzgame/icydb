//! Module: db::executor::aggregate::contracts::grouped::context
//! Defines grouped aggregate execution context shared across grouped runtime
//! preparation and evaluation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{
    aggregate::contracts::error::GroupError,
    group::{GroupKey, GroupKeySet},
};
use std::mem::size_of;

use crate::db::executor::aggregate::contracts::state::GroupedTerminalAggregateState;
#[cfg(test)]
use crate::{
    db::{
        direction::Direction,
        executor::aggregate::contracts::{
            grouped::engine::GroupedAggregateState, spec::AggregateKind,
        },
        query::plan::FieldSlot,
    },
    error::InternalError,
};

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
    distinct_values: u64,
}

impl ExecutionBudget {
    /// Build one zeroed grouped-execution budget.
    #[must_use]
    pub(in crate::db::executor) const fn new() -> Self {
        Self {
            groups: 0,
            aggregate_states: 0,
            estimated_bytes: 0,
            distinct_values: 0,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn groups(&self) -> u64 {
        self.groups
    }

    #[must_use]
    pub(in crate::db::executor) const fn aggregate_states(&self) -> u64 {
        self.aggregate_states
    }

    #[must_use]
    pub(in crate::db::executor) const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    #[must_use]
    pub(in crate::db::executor) const fn distinct_values(&self) -> u64 {
        self.distinct_values
    }

    fn record_new_group_state(
        &mut self,
        config: &ExecutionConfig,
        new_group_key: bool,
        group_count_before_insert: usize,
        group_capacity_before_insert: usize,
    ) -> Result<(), GroupError> {
        let next_groups = if new_group_key {
            self.groups.saturating_add(1)
        } else {
            self.groups
        };
        if next_groups > config.max_groups() {
            return Err(GroupError::memory_limit_exceeded(
                "groups",
                next_groups,
                config.max_groups(),
            ));
        }

        let bytes_delta =
            estimated_new_group_bytes(group_count_before_insert, group_capacity_before_insert);
        let next_bytes = self.estimated_bytes.saturating_add(bytes_delta);
        if next_bytes > config.max_group_bytes() {
            return Err(GroupError::memory_limit_exceeded(
                "estimated_bytes",
                next_bytes,
                config.max_group_bytes(),
            ));
        }

        self.groups = next_groups;
        self.aggregate_states = self.aggregate_states.saturating_add(1);
        self.estimated_bytes = next_bytes;

        Ok(())
    }

    const fn record_distinct_value(&mut self, config: &ExecutionConfig) -> Result<(), GroupError> {
        let attempted = self.distinct_values.saturating_add(1);
        if attempted > config.max_distinct_values_total() {
            return Err(GroupError::distinct_budget_exceeded(
                "distinct_values_total",
                attempted,
                config.max_distinct_values_total(),
            ));
        }

        self.distinct_values = attempted;

        Ok(())
    }
}

impl Default for ExecutionBudget {
    fn default() -> Self {
        Self::new()
    }
}

///
/// ExecutionConfig
///
/// ExecutionConfig defines hard grouped-execution limits selected by planning.
/// Limits stay policy-owned at executor boundaries instead of inside operator
/// state containers so memory policy remains centralized and composable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_field_names)]
pub(in crate::db::executor) struct ExecutionConfig {
    max_groups: u64,
    max_group_bytes: u64,
    max_distinct_values_per_group: u64,
    max_distinct_values_total: u64,
}

///
/// ExecutionContext
///
/// ExecutionContext carries grouped execution policy plus mutable budget usage.
/// Planner/executor boundaries own this context and pass it down to grouped
/// operators so accounting is consistent across all future grouped operators.
///

#[derive(Debug)]
pub(in crate::db::executor) struct ExecutionContext {
    config: ExecutionConfig,
    budget: ExecutionBudget,
    #[cfg(test)]
    seen_groups: GroupKeySet,
}
impl ExecutionConfig {
    /// Build one grouped hard-limit configuration.
    #[must_use]
    pub(in crate::db::executor) const fn with_hard_limits(
        max_groups: u64,
        max_group_bytes: u64,
    ) -> Self {
        let max_distinct_values_per_group = derived_max_distinct_values_per_group(max_group_bytes);
        let max_distinct_values_total = max_distinct_values_per_group.saturating_mul(max_groups);

        Self {
            max_groups,
            max_group_bytes,
            max_distinct_values_per_group,
            max_distinct_values_total,
        }
    }

    /// Build one grouped hard-limit configuration with explicit DISTINCT limits.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) const fn with_hard_limits_and_distinct(
        max_groups: u64,
        max_group_bytes: u64,
        max_distinct_values_per_group: u64,
        max_distinct_values_total: u64,
    ) -> Self {
        Self {
            max_groups,
            max_group_bytes,
            max_distinct_values_per_group,
            max_distinct_values_total,
        }
    }

    /// Build one unbounded grouped configuration for tests.
    #[cfg(test)]
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

    #[must_use]
    pub(in crate::db::executor) const fn max_distinct_values_per_group(&self) -> u64 {
        self.max_distinct_values_per_group
    }

    #[must_use]
    pub(in crate::db::executor) const fn max_distinct_values_total(&self) -> u64 {
        self.max_distinct_values_total
    }
}

impl ExecutionContext {
    /// Build one execution context from grouped hard-limit policy.
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub(in crate::db::executor) fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            budget: ExecutionBudget::new(),
            #[cfg(test)]
            seen_groups: GroupKeySet::new(),
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn config(&self) -> &ExecutionConfig {
        &self.config
    }

    #[must_use]
    pub(in crate::db::executor) const fn budget(&self) -> &ExecutionBudget {
        &self.budget
    }

    /// Build one grouped aggregate state through the execution-context boundary.
    ///
    /// This keeps grouped state construction policy-owned by executor context
    /// so grouped operators cannot bypass centralized budget/config plumbing.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) fn create_grouped_state(
        &self,
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
    ) -> GroupedAggregateState {
        self.create_grouped_state_with_target(kind, direction, distinct, None)
            .expect("grouped test helper should only construct admitted grouped state kinds")
    }

    /// Build one grouped aggregate state with one optional field-target slot.
    ///
    /// This keeps grouped field-target widening structural without forcing
    /// existing grouped callers to thread unused target-slot inputs.
    #[cfg(test)]
    pub(in crate::db::executor) fn create_grouped_state_with_target(
        &self,
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        target_field: Option<FieldSlot>,
    ) -> Result<GroupedAggregateState, InternalError> {
        debug_assert!(
            self.config.max_groups() > 0 || self.config.max_group_bytes() > 0,
            "grouped execution config must expose at least one positive hard limit"
        );
        GroupedAggregateState::new_with_target(
            kind,
            direction,
            distinct,
            target_field,
            self.config.max_distinct_values_per_group(),
        )
    }

    pub(in crate::db::executor::aggregate) fn record_new_group(
        &mut self,
        group_count_before_insert: usize,
        group_capacity_before_insert: usize,
    ) -> Result<(), GroupError> {
        self.budget.record_new_group_state(
            &self.config,
            true,
            group_count_before_insert,
            group_capacity_before_insert,
        )
    }

    // Record one canonical grouped key through the shared grouped budget so
    // test-only grouped state containers still count `max_groups` once across
    // multiple grouped terminal states.
    #[cfg(test)]
    pub(in crate::db::executor::aggregate) fn record_new_canonical_group(
        &mut self,
        key: &GroupKey,
        group_count_before_insert: usize,
        group_capacity_before_insert: usize,
    ) -> Result<(), GroupError> {
        let new_group_key = self.seen_groups.insert_key(key.clone());

        self.budget.record_new_group_state(
            &self.config,
            new_group_key,
            group_count_before_insert,
            group_capacity_before_insert,
        )
    }

    // Record one newly seen grouped key plus an explicit number of aggregate
    // state slots so bundle-based grouped execution can preserve the previous
    // per-aggregate-state budget accounting model.
    pub(in crate::db::executor::aggregate) fn record_new_group_states(
        &mut self,
        group_count_before_insert: usize,
        group_capacity_before_insert: usize,
        aggregate_state_count: usize,
    ) -> Result<(), GroupError> {
        debug_assert!(
            aggregate_state_count > 0,
            "grouped budget accounting must record at least one aggregate state",
        );

        // Keep the dedicated single-state case on the direct budget path so
        // grouped count and other one-aggregate shapes do not pay the generic
        // bundle loop on every new group insert.
        if aggregate_state_count == 1 {
            return self.record_new_group(group_count_before_insert, group_capacity_before_insert);
        }

        // Count `max_groups` against caller-proven unique canonical group keys,
        // not per-aggregate state machine instance. Grouped runtime owns the
        // canonical group table already, so this budget layer does not re-check
        // uniqueness through a second `GroupKeySet`.
        for state_index in 0..aggregate_state_count {
            self.budget.record_new_group_state(
                &self.config,
                state_index == 0,
                group_count_before_insert,
                group_capacity_before_insert,
            )?;
        }

        Ok(())
    }

    pub(in crate::db::executor) const fn record_distinct_value(
        &mut self,
    ) -> Result<(), GroupError> {
        self.budget.record_distinct_value(&self.config)
    }

    /// Admit one grouped DISTINCT key through execution-context budget
    /// accounting and per-group cardinality enforcement.
    pub(in crate::db::executor) fn admit_distinct_key(
        &mut self,
        distinct_keys: &mut GroupKeySet,
        max_distinct_values_per_group: u64,
        key: GroupKey,
    ) -> Result<bool, GroupError> {
        if distinct_keys.contains_key(&key) {
            return Ok(false);
        }

        // Preserve deterministic error ordering: enforce total cap first,
        // then enforce per-group cap, before mutating key state.
        let attempted_total = self.budget.distinct_values().saturating_add(1);
        if attempted_total > self.config.max_distinct_values_total() {
            return Err(GroupError::distinct_budget_exceeded(
                "distinct_values_total",
                attempted_total,
                self.config.max_distinct_values_total(),
            ));
        }

        let attempted_per_group = u64::try_from(distinct_keys.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        if attempted_per_group > max_distinct_values_per_group {
            return Err(GroupError::distinct_budget_exceeded(
                "distinct_values_per_group",
                attempted_per_group,
                max_distinct_values_per_group,
            ));
        }

        let inserted = distinct_keys.insert_key(key);
        debug_assert!(inserted, "new distinct key must insert exactly once");
        self.record_distinct_value()?;

        Ok(true)
    }

    /// Record one implicit singleton group for grouped shapes that are modeled
    /// without explicit group-key boundary transitions (for example zero-key
    /// global grouped aggregates).
    pub(in crate::db::executor) fn record_implicit_single_group(
        &mut self,
    ) -> Result<(), GroupError> {
        self.record_new_group(0, 0)
    }
}

fn estimated_new_group_bytes(
    group_count_before_insert: usize,
    group_capacity_before_insert: usize,
) -> u64 {
    let entry_size = size_of::<(GroupKey, GroupedTerminalAggregateState)>();
    let entry_growth = if group_count_before_insert < group_capacity_before_insert {
        entry_size
    } else {
        let projected_capacity = projected_capacity_after_insert(group_capacity_before_insert);
        projected_capacity
            .saturating_sub(group_capacity_before_insert)
            .saturating_mul(entry_size)
    };

    saturating_u64_from_usize(entry_growth)
}

const fn derived_max_distinct_values_per_group(max_group_bytes: u64) -> u64 {
    let derived = max_group_bytes / 64;
    if derived == 0 { 1 } else { derived }
}

const fn projected_capacity_after_insert(current_capacity: usize) -> usize {
    if current_capacity == 0 {
        1
    } else {
        current_capacity.saturating_mul(2)
    }
}

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
