//! Module: db::executor::aggregate::contracts::grouped::context
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::contracts::grouped::context.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        direction::Direction,
        executor::{
            aggregate::contracts::{error::GroupError, spec::AggregateKind},
            group::{CanonicalKey, GroupKey, GroupKeySet, StableHash},
        },
    },
    value::Value,
};
use std::mem::size_of;

use crate::db::executor::aggregate::contracts::grouped::engine::{
    GroupedAggregateState, GroupedAggregateStateSlot,
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
        created_bucket: bool,
        bucket_len: usize,
        bucket_capacity: usize,
    ) -> Result<(), GroupError> {
        let next_groups = if new_group_key {
            self.groups.saturating_add(1)
        } else {
            self.groups
        };
        if next_groups > config.max_groups() {
            return Err(GroupError::MemoryLimitExceeded {
                resource: "groups",
                attempted: next_groups,
                limit: config.max_groups(),
            });
        }

        let bytes_delta = estimated_new_group_bytes(created_bucket, bucket_len, bucket_capacity);
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

    const fn record_distinct_value(&mut self, config: &ExecutionConfig) -> Result<(), GroupError> {
        let attempted = self.distinct_values.saturating_add(1);
        if attempted > config.max_distinct_values_total() {
            return Err(GroupError::DistinctBudgetExceeded {
                resource: "distinct_values_total",
                attempted,
                limit: config.max_distinct_values_total(),
            });
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
    #[must_use]
    pub(in crate::db::executor) const fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            budget: ExecutionBudget::new(),
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
    #[must_use]
    pub(in crate::db::executor) fn create_grouped_state(
        &self,
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
    ) -> GroupedAggregateState {
        debug_assert!(
            self.config.max_groups() > 0 || self.config.max_group_bytes() > 0,
            "grouped execution config must expose at least one positive hard limit"
        );
        GroupedAggregateState::new(
            kind,
            direction,
            distinct,
            self.config.max_distinct_values_per_group(),
        )
    }

    pub(in crate::db::executor::aggregate::contracts::grouped) fn record_new_group(
        &mut self,
        group_key: &GroupKey,
        created_bucket: bool,
        bucket_len: usize,
        bucket_capacity: usize,
    ) -> Result<(), GroupError> {
        // Count `max_groups` against unique canonical group keys across the
        // full grouped query, not per-aggregate state machine instance.
        let new_group_key = !self.seen_groups.contains_key(group_key);
        self.budget.record_new_group_state(
            &self.config,
            new_group_key,
            created_bucket,
            bucket_len,
            bucket_capacity,
        )?;
        if new_group_key {
            let inserted = self.seen_groups.insert_key(group_key.clone());
            debug_assert!(
                inserted,
                "new_group_key must imply one successful seen-groups insertion",
            );
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
            return Err(GroupError::DistinctBudgetExceeded {
                resource: "distinct_values_total",
                attempted: attempted_total,
                limit: self.config.max_distinct_values_total(),
            });
        }

        let attempted_per_group = u64::try_from(distinct_keys.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        if attempted_per_group > max_distinct_values_per_group {
            return Err(GroupError::DistinctBudgetExceeded {
                resource: "distinct_values_per_group",
                attempted: attempted_per_group,
                limit: max_distinct_values_per_group,
            });
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
        let implicit_group_key = Value::List(Vec::new())
            .canonical_key()
            .map_err(crate::db::executor::group::KeyCanonicalError::into_group_error)?;

        self.record_new_group(&implicit_group_key, true, 0, 0)
    }
}

fn estimated_new_group_bytes(
    created_bucket: bool,
    bucket_len: usize,
    bucket_capacity: usize,
) -> u64 {
    let slot_size = size_of::<GroupedAggregateStateSlot>();
    let map_entry_size = if created_bucket {
        size_of::<(StableHash, Vec<GroupedAggregateStateSlot>)>()
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

const fn derived_max_distinct_values_per_group(max_group_bytes: u64) -> u64 {
    let derived = max_group_bytes / 64;
    if derived == 0 { 1 } else { derived }
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
