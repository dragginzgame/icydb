//! Module: executor::group
//! Responsibility: grouped key canonicalization, hashing, and grouped budget policies.
//! Does not own: aggregate fold algorithms or logical planner validation.
//! Boundary: grouped execution substrate shared by grouped load/aggregate paths.

#[cfg(test)]
mod tests;

mod hash;
mod key;

pub(in crate::db) use hash::{StableHash, stable_hash_value};
pub(in crate::db) use key::{
    CanonicalKey, GroupKey, GroupKeySet, KeyCanonicalError, canonical_group_key_equals,
};

///
/// Grouped execution ownership boundary.
///
/// This module owns grouped key canonicalization/hashing plus grouped
/// execution budget policy translation between query planning and executor
/// runtime contracts.
///
use crate::db::{
    executor::aggregate::{ExecutionConfig, ExecutionContext},
    query::plan::GroupedExecutionConfig,
};

const GROUPED_DEFAULT_MAX_GROUPS: u64 = 10_000;
const GROUPED_DEFAULT_MAX_GROUP_BYTES: u64 = 16 * 1024 * 1024;

///
/// GroupedBudgetObservability
///
/// Grouped budget counters and hard limits projected for observability.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedBudgetObservability {
    groups: u64,
    aggregate_states: u64,
    estimated_bytes: u64,
    max_groups: u64,
    max_group_bytes: u64,
}

impl GroupedBudgetObservability {
    /// Return observed group-count usage.
    #[must_use]
    pub(in crate::db::executor) const fn groups(self) -> u64 {
        self.groups
    }

    /// Return observed aggregate-state counter.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_states(self) -> u64 {
        self.aggregate_states
    }

    /// Return observed grouped-budget byte estimate.
    #[must_use]
    pub(in crate::db::executor) const fn estimated_bytes(self) -> u64 {
        self.estimated_bytes
    }

    /// Return configured max group count.
    #[must_use]
    pub(in crate::db::executor) const fn max_groups(self) -> u64 {
        self.max_groups
    }

    /// Return configured max grouped-budget bytes.
    #[must_use]
    pub(in crate::db::executor) const fn max_group_bytes(self) -> u64 {
        self.max_group_bytes
    }
}

/// default_grouped_execution_config
///
/// Build one default grouped execution hard-limit policy.
/// Defaults remain conservative and bounded until planner-owned policy tuning.
#[must_use]
pub(in crate::db::executor) const fn default_grouped_execution_config() -> ExecutionConfig {
    ExecutionConfig::with_hard_limits(GROUPED_DEFAULT_MAX_GROUPS, GROUPED_DEFAULT_MAX_GROUP_BYTES)
}

/// grouped_execution_config_from_planner_config
///
/// Resolve one executor grouped hard-limit policy from optional planner config.
/// Executor owns final policy resolution so defaults remain centralized even
/// when planner does not provide explicit grouped limits.
#[must_use]
pub(in crate::db::executor) const fn grouped_execution_config_from_planner_config(
    planner_config: Option<GroupedExecutionConfig>,
) -> ExecutionConfig {
    let Some(planner_config) = planner_config else {
        return default_grouped_execution_config();
    };

    ExecutionConfig::with_hard_limits(
        planner_config.max_groups(),
        planner_config.max_group_bytes(),
    )
}

/// grouped_execution_context_from_planner_config
///
/// Build one grouped execution context from optional planner-side limits.
///
#[must_use]
pub(in crate::db::executor) const fn grouped_execution_context_from_planner_config(
    planner_config: Option<GroupedExecutionConfig>,
) -> ExecutionContext {
    ExecutionContext::new(grouped_execution_config_from_planner_config(planner_config))
}

/// grouped_budget_observability
///
/// Project grouped budget counters and hard limits for route/metrics reporting.
#[must_use]
pub(in crate::db::executor) const fn grouped_budget_observability(
    context: &ExecutionContext,
) -> GroupedBudgetObservability {
    GroupedBudgetObservability {
        groups: context.budget().groups(),
        aggregate_states: context.budget().aggregate_states(),
        estimated_bytes: context.budget().estimated_bytes(),
        max_groups: context.config().max_groups(),
        max_group_bytes: context.config().max_group_bytes(),
    }
}
