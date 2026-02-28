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
    #[must_use]
    pub(in crate::db::executor) const fn groups(self) -> u64 {
        self.groups
    }

    #[must_use]
    pub(in crate::db::executor) const fn aggregate_states(self) -> u64 {
        self.aggregate_states
    }

    #[must_use]
    pub(in crate::db::executor) const fn estimated_bytes(self) -> u64 {
        self.estimated_bytes
    }

    #[must_use]
    pub(in crate::db::executor) const fn max_groups(self) -> u64 {
        self.max_groups
    }

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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        GroupedExecutionConfig, grouped_budget_observability,
        grouped_execution_config_from_planner_config,
        grouped_execution_context_from_planner_config,
    };

    #[test]
    fn grouped_execution_config_from_planner_config_prefers_planner_limits() {
        let config = grouped_execution_config_from_planner_config(Some(
            GroupedExecutionConfig::with_hard_limits(11, 2048),
        ));

        assert_eq!(config.max_groups(), 11);
        assert_eq!(config.max_group_bytes(), 2048);
    }

    #[test]
    fn grouped_execution_context_from_planner_config_defaults_when_absent() {
        let context = grouped_execution_context_from_planner_config(None);

        assert_eq!(context.config().max_groups(), 10_000);
        assert_eq!(context.config().max_group_bytes(), 16 * 1024 * 1024);
        assert_eq!(context.budget().groups(), 0);
        assert_eq!(context.budget().aggregate_states(), 0);
        assert_eq!(context.budget().estimated_bytes(), 0);
    }

    #[test]
    fn grouped_budget_observability_projects_budget_and_limits() {
        let context = grouped_execution_context_from_planner_config(Some(
            GroupedExecutionConfig::with_hard_limits(11, 2048),
        ));
        let budget = grouped_budget_observability(&context);

        assert_eq!(budget.groups(), 0);
        assert_eq!(budget.aggregate_states(), 0);
        assert_eq!(budget.estimated_bytes(), 0);
        assert_eq!(budget.max_groups(), 11);
        assert_eq!(budget.max_group_bytes(), 2048);
    }

    #[test]
    fn grouped_budget_observability_contract_vectors_are_frozen() {
        let default_context = grouped_execution_context_from_planner_config(None);
        let constrained_context = grouped_execution_context_from_planner_config(Some(
            GroupedExecutionConfig::with_hard_limits(11, 2048),
        ));
        let actual_vectors = vec![
            grouped_budget_observability(&default_context),
            grouped_budget_observability(&constrained_context),
        ]
        .into_iter()
        .map(|budget| {
            (
                budget.groups(),
                budget.aggregate_states(),
                budget.estimated_bytes(),
                budget.max_groups(),
                budget.max_group_bytes(),
            )
        })
        .collect::<Vec<_>>();
        let expected_vectors = vec![(0, 0, 0, 10_000, 16 * 1024 * 1024), (0, 0, 0, 11, 2048)];

        assert_eq!(actual_vectors, expected_vectors);
    }
}
