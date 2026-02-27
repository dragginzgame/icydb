mod hash;
mod key;

pub(in crate::db) use hash::{StableHash, stable_hash_value};
#[cfg(test)]
pub(in crate::db) use key::CanonicalKey;
pub(in crate::db) use key::{GroupKey, GroupKeySet, KeyCanonicalError};

///
/// GROUPED EXECUTION SCAFFOLD
///
/// WIP ownership note:
/// GROUP BY is intentionally isolated behind this module for now.
/// Keep grouped scaffold code behind this boundary for the time being and do not remove it.
///
/// Explicit ownership boundary for grouped execution-route/reducer scaffold.
/// Grouped execution contracts are re-exported here so grouped runtime work has
/// one obvious executor entrypoint.
///
use crate::db::{
    executor::aggregate::{ExecutionConfig, ExecutionContext},
    query::grouped::GroupedExecutionConfig,
};

const GROUPED_DEFAULT_MAX_GROUPS: u64 = 10_000;
const GROUPED_DEFAULT_MAX_GROUP_BYTES: u64 = 16 * 1024 * 1024;

///
/// default_grouped_execution_config
///
/// Build one default grouped execution hard-limit policy.
/// Grouped execution remains disabled in this release, so defaults are
/// intentionally conservative and bounded until planner-owned policy tuning.
///
#[must_use]
pub(in crate::db::executor) const fn default_grouped_execution_config() -> ExecutionConfig {
    ExecutionConfig::with_hard_limits(GROUPED_DEFAULT_MAX_GROUPS, GROUPED_DEFAULT_MAX_GROUP_BYTES)
}

///
/// grouped_execution_config_from_planner_config
///
/// Resolve one executor grouped hard-limit policy from optional planner config.
/// Executor owns final policy resolution so defaults remain centralized even
/// when planner does not provide explicit grouped limits.
///
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

///
/// grouped_execution_context_from_planner_config
///
/// Build one grouped execution context from optional planner-side limits.
/// This keeps context creation routed through one executor-owned policy bridge.
///
#[must_use]
pub(in crate::db::executor) const fn grouped_execution_context_from_planner_config(
    planner_config: Option<GroupedExecutionConfig>,
) -> ExecutionContext {
    ExecutionContext::new(grouped_execution_config_from_planner_config(planner_config))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        GroupedExecutionConfig, grouped_execution_config_from_planner_config,
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
}
