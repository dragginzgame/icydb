//! Reduced SQL execute-phase diagnostics DTOs.
//! Does not own: raw execute-phase counter construction.

use super::phase::SqlExecutePhaseAttribution;
use candid::CandidType;
use serde::Deserialize;

///
/// SqlExecutionAttribution
///
/// Candid diagnostics payload for the reduced SQL execute phase.
/// Planner, store, executor invocation, executor runtime, and response
/// finalization counters stay together under the `execution` parent field.
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlExecutionAttribution {
    pub planner_local_instructions: u64,
    pub planner_schema_info_local_instructions: u64,
    pub planner_prepare_local_instructions: u64,
    pub planner_cache_key_local_instructions: u64,
    pub planner_cache_lookup_local_instructions: u64,
    pub planner_plan_build_local_instructions: u64,
    pub planner_cache_insert_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
}

impl SqlExecutionAttribution {
    pub(in crate::db::session::sql) const fn from_phase(
        phase: &SqlExecutePhaseAttribution,
    ) -> Self {
        Self {
            planner_local_instructions: phase.planner_local_instructions,
            planner_schema_info_local_instructions: phase.planner_schema_info_local_instructions,
            planner_prepare_local_instructions: phase.planner_prepare_local_instructions,
            planner_cache_key_local_instructions: phase.planner_cache_key_local_instructions,
            planner_cache_lookup_local_instructions: phase.planner_cache_lookup_local_instructions,
            planner_plan_build_local_instructions: phase.planner_plan_build_local_instructions,
            planner_cache_insert_local_instructions: phase.planner_cache_insert_local_instructions,
            store_local_instructions: phase.store_local_instructions,
            executor_invocation_local_instructions: phase.executor_invocation_local_instructions,
            executor_local_instructions: phase.executor_local_instructions,
            response_finalization_local_instructions: phase
                .response_finalization_local_instructions,
        }
    }
}
