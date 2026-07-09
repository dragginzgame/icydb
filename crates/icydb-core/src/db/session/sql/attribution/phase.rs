//! Raw SQL execute-phase diagnostics counters.
//! Does not own: Candid-facing reduced SQL attribution DTOs.

use crate::db::{
    DirectDataRowAttribution, KernelRowAttribution,
    executor::{
        GroupedCountAttribution as ExecutorGroupedCountAttribution, GroupedExecutePhaseAttribution,
        ScalarAggregateTerminalAttribution,
    },
    session::query::QueryPlanCompilePhaseAttribution,
};

///
/// SqlExecutePhaseAttribution
///
/// SqlExecutePhaseAttribution keeps the execute side split into select-plan
/// work, physical store/index access, and narrower runtime execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlExecutePhaseAttribution {
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
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count: ExecutorGroupedCountAttribution,
    pub scalar_aggregate_terminal: ScalarAggregateTerminalAttribution,
    pub direct_data_row: Option<DirectDataRowAttribution>,
    pub kernel_row: Option<KernelRowAttribution>,
}

impl SqlExecutePhaseAttribution {
    /// Build execute-phase attribution from aggregate execute and store totals.
    #[must_use]
    pub(in crate::db) const fn from_execute_total_and_store_total(
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self {
            planner_local_instructions: 0,
            planner_schema_info_local_instructions: 0,
            planner_prepare_local_instructions: 0,
            planner_cache_key_local_instructions: 0,
            planner_cache_lookup_local_instructions: 0,
            planner_plan_build_local_instructions: 0,
            planner_cache_insert_local_instructions: 0,
            store_local_instructions,
            executor_invocation_local_instructions: execute_local_instructions,
            executor_local_instructions: execute_local_instructions
                .saturating_sub(store_local_instructions),
            response_finalization_local_instructions: 0,
            grouped_stream_local_instructions: 0,
            grouped_fold_local_instructions: 0,
            grouped_finalize_local_instructions: 0,
            grouped_count: ExecutorGroupedCountAttribution::none(),
            scalar_aggregate_terminal: ScalarAggregateTerminalAttribution::none(),
            direct_data_row: None,
            kernel_row: None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn from_query_plan_execute_total_and_store_total(
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self::from_execute_total_and_store_total(
            execute_local_instructions,
            store_local_instructions,
        )
        .with_query_plan_compile_attribution(planner_local_instructions, plan_compile_attribution)
    }

    #[must_use]
    pub(in crate::db) const fn from_grouped_select_phase(
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        execute_local_instructions: u64,
        store_local_instructions: u64,
        response_finalization_local_instructions: u64,
        grouped_phase_attribution: GroupedExecutePhaseAttribution,
    ) -> Self {
        let execute_without_response =
            execute_local_instructions.saturating_sub(response_finalization_local_instructions);
        let mut attribution = Self::from_query_plan_execute_total_and_store_total(
            planner_local_instructions,
            plan_compile_attribution,
            execute_without_response,
            store_local_instructions,
        );
        attribution.response_finalization_local_instructions =
            response_finalization_local_instructions;
        attribution.grouped_stream_local_instructions =
            grouped_phase_attribution.stream_local_instructions;
        attribution.grouped_fold_local_instructions =
            grouped_phase_attribution.fold_local_instructions;
        attribution.grouped_finalize_local_instructions =
            grouped_phase_attribution.finalize_local_instructions;
        attribution.grouped_count = grouped_phase_attribution.grouped_count;

        attribution
    }

    #[must_use]
    pub(in crate::db) const fn from_projection_select_phase(
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        execute_local_instructions: u64,
        store_local_instructions: u64,
        response_finalization_local_instructions: u64,
        direct_data_row: Option<DirectDataRowAttribution>,
        kernel_row: Option<KernelRowAttribution>,
    ) -> Self {
        let mut attribution = Self::from_query_plan_execute_total_and_store_total(
            planner_local_instructions,
            plan_compile_attribution,
            execute_local_instructions,
            store_local_instructions,
        );
        attribution.response_finalization_local_instructions =
            response_finalization_local_instructions;
        attribution.direct_data_row = direct_data_row;
        attribution.kernel_row = kernel_row;

        attribution
    }

    #[must_use]
    pub(in crate::db) const fn with_query_plan_compile_attribution(
        mut self,
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
    ) -> Self {
        self.planner_local_instructions = planner_local_instructions;
        self.planner_schema_info_local_instructions = plan_compile_attribution.schema_info;
        self.planner_prepare_local_instructions = plan_compile_attribution.prepare;
        self.planner_cache_key_local_instructions = plan_compile_attribution.cache_key;
        self.planner_cache_lookup_local_instructions = plan_compile_attribution.cache_lookup;
        self.planner_plan_build_local_instructions = plan_compile_attribution.plan_build;
        self.planner_cache_insert_local_instructions = plan_compile_attribution.cache_insert;

        self
    }

    #[must_use]
    pub(in crate::db) const fn with_scalar_aggregate_terminal(
        mut self,
        scalar_aggregate_terminal: ScalarAggregateTerminalAttribution,
    ) -> Self {
        self.scalar_aggregate_terminal = scalar_aggregate_terminal;

        self
    }
}
