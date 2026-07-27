//! Module: db::executor::explain
//! Responsibility: assemble executor-owned EXPLAIN descriptor payloads.
//! Does not own: explain rendering formats or logical plan projection.
//! Boundary: centralized execution-plan-to-descriptor mapping used by EXPLAIN surfaces.

mod descriptor;

use crate::db::{
    TraceReuseEvent,
    executor::EntityAuthority,
    query::{
        admission::{QueryAdmissionLane, QueryAdmissionPolicy, QueryAdmissionSummary},
        explain::{
            ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainOrderPushdown,
            FinalizedQueryDiagnostics,
        },
        intent::{QueryError, StructuralQuery},
        plan::AccessPlannedQuery,
    },
};

use descriptor::assemble_load_execution_verbose_diagnostics_from_route_facts;
#[cfg(feature = "sql-explain")]
pub(in crate::db) use descriptor::assemble_scalar_aggregate_execution_descriptor_with_projection;
pub(in crate::db) use descriptor::{
    assemble_load_execution_node_descriptor_from_route_facts,
    freeze_load_execution_route_facts_for_authority,
};

struct DescriptorStagePresence {
    present: [bool; Self::STAGE_COUNT],
}

impl DescriptorStagePresence {
    const STAGE_COUNT: usize = 4;
    const TOP_N_SEEK: usize = 0;
    const INDEX_RANGE_LIMIT_PUSHDOWN: usize = 1;
    const INDEX_PREDICATE_PREFILTER: usize = 2;
    const RESIDUAL_FILTER: usize = 3;

    fn from_descriptor(descriptor: &ExplainExecutionNodeDescriptor) -> Self {
        let mut presence = Self {
            present: [false; Self::STAGE_COUNT],
        };

        descriptor.for_each_preorder(&mut |node| match node.node_type() {
            ExplainExecutionNodeType::TopNSeek => presence.present[Self::TOP_N_SEEK] = true,
            ExplainExecutionNodeType::IndexRangeLimitPushdown => {
                presence.present[Self::INDEX_RANGE_LIMIT_PUSHDOWN] = true;
            }
            ExplainExecutionNodeType::IndexPredicatePrefilter => {
                presence.present[Self::INDEX_PREDICATE_PREFILTER] = true;
            }
            ExplainExecutionNodeType::ResidualFilter => {
                presence.present[Self::RESIDUAL_FILTER] = true;
            }
            _ => {}
        });

        presence
    }

    const fn has_top_n_seek(&self) -> bool {
        self.present[Self::TOP_N_SEEK]
    }

    const fn has_index_range_limit_pushdown(&self) -> bool {
        self.present[Self::INDEX_RANGE_LIMIT_PUSHDOWN]
    }

    const fn has_index_predicate_prefilter(&self) -> bool {
        self.present[Self::INDEX_PREDICATE_PREFILTER]
    }

    const fn has_residual_filter(&self) -> bool {
        self.present[Self::RESIDUAL_FILTER]
    }
}

impl StructuralQuery {
    // Assemble one finalized diagnostics artifact from route facts that were
    // already frozen by the caller-selected schema authority.
    fn finalized_execution_diagnostics_from_route_facts(
        plan: &AccessPlannedQuery,
        route_facts: &crate::db::executor::explain::descriptor::LoadExecutionRouteFacts,
        reuse: Option<TraceReuseEvent>,
    ) -> Result<FinalizedQueryDiagnostics, QueryError> {
        let descriptor =
            assemble_load_execution_node_descriptor_from_route_facts(plan, route_facts)
                .map_err(QueryError::execute)?;
        let route_diagnostics =
            assemble_load_execution_verbose_diagnostics_from_route_facts(plan, route_facts)
                .map_err(QueryError::execute)?;
        let explain = plan.explain();

        // Phase 1: add descriptor-stage summaries for key execution operators.
        let stage_presence = DescriptorStagePresence::from_descriptor(&descriptor);
        let mut logical_diagnostics = Vec::new();
        logical_diagnostics.push(format!(
            "diag.d.has_top_n_seek={}",
            stage_presence.has_top_n_seek()
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_index_range_limit_pushdown={}",
            stage_presence.has_index_range_limit_pushdown()
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_index_predicate_prefilter={}",
            stage_presence.has_index_predicate_prefilter()
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_residual_filter={}",
            stage_presence.has_residual_filter()
        ));

        // Phase 2: append logical-plan diagnostics relevant to verbose explain.
        logical_diagnostics.push(format!("diag.p.mode={:?}", explain.mode()));
        logical_diagnostics.push(format!(
            "diag.p.order_pushdown={}",
            plan_order_pushdown_label(explain.order_pushdown())
        ));
        logical_diagnostics.push(format!(
            "diag.p.predicate_pushdown={}",
            plan.predicate_pushdown_label()
        ));
        logical_diagnostics.push(format!(
            "diag.p.predicate_pushdown_outcome={}",
            plan.predicate_pushdown_outcome_label()
        ));
        logical_diagnostics.push(format!(
            "diag.p.predicate_pushdown_reason={}",
            plan.predicate_pushdown_reason_label()
        ));
        logical_diagnostics.push(format!("diag.p.distinct={}", explain.distinct()));
        logical_diagnostics.push(format!("diag.p.page={:?}", explain.page()));
        logical_diagnostics.push(format!("diag.p.consistency={:?}", explain.consistency()));

        let admission = QueryAdmissionPolicy::diagnostic_explain().evaluate(
            QueryAdmissionSummary::from_plan(QueryAdmissionLane::DiagnosticExplain, plan),
        );

        Ok(FinalizedQueryDiagnostics::new(
            descriptor,
            route_diagnostics,
            logical_diagnostics,
            reuse,
        )
        .with_admission(admission))
    }

    // Assemble one execution descriptor from accepted executor authority.

    /// Freeze one immutable diagnostics artifact through accepted executor
    /// authority while still allowing one caller-owned descriptor mutation.
    pub(in crate::db) fn finalized_execution_diagnostics_from_plan_with_authority_and_descriptor_mutator(
        plan: &AccessPlannedQuery,
        authority: &EntityAuthority,
        reuse: Option<TraceReuseEvent>,
        mutate_descriptor: impl FnOnce(&mut ExplainExecutionNodeDescriptor),
    ) -> Result<FinalizedQueryDiagnostics, QueryError> {
        let route_facts = freeze_load_execution_route_facts_for_authority(authority, plan)
            .map_err(QueryError::execute)?;
        let mut diagnostics =
            Self::finalized_execution_diagnostics_from_route_facts(plan, &route_facts, reuse)?;
        mutate_descriptor(&mut diagnostics.execution);

        Ok(diagnostics)
    }
}

// Render the logical ORDER pushdown label for verbose execution diagnostics.
fn plan_order_pushdown_label(order_pushdown: &ExplainOrderPushdown) -> String {
    match order_pushdown {
        ExplainOrderPushdown::MissingModelContext => "missing_model_context".to_string(),
        ExplainOrderPushdown::EligibleSecondaryIndex { index, prefix_len } => {
            format!("eligible(index={index},prefix_len={prefix_len})")
        }
        ExplainOrderPushdown::Rejected(reason) => format!("rejected({reason:?})"),
    }
}
