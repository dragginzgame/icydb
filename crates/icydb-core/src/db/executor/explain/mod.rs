//! Module: db::executor::explain
//! Responsibility: assemble executor-owned EXPLAIN descriptor payloads.
//! Does not own: explain rendering formats or logical plan projection.
//! Boundary: centralized execution-plan-to-descriptor mapping used by EXPLAIN surfaces.

mod descriptor;

#[cfg(test)]
use crate::db::executor::planning::route::AggregateRouteShape;
#[cfg(test)]
use crate::db::query::builder::AggregateExpr;
use crate::{
    db::{
        Query, TraceReuseEvent,
        executor::{BytesByProjectionMode, EntityAuthority, PreparedExecutionPlan},
        query::{
            builder::{AggregateExplain, ProjectionExplain},
            explain::{
                ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor,
                ExplainExecutionNodeType, ExplainOrderPushdown, FinalizedQueryDiagnostics,
                property_keys,
            },
            intent::{QueryError, StructuralQuery},
            plan::{AccessPlannedQuery, VisibleIndexes},
        },
    },
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue},
    value::Value,
};

#[cfg(test)]
pub(in crate::db) use descriptor::assemble_load_execution_node_descriptor;
use descriptor::assemble_load_execution_verbose_diagnostics_from_route_facts;
#[cfg(feature = "sql")]
pub(in crate::db) use descriptor::assemble_scalar_aggregate_execution_descriptor_with_projection;
pub(in crate::db) use descriptor::{
    assemble_aggregate_terminal_execution_descriptor,
    assemble_load_execution_node_descriptor_for_authority,
    assemble_load_execution_node_descriptor_from_route_facts,
    freeze_load_execution_route_facts_for_authority,
    freeze_load_execution_route_facts_for_model_only,
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
    ) -> FinalizedQueryDiagnostics {
        let descriptor =
            assemble_load_execution_node_descriptor_from_route_facts(plan, route_facts);
        let route_diagnostics =
            assemble_load_execution_verbose_diagnostics_from_route_facts(plan, route_facts);
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

        FinalizedQueryDiagnostics::new(descriptor, route_diagnostics, logical_diagnostics, reuse)
    }

    // Assemble one model-only execution descriptor from a previously built
    // access plan so standalone text/json/verbose explain surfaces do not each
    // rebuild it.
    pub(in crate::db) fn explain_execution_descriptor_from_model_only_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let primary_key_names = model_primary_key_names(self.model());
        let route_facts = freeze_load_execution_route_facts_for_model_only(
            self.model().fields(),
            primary_key_names.as_slice(),
            plan,
        )
        .map_err(QueryError::execute)?;

        Ok(assemble_load_execution_node_descriptor_from_route_facts(
            plan,
            &route_facts,
        ))
    }

    // Assemble one execution descriptor from accepted executor authority.
    pub(in crate::db) fn explain_execution_descriptor_from_plan_with_authority(
        &self,
        plan: &AccessPlannedQuery,
        authority: &EntityAuthority,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        debug_assert_eq!(self.model().path(), authority.entity_path());
        let route_facts = freeze_load_execution_route_facts_for_authority(authority, plan)
            .map_err(QueryError::execute)?;

        Ok(assemble_load_execution_node_descriptor_from_route_facts(
            plan,
            &route_facts,
        ))
    }

    // Render one standalone model-only verbose execution explain payload from
    // a single access plan, freezing one immutable diagnostics artifact instead
    // of returning one wrapper-owned line list that callers still have to
    // extend locally.
    fn finalized_execution_diagnostics_from_model_only_plan(
        &self,
        plan: &AccessPlannedQuery,
        reuse: Option<TraceReuseEvent>,
    ) -> Result<FinalizedQueryDiagnostics, QueryError> {
        let primary_key_names = model_primary_key_names(self.model());
        let route_facts = freeze_load_execution_route_facts_for_model_only(
            self.model().fields(),
            primary_key_names.as_slice(),
            plan,
        )
        .map_err(QueryError::execute)?;

        Ok(Self::finalized_execution_diagnostics_from_route_facts(
            plan,
            &route_facts,
            reuse,
        ))
    }

    /// Freeze one immutable diagnostics artifact through accepted executor
    /// authority while still allowing one caller-owned descriptor mutation.
    pub(in crate::db) fn finalized_execution_diagnostics_from_plan_with_authority_and_descriptor_mutator(
        &self,
        plan: &AccessPlannedQuery,
        authority: &EntityAuthority,
        reuse: Option<TraceReuseEvent>,
        mutate_descriptor: impl FnOnce(&mut ExplainExecutionNodeDescriptor),
    ) -> Result<FinalizedQueryDiagnostics, QueryError> {
        debug_assert_eq!(self.model().path(), authority.entity_path());
        let route_facts = freeze_load_execution_route_facts_for_authority(authority, plan)
            .map_err(QueryError::execute)?;
        let mut diagnostics =
            Self::finalized_execution_diagnostics_from_route_facts(plan, &route_facts, reuse);
        mutate_descriptor(&mut diagnostics.execution);

        Ok(diagnostics)
    }

    // Render one verbose execution explain payload using only the canonical
    // diagnostics artifact owned by this executor boundary.
    fn explain_execution_verbose_from_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Result<String, QueryError> {
        self.finalized_execution_diagnostics_from_model_only_plan(plan, None)
            .map(|diagnostics| diagnostics.render_text_verbose())
    }

    // Freeze one explain-only access-choice snapshot from accepted
    // planner-visible indexes before building descriptor diagnostics.
    fn finalize_explain_access_choice_for_visible_indexes(
        &self,
        plan: &mut AccessPlannedQuery,
        visible_indexes: &VisibleIndexes<'_>,
    ) {
        if let Some(schema_info) = visible_indexes.accepted_schema_info() {
            plan.finalize_access_choice_for_model_with_semantic_indexes_and_schema(
                self.model(),
                visible_indexes.accepted_semantic_index_contracts(),
                schema_info,
            );
            return;
        }

        plan.finalize_access_choice_for_model_only_with_indexes(
            self.model(),
            visible_indexes.generated_model_only_indexes(),
        );
    }

    // Freeze one explicit model-only access-choice snapshot for standalone
    // query explain surfaces that intentionally do not have accepted runtime
    // schema authority.
    fn finalize_explain_access_choice_for_model_only(&self, plan: &mut AccessPlannedQuery) {
        plan.finalize_access_choice_for_model_only_with_indexes(
            self.model(),
            self.model().indexes(),
        );
    }

    // Build and freeze one explicit model-only access-plan snapshot for
    // standalone query explain surfaces.
    fn finalized_model_only_explain_plan(&self) -> Result<AccessPlannedQuery, QueryError> {
        let mut plan = self.build_plan()?;
        self.finalize_explain_access_choice_for_model_only(&mut plan);

        Ok(plan)
    }

    // Build and freeze one access-plan snapshot using a caller-provided
    // visible-index slice for runtime/session explain surfaces.
    fn finalized_visible_indexes_explain_plan(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        let mut plan = self.build_plan_with_visible_indexes(visible_indexes)?;
        self.finalize_explain_access_choice_for_visible_indexes(&mut plan, visible_indexes);

        Ok(plan)
    }

    // Build one explicit model-only execution descriptor for standalone query
    // surfaces that are not bound to a recovered store/accepted schema.
    fn explain_execution_descriptor_for_model_only(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let plan = self.finalized_model_only_explain_plan()?;

        self.explain_execution_descriptor_from_model_only_plan(&plan)
    }

    // Build one execution descriptor using the caller-resolved accepted visible
    // indexes for runtime/session explain.
    fn explain_execution_descriptor_for_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let plan = self.finalized_visible_indexes_explain_plan(visible_indexes)?;

        self.explain_execution_descriptor_from_model_only_plan(&plan)
    }

    // Render one explicit model-only verbose execution payload for standalone
    // query surfaces that are not bound to a recovered store/accepted schema.
    fn render_execution_verbose_for_model_only(&self) -> Result<String, QueryError> {
        let plan = self.finalized_model_only_explain_plan()?;

        self.explain_execution_verbose_from_plan(&plan)
    }

    // Render one verbose execution payload using the caller-resolved accepted
    // visible indexes for runtime/session explain.
    fn explain_execution_verbose_for_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        let plan = self.finalized_visible_indexes_explain_plan(visible_indexes)?;

        self.explain_execution_verbose_from_plan(&plan)
    }

    /// Explain one model-only load execution shape through the structural query core.
    #[inline(never)]
    pub(in crate::db) fn explain_execution_for_model_only(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        self.explain_execution_descriptor_for_model_only()
    }

    /// Explain one load execution shape using a caller-visible index slice.
    #[inline(never)]
    pub(in crate::db) fn explain_execution_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        self.explain_execution_descriptor_for_visible_indexes(visible_indexes)
    }

    /// Render one model-only verbose scalar load execution payload through the
    /// shared structural descriptor and route-diagnostics paths.
    #[inline(never)]
    pub(in crate::db) fn explain_execution_verbose_for_model_only(
        &self,
    ) -> Result<String, QueryError> {
        self.render_execution_verbose_for_model_only()
    }

    /// Render one verbose scalar load execution payload using visible indexes.
    #[inline(never)]
    pub(in crate::db) fn explain_execution_verbose_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        self.explain_execution_verbose_for_visible_indexes(visible_indexes)
    }

    /// Explain one aggregate terminal execution route without running it.
    #[inline(never)]
    #[cfg(test)]
    pub(in crate::db) fn explain_aggregate_terminal_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        aggregate: AggregateRouteShape<'_>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError> {
        let plan = self.build_plan_with_visible_indexes(visible_indexes)?;
        let query_explain = plan.explain();
        let terminal = aggregate.kind();
        let execution = assemble_aggregate_terminal_execution_descriptor(&plan, aggregate);

        Ok(ExplainAggregateTerminalPlan::new(
            query_explain,
            terminal,
            execution,
        ))
    }
}

impl<E> PreparedExecutionPlan<E>
where
    E: EntityValue + EntityKind,
{
    /// Explain one cached prepared aggregate terminal route without running it.
    pub(in crate::db) fn explain_prepared_aggregate_terminal<S>(
        &self,
        strategy: &S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        S: AggregateExplain,
    {
        let Some(kind) = strategy.explain_aggregate_kind() else {
            return Err(QueryError::invariant());
        };
        let aggregate = self
            .authority()
            .aggregate_route_shape(kind, strategy.explain_projected_field())
            .map_err(QueryError::execute)?;
        let execution =
            assemble_aggregate_terminal_execution_descriptor(self.logical_plan(), aggregate);

        Ok(ExplainAggregateTerminalPlan::new(
            self.logical_plan().explain(),
            kind,
            execution,
        ))
    }

    fn explain_prepared_terminal_load_descriptor(
        &self,
        terminal_label: &str,
        field_label: &str,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let mut descriptor = self
            .explain_load_execution_node_descriptor()
            .map_err(QueryError::execute)?;

        descriptor
            .node_properties
            .insert(property_keys::TERMINAL, Value::from(terminal_label));
        descriptor.node_properties.insert(
            property_keys::TERMINAL_FIELD,
            Value::from(field_label.to_string()),
        );

        Ok(descriptor)
    }

    /// Explain one cached prepared `bytes_by(field)` terminal route without running it.
    pub(in crate::db) fn explain_bytes_by_terminal(
        &self,
        target_field: &str,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let mut descriptor =
            self.explain_prepared_terminal_load_descriptor("bytes_by", target_field)?;
        let projection_mode = self.bytes_by_projection_mode(target_field);
        let projection_mode_label = Self::bytes_by_projection_mode_label(projection_mode);

        descriptor.node_properties.insert(
            property_keys::TERMINAL_PROJECTION_MODE,
            Value::from(projection_mode_label),
        );
        descriptor.node_properties.insert(
            property_keys::TERMINAL_INDEX_ONLY,
            Value::from(matches!(
                projection_mode,
                BytesByProjectionMode::CoveringIndex | BytesByProjectionMode::CoveringConstant
            )),
        );

        Ok(descriptor)
    }

    /// Explain one cached prepared projection terminal route without running it.
    pub(in crate::db) fn explain_prepared_projection_terminal<S>(
        &self,
        strategy: &S,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        S: ProjectionExplain,
    {
        let projection_descriptor = strategy.explain_projection_descriptor();
        let mut descriptor = self.explain_prepared_terminal_load_descriptor(
            projection_descriptor.terminal_label(),
            projection_descriptor.field_label(),
        )?;
        descriptor.node_properties.insert(
            property_keys::TERMINAL_OUTPUT,
            Value::from(projection_descriptor.output_label()),
        );

        Ok(descriptor)
    }
}

impl<E> Query<E>
where
    E: EntityValue + EntityKind,
{
    // Resolve the structural execution descriptor through either the explicit
    // model-only lane or one caller-provided visible-index slice.
    fn explain_execution_descriptor_for_model_only_or_visible_indexes(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        match visible_indexes {
            Some(visible_indexes) => self
                .structural()
                .explain_execution_with_visible_indexes(visible_indexes),
            None => self.structural().explain_execution_for_model_only(),
        }
    }

    // Render one descriptor-derived execution surface after resolving the
    // visibility slice once at the typed query boundary.
    fn render_execution_descriptor_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
        render: impl FnOnce(ExplainExecutionNodeDescriptor) -> String,
    ) -> Result<String, QueryError> {
        let descriptor =
            self.explain_execution_descriptor_for_model_only_or_visible_indexes(visible_indexes)?;

        Ok(render(descriptor))
    }

    // Render one verbose execution explain payload after choosing the explicit
    // model-only lane or the accepted visible-index lane once.
    fn explain_execution_verbose_for_model_only_or_visible_indexes(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<String, QueryError> {
        match visible_indexes {
            Some(visible_indexes) => self
                .structural()
                .explain_execution_verbose_with_visible_indexes(visible_indexes),
            None => self.structural().explain_execution_verbose_for_model_only(),
        }
    }

    /// Explain executor-selected load execution shape without running it.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        self.explain_execution_descriptor_for_model_only_or_visible_indexes(None)
    }

    /// Explain executor-selected load execution shape with caller-visible indexes.
    #[cfg(test)]
    pub(in crate::db) fn explain_execution_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        self.explain_execution_descriptor_for_model_only_or_visible_indexes(Some(visible_indexes))
    }

    /// Explain executor-selected load execution shape as deterministic text.
    pub fn explain_execution_text(&self) -> Result<String, QueryError> {
        self.render_execution_descriptor_for_visibility(None, |descriptor| {
            descriptor.render_text_tree()
        })
    }

    /// Explain executor-selected load execution shape as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, QueryError> {
        self.render_execution_descriptor_for_visibility(None, |descriptor| {
            descriptor.render_json_canonical()
        })
    }

    /// Explain executor-selected load execution shape with route diagnostics.
    #[inline(never)]
    pub fn explain_execution_verbose(&self) -> Result<String, QueryError> {
        self.explain_execution_verbose_for_model_only_or_visible_indexes(None)
    }

    /// Explain one aggregate terminal execution route without running it.
    #[cfg(test)]
    #[inline(never)]
    pub(in crate::db) fn explain_aggregate_terminal(
        &self,
        aggregate: AggregateExpr,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError> {
        self.structural()
            .explain_aggregate_terminal_with_visible_indexes(
                &VisibleIndexes::generated_model_only(E::MODEL.indexes()),
                AggregateRouteShape::new_from_fields(
                    aggregate.kind(),
                    aggregate.target_field(),
                    E::MODEL.fields(),
                    model_primary_key_names(E::MODEL).as_slice(),
                ),
            )
    }
}

fn model_primary_key_names(model: &EntityModel) -> Vec<&'static str> {
    model
        .primary_key_model()
        .fields()
        .iter()
        .map(crate::model::field::FieldModel::name)
        .collect()
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
