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
        predicate::{CoercionId, CompareOp},
        query::{
            builder::{AggregateExplain, ProjectionExplain},
            explain::{
                ExplainAccessPath, ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor,
                ExplainExecutionNodeType, ExplainOrderPushdown, ExplainPredicate,
                FinalizedQueryDiagnostics,
            },
            intent::{QueryError, StructuralQuery},
            plan::{AccessPlannedQuery, VisibleIndexes, explain_access_kind_label},
        },
    },
    traits::{EntityKind, EntityValue},
    value::Value,
};

#[cfg(test)]
pub(in crate::db) use descriptor::assemble_load_execution_node_descriptor;
use descriptor::assemble_load_execution_verbose_diagnostics_from_route_facts;
pub(in crate::db) use descriptor::{
    assemble_aggregate_terminal_execution_descriptor,
    assemble_load_execution_node_descriptor_for_authority,
    assemble_load_execution_node_descriptor_from_route_facts,
    assemble_scalar_aggregate_execution_descriptor_with_projection,
    freeze_load_execution_route_facts_for_authority,
    freeze_load_execution_route_facts_for_model_only,
};

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
        let mut logical_diagnostics = Vec::new();
        logical_diagnostics.push(format!(
            "diag.d.has_top_n_seek={}",
            descriptor.contains_type(ExplainExecutionNodeType::TopNSeek)
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_index_range_limit_pushdown={}",
            descriptor.contains_type(ExplainExecutionNodeType::IndexRangeLimitPushdown)
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_index_predicate_prefilter={}",
            descriptor.contains_type(ExplainExecutionNodeType::IndexPredicatePrefilter)
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_residual_filter={}",
            descriptor.contains_type(ExplainExecutionNodeType::ResidualFilter)
        ));

        // Phase 2: append logical-plan diagnostics relevant to verbose explain.
        logical_diagnostics.push(format!("diag.p.mode={:?}", explain.mode()));
        logical_diagnostics.push(format!(
            "diag.p.order_pushdown={}",
            plan_order_pushdown_label(explain.order_pushdown())
        ));
        logical_diagnostics.push(format!(
            "diag.p.predicate_pushdown={}",
            plan_predicate_pushdown_label(explain.predicate(), explain.access())
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
        let route_facts = freeze_load_execution_route_facts_for_model_only(
            self.model().fields(),
            self.model().primary_key().name(),
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
        let route_facts = freeze_load_execution_route_facts_for_model_only(
            self.model().fields(),
            self.model().primary_key().name(),
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
            plan.finalize_access_choice_for_model_with_accepted_indexes_and_schema(
                self.model(),
                visible_indexes.generated_static_bridge_indexes(),
                visible_indexes.accepted_field_path_indexes(),
                schema_info,
            );
            return;
        }

        plan.finalize_access_choice_for_model_only_with_indexes(
            self.model(),
            visible_indexes.generated_static_bridge_indexes(),
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

    // Build one explicit model-only execution descriptor for standalone query
    // surfaces that are not bound to a recovered store/accepted schema.
    fn explain_execution_descriptor_for_model_only(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let mut plan = self.build_plan()?;
        self.finalize_explain_access_choice_for_model_only(&mut plan);

        self.explain_execution_descriptor_from_model_only_plan(&plan)
    }

    // Build one execution descriptor using the caller-resolved accepted visible
    // indexes for runtime/session explain.
    fn explain_execution_descriptor_for_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let mut plan = self.build_plan_with_visible_indexes(visible_indexes)?;
        self.finalize_explain_access_choice_for_visible_indexes(&mut plan, visible_indexes);

        self.explain_execution_descriptor_from_model_only_plan(&plan)
    }

    // Render one explicit model-only verbose execution payload for standalone
    // query surfaces that are not bound to a recovered store/accepted schema.
    fn render_execution_verbose_for_model_only(&self) -> Result<String, QueryError> {
        let mut plan = self.build_plan()?;
        self.finalize_explain_access_choice_for_model_only(&mut plan);

        self.explain_execution_verbose_from_plan(&plan)
    }

    // Render one verbose execution payload using the caller-resolved accepted
    // visible indexes for runtime/session explain.
    fn explain_execution_verbose_for_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        let mut plan = self.build_plan_with_visible_indexes(visible_indexes)?;
        self.finalize_explain_access_choice_for_visible_indexes(&mut plan, visible_indexes);

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
            return Err(QueryError::invariant(
                "prepared fluent aggregate explain requires an explain-visible aggregate kind",
            ));
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

    /// Explain one cached prepared `bytes_by(field)` terminal route without running it.
    pub(in crate::db) fn explain_bytes_by_terminal(
        &self,
        target_field: &str,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let mut descriptor = self
            .explain_load_execution_node_descriptor()
            .map_err(QueryError::execute)?;
        let projection_mode = self.bytes_by_projection_mode(target_field);
        let projection_mode_label = Self::bytes_by_projection_mode_label(projection_mode);

        descriptor
            .node_properties
            .insert("terminal", Value::from("bytes_by"));
        descriptor
            .node_properties
            .insert("terminal_field", Value::from(target_field.to_string()));
        descriptor.node_properties.insert(
            "terminal_projection_mode",
            Value::from(projection_mode_label),
        );
        descriptor.node_properties.insert(
            "terminal_index_only",
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
        let mut descriptor = self
            .explain_load_execution_node_descriptor()
            .map_err(QueryError::execute)?;
        let projection_descriptor = strategy.explain_projection_descriptor();

        descriptor.node_properties.insert(
            "terminal",
            Value::from(projection_descriptor.terminal_label()),
        );
        descriptor.node_properties.insert(
            "terminal_field",
            Value::from(projection_descriptor.field_label().to_string()),
        );
        descriptor.node_properties.insert(
            "terminal_output",
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
                &VisibleIndexes::schema_owned(E::MODEL.indexes()),
                AggregateRouteShape::new_from_fields(
                    aggregate.kind(),
                    aggregate.target_field(),
                    E::MODEL.fields(),
                    E::MODEL.primary_key().name(),
                ),
            )
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

// Render the logical predicate pushdown label for verbose execution diagnostics.
fn plan_predicate_pushdown_label(
    predicate: &ExplainPredicate,
    access: &ExplainAccessPath,
) -> String {
    let access_label = explain_access_kind_label(access);
    if matches!(predicate, ExplainPredicate::None) {
        return "none".to_string();
    }
    if access_label == "full_scan" {
        if explain_predicate_contains_non_strict_compare(predicate) {
            return "fallback(non_strict_compare_coercion)".to_string();
        }
        if explain_predicate_contains_empty_prefix_starts_with(predicate) {
            return "fallback(starts_with_empty_prefix)".to_string();
        }
        if explain_predicate_contains_is_null(predicate) {
            return "fallback(is_null_full_scan)".to_string();
        }
        if explain_predicate_contains_text_scan_operator(predicate) {
            return "fallback(text_operator_full_scan)".to_string();
        }

        return format!("fallback({access_label})");
    }

    format!("applied({access_label})")
}

// Detect predicates that force non-strict compare fallback diagnostics.
fn explain_predicate_contains_non_strict_compare(predicate: &ExplainPredicate) -> bool {
    match predicate {
        ExplainPredicate::Compare { coercion, .. }
        | ExplainPredicate::CompareFields { coercion, .. } => coercion.id != CoercionId::Strict,
        ExplainPredicate::And(children) | ExplainPredicate::Or(children) => children
            .iter()
            .any(explain_predicate_contains_non_strict_compare),
        ExplainPredicate::Not(inner) => explain_predicate_contains_non_strict_compare(inner),
        ExplainPredicate::None
        | ExplainPredicate::True
        | ExplainPredicate::False
        | ExplainPredicate::IsNull { .. }
        | ExplainPredicate::IsNotNull { .. }
        | ExplainPredicate::IsMissing { .. }
        | ExplainPredicate::IsEmpty { .. }
        | ExplainPredicate::IsNotEmpty { .. }
        | ExplainPredicate::TextContains { .. }
        | ExplainPredicate::TextContainsCi { .. } => false,
    }
}

// Detect IS NULL predicates that force full-scan fallback diagnostics.
fn explain_predicate_contains_is_null(predicate: &ExplainPredicate) -> bool {
    match predicate {
        ExplainPredicate::IsNull { .. } => true,
        ExplainPredicate::And(children) | ExplainPredicate::Or(children) => {
            children.iter().any(explain_predicate_contains_is_null)
        }
        ExplainPredicate::Not(inner) => explain_predicate_contains_is_null(inner),
        ExplainPredicate::None
        | ExplainPredicate::True
        | ExplainPredicate::False
        | ExplainPredicate::Compare { .. }
        | ExplainPredicate::CompareFields { .. }
        | ExplainPredicate::IsNotNull { .. }
        | ExplainPredicate::IsMissing { .. }
        | ExplainPredicate::IsEmpty { .. }
        | ExplainPredicate::IsNotEmpty { .. }
        | ExplainPredicate::TextContains { .. }
        | ExplainPredicate::TextContainsCi { .. } => false,
    }
}

// Detect empty starts_with predicates that force fallback diagnostics.
fn explain_predicate_contains_empty_prefix_starts_with(predicate: &ExplainPredicate) -> bool {
    match predicate {
        ExplainPredicate::Compare {
            op: CompareOp::StartsWith,
            value: Value::Text(prefix),
            ..
        } => prefix.is_empty(),
        ExplainPredicate::And(children) | ExplainPredicate::Or(children) => children
            .iter()
            .any(explain_predicate_contains_empty_prefix_starts_with),
        ExplainPredicate::Not(inner) => explain_predicate_contains_empty_prefix_starts_with(inner),
        ExplainPredicate::None
        | ExplainPredicate::True
        | ExplainPredicate::False
        | ExplainPredicate::Compare { .. }
        | ExplainPredicate::CompareFields { .. }
        | ExplainPredicate::IsNull { .. }
        | ExplainPredicate::IsNotNull { .. }
        | ExplainPredicate::IsMissing { .. }
        | ExplainPredicate::IsEmpty { .. }
        | ExplainPredicate::IsNotEmpty { .. }
        | ExplainPredicate::TextContains { .. }
        | ExplainPredicate::TextContainsCi { .. } => false,
    }
}

// Detect text scan predicates that force full-scan fallback diagnostics.
fn explain_predicate_contains_text_scan_operator(predicate: &ExplainPredicate) -> bool {
    match predicate {
        ExplainPredicate::Compare {
            op: CompareOp::EndsWith,
            ..
        }
        | ExplainPredicate::TextContains { .. }
        | ExplainPredicate::TextContainsCi { .. } => true,
        ExplainPredicate::And(children) | ExplainPredicate::Or(children) => children
            .iter()
            .any(explain_predicate_contains_text_scan_operator),
        ExplainPredicate::Not(inner) => explain_predicate_contains_text_scan_operator(inner),
        ExplainPredicate::Compare { .. }
        | ExplainPredicate::CompareFields { .. }
        | ExplainPredicate::None
        | ExplainPredicate::True
        | ExplainPredicate::False
        | ExplainPredicate::IsNull { .. }
        | ExplainPredicate::IsNotNull { .. }
        | ExplainPredicate::IsMissing { .. }
        | ExplainPredicate::IsEmpty { .. }
        | ExplainPredicate::IsNotEmpty { .. } => false,
    }
}
