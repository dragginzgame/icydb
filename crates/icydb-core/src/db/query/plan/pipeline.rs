//! Module: db::query::plan::pipeline
//! Responsibility: orchestrate query-intent planning phases.
//! Does not own: query intent mutation, access-path scoring internals, or executor runtime.
//! Boundary: turns `QueryModel` data into finalized `AccessPlannedQuery` contracts.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{CompareOp, Predicate},
        query::{
            intent::{QueryError, QueryModel},
            plan::{
                AccessPlannedQuery, AccessPlanningInputs, LogicalPlan, LogicalPlanningInputs,
                OrderSpec, PlannedAccessSelection, PlannedNonIndexAccessReason,
                PrimaryKeyAccessProof, PrimaryKeyInputResourceSummary, VisibleIndexes,
                build_logical_plan, fold_constant_predicate, is_limit_zero_load_window,
                logical_query_from_logical_inputs, normalize_query_predicate, plan_query_access,
                predicate_is_constant_false, primary_key_input_resource_from_value_list,
                rerank_access_plan_by_residual_burden_with_indexes,
                rerank_access_plan_by_residual_burden_with_semantic_indexes,
                validate_group_query_semantics, validate_query_semantics,
            },
        },
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
    traits::KeyValueCodec,
    value::Value,
};

#[cfg(feature = "sql")]
use crate::db::{
    access::SemanticIndexAccessContract,
    predicate::{CoercionId, ComparePredicate},
    query::plan::planner::index_field_literal_matcher,
};

///
/// PreparedScalarPlanningState
///
/// PreparedScalarPlanningState captures the validated scalar planning inputs
/// that both cache-key construction and planner misses need to reuse.
/// This exists so the miss path can normalize one predicate and materialize one
/// key-access override exactly once before handing the same state to planning.
///

pub(in crate::db) struct PreparedScalarPlanningState<'a> {
    schema_info: SchemaInfo,
    access_inputs: AccessPlanningInputs<'a>,
    normalized_predicate: Option<Predicate>,
    primary_key_input_resource: Option<PrimaryKeyInputResourceSummary>,
}

impl<'a> PreparedScalarPlanningState<'a> {
    // Build one reusable scalar planning-state bundle after policy validation
    // and predicate normalization have already succeeded.
    const fn new(
        schema_info: SchemaInfo,
        access_inputs: AccessPlanningInputs<'a>,
        normalized_predicate: Option<Predicate>,
        primary_key_input_resource: Option<PrimaryKeyInputResourceSummary>,
    ) -> Self {
        Self {
            schema_info,
            access_inputs,
            normalized_predicate,
            primary_key_input_resource,
        }
    }

    #[must_use]
    pub(in crate::db) const fn normalized_predicate(&self) -> Option<&Predicate> {
        self.normalized_predicate.as_ref()
    }
}

#[cfg(feature = "sql")]
pub(in crate::db) struct CountCardinalityPrefixAccess<'a> {
    index: SemanticIndexAccessContract,
    values: CountCardinalityPrefixValues<'a>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Copy)]
pub(in crate::db) enum CountCardinalityPrefixValues<'a> {
    One(&'a Value),
    Many(&'a [Value]),
}

#[cfg(feature = "sql")]
impl CountCardinalityPrefixValues<'_> {
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        match self {
            Self::One(_) => false,
            Self::Many(values) => values.is_empty(),
        }
    }
}

#[cfg(feature = "sql")]
impl<'a> CountCardinalityPrefixAccess<'a> {
    const fn new(
        index: SemanticIndexAccessContract,
        values: CountCardinalityPrefixValues<'a>,
    ) -> Self {
        Self { index, values }
    }

    #[must_use]
    pub(in crate::db) const fn index(&self) -> &SemanticIndexAccessContract {
        &self.index
    }

    #[must_use]
    pub(in crate::db) const fn values(&self) -> CountCardinalityPrefixValues<'a> {
        self.values
    }
}

/// Build a standalone model-only query plan using the generated schema-owned
/// index set.
#[inline(never)]
pub(in crate::db::query) fn build_query_model_plan_for_model_only<K>(
    query: &QueryModel<'_, K>,
) -> Result<AccessPlannedQuery, QueryError>
where
    K: KeyValueCodec,
{
    build_query_model_plan_with_indexes_for_model_only(
        query,
        &VisibleIndexes::generated_model_only(query.model().indexes()),
    )
}

/// Build a standalone model-only query plan using one explicit
/// planner-visible index set.
#[inline(never)]
pub(in crate::db::query) fn build_query_model_plan_with_indexes_for_model_only<K>(
    query: &QueryModel<'_, K>,
    visible_indexes: &VisibleIndexes<'_>,
) -> Result<AccessPlannedQuery, QueryError>
where
    K: KeyValueCodec,
{
    let planning_state = prepare_query_model_scalar_planning_state_for_model_only(query)?;

    build_query_model_plan_with_indexes_from_scalar_planning_state(
        query,
        visible_indexes,
        planning_state,
    )
}

/// Build a query model plan from an already prepared scalar planning state.
pub(in crate::db::query) fn build_query_model_plan_with_indexes_from_scalar_planning_state<K>(
    query: &QueryModel<'_, K>,
    visible_indexes: &VisibleIndexes<'_>,
    planning_state: PreparedScalarPlanningState<'_>,
) -> Result<AccessPlannedQuery, QueryError>
where
    K: KeyValueCodec,
{
    // Phase 1: reuse the caller-provided validated scalar planning state so
    // cache-key construction and planner misses share one normalized predicate
    // plus one explicit key-access override materialization.
    let PreparedScalarPlanningState {
        schema_info,
        access_inputs,
        normalized_predicate,
        primary_key_input_resource,
    } = planning_state;
    let access_order = access_inputs.order();
    let key_access_override = access_inputs.into_key_access_override();

    // Phase 2: choose one access path from the shared normalized predicate and
    // the already-projected planner access inputs.
    let access_selection = plan_access_from_normalized_predicate(
        query,
        visible_indexes,
        &schema_info,
        normalized_predicate.as_ref(),
        access_order,
        key_access_override,
    )?;
    let (access_plan_value, planned_non_index_reason) =
        access_selection.into_access_and_non_index_reason();
    let logical_inputs = query.planning_logical_inputs();
    let primary_key_strip = strip_redundant_primary_key_predicate_for_exact_access(
        &schema_info,
        &access_plan_value,
        normalized_predicate,
    );
    let normalized_predicate = primary_key_strip.predicate;
    let logical_inputs = if primary_key_strip.stripped {
        logical_inputs.without_filter_expr()
    } else {
        logical_inputs
    };

    // Phase 3: assemble logical plan from normalized scalar/grouped intent.
    let logical_query = logical_query_from_logical_inputs(
        logical_inputs,
        normalized_predicate,
        query.consistency(),
    );
    let logical = build_logical_plan(&schema_info, logical_query);
    let mut plan = AccessPlannedQuery::from_planned_access_with_projection(
        logical,
        access_plan_value,
        query.scalar_projection_selection().clone(),
        planned_non_index_reason,
    );
    let preferred_access = if visible_indexes.accepted_field_path_index_count().is_some() {
        rerank_access_plan_by_residual_burden_with_semantic_indexes(
            query.model(),
            visible_indexes.accepted_semantic_index_contracts(),
            &schema_info,
            &plan,
        )
    } else {
        rerank_access_plan_by_residual_burden_with_indexes(
            query.model(),
            visible_indexes.generated_model_only_indexes(),
            &schema_info,
            &plan,
        )
    };
    if let Some(preferred_access) = preferred_access {
        plan = AccessPlannedQuery::from_planned_access_with_projection(
            plan.logical.clone(),
            preferred_access,
            plan.projection_selection.clone(),
            None,
        );
    }
    attach_primary_key_input_resource_if_exact_access(&mut plan, primary_key_input_resource);
    simplify_limit_one_page_for_by_key_access(&mut plan);

    // Phase 4: freeze the planner-owned route profile before validation so
    // policy gates that depend on finalized access/order contracts, such as
    // expression ORDER BY support, see the accepted route semantics.
    plan.finalize_planner_route_profile_for_model_with_schema(&schema_info);

    // Phase 5: validate the assembled plan against schema, access-shape, and
    // planner-policy contracts before projecting explain metadata.
    validate_plan_semantics(query.model(), &schema_info, &plan)?;

    // Phase 6: freeze planner-owned execution metadata only after semantic
    // validation succeeds so user-facing projection/order errors remain
    // planner-domain failures instead of executor invariant violations.
    plan.finalize_static_execution_planning_contract_for_model_with_schema(
        query.model(),
        &schema_info,
    )
    .map_err(QueryError::execute)?;

    Ok(plan)
}

/// Build the exact-prefix COUNT metadata access proof directly from query
/// intent that already carries a normalized SQL predicate subset.
#[cfg(feature = "sql")]
pub(in crate::db::query) fn try_build_count_cardinality_prefix_access_from_query_model<'query, K>(
    query: &'query QueryModel<'_, K>,
    visible_indexes: &VisibleIndexes<'_>,
    schema_info: &SchemaInfo,
) -> Result<Option<CountCardinalityPrefixAccess<'query>>, QueryError>
where
    K: KeyValueCodec,
{
    query.validate_policy_shape()?;
    let access_inputs = query.planning_access_inputs();
    let logical_inputs = query.planning_logical_inputs();
    if access_inputs.order().is_some()
        || access_inputs.has_key_access_override()
        || logical_inputs.distinct()
        || logical_inputs.has_group()
        || logical_inputs.has_having_expr()
        || (logical_inputs.has_filter_expr() && !logical_inputs.filter_predicate_covers_expr())
    {
        return Ok(None);
    }
    let crate::db::query::plan::QueryMode::Load(load_spec) = query.mode() else {
        return Ok(None);
    };
    if load_spec.limit().is_some() || load_spec.offset() > 0 {
        return Ok(None);
    }
    let Some(predicate) = access_inputs.predicate() else {
        return Ok(None);
    };

    Ok(direct_count_cardinality_prefix_access_from_predicate(
        visible_indexes,
        schema_info,
        predicate,
    ))
}

#[cfg(feature = "sql")]
fn direct_count_cardinality_prefix_access_from_predicate<'predicate>(
    visible_indexes: &VisibleIndexes<'_>,
    schema_info: &SchemaInfo,
    normalized_predicate: &'predicate Predicate,
) -> Option<CountCardinalityPrefixAccess<'predicate>> {
    visible_indexes.accepted_field_path_index_count()?;
    let cmp = direct_count_exact_prefix_compare(normalized_predicate)?;
    let values = direct_count_exact_prefix_values(schema_info, cmp)?;
    let index = direct_count_exact_prefix_index(visible_indexes, cmp.field.as_str())?;

    Some(CountCardinalityPrefixAccess::new(index, values))
}

#[cfg(feature = "sql")]
fn direct_count_exact_prefix_compare(predicate: &Predicate) -> Option<&ComparePredicate> {
    let Predicate::Compare(cmp) = predicate else {
        return None;
    };
    if !matches!(cmp.op, CompareOp::Eq | CompareOp::In) || cmp.coercion.id != CoercionId::Strict {
        return None;
    }

    Some(cmp)
}

#[cfg(feature = "sql")]
fn direct_count_exact_prefix_values<'predicate>(
    schema_info: &SchemaInfo,
    cmp: &'predicate ComparePredicate,
) -> Option<CountCardinalityPrefixValues<'predicate>> {
    let values = match cmp.op {
        CompareOp::Eq => CountCardinalityPrefixValues::One(&cmp.value),
        CompareOp::In => {
            let Value::List(values) = &cmp.value else {
                return None;
            };
            CountCardinalityPrefixValues::Many(values.as_slice())
        }
        CompareOp::Ne
        | CompareOp::NotIn
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte
        | CompareOp::StartsWith
        | CompareOp::Contains
        | CompareOp::EndsWith => return None,
    };
    if values.is_empty() || direct_count_exact_prefix_values_mismatch(schema_info, cmp, values) {
        return None;
    }

    (!values.is_empty()).then_some(values)
}

#[cfg(feature = "sql")]
fn direct_count_exact_prefix_values_mismatch(
    schema_info: &SchemaInfo,
    cmp: &ComparePredicate,
    values: CountCardinalityPrefixValues<'_>,
) -> bool {
    let matcher = index_field_literal_matcher(schema_info, &cmp.field);
    match values {
        CountCardinalityPrefixValues::One(value) => !matcher.matches(value),
        CountCardinalityPrefixValues::Many(values) => {
            values.iter().any(|value| !matcher.matches(value))
        }
    }
}

#[cfg(feature = "sql")]
fn direct_count_exact_prefix_index(
    visible_indexes: &VisibleIndexes<'_>,
    field: &str,
) -> Option<SemanticIndexAccessContract> {
    let mut best: Option<SemanticIndexAccessContract> = None;
    for candidate in visible_indexes.accepted_field_path_indexes() {
        let index = candidate.semantic_access_contract();
        if direct_count_index_supports_exact_prefix(&index, field)
            && best.as_ref().is_none_or(|best| {
                index.key_arity() < best.key_arity()
                    || (index.key_arity() == best.key_arity() && index.name() < best.name())
            })
        {
            best = Some(index);
        }
    }

    best
}

#[cfg(feature = "sql")]
fn direct_count_index_supports_exact_prefix(
    index: &SemanticIndexAccessContract,
    field: &str,
) -> bool {
    !index.is_filtered()
        && !index.has_expression_key_items()
        && index.key_field_at(0) == Some(field)
}

/// Build the model-only no-predicate scalar-load fast path when the query shape
/// is trivial.
#[cfg(test)]
pub(in crate::db::query) fn try_build_trivial_scalar_load_plan_for_model_only<K>(
    query: &QueryModel<'_, K>,
) -> Result<Option<AccessPlannedQuery>, QueryError>
where
    K: KeyValueCodec,
{
    try_build_trivial_scalar_load_plan_with_schema_info(
        query,
        SchemaInfo::cached_for_generated_entity_model(query.model()).clone(),
    )
}

/// Build the no-predicate scalar-load fast path using explicit schema authority.
pub(in crate::db::query) fn try_build_trivial_scalar_load_plan_with_schema_info<K>(
    query: &QueryModel<'_, K>,
    schema_info: SchemaInfo,
) -> Result<Option<AccessPlannedQuery>, QueryError>
where
    K: KeyValueCodec,
{
    // Phase 1: keep this path deliberately narrow so it only bypasses work the
    // general planner would do for a full-scan primary-order scalar load.
    if !query.trivial_scalar_load_fast_path_eligible_with_schema(&schema_info) {
        return Ok(None);
    }

    // Phase 2: assemble the same logical scalar plan shape without projecting
    // access-planning inputs or normalizing an absent predicate.
    let logical_inputs = LogicalPlanningInputs::new(
        query.mode(),
        None,
        false,
        query.scalar_order_for_trivial_fast_path().cloned(),
        false,
        None,
        None,
    );
    let logical_query =
        logical_query_from_logical_inputs(logical_inputs, None, query.consistency());
    let logical = build_logical_plan(&schema_info, logical_query);
    let mut plan = AccessPlannedQuery::from_planned_access_with_projection(
        logical,
        AccessPlan::<Value>::full_scan(),
        query.scalar_projection_selection().clone(),
        Some(PlannedNonIndexAccessReason::PlannerFullScanFallback),
    );

    // Phase 3: preserve the finalized planner/executor contracts produced by
    // the general pipeline for this same simple shape.
    plan.finalize_planner_route_profile_for_model_with_schema(&schema_info);
    plan.finalize_static_execution_planning_contract_for_model_with_schema(
        query.model(),
        &schema_info,
    )
    .map_err(QueryError::execute)?;

    Ok(Some(plan))
}

/// Prepare model-only scalar planning inputs from generated schema metadata.
pub(in crate::db::query) fn prepare_query_model_scalar_planning_state_for_model_only<'a, K>(
    query: &'a QueryModel<'_, K>,
) -> Result<PreparedScalarPlanningState<'a>, QueryError>
where
    K: KeyValueCodec,
{
    prepare_query_model_scalar_planning_state_with_schema_info(
        query,
        SchemaInfo::cached_for_generated_entity_model(query.model()).clone(),
    )
}

/// Prepare scalar planning inputs using the caller-provided schema authority.
pub(in crate::db::query) fn prepare_query_model_scalar_planning_state_with_schema_info<'a, K>(
    query: &'a QueryModel<'_, K>,
    schema_info: SchemaInfo,
) -> Result<PreparedScalarPlanningState<'a>, QueryError>
where
    K: KeyValueCodec,
{
    // Phase 1: validate query-intent policy shape before any cache or planner
    // work so compile attribution keeps policy failures honest.
    query.validate_policy_shape()?;

    // Phase 2: project the planner access inputs once so cache-key construction
    // and miss-path planning reuse the same explicit key-access override
    // materialization.
    let access_inputs = query.planning_access_inputs();
    let primary_key_input_resource = access_inputs.key_access_input_resource().or_else(|| {
        primary_key_input_resource_from_predicate(&schema_info, access_inputs.predicate())
    });
    let normalized_predicate = fold_constant_predicate(normalize_query_predicate(
        &schema_info,
        access_inputs.predicate(),
    )?);

    Ok(PreparedScalarPlanningState::new(
        schema_info,
        access_inputs,
        normalized_predicate,
        primary_key_input_resource,
    ))
}

// Reuse the caller-provided normalized predicate to choose one access path
// without recomputing planner inputs or scattering the empty-window gates.
fn plan_access_from_normalized_predicate<K>(
    query: &QueryModel<'_, K>,
    visible_indexes: &VisibleIndexes<'_>,
    schema_info: &SchemaInfo,
    normalized_predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    key_access_override: Option<AccessPlan<Value>>,
) -> Result<PlannedAccessSelection, QueryError>
where
    K: KeyValueCodec,
{
    let limit_zero_window = is_limit_zero_load_window(query.mode());
    let constant_false_predicate = predicate_is_constant_false(normalized_predicate);
    if limit_zero_window {
        return Ok(PlannedAccessSelection::new(
            AccessPlan::by_keys(Vec::new()),
            Some(PlannedNonIndexAccessReason::LimitZeroWindow),
        ));
    }
    if constant_false_predicate {
        return Ok(PlannedAccessSelection::new(
            AccessPlan::by_keys(Vec::new()),
            Some(PlannedNonIndexAccessReason::ConstantFalsePredicate),
        ));
    }

    plan_query_access(
        query.model(),
        visible_indexes,
        schema_info,
        normalized_predicate,
        order,
        query.is_grouped(),
        key_access_override,
    )
    .map_err(QueryError::from)
}

// Keep grouped and scalar semantic validation behind one pipeline-local gate so
// handoff code does not duplicate the route-shape branch.
fn validate_plan_semantics(
    model: &EntityModel,
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Result<(), QueryError> {
    if plan.grouped_plan().is_some() {
        validate_group_query_semantics(schema_info, model, plan)?;
    } else {
        validate_query_semantics(schema_info, model, plan)?;
    }

    Ok(())
}

fn attach_primary_key_input_resource_if_exact_access(
    plan: &mut AccessPlannedQuery,
    resource: Option<PrimaryKeyInputResourceSummary>,
) {
    let Some(resource) = resource else {
        return;
    };
    if PrimaryKeyAccessProof::from_access(&plan.access).is_none() {
        return;
    }

    plan.access_choice = plan
        .access_choice
        .clone()
        .with_primary_key_input_resource(resource);
}

fn primary_key_input_resource_from_predicate(
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Option<PrimaryKeyInputResourceSummary> {
    let primary_key_name = scalar_primary_key_name(schema_info)?;
    let mut resource = PrimaryKeyInputResourceAccumulator::default();
    collect_primary_key_in_resource(predicate?, primary_key_name, &mut resource);

    resource.into_summary()
}

fn collect_primary_key_in_resource(
    predicate: &Predicate,
    primary_key_name: &str,
    resource: &mut PrimaryKeyInputResourceAccumulator,
) {
    match predicate {
        Predicate::Compare(cmp) if cmp.field == primary_key_name && cmp.op == CompareOp::In => {
            if let Value::List(values) = &cmp.value {
                resource.add_values(values);
            }
        }
        Predicate::And(children) => {
            for child in children {
                collect_primary_key_in_resource(child, primary_key_name, resource);
            }
        }
        Predicate::Or(_)
        | Predicate::Not(_)
        | Predicate::Compare(_)
        | Predicate::CompareFields(_)
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::True
        | Predicate::False => {}
    }
}

#[derive(Default)]
struct PrimaryKeyInputResourceAccumulator {
    raw_term_count: u32,
    estimated_payload_bytes: u32,
}

impl PrimaryKeyInputResourceAccumulator {
    fn add_values(&mut self, values: &[Value]) {
        let Some(summary) = primary_key_input_resource_from_value_list(values) else {
            return;
        };
        self.raw_term_count = self.raw_term_count.saturating_add(summary.raw_term_count());
        self.estimated_payload_bytes = self
            .estimated_payload_bytes
            .saturating_add(summary.estimated_payload_bytes());
    }

    const fn into_summary(self) -> Option<PrimaryKeyInputResourceSummary> {
        if self.raw_term_count == 0 {
            return None;
        }

        Some(PrimaryKeyInputResourceSummary::new(
            self.raw_term_count,
            self.estimated_payload_bytes,
        ))
    }
}

struct PrimaryKeyPredicateStripResult {
    predicate: Option<Predicate>,
    stripped: bool,
}

impl PrimaryKeyPredicateStripResult {
    const fn kept(predicate: Option<Predicate>) -> Self {
        Self {
            predicate,
            stripped: false,
        }
    }

    const fn stripped() -> Self {
        Self {
            predicate: None,
            stripped: true,
        }
    }
}

// Drop one normalized primary-key predicate when access planning already
// resolved the exact same authoritative PK access path. The result also owns
// the matching "filter expression is redundant" fact so planning does not
// evaluate the selected primary-key proof twice.
fn strip_redundant_primary_key_predicate_for_exact_access(
    schema_info: &SchemaInfo,
    access: &AccessPlan<Value>,
    normalized_predicate: Option<Predicate>,
) -> PrimaryKeyPredicateStripResult {
    let Some(predicate) = normalized_predicate else {
        return PrimaryKeyPredicateStripResult::kept(None);
    };

    if scalar_primary_key_name(schema_info).is_some_and(|primary_key_name| {
        PrimaryKeyAccessProof::from_access(access)
            .is_some_and(|access| access.matches_predicate(&predicate, primary_key_name))
    }) {
        return PrimaryKeyPredicateStripResult::stripped();
    }

    PrimaryKeyPredicateStripResult::kept(Some(predicate))
}

fn scalar_primary_key_name(schema_info: &SchemaInfo) -> Option<&str> {
    schema_info.scalar_primary_key_name()
}

// Collapse `LIMIT 1` pagination overhead when access is already one exact
// primary-key lookup and no offset is requested.
fn simplify_limit_one_page_for_by_key_access(plan: &mut AccessPlannedQuery) {
    if plan.access.as_by_key_path().is_none() {
        return;
    }

    let scalar = match &mut plan.logical {
        LogicalPlan::Scalar(scalar) => scalar,
        LogicalPlan::Grouped(grouped) => &mut grouped.scalar,
    };
    let Some(page) = scalar.page.as_ref() else {
        return;
    };
    if page.offset != 0 || page.limit != Some(1) {
        return;
    }

    scalar.page = None;
}
