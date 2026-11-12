//! Module: db::query::plan::pipeline
//! Responsibility: orchestrate query-intent planning phases.
//! Does not own: query intent mutation, access-path scoring internals, or executor runtime.
//! Boundary: turns `QueryModel` data into finalized `AccessPlannedQuery` contracts.

use crate::db::query::preparation::PreparationWork;
use std::rc::Rc;

use crate::{
    db::{
        access::{AccessPlan, MAX_INDEX_BRANCH_SET_VALUES},
        predicate::{CompareOp, Predicate},
        query::{
            intent::{QueryError, QueryModel},
            plan::{
                AccessPlannedQuery, AccessPlanningInputs, CardinalityTiebreakState, LogicalPlan,
                LogicalPlanningInputs, OrderSpec, PlannedAccessSelection,
                PlannedNonIndexAccessReason, PrimaryKeyAccessProof, PrimaryKeyInputResourceSummary,
                VisibleIndexes, build_logical_plan, is_limit_zero_load_window,
                logical_query_from_logical_inputs,
                plan_access_selection_with_order_and_semantic_indexes, predicate_is_constant_false,
                rerank_access_plan_by_residual_burden_with_semantic_indexes,
                residual_query_predicate_after_access_path_bounds,
                validate_group_query_semantics_with_schema, validate_query_semantics_with_schema,
            },
        },
        schema::SchemaInfo,
    },
    value::Value,
};

use crate::db::{
    access::SemanticIndexAccessContract,
    predicate::{CoercionId, ComparePredicate},
    query::plan::planner::{
        count_cardinality_index_branch_set_from_and, index_field_literal_matcher,
        index_stream_is_complete_for_query,
    },
};

/// Evidence-bounded exact-count prefix-key cap. Row-producing branch routes
/// remain governed independently by `MAX_INDEX_BRANCH_SET_VALUES`.
pub(in crate::db) const MAX_EXACT_COUNT_PREFIX_CARDINALITY_KEYS: usize = 17;

///
/// PreparedScalarPlanningState
///
/// PreparedScalarPlanningState captures the validated scalar planning inputs
/// that both cache-key construction and planner misses need to reuse.
/// Cache lookup and plan construction share the normalized predicate and
/// primary-key input resource summary without preparing either twice.
/// Schema metadata stays shared with its immutable accepted root; retaining this
/// state does not make that root current authority for a later execution.
///

pub(in crate::db) struct PreparedScalarPlanningState<'a> {
    schema_info: Rc<SchemaInfo>,
    access_inputs: AccessPlanningInputs<'a>,
    normalized_predicate: Option<&'a Predicate>,
    primary_key_input_resource: Option<PrimaryKeyInputResourceSummary>,
}

impl<'a> PreparedScalarPlanningState<'a> {
    // Build one reusable scalar planning-state bundle after policy validation
    // and predicate normalization have already succeeded.
    const fn new(
        schema_info: Rc<SchemaInfo>,
        access_inputs: AccessPlanningInputs<'a>,
        normalized_predicate: Option<&'a Predicate>,
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
        self.normalized_predicate
    }

    #[must_use]
    pub(in crate::db) fn schema_info(&self) -> &SchemaInfo {
        &self.schema_info
    }
}

pub(in crate::db) struct CountCardinalityPrefixAccess<'a> {
    index: SemanticIndexAccessContract,
    values: CountCardinalityPrefixValues<'a>,
}

pub(in crate::db) enum CountCardinalityPrefixValues<'a> {
    One(&'a Value),
    Many(&'a [Value]),
    ExactPrefixes(Vec<Vec<Value>>),
}

impl CountCardinalityPrefixValues<'_> {
    #[must_use]
    pub(in crate::db) fn is_empty(&self) -> bool {
        match self {
            Self::One(_) => false,
            Self::Many(values) => values.is_empty(),
            Self::ExactPrefixes(prefixes) => {
                prefixes.is_empty() || prefixes.iter().any(Vec::is_empty)
            }
        }
    }
}

impl<'a> CountCardinalityPrefixAccess<'a> {
    const fn new(
        index: SemanticIndexAccessContract,
        values: CountCardinalityPrefixValues<'a>,
    ) -> Self {
        Self { index, values }
    }

    const fn from_exact_prefixes(
        index: SemanticIndexAccessContract,
        prefixes: Vec<Vec<Value>>,
    ) -> Self {
        Self::new(index, CountCardinalityPrefixValues::ExactPrefixes(prefixes))
    }

    #[must_use]
    pub(in crate::db) const fn index(&self) -> &SemanticIndexAccessContract {
        &self.index
    }

    #[must_use]
    pub(in crate::db) const fn values(&self) -> &CountCardinalityPrefixValues<'a> {
        &self.values
    }
}

/// Build a plan from prepared inputs and accepted candidate indexes. Cold
/// planning supplies currently visible indexes; template rebinding supplies its
/// retained candidates after the cache owner checks current authority/visibility.
pub(in crate::db::query) fn build_query_model_plan_with_indexes_from_scalar_planning_state(
    query: &QueryModel,
    indexes: &[SemanticIndexAccessContract],
    planning_state: PreparedScalarPlanningState<'_>,
    work: &PreparationWork<'_>,
) -> Result<AccessPlannedQuery, QueryError> {
    // Phase 1: reuse the caller-provided validated scalar planning state so
    // cache-key construction and planner misses share one normalized predicate
    // and primary-key input resource summary.
    let PreparedScalarPlanningState {
        schema_info,
        access_inputs,
        normalized_predicate,
        primary_key_input_resource,
    } = planning_state;
    let normalized_predicate = normalized_predicate
        .map(|predicate| work.copy_predicate(predicate))
        .transpose()?;
    let canonical_order = access_inputs.canonical_order(&schema_info, query.is_grouped(), work)?;

    // Phase 2: choose one access path from the shared normalized predicate and
    // the already-projected planner access inputs.
    let access_selection = plan_access_from_normalized_predicate(
        query,
        indexes,
        &schema_info,
        normalized_predicate.as_ref(),
        canonical_order.as_ref(),
        work,
    )?;
    let (access_plan_value, planned_non_index_reason) =
        access_selection.into_access_and_non_index_reason();

    assemble_query_model_plan(
        query,
        indexes,
        schema_info,
        normalized_predicate,
        canonical_order,
        primary_key_input_resource,
        access_plan_value,
        planned_non_index_reason,
        work,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "assembly keeps shared request authority explicit alongside accepted planning inputs"
)]
pub(in crate::db::query::plan) fn assemble_query_model_plan(
    query: &QueryModel,
    rerank_indexes: &[SemanticIndexAccessContract],
    schema_info: Rc<SchemaInfo>,
    normalized_predicate: Option<Predicate>,
    canonical_order: Option<OrderSpec>,
    primary_key_input_resource: Option<PrimaryKeyInputResourceSummary>,
    access_plan_value: AccessPlan<Value>,
    planned_non_index_reason: Option<PlannedNonIndexAccessReason>,
    work: &PreparationWork<'_>,
) -> Result<AccessPlannedQuery, QueryError> {
    let logical_inputs = query.planning_logical_inputs();
    let primary_key_strip = strip_redundant_primary_key_predicate_for_exact_access(
        &schema_info,
        &access_plan_value,
        normalized_predicate,
    );
    let normalized_predicate = primary_key_strip.predicate;
    // Exact access proves the predicate subset. Intent coverage still owns
    // whether that proof also discharges the complete filter expression.
    let logical_inputs =
        if primary_key_strip.stripped && query.filter_predicate_fully_covers_expression() {
            logical_inputs.without_filter_expr()
        } else {
            logical_inputs
        };

    // Phase 3: assemble logical plan from normalized scalar/grouped intent.
    let logical_query = logical_query_from_logical_inputs(
        logical_inputs,
        normalized_predicate,
        canonical_order,
        query.consistency(),
        work,
    )?;
    let logical = build_logical_plan(logical_query);
    let mut plan = AccessPlannedQuery::from_planned_access_with_projection(
        logical,
        access_plan_value,
        query
            .scalar_projection_selection()
            .copy_for_preparation(work)?,
        planned_non_index_reason,
    );
    let preferred_access = rerank_access_plan_by_residual_burden_with_semantic_indexes(
        rerank_indexes,
        &schema_info,
        &plan,
        work,
    )
    .map_err(QueryError::execute)?;
    if let Some(preferred_access) = preferred_access {
        plan = AccessPlannedQuery::from_planned_access_with_projection(
            plan.logical,
            preferred_access,
            plan.projection_selection,
            None,
        );
    }
    attach_primary_key_input_resource_if_exact_access(&mut plan, primary_key_input_resource);
    simplify_limit_one_page_for_by_key_access(&mut plan);

    finalize_query_model_plan(&schema_info, plan, work)
}

fn finalize_query_model_plan(
    schema_info: &SchemaInfo,
    mut plan: AccessPlannedQuery,
    work: &PreparationWork<'_>,
) -> Result<AccessPlannedQuery, QueryError> {
    // Phase 4: freeze the planner-owned route profile before validation so
    // policy gates that depend on finalized access/order contracts, such as
    // expression ORDER BY support, see the accepted route semantics.
    plan.finalize_planner_route_profile_for_model_with_schema(schema_info, work)
        .map_err(QueryError::execute)?;

    // Phase 5: validate the assembled plan against schema, access-shape, and
    // planner-policy contracts before projecting explain metadata.
    let projection = plan.prepare_projection(schema_info, work)?;
    validate_plan_semantics(schema_info, &plan, &projection, work)?;

    // Phase 6: freeze planner-owned execution metadata only after semantic
    // validation succeeds so user-facing projection/order errors remain
    // planner-domain failures instead of executor invariant violations.
    plan.finalize_static_execution_planning_contract_with_schema(schema_info, projection, work)?;

    Ok(plan)
}

/// Freeze one optional exact-cardinality decision through the canonical plan finalizer.
pub(in crate::db) fn apply_exact_cardinality_tiebreak_selection(
    mut plan: AccessPlannedQuery,
    selected_access: Option<AccessPlan<Value>>,
    state: CardinalityTiebreakState,
    schema_info: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<AccessPlannedQuery, QueryError> {
    let Some(selected_access) = selected_access else {
        plan.set_cardinality_tiebreak(state);
        return Ok(plan);
    };
    if selected_access == plan.access {
        plan.set_cardinality_tiebreak(state);
        return Ok(plan);
    }

    let mut reselected = AccessPlannedQuery::from_planned_access_with_projection(
        plan.logical,
        selected_access,
        plan.projection_selection,
        None,
    );
    reselected.set_cardinality_tiebreak(state);
    simplify_limit_one_page_for_by_key_access(&mut reselected);
    finalize_query_model_plan(schema_info, reselected, work)
}

/// Build the exact-prefix COUNT metadata access proof directly from query
/// intent that already carries one normalized predicate subset.
pub(in crate::db::query) fn try_build_count_cardinality_prefix_access_from_query_model<'query>(
    query: &'query QueryModel,
    visible_indexes: &VisibleIndexes,
    schema_info: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<Option<CountCardinalityPrefixAccess<'query>>, QueryError> {
    query.admitted_filter(schema_info)?;
    let Some(predicate) = query.direct_count_cardinality_prefix_predicate()? else {
        return Ok(None);
    };

    direct_count_cardinality_prefix_access_from_predicate(
        visible_indexes,
        schema_info,
        predicate,
        work,
    )
}

fn direct_count_cardinality_prefix_access_from_predicate<'predicate>(
    visible_indexes: &VisibleIndexes,
    schema_info: &SchemaInfo,
    normalized_predicate: &'predicate Predicate,
    work: &PreparationWork<'_>,
) -> Result<Option<CountCardinalityPrefixAccess<'predicate>>, QueryError> {
    if visible_indexes.accepted_field_path_index_count().is_none() {
        return Ok(None);
    }
    if let Some(cmp) = direct_count_exact_prefix_compare(normalized_predicate) {
        let Some(values) = direct_count_exact_prefix_values(schema_info, cmp) else {
            return Ok(None);
        };
        let Some(index) = direct_count_exact_prefix_index(
            visible_indexes,
            schema_info,
            normalized_predicate,
            cmp.field.as_str(),
            work,
        )?
        else {
            return Ok(None);
        };

        return Ok(Some(CountCardinalityPrefixAccess::new(index, values)));
    }

    let Predicate::And(children) = normalized_predicate else {
        return Ok(None);
    };
    direct_count_exact_composite_prefix_access(
        visible_indexes,
        schema_info,
        normalized_predicate,
        children,
        work,
    )
}

fn direct_count_exact_composite_prefix_access<'predicate>(
    visible_indexes: &VisibleIndexes,
    schema_info: &SchemaInfo,
    normalized_predicate: &'predicate Predicate,
    children: &[Predicate],
    work: &PreparationWork<'_>,
) -> Result<Option<CountCardinalityPrefixAccess<'predicate>>, QueryError> {
    // Keep selection authority on accepted field-path contracts, then require
    // the concrete bounds to discharge the entire predicate before retaining
    // only metadata prefixes from this non-executable proof plan.
    let mut candidate_indexes = Vec::new();
    for accepted in visible_indexes.accepted_field_path_indexes() {
        let index = accepted.semantic_access_contract();
        if !index.is_filtered()
            && !index.has_expression_key_items()
            && index_stream_is_complete_for_query(schema_info, &index, normalized_predicate, work)
                .map_err(QueryError::execute)?
        {
            candidate_indexes.push(index);
        }
    }
    let Some(access) = count_cardinality_index_branch_set_from_and(
        candidate_indexes.as_slice(),
        schema_info,
        children,
        MAX_EXACT_COUNT_PREFIX_CARDINALITY_KEYS,
        work,
    )
    .map_err(QueryError::execute)?
    else {
        return Ok(None);
    };
    let Some(path) = access.as_path() else {
        return Ok(None);
    };
    if residual_query_predicate_after_access_path_bounds(
        Some(path),
        work.copy_predicate(normalized_predicate)?,
        work,
    )
    .map_err(QueryError::execute)?
    .is_some()
    {
        return Ok(None);
    }

    if path.as_index_prefix_contract().is_some() {
        return Ok(None);
    }
    let Some(branch_set) = path.as_index_branch_set_spec() else {
        return Ok(None);
    };
    let prefixes = branch_set
        .branch_values()
        .iter()
        .map(|branch_value| {
            let mut prefix = branch_set.fixed_values().to_vec();
            prefix.push(branch_value.clone());
            prefix
        })
        .collect::<Vec<_>>();
    if prefixes.len() <= MAX_INDEX_BRANCH_SET_VALUES {
        return Ok(None);
    }

    Ok(Some(CountCardinalityPrefixAccess::from_exact_prefixes(
        branch_set.index(),
        prefixes,
    )))
}

fn direct_count_exact_prefix_compare(predicate: &Predicate) -> Option<&ComparePredicate> {
    let Predicate::Compare(cmp) = predicate else {
        return None;
    };
    if !matches!(cmp.op, CompareOp::Eq | CompareOp::In) || cmp.coercion.id != CoercionId::Strict {
        return None;
    }

    Some(cmp)
}

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
            if values.len() > MAX_EXACT_COUNT_PREFIX_CARDINALITY_KEYS {
                return None;
            }
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
    if values.is_empty() || direct_count_exact_prefix_values_mismatch(schema_info, cmp, &values) {
        return None;
    }

    (!values.is_empty()).then_some(values)
}

fn direct_count_exact_prefix_values_mismatch(
    schema_info: &SchemaInfo,
    cmp: &ComparePredicate,
    values: &CountCardinalityPrefixValues<'_>,
) -> bool {
    let matcher = index_field_literal_matcher(schema_info, &cmp.field);
    match values {
        CountCardinalityPrefixValues::One(value) => !matcher.matches(value),
        CountCardinalityPrefixValues::Many(values) => {
            values.iter().any(|value| !matcher.matches(value))
        }
        CountCardinalityPrefixValues::ExactPrefixes(_) => true,
    }
}

fn direct_count_exact_prefix_index(
    visible_indexes: &VisibleIndexes,
    schema_info: &SchemaInfo,
    predicate: &Predicate,
    field: &str,
    work: &PreparationWork<'_>,
) -> Result<Option<SemanticIndexAccessContract>, QueryError> {
    best_exact_field_path_index(visible_indexes, |index| {
        Ok(direct_count_index_supports_exact_prefix(index, field)
            && index_stream_is_complete_for_query(schema_info, index, predicate, work)?)
    })
    .map_err(QueryError::execute)
}

/// Select one complete accepted index for exact leading-component metadata.
#[cfg(feature = "sql")]
pub(in crate::db) fn exact_first_component_metadata_index(
    visible_indexes: &VisibleIndexes,
    schema_info: &SchemaInfo,
    field: &str,
) -> Option<SemanticIndexAccessContract> {
    if visible_indexes.accepted_field_path_index_count()
        != Some(schema_info.field_path_indexes().len())
    {
        return None;
    }

    match best_exact_field_path_index(visible_indexes, |index| {
        Ok::<_, std::convert::Infallible>(
            !index.is_filtered()
                && index.key_field_at(0) == Some(field)
                && (0..index.key_arity()).all(|slot| {
                    index.key_field_at(slot).is_some_and(|key_field| {
                        schema_info.accepted_field_is_nullable(key_field) == Some(false)
                    })
                }),
        )
    }) {
        Ok(index) => index,
        Err(never) => match never {},
    }
}

fn best_exact_field_path_index<E>(
    visible_indexes: &VisibleIndexes,
    supports: impl Fn(&SemanticIndexAccessContract) -> Result<bool, E>,
) -> Result<Option<SemanticIndexAccessContract>, E> {
    let mut best: Option<SemanticIndexAccessContract> = None;
    for accepted in visible_indexes.accepted_field_path_indexes() {
        let index = accepted.semantic_access_contract();
        if supports(&index)?
            && best.as_ref().is_none_or(|current| {
                index
                    .key_arity()
                    .cmp(&current.key_arity())
                    .then_with(|| index.name().cmp(current.name()))
                    .is_lt()
            })
        {
            best = Some(index);
        }
    }
    Ok(best)
}

fn direct_count_index_supports_exact_prefix(
    index: &SemanticIndexAccessContract,
    field: &str,
) -> bool {
    !index.is_filtered()
        && !index.has_expression_key_items()
        && index.key_field_at(0) == Some(field)
}

/// Build the no-predicate scalar-load fast path using explicit schema authority.
pub(in crate::db::query) fn try_build_trivial_scalar_load_plan_with_schema_info(
    query: &QueryModel,
    schema_info: Rc<SchemaInfo>,
    work: &PreparationWork<'_>,
) -> Result<Option<AccessPlannedQuery>, QueryError> {
    // Phase 1: keep this path deliberately narrow so it only bypasses work the
    // general planner would do for a full-scan primary-order scalar load.
    if !query.trivial_scalar_load_fast_path_eligible_with_schema(&schema_info) {
        return Ok(None);
    }

    // Phase 2: assemble the same logical scalar plan shape without access
    // selection or normalizing an absent predicate.
    let logical_inputs = LogicalPlanningInputs::new(query.mode(), None, false, false, None, None);
    let canonical_order =
        query
            .planning_access_inputs()
            .canonical_order(&schema_info, false, work)?;
    let logical_query = logical_query_from_logical_inputs(
        logical_inputs,
        None,
        canonical_order,
        query.consistency(),
        work,
    )?;
    let logical = build_logical_plan(logical_query);
    let mut plan = AccessPlannedQuery::from_planned_access_with_projection(
        logical,
        AccessPlan::<Value>::full_scan(),
        query
            .scalar_projection_selection()
            .copy_for_preparation(work)?,
        Some(PlannedNonIndexAccessReason::PlannerFullScanFallback),
    );

    // Phase 3: preserve the finalized planner/executor contracts produced by
    // the general pipeline for this same simple shape.
    plan.finalize_planner_route_profile_for_model_with_schema(&schema_info, work)
        .map_err(QueryError::execute)?;
    let projection = plan.prepare_projection(&schema_info, work)?;
    plan.finalize_static_execution_planning_contract_with_schema(&schema_info, projection, work)?;

    Ok(Some(plan))
}

/// Prepare scalar planning inputs using the caller-provided schema authority.
pub(in crate::db::query) fn prepare_query_model_scalar_planning_state_with_schema_info<'query>(
    query: &'query QueryModel,
    schema_info: Rc<SchemaInfo>,
    _work: &PreparationWork<'_>,
) -> Result<PreparedScalarPlanningState<'query>, QueryError> {
    // Phase 1: validate query-intent policy shape before any cache or planner
    // work so compile attribution keeps policy failures honest.
    query.validate_policy_shape()?;

    // Phase 2: retain borrowed intent inputs alongside the normalized predicate
    // and primary-key resource facts needed by cache lookup and planning.
    let access_inputs = query.planning_access_inputs();
    let filter = query.admitted_filter(&schema_info)?;
    let primary_key_input_resource = filter
        .and_then(crate::db::query::intent::filter::AdmittedFilter::primary_key_input_resource);
    let normalized_predicate = access_inputs
        .predicate()
        .filter(|predicate| !matches!(predicate, Predicate::True));

    Ok(PreparedScalarPlanningState::new(
        schema_info,
        access_inputs,
        normalized_predicate,
        primary_key_input_resource,
    ))
}

// Reuse the caller-provided normalized predicate to choose one access path
// without recomputing planner inputs or scattering the empty-window gates.
fn plan_access_from_normalized_predicate(
    query: &QueryModel,
    indexes: &[SemanticIndexAccessContract],
    schema_info: &SchemaInfo,
    normalized_predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    work: &PreparationWork<'_>,
) -> Result<PlannedAccessSelection, QueryError> {
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

    plan_access_selection_with_order_and_semantic_indexes(
        indexes,
        schema_info,
        normalized_predicate,
        order,
        query.is_grouped(),
        work,
    )
    .map_err(QueryError::from)
}

// Keep grouped and scalar semantic validation behind one pipeline-local gate so
// handoff code does not duplicate the route-shape branch.
fn validate_plan_semantics(
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    projection: &crate::db::query::plan::expr::ProjectionSpec,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    if plan.grouped_plan().is_some() {
        validate_group_query_semantics_with_schema(schema_info, plan, projection, work)?;
    } else {
        validate_query_semantics_with_schema(schema_info, plan, projection, work)?;
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
// resolved the exact same authoritative PK access path. This proves only the
// predicate; the caller must establish intent coverage before dropping a filter
// expression.
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

///
/// TESTS
///

#[cfg(all(test, feature = "sql"))]
mod tests;
