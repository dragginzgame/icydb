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
                OrderSpec, PlannedAccessSelection, PlannedNonIndexAccessReason, VisibleIndexes,
                build_logical_plan, fold_constant_predicate, is_limit_zero_load_window,
                logical_query_from_logical_inputs, normalize_query_predicate, plan_query_access,
                predicate_is_constant_false, rerank_access_plan_by_residual_burden_with_indexes,
                validate_group_query_semantics, validate_query_semantics,
            },
        },
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
    traits::KeyValueCodec,
    value::{Value, canonicalize_value_set},
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
}

impl<'a> PreparedScalarPlanningState<'a> {
    // Build one reusable scalar planning-state bundle after policy validation
    // and predicate normalization have already succeeded.
    const fn new(
        schema_info: SchemaInfo,
        access_inputs: AccessPlanningInputs<'a>,
        normalized_predicate: Option<Predicate>,
    ) -> Self {
        Self {
            schema_info,
            access_inputs,
            normalized_predicate,
        }
    }

    #[must_use]
    pub(in crate::db) const fn normalized_predicate(&self) -> Option<&Predicate> {
        self.normalized_predicate.as_ref()
    }
}

/// Build a query model plan using the schema-owned index set.
#[inline(never)]
pub(in crate::db::query) fn build_query_model_plan<K>(
    query: &QueryModel<'_, K>,
) -> Result<AccessPlannedQuery, QueryError>
where
    K: KeyValueCodec,
{
    build_query_model_plan_with_indexes(
        query,
        &VisibleIndexes::schema_owned(query.model().indexes()),
    )
}

/// Build a query model plan using one explicit planner-visible index set.
#[inline(never)]
pub(in crate::db::query) fn build_query_model_plan_with_indexes<K>(
    query: &QueryModel<'_, K>,
    visible_indexes: &VisibleIndexes<'_>,
) -> Result<AccessPlannedQuery, QueryError>
where
    K: KeyValueCodec,
{
    let planning_state = prepare_query_model_scalar_planning_state(query)?;

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
    let (access_plan_value, planned_non_index_reason) = access_selection.into_parts();
    let logical_inputs = query.planning_logical_inputs();
    let redundant_primary_key_filter = normalized_predicate.as_ref().is_some_and(|predicate| {
        ExactPrimaryKeyAccess::from_access(&access_plan_value).is_some_and(|access| {
            access.matches_predicate(predicate, query.model().primary_key.name)
        })
    });
    let normalized_predicate = strip_redundant_primary_key_predicate_for_exact_access(
        query.model(),
        &access_plan_value,
        normalized_predicate,
    );
    let logical_inputs = if redundant_primary_key_filter {
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
    let logical = build_logical_plan(query.model(), logical_query);
    let mut plan = AccessPlannedQuery::from_planned_parts_with_projection(
        logical,
        access_plan_value,
        query.scalar_projection_selection().clone(),
        planned_non_index_reason,
    );
    if let Some(preferred_access) = rerank_access_plan_by_residual_burden_with_indexes(
        query.model(),
        visible_indexes.as_slice(),
        &schema_info,
        &plan,
    ) {
        plan = AccessPlannedQuery::from_planned_parts_with_projection(
            plan.logical.clone(),
            preferred_access,
            plan.projection_selection.clone(),
            None,
        );
    }
    simplify_limit_one_page_for_by_key_access(&mut plan);

    // Phase 4: freeze the planner-owned route profile before validation so
    // policy gates that depend on finalized access/order contracts, such as
    // expression ORDER BY support, see the accepted route semantics.
    plan.finalize_planner_route_profile_for_model(query.model());

    // Phase 5: validate the assembled plan against schema, access-shape, and
    // planner-policy contracts before projecting explain metadata.
    validate_plan_semantics(query.model(), &schema_info, &plan)?;

    // Phase 6: freeze planner-owned execution metadata only after semantic
    // validation succeeds so user-facing projection/order errors remain
    // planner-domain failures instead of executor invariant violations.
    plan.finalize_static_planning_shape_for_model_with_schema(query.model(), &schema_info)
        .map_err(QueryError::execute)?;

    Ok(plan)
}

/// Build the no-predicate scalar-load fast path when the query shape is trivial.
pub(in crate::db::query) fn try_build_trivial_scalar_load_plan<K>(
    query: &QueryModel<'_, K>,
) -> Result<Option<AccessPlannedQuery>, QueryError>
where
    K: KeyValueCodec,
{
    // Phase 1: keep this path deliberately narrow so it only bypasses work the
    // general planner would do for a full-scan primary-order scalar load.
    if !query.trivial_scalar_load_fast_path_eligible() {
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
    let logical = build_logical_plan(query.model(), logical_query);
    let mut plan = AccessPlannedQuery::from_planned_parts_with_projection(
        logical,
        AccessPlan::<Value>::full_scan(),
        query.scalar_projection_selection().clone(),
        Some(PlannedNonIndexAccessReason::PlannerFullScanFallback),
    );

    // Phase 3: preserve the finalized planner/executor contracts produced by
    // the general pipeline for this same simple shape.
    plan.finalize_planner_route_profile_for_model(query.model());
    plan.finalize_static_planning_shape_for_model(query.model())
        .map_err(QueryError::execute)?;

    Ok(Some(plan))
}

/// Prepare scalar planning inputs shared by cache-key and miss-path planning.
pub(in crate::db::query) fn prepare_query_model_scalar_planning_state<'a, K>(
    query: &'a QueryModel<'_, K>,
) -> Result<PreparedScalarPlanningState<'a>, QueryError>
where
    K: KeyValueCodec,
{
    prepare_query_model_scalar_planning_state_with_schema_info(
        query,
        SchemaInfo::cached_for_entity_model(query.model()).clone(),
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
    let normalized_predicate = fold_constant_predicate(normalize_query_predicate(
        &schema_info,
        access_inputs.predicate(),
    )?);

    Ok(PreparedScalarPlanningState::new(
        schema_info,
        access_inputs,
        normalized_predicate,
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
        visible_indexes.as_slice(),
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

// Drop one normalized primary-key predicate when access planning already
// resolved the exact same authoritative PK access path. This prevents duplicate
// predicate evaluation and unlocks downstream PK fast paths.
fn strip_redundant_primary_key_predicate_for_exact_access(
    model: &EntityModel,
    access: &AccessPlan<Value>,
    normalized_predicate: Option<Predicate>,
) -> Option<Predicate> {
    let predicate = normalized_predicate?;

    if ExactPrimaryKeyAccess::from_access(access)
        .is_some_and(|access| access.matches_predicate(&predicate, model.primary_key.name))
    {
        return None;
    }

    Some(predicate)
}

///
/// ExactPrimaryKeyAccess
///
/// Local exact-primary-key access shape used by query planning to decide
/// whether one normalized predicate is already guaranteed by the chosen
/// authoritative access path.
///

enum ExactPrimaryKeyAccess<'a> {
    ByKey(&'a Value),
    ByKeys(&'a [Value]),
    HalfOpenRange { start: &'a Value, end: &'a Value },
}

impl<'a> ExactPrimaryKeyAccess<'a> {
    // Project one planner access path into the exact primary-key shapes that
    // can make a normalized predicate redundant.
    fn from_access(access: &'a AccessPlan<Value>) -> Option<Self> {
        if let Some(access_keys) = access.as_by_keys_path()
            && !access_keys.is_empty()
        {
            return Some(Self::ByKeys(access_keys));
        }
        if let Some(access_key) = access.as_by_key_path() {
            return Some(Self::ByKey(access_key));
        }

        access
            .as_primary_key_range_path()
            .map(|(start, end)| Self::HalfOpenRange { start, end })
    }

    // Return whether one normalized predicate is exactly the same primary-key
    // contract already guaranteed by this authoritative access path.
    fn matches_predicate(self, predicate: &Predicate, primary_key_name: &str) -> bool {
        match self {
            Self::ByKey(access_key) => {
                matches_primary_key_eq_predicate(predicate, primary_key_name, access_key)
            }
            Self::ByKeys(access_keys) => {
                matches_primary_key_in_predicate(predicate, primary_key_name, access_keys)
            }
            Self::HalfOpenRange { start, end } => {
                matches_primary_key_half_open_range(predicate, primary_key_name, start, end)
            }
        }
    }
}

// Return whether one normalized predicate is exactly the same primary-key
// equality already guaranteed by one canonical `ByKey` access path.
fn matches_primary_key_eq_predicate(
    predicate: &Predicate,
    primary_key_name: &str,
    access_key: &Value,
) -> bool {
    let Predicate::Compare(cmp) = predicate else {
        return false;
    };
    cmp.field == primary_key_name && cmp.op == CompareOp::Eq && cmp.value == *access_key
}

// Return whether one normalized predicate is exactly the same primary-key IN
// set already guaranteed by one canonical `ByKeys` access path.
fn matches_primary_key_in_predicate(
    predicate: &Predicate,
    primary_key_name: &str,
    access_keys: &[Value],
) -> bool {
    let Predicate::Compare(cmp) = predicate else {
        return false;
    };
    if cmp.field != primary_key_name || cmp.op != CompareOp::In {
        return false;
    }

    let Value::List(predicate_keys) = &cmp.value else {
        return false;
    };

    let mut canonical_predicate_keys = predicate_keys.clone();
    canonicalize_value_set(&mut canonical_predicate_keys);

    canonical_predicate_keys == access_keys
}

// Return whether one normalized predicate is exactly the same half-open
// primary-key range already guaranteed by one `KeyRange` access path.
fn matches_primary_key_half_open_range(
    predicate: &Predicate,
    primary_key_name: &str,
    start: &Value,
    end: &Value,
) -> bool {
    let Predicate::And(children) = predicate else {
        return false;
    };
    if children.len() != 2 {
        return false;
    }

    let mut lower_matches = false;
    let mut upper_matches = false;
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return false;
        };
        if cmp.field != primary_key_name {
            return false;
        }

        match cmp.op {
            CompareOp::Gte if cmp.value == *start => lower_matches = true,
            CompareOp::Lt if cmp.value == *end => upper_matches = true,
            _ => return false,
        }
    }

    lower_matches && upper_matches
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
