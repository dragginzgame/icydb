//! Query-plan validation for planner-owned logical semantics.
//!
//! Validation ownership contract:
//! - `validate_query_semantics` owns user-facing query semantics and emits `PlanError`.
//! - executor-boundary defensive checks live in `db::executor::plan_validate`.
//!
//! Future rule changes must declare a semantic owner. Defensive re-check layers may mirror
//! rules, but must not reinterpret semantics or error class intent.

use crate::{
    db::{
        access::{
            AccessPlanError,
            validate_access_structure_model as validate_access_structure_model_shared,
        },
        cursor::CursorPlanError,
        predicate::{CompareOp, SchemaInfo, ValidateError, validate},
        query::plan::{
            AccessPlannedQuery, FieldSlot, GroupAggregateSpec, GroupDistinctAdmissibility,
            GroupDistinctPolicyReason, GroupHavingSpec, GroupHavingSymbol, GroupSpec, LoadSpec,
            LogicalPlan, OrderSpec, QueryMode, ScalarPlan,
            global_distinct_field_aggregate_admissibility, grouped_distinct_admissibility,
            is_global_distinct_field_aggregate_candidate,
        },
    },
    model::entity::EntityModel,
    value::Value,
};
use std::collections::BTreeSet;
use thiserror::Error as ThisError;

///
/// PlanError
///
/// Executor-visible validation failures for logical plans.
///
/// These errors indicate that a plan cannot be safely executed against the
/// current schema or entity definition. They are *not* planner bugs.
///

#[derive(Debug, ThisError)]
pub enum PlanError {
    #[error("predicate validation failed: {0}")]
    PredicateInvalid(Box<ValidateError>),

    #[error("{0}")]
    Order(Box<OrderPlanError>),

    #[error("{0}")]
    Access(Box<AccessPlanError>),

    #[error("{0}")]
    Policy(Box<PolicyPlanError>),

    #[error("{0}")]
    Cursor(Box<CursorPlanError>),

    #[error("{0}")]
    Group(Box<GroupPlanError>),
}

///
/// OrderPlanError
///
/// ORDER BY-specific validation failures.
///

#[derive(Debug, ThisError)]
pub enum OrderPlanError {
    /// ORDER BY references an unknown field.
    #[error("unknown order field '{field}'")]
    UnknownField { field: String },

    /// ORDER BY references a field that cannot be ordered.
    #[error("order field '{field}' is not orderable")]
    UnorderableField { field: String },

    /// ORDER BY references the same non-primary-key field multiple times.
    #[error("order field '{field}' appears multiple times")]
    DuplicateOrderField { field: String },

    /// Ordered plans must terminate with the primary-key tie-break.
    #[error("order specification must end with primary key '{field}' as deterministic tie-break")]
    MissingPrimaryKeyTieBreak { field: String },
}

///
/// PolicyPlanError
///
/// Plan-shape policy failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum PolicyPlanError {
    /// ORDER BY must specify at least one field.
    #[error("order specification must include at least one field")]
    EmptyOrderSpec,

    /// Delete plans must not carry pagination.
    #[error("delete plans must not include pagination")]
    DeletePlanWithPagination,

    /// Load plans must not carry delete limits.
    #[error("load plans must not include delete limits")]
    LoadPlanWithDeleteLimit,

    /// Delete limits require an explicit ordering.
    #[error("delete limit requires an explicit ordering")]
    DeleteLimitRequiresOrder,

    /// Pagination requires an explicit ordering.
    #[error(
        "Unordered pagination is not allowed.\nThis query uses LIMIT or OFFSET without an ORDER BY clause.\nPagination without a total ordering is non-deterministic.\nAdd an explicit order_by(...) to make the query stable."
    )]
    UnorderedPagination,
}

///
/// CursorPagingPolicyError
///
/// Cursor pagination readiness errors shared by intent/fluent entry surfaces.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum CursorPagingPolicyError {
    #[error(
        "{message}",
        message = CursorPlanError::cursor_requires_order_message()
    )]
    CursorRequiresOrder,

    #[error(
        "{message}",
        message = CursorPlanError::cursor_requires_limit_message()
    )]
    CursorRequiresLimit,
}

///
/// GroupPlanError
///
/// GROUP BY wrapper validation failures owned by query planning.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum GroupPlanError {
    /// HAVING requires GROUP BY grouped plan shape.
    #[error("HAVING is only supported for GROUP BY queries in this release")]
    HavingRequiresGroupBy,

    /// Grouped validation entrypoint received a scalar logical plan.
    #[error("group query validation requires grouped logical plan variant")]
    GroupedLogicalPlanRequired,

    /// GROUP BY requires at least one declared grouping field.
    #[error("group specification must include at least one group field")]
    EmptyGroupFields,

    /// Global DISTINCT aggregate shapes without GROUP BY are restricted.
    #[error(
        "global DISTINCT aggregate without GROUP BY must declare exactly one DISTINCT field-target aggregate in this release"
    )]
    GlobalDistinctAggregateShapeUnsupported,

    /// GROUP BY requires at least one aggregate terminal.
    #[error("group specification must include at least one aggregate terminal")]
    EmptyAggregates,

    /// GROUP BY references an unknown group field.
    #[error("unknown group field '{field}'")]
    UnknownGroupField { field: String },

    /// GROUP BY must not repeat the same resolved group slot.
    #[error("group specification has duplicate group key: '{field}'")]
    DuplicateGroupField { field: String },

    /// GROUP BY v1 does not accept DISTINCT unless adjacency eligibility is explicit.
    #[error(
        "grouped DISTINCT requires adjacency-based ordered-group eligibility proof in this release"
    )]
    DistinctAdjacencyEligibilityRequired,

    /// GROUP BY ORDER BY shape must start with grouped-key prefix.
    #[error("grouped ORDER BY must start with GROUP BY key prefix in this release")]
    OrderPrefixNotAlignedWithGroupKeys,

    /// GROUP BY ORDER BY requires an explicit LIMIT in grouped v1.
    #[error("grouped ORDER BY requires LIMIT in this release")]
    OrderRequiresLimit,

    /// HAVING with DISTINCT is deferred until grouped DISTINCT support expands.
    #[error("grouped HAVING with DISTINCT is not supported in this release")]
    DistinctHavingUnsupported,

    /// HAVING currently supports compare operators only.
    #[error("grouped HAVING clause at index={index} uses unsupported operator: {op}")]
    HavingUnsupportedCompareOp { index: usize, op: String },

    /// HAVING group-field symbols must reference declared grouped keys.
    #[error("grouped HAVING clause at index={index} references non-group field '{field}'")]
    HavingNonGroupFieldReference { index: usize, field: String },

    /// HAVING aggregate references must resolve to declared grouped terminals.
    #[error(
        "grouped HAVING clause at index={index} references aggregate index {aggregate_index} but aggregate_count={aggregate_count}"
    )]
    HavingAggregateIndexOutOfBounds {
        index: usize,
        aggregate_index: usize,
        aggregate_count: usize,
    },

    /// DISTINCT grouped terminal kinds are intentionally conservative in v1.
    #[error(
        "grouped DISTINCT aggregate at index={index} uses unsupported kind '{kind}' in this release"
    )]
    DistinctAggregateKindUnsupported { index: usize, kind: String },

    /// DISTINCT over grouped field-target terminals is deferred with field-target support.
    #[error(
        "grouped DISTINCT aggregate at index={index} cannot target field '{field}' in this release: found {kind}"
    )]
    DistinctAggregateFieldTargetUnsupported {
        index: usize,
        kind: String,
        field: String,
    },

    /// Aggregate target fields must resolve in the model schema.
    #[error("unknown grouped aggregate target field at index={index}: '{field}'")]
    UnknownAggregateTargetField { index: usize, field: String },

    /// Global DISTINCT SUM requires a numeric field target.
    #[error(
        "global DISTINCT SUM aggregate target field at index={index} is not numeric: '{field}'"
    )]
    GlobalDistinctSumTargetNotNumeric { index: usize, field: String },

    /// Field-target grouped terminals are not enabled in grouped execution v1.
    #[error(
        "grouped aggregate at index={index} cannot target field '{field}' in this release: found {kind}"
    )]
    FieldTargetAggregatesUnsupported {
        index: usize,
        kind: String,
        field: String,
    },
}

///
/// CursorOrderPlanShapeError
///
/// Logical cursor-order plan-shape failures used by cursor/runtime boundary adapters.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CursorOrderPlanShapeError {
    MissingExplicitOrder,
    EmptyOrderSpec,
}

///
/// IntentKeyAccessKind
///
/// Key-access shape used by intent policy validation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IntentKeyAccessKind {
    Single,
    Many,
    Only,
}

///
/// IntentKeyAccessPolicyViolation
///
/// Logical key-access policy violations at query-intent boundaries.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IntentKeyAccessPolicyViolation {
    KeyAccessConflict,
    ByIdsWithPredicate,
    OnlyWithPredicate,
}

///
/// FluentLoadPolicyViolation
///
/// Fluent load-entry policy violations.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FluentLoadPolicyViolation {
    CursorRequiresPagedExecution,
    GroupedRequiresExecuteGrouped,
    CursorRequiresOrder,
    CursorRequiresLimit,
}

impl From<ValidateError> for PlanError {
    fn from(err: ValidateError) -> Self {
        Self::PredicateInvalid(Box::new(err))
    }
}

impl From<OrderPlanError> for PlanError {
    fn from(err: OrderPlanError) -> Self {
        Self::Order(Box::new(err))
    }
}

impl From<AccessPlanError> for PlanError {
    fn from(err: AccessPlanError) -> Self {
        Self::Access(Box::new(err))
    }
}

impl From<PolicyPlanError> for PlanError {
    fn from(err: PolicyPlanError) -> Self {
        Self::Policy(Box::new(err))
    }
}

impl From<CursorPlanError> for PlanError {
    fn from(err: CursorPlanError) -> Self {
        Self::Cursor(Box::new(err))
    }
}

impl From<GroupPlanError> for PlanError {
    fn from(err: GroupPlanError) -> Self {
        Self::Group(Box::new(err))
    }
}

/// Validate a logical plan with model-level key values.
///
/// Ownership:
/// - semantic owner for user-facing query validity at planning boundaries
/// - failures here are user-visible planning failures (`PlanError`)
///
/// New user-facing validation rules must be introduced here first, then mirrored
/// defensively in downstream layers without changing semantics.
pub(crate) fn validate_query_semantics(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &AccessPlannedQuery<Value>,
) -> Result<(), PlanError> {
    let logical = plan.scalar_plan();

    validate_plan_core(
        schema,
        model,
        logical,
        plan,
        validate_order,
        |schema, model, plan| {
            validate_access_structure_model_shared(schema, model, &plan.access)
                .map_err(PlanError::from)
        },
    )?;

    Ok(())
}

/// Validate grouped query semantics for one grouped plan wrapper.
///
/// Ownership:
/// - semantic owner for GROUP BY wrapper validation
/// - failures here are user-visible planning failures (`PlanError`)
pub(crate) fn validate_group_query_semantics(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &AccessPlannedQuery<Value>,
) -> Result<(), PlanError> {
    let (logical, group, having) = match &plan.logical {
        LogicalPlan::Grouped(grouped) => (&grouped.scalar, &grouped.group, grouped.having.as_ref()),
        LogicalPlan::Scalar(_) => {
            return Err(PlanError::from(GroupPlanError::GroupedLogicalPlanRequired));
        }
    };

    validate_plan_core(
        schema,
        model,
        logical,
        plan,
        validate_order,
        |schema, model, plan| {
            validate_access_structure_model_shared(schema, model, &plan.access)
                .map_err(PlanError::from)
        },
    )?;
    validate_group_structure(schema, model, group, having)?;
    validate_group_policy(schema, logical, group, having)?;
    validate_group_cursor_constraints(logical, group)?;

    Ok(())
}

// Validate grouped structural invariants before policy/cursor gates.
fn validate_group_structure(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() && having.is_some() {
        return Err(PlanError::from(
            GroupPlanError::GlobalDistinctAggregateShapeUnsupported,
        ));
    }

    validate_group_spec_structure(schema, model, group)?;
    validate_grouped_having_structure(group, having)?;

    Ok(())
}

// Validate grouped policy gates independent from structural shape checks.
fn validate_group_policy(
    schema: &SchemaInfo,
    logical: &ScalarPlan,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    validate_grouped_distinct_policy(logical, having.is_some())?;
    validate_grouped_having_policy(having)?;
    validate_group_spec_policy(schema, group, having)?;

    Ok(())
}

// Validate grouped cursor-order constraints in one dedicated gate.
fn validate_group_cursor_constraints(
    logical: &ScalarPlan,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    // Grouped pagination/order constraints are cursor-domain policy:
    // grouped ORDER BY requires LIMIT and must align with grouped-key prefix.
    let Some(order) = logical.order.as_ref() else {
        return Ok(());
    };
    if logical.page.as_ref().and_then(|page| page.limit).is_none() {
        return Err(PlanError::from(GroupPlanError::OrderRequiresLimit));
    }
    if order_prefix_aligned_with_group_fields(order, group.group_fields.as_slice()) {
        return Ok(());
    }

    Err(PlanError::from(
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys,
    ))
}

// Validate grouped DISTINCT policy gates for grouped v1 hardening.
fn validate_grouped_distinct_policy(
    logical: &ScalarPlan,
    has_having: bool,
) -> Result<(), PlanError> {
    match grouped_distinct_admissibility(logical.distinct, has_having) {
        GroupDistinctAdmissibility::Allowed => Ok(()),
        GroupDistinctAdmissibility::Disallowed(reason) => Err(PlanError::from(
            group_plan_error_from_distinct_policy_reason(reason, None),
        )),
    }
}

// Validate grouped HAVING structural symbol/reference compatibility.
fn validate_grouped_having_structure(
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    let Some(having) = having else {
        return Ok(());
    };

    for (index, clause) in having.clauses().iter().enumerate() {
        match clause.symbol() {
            GroupHavingSymbol::GroupField(field_slot) => {
                if !group
                    .group_fields
                    .iter()
                    .any(|group_field| group_field.index() == field_slot.index())
                {
                    return Err(PlanError::from(
                        GroupPlanError::HavingNonGroupFieldReference {
                            index,
                            field: field_slot.field().to_string(),
                        },
                    ));
                }
            }
            GroupHavingSymbol::AggregateIndex(aggregate_index) => {
                if *aggregate_index >= group.aggregates.len() {
                    return Err(PlanError::from(
                        GroupPlanError::HavingAggregateIndexOutOfBounds {
                            index,
                            aggregate_index: *aggregate_index,
                            aggregate_count: group.aggregates.len(),
                        },
                    ));
                }
            }
        }
    }

    Ok(())
}

// Validate grouped HAVING policy gates and operator support.
fn validate_grouped_having_policy(having: Option<&GroupHavingSpec>) -> Result<(), PlanError> {
    let Some(having) = having else {
        return Ok(());
    };

    for (index, clause) in having.clauses().iter().enumerate() {
        if !having_compare_op_supported(clause.op()) {
            return Err(PlanError::from(
                GroupPlanError::HavingUnsupportedCompareOp {
                    index,
                    op: format!("{:?}", clause.op()),
                },
            ));
        }
    }

    Ok(())
}

const fn having_compare_op_supported(op: CompareOp) -> bool {
    matches!(
        op,
        CompareOp::Eq
            | CompareOp::Ne
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
    )
}

// Return true when ORDER BY starts with GROUP BY key fields in declaration order.
fn order_prefix_aligned_with_group_fields(order: &OrderSpec, group_fields: &[FieldSlot]) -> bool {
    if order.fields.len() < group_fields.len() {
        return false;
    }

    group_fields
        .iter()
        .zip(order.fields.iter())
        .all(|(group_field, (order_field, _))| order_field == group_field.field())
}

// Validate grouped structural declarations against model/schema shape.
fn validate_group_spec_structure(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        if group.aggregates.iter().any(GroupAggregateSpec::distinct) {
            return Ok(());
        }

        return Err(PlanError::from(GroupPlanError::EmptyGroupFields));
    }
    if group.aggregates.is_empty() {
        return Err(PlanError::from(GroupPlanError::EmptyAggregates));
    }

    let mut seen_group_slots = BTreeSet::<usize>::new();
    for field_slot in &group.group_fields {
        if model.fields.get(field_slot.index()).is_none() {
            return Err(PlanError::from(GroupPlanError::UnknownGroupField {
                field: field_slot.field().to_string(),
            }));
        }
        if !seen_group_slots.insert(field_slot.index()) {
            return Err(PlanError::from(GroupPlanError::DuplicateGroupField {
                field: field_slot.field().to_string(),
            }));
        }
    }

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        let Some(target_field) = aggregate.target_field.as_ref() else {
            continue;
        };
        if schema.field(target_field).is_none() {
            return Err(PlanError::from(
                GroupPlanError::UnknownAggregateTargetField {
                    index,
                    field: target_field.clone(),
                },
            ));
        }
    }

    Ok(())
}

// Validate grouped execution policy over a structurally valid grouped spec.
fn validate_group_spec_policy(
    schema: &SchemaInfo,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        validate_global_distinct_aggregate_without_group_keys(schema, group, having)?;
        return Ok(());
    }

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        if aggregate.distinct() && !aggregate.kind().supports_grouped_distinct_v1() {
            return Err(PlanError::from(
                GroupPlanError::DistinctAggregateKindUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                },
            ));
        }

        let Some(target_field) = aggregate.target_field.as_ref() else {
            continue;
        };
        if aggregate.distinct() {
            return Err(PlanError::from(
                GroupPlanError::DistinctAggregateFieldTargetUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                    field: target_field.clone(),
                },
            ));
        }
        return Err(PlanError::from(
            GroupPlanError::FieldTargetAggregatesUnsupported {
                index,
                kind: format!("{:?}", aggregate.kind()),
                field: target_field.clone(),
            },
        ));
    }

    Ok(())
}

// Validate the restricted global DISTINCT aggregate shape (`GROUP BY` omitted).
fn validate_global_distinct_aggregate_without_group_keys(
    schema: &SchemaInfo,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    if !is_global_distinct_field_aggregate_candidate(
        group.group_fields.as_slice(),
        group.aggregates.as_slice(),
    ) {
        return Err(PlanError::from(
            GroupPlanError::GlobalDistinctAggregateShapeUnsupported,
        ));
    }
    let aggregate = &group.aggregates[0];
    match global_distinct_field_aggregate_admissibility(group.aggregates.as_slice(), having) {
        GroupDistinctAdmissibility::Allowed => {}
        GroupDistinctAdmissibility::Disallowed(reason) => {
            return Err(PlanError::from(
                group_plan_error_from_distinct_policy_reason(reason, Some(aggregate)),
            ));
        }
    }

    let Some(target_field) = aggregate.target_field() else {
        return Err(PlanError::from(
            GroupPlanError::GlobalDistinctAggregateShapeUnsupported,
        ));
    };
    let Some(field_type) = schema.field(target_field) else {
        return Err(PlanError::from(
            GroupPlanError::UnknownAggregateTargetField {
                index: 0,
                field: target_field.to_string(),
            },
        ));
    };
    if aggregate.kind().is_sum() && !field_type.supports_numeric_coercion() {
        return Err(PlanError::from(
            GroupPlanError::GlobalDistinctSumTargetNotNumeric {
                index: 0,
                field: target_field.to_string(),
            },
        ));
    }

    Ok(())
}

// Map one grouped DISTINCT policy reason to planner-visible grouped plan errors.
fn group_plan_error_from_distinct_policy_reason(
    reason: GroupDistinctPolicyReason,
    aggregate: Option<&GroupAggregateSpec>,
) -> GroupPlanError {
    match reason {
        GroupDistinctPolicyReason::DistinctHavingUnsupported => {
            GroupPlanError::DistinctHavingUnsupported
        }
        GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired => {
            GroupPlanError::DistinctAdjacencyEligibilityRequired
        }
        GroupDistinctPolicyReason::GlobalDistinctHavingUnsupported
        | GroupDistinctPolicyReason::GlobalDistinctRequiresSingleAggregate
        | GroupDistinctPolicyReason::GlobalDistinctRequiresFieldTargetAggregate
        | GroupDistinctPolicyReason::GlobalDistinctRequiresDistinctAggregateTerminal => {
            GroupPlanError::GlobalDistinctAggregateShapeUnsupported
        }
        GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind => {
            let kind = aggregate.map_or_else(
                || "Unknown".to_string(),
                |aggregate| format!("{:?}", aggregate.kind()),
            );
            GroupPlanError::DistinctAggregateKindUnsupported { index: 0, kind }
        }
    }
}

// Shared logical plan validation core owned by planner semantics.
fn validate_plan_core<K, FOrder, FAccess>(
    schema: &SchemaInfo,
    model: &EntityModel,
    logical: &ScalarPlan,
    plan: &AccessPlannedQuery<K>,
    validate_order_fn: FOrder,
    validate_access_fn: FAccess,
) -> Result<(), PlanError>
where
    FOrder: Fn(&SchemaInfo, &OrderSpec) -> Result<(), PlanError>,
    FAccess: Fn(&SchemaInfo, &EntityModel, &AccessPlannedQuery<K>) -> Result<(), PlanError>,
{
    if let Some(predicate) = &logical.predicate {
        validate(schema, predicate)?;
    }

    if let Some(order) = &logical.order {
        validate_order_fn(schema, order)?;
        validate_no_duplicate_non_pk_order_fields(model, order)?;
        validate_primary_key_tie_break(model, order)?;
    }

    validate_access_fn(schema, model, plan)?;
    validate_plan_shape(&plan.logical)?;

    Ok(())
}
// ORDER validation ownership contract:
// - This module owns ORDER semantic validation (field existence/orderability/tie-break).
// - ORDER canonicalization (primary-key tie-break insertion) is performed at the
//   intent boundary via `canonicalize_order_spec` before plan validation.
// - Shape-policy checks (for example empty ORDER, pagination/order coupling) are owned here.
// - Executor/runtime layers may defend execution preconditions only.

/// Return true when an ORDER BY exists and contains at least one field.
#[must_use]
pub(crate) fn has_explicit_order(order: Option<&OrderSpec>) -> bool {
    order.is_some_and(|order| !order.fields.is_empty())
}

/// Return true when an ORDER BY exists but is empty.
#[must_use]
pub(crate) fn has_empty_order(order: Option<&OrderSpec>) -> bool {
    order.is_some_and(|order| order.fields.is_empty())
}

/// Validate order-shape rules shared across intent and logical plan boundaries.
pub(crate) fn validate_order_shape(order: Option<&OrderSpec>) -> Result<(), PolicyPlanError> {
    if has_empty_order(order) {
        return Err(PolicyPlanError::EmptyOrderSpec);
    }

    Ok(())
}

/// Validate intent-level plan-shape rules derived from query mode + order.
pub(crate) fn validate_intent_plan_shape(
    mode: QueryMode,
    order: Option<&OrderSpec>,
) -> Result<(), PolicyPlanError> {
    validate_order_shape(order)?;

    let has_order = has_explicit_order(order);
    if matches!(mode, QueryMode::Delete(spec) if spec.limit.is_some()) && !has_order {
        return Err(PolicyPlanError::DeleteLimitRequiresOrder);
    }

    Ok(())
}

/// Validate cursor-pagination readiness for a load-spec + ordering pair.
pub(crate) const fn validate_cursor_paging_requirements(
    has_order: bool,
    spec: LoadSpec,
) -> Result<(), CursorPagingPolicyError> {
    if !has_order {
        return Err(CursorPagingPolicyError::CursorRequiresOrder);
    }
    if spec.limit.is_none() {
        return Err(CursorPagingPolicyError::CursorRequiresLimit);
    }

    Ok(())
}

/// Validate cursor-order shape and return the logical order contract when present.
pub(crate) const fn validate_cursor_order_plan_shape(
    order: Option<&OrderSpec>,
    require_explicit_order: bool,
) -> Result<Option<&OrderSpec>, CursorOrderPlanShapeError> {
    let Some(order) = order else {
        if require_explicit_order {
            return Err(CursorOrderPlanShapeError::MissingExplicitOrder);
        }

        return Ok(None);
    };

    if order.fields.is_empty() {
        return Err(CursorOrderPlanShapeError::EmptyOrderSpec);
    }

    Ok(Some(order))
}

/// Resolve one grouped field into a stable field slot.
pub(crate) fn resolve_group_field_slot(
    model: &EntityModel,
    field: &str,
) -> Result<FieldSlot, PlanError> {
    FieldSlot::resolve(model, field).ok_or_else(|| {
        PlanError::from(GroupPlanError::UnknownGroupField {
            field: field.to_string(),
        })
    })
}

/// Validate intent key-access policy before planning.
pub(crate) const fn validate_intent_key_access_policy(
    key_access_conflict: bool,
    key_access_kind: Option<IntentKeyAccessKind>,
    has_predicate: bool,
) -> Result<(), IntentKeyAccessPolicyViolation> {
    if key_access_conflict {
        return Err(IntentKeyAccessPolicyViolation::KeyAccessConflict);
    }

    match key_access_kind {
        Some(IntentKeyAccessKind::Many) if has_predicate => {
            Err(IntentKeyAccessPolicyViolation::ByIdsWithPredicate)
        }
        Some(IntentKeyAccessKind::Only) if has_predicate => {
            Err(IntentKeyAccessPolicyViolation::OnlyWithPredicate)
        }
        Some(
            IntentKeyAccessKind::Single | IntentKeyAccessKind::Many | IntentKeyAccessKind::Only,
        )
        | None => Ok(()),
    }
}

/// Validate fluent non-paged load entry policy.
pub(crate) const fn validate_fluent_non_paged_mode(
    has_cursor_token: bool,
    has_grouping: bool,
) -> Result<(), FluentLoadPolicyViolation> {
    if has_cursor_token {
        return Err(FluentLoadPolicyViolation::CursorRequiresPagedExecution);
    }
    if has_grouping {
        return Err(FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped);
    }

    Ok(())
}

/// Validate fluent paged load entry policy.
pub(crate) fn validate_fluent_paged_mode(
    has_grouping: bool,
    has_explicit_order: bool,
    spec: Option<LoadSpec>,
) -> Result<(), FluentLoadPolicyViolation> {
    if has_grouping {
        return Err(FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped);
    }

    let Some(spec) = spec else {
        return Ok(());
    };

    validate_cursor_paging_requirements(has_explicit_order, spec).map_err(|err| match err {
        CursorPagingPolicyError::CursorRequiresOrder => {
            FluentLoadPolicyViolation::CursorRequiresOrder
        }
        CursorPagingPolicyError::CursorRequiresLimit => {
            FluentLoadPolicyViolation::CursorRequiresLimit
        }
    })
}

/// Validate mode/order/pagination invariants for one logical plan.
pub(crate) fn validate_plan_shape(plan: &LogicalPlan) -> Result<(), PolicyPlanError> {
    let grouped = matches!(plan, LogicalPlan::Grouped(_));
    let plan = match plan {
        LogicalPlan::Scalar(plan) => plan,
        LogicalPlan::Grouped(plan) => &plan.scalar,
    };
    validate_order_shape(plan.order.as_ref())?;

    let has_order = has_explicit_order(plan.order.as_ref());
    if plan.delete_limit.is_some() && !has_order {
        return Err(PolicyPlanError::DeleteLimitRequiresOrder);
    }

    match plan.mode {
        QueryMode::Delete(_) => {
            if plan.page.is_some() {
                return Err(PolicyPlanError::DeletePlanWithPagination);
            }
        }
        QueryMode::Load(_) => {
            if plan.delete_limit.is_some() {
                return Err(PolicyPlanError::LoadPlanWithDeleteLimit);
            }
            // GROUP BY v1 uses canonical grouped key ordering when ORDER BY is
            // omitted, so grouped pagination remains deterministic without an
            // explicit sort clause.
            if plan.page.is_some() && !has_order && !grouped {
                return Err(PolicyPlanError::UnorderedPagination);
            }
        }
    }

    Ok(())
}

/// Validate ORDER BY fields against the schema.
pub(crate) fn validate_order(schema: &SchemaInfo, order: &OrderSpec) -> Result<(), PlanError> {
    for (field, _) in &order.fields {
        let field_type = schema
            .field(field)
            .ok_or_else(|| OrderPlanError::UnknownField {
                field: field.clone(),
            })
            .map_err(PlanError::from)?;

        if !field_type.is_orderable() {
            // CONTRACT: ORDER BY rejects non-queryable or unordered fields.
            return Err(PlanError::from(OrderPlanError::UnorderableField {
                field: field.clone(),
            }));
        }
    }

    Ok(())
}

/// Reject duplicate non-primary-key fields in ORDER BY.
pub(crate) fn validate_no_duplicate_non_pk_order_fields(
    model: &EntityModel,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    let mut seen = BTreeSet::new();
    let pk_field = model.primary_key.name;

    for (field, _) in &order.fields {
        if field == pk_field {
            continue;
        }
        if !seen.insert(field.as_str()) {
            return Err(PlanError::from(OrderPlanError::DuplicateOrderField {
                field: field.clone(),
            }));
        }
    }

    Ok(())
}

// Ordered plans must include exactly one terminal primary-key field so ordering is total and
// deterministic across explain, fingerprint, and executor comparison paths.
pub(crate) fn validate_primary_key_tie_break(
    model: &EntityModel,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    if order.fields.is_empty() {
        return Ok(());
    }

    let pk_field = model.primary_key.name;
    let pk_count = order
        .fields
        .iter()
        .filter(|(field, _)| field == pk_field)
        .count();
    let trailing_pk = order
        .fields
        .last()
        .is_some_and(|(field, _)| field == pk_field);

    if pk_count == 1 && trailing_pk {
        Ok(())
    } else {
        Err(PlanError::from(OrderPlanError::MissingPrimaryKeyTieBreak {
            field: pk_field.to_string(),
        }))
    }
}
