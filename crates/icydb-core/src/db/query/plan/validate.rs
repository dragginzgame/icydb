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
        contracts::{SchemaInfo, ValidateError},
        cursor::CursorPlanError,
        policy::{self, PlanPolicyError},
        query::{
            group::{GroupAggregateKind, GroupSpec},
            plan::{AccessPlannedQuery, LogicalPlan, OrderSpec, ScalarPlan},
            predicate,
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

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
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
/// GroupPlanError
///
/// GROUP BY wrapper validation failures owned by query planning.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum GroupPlanError {
    /// Grouped validation entrypoint received a scalar logical plan.
    #[error("group query validation requires grouped logical plan variant")]
    GroupedLogicalPlanRequired,

    /// GROUP BY requires at least one declared grouping field.
    #[error("group specification must include at least one group field")]
    EmptyGroupFields,

    /// GROUP BY requires at least one aggregate terminal.
    #[error("group specification must include at least one aggregate terminal")]
    EmptyAggregates,

    /// GROUP BY references an unknown group field.
    #[error("unknown group field '{field}'")]
    UnknownGroupField { field: String },

    /// Aggregate target fields must resolve in the model schema.
    #[error("unknown grouped aggregate target field at index={index}: '{field}'")]
    UnknownAggregateTargetField { index: usize, field: String },

    /// Field-target grouped terminals are currently limited to MIN/MAX.
    #[error(
        "grouped aggregate at index={index} requires MIN/MAX when targeting field '{field}': found {kind}"
    )]
    FieldTargetRequiresExtrema {
        index: usize,
        kind: String,
        field: String,
    },
}

impl From<PlanPolicyError> for PolicyPlanError {
    fn from(err: PlanPolicyError) -> Self {
        match err {
            PlanPolicyError::EmptyOrderSpec => Self::EmptyOrderSpec,
            PlanPolicyError::DeletePlanWithPagination => Self::DeletePlanWithPagination,
            PlanPolicyError::LoadPlanWithDeleteLimit => Self::LoadPlanWithDeleteLimit,
            PlanPolicyError::DeleteLimitRequiresOrder => Self::DeleteLimitRequiresOrder,
            PlanPolicyError::UnorderedPagination => Self::UnorderedPagination,
        }
    }
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

impl From<PlanPolicyError> for PlanError {
    fn from(err: PlanPolicyError) -> Self {
        Self::from(PolicyPlanError::from(err))
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
    let logical = plan.scalar_plan();
    let group = match &plan.logical {
        LogicalPlan::Grouped(grouped) => &grouped.group,
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
    validate_group_spec(schema, model, group)?;

    Ok(())
}

/// Validate one grouped declarative spec against schema-level field surface.
pub(crate) fn validate_group_spec(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        return Err(PlanError::from(GroupPlanError::EmptyGroupFields));
    }
    if group.aggregates.is_empty() {
        return Err(PlanError::from(GroupPlanError::EmptyAggregates));
    }

    for field_slot in &group.group_fields {
        if model.fields.get(field_slot.index()).is_none() {
            return Err(PlanError::from(GroupPlanError::UnknownGroupField {
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
        if !matches!(
            aggregate.kind,
            GroupAggregateKind::Min | GroupAggregateKind::Max
        ) {
            return Err(PlanError::from(
                GroupPlanError::FieldTargetRequiresExtrema {
                    index,
                    kind: format!("{:?}", aggregate.kind),
                    field: target_field.clone(),
                },
            ));
        }
    }

    Ok(())
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
        predicate::validate(schema, predicate)?;
    }

    if let Some(order) = &logical.order {
        validate_order_fn(schema, order)?;
        validate_no_duplicate_non_pk_order_fields(model, order)?;
        validate_primary_key_tie_break(model, order)?;
    }

    validate_access_fn(schema, model, plan)?;
    policy::validate_plan_shape(&plan.logical)?;

    Ok(())
}
// ORDER validation ownership contract:
// - This module owns ORDER semantic validation (field existence/orderability/tie-break).
// - ORDER canonicalization (primary-key tie-break insertion) is performed at the
//   intent boundary via `canonicalize_order_spec` before plan validation.
// - Shape-policy checks (for example empty ORDER, pagination/order coupling) are owned by
//   `db::policy`.
// - Executor/runtime layers may defend execution preconditions only.

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
