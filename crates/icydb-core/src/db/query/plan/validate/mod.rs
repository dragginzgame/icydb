//! Query-plan validation for planner-owned logical semantics.
//!
//! Validation ownership contract:
//! - `validate_query_semantics` owns user-facing query semantics and emits `PlanError`.
//! - executor-boundary defensive checks live in `db::executor::plan_validate`.
//!
//! Future rule changes must declare a semantic owner. Defensive re-check layers may mirror
//! rules, but must not reinterpret semantics or error class intent.

mod order;

#[cfg(test)]
mod tests;

use crate::db::query::plan::{AccessPlannedQuery, OrderSpec};
use crate::{
    db::{
        access::{
            AccessPlanError,
            validate_access_structure_model as validate_access_structure_model_shared,
        },
        contracts::SchemaInfo,
        cursor::CursorPlanError,
        policy::{self, PlanPolicyError},
        query::predicate::{self},
    },
    model::entity::EntityModel,
    value::Value,
};
use thiserror::Error as ThisError;

pub(crate) use order::{
    validate_no_duplicate_non_pk_order_fields, validate_order, validate_primary_key_tie_break,
};

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
    PredicateInvalid(Box<predicate::ValidateError>),

    #[error("{0}")]
    Order(Box<OrderPlanError>),

    #[error("{0}")]
    Access(Box<AccessPlanError>),

    #[error("{0}")]
    Policy(Box<PolicyPlanError>),

    #[error("{0}")]
    Cursor(Box<CursorPlanError>),
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

impl From<predicate::ValidateError> for PlanError {
    fn from(err: predicate::ValidateError) -> Self {
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
    validate_plan_core(
        schema,
        model,
        plan,
        validate_order,
        |schema, model, plan| {
            validate_access_structure_model_shared(schema, model, &plan.access)
                .map_err(PlanError::from)
        },
    )?;

    Ok(())
}

// Shared logical plan validation core owned by planner semantics.
fn validate_plan_core<K, FOrder, FAccess>(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
    validate_order_fn: FOrder,
    validate_access_fn: FAccess,
) -> Result<(), PlanError>
where
    FOrder: Fn(&SchemaInfo, &OrderSpec) -> Result<(), PlanError>,
    FAccess: Fn(&SchemaInfo, &EntityModel, &AccessPlannedQuery<K>) -> Result<(), PlanError>,
{
    if let Some(predicate) = &plan.predicate {
        predicate::validate(schema, predicate)?;
    }

    if let Some(order) = &plan.order {
        validate_order_fn(schema, order)?;
        validate_no_duplicate_non_pk_order_fields(model, order)?;
        validate_primary_key_tie_break(model, order)?;
    }

    validate_access_fn(schema, model, plan)?;
    policy::validate_plan_shape(plan)?;

    Ok(())
}
