//! Query-plan validation at logical and executor boundaries.
//!
//! Validation ownership contract:
//! - `validate_logical_plan_model` owns user-facing query semantics and emits `PlanError`.
//! - `validate_executor_plan` is defensive: it re-checks owned semantics/invariants before
//!   execution and must not introduce new user-visible semantics.
//!
//! Future rule changes must declare a semantic owner. Defensive re-check layers may mirror
//! rules, but must not reinterpret semantics or error class intent.

mod access;
mod order;
mod pushdown;
mod semantics;

#[cfg(test)]
mod tests;

use crate::{
    db::{
        cursor::CursorDecodeError,
        query::{
            plan::LogicalPlan,
            policy::PlanPolicyError,
            predicate::{self, SchemaInfo},
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{entity::EntityModel, index::IndexModel},
    traits::EntityKind,
    value::Value,
};
use thiserror::Error as ThisError;

// re-exports
pub(crate) use access::{validate_access_plan, validate_access_plan_model};
pub(crate) use order::{validate_order, validate_primary_key_tie_break};
#[cfg(test)]
pub(crate) use pushdown::assess_secondary_order_pushdown_if_applicable;
pub(crate) use pushdown::{
    PushdownApplicability, PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility,
    SecondaryOrderPushdownRejection, assess_secondary_order_pushdown,
    assess_secondary_order_pushdown_if_applicable_validated,
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

    /// Ordered plans must terminate with the primary-key tie-break.
    #[error("order specification must end with primary key '{field}' as deterministic tie-break")]
    MissingPrimaryKeyTieBreak { field: String },
}

///
/// AccessPlanError
///
/// Access-path and key-shape validation failures.
///
#[derive(Debug, ThisError)]
pub enum AccessPlanError {
    /// Access plan references an index not declared on the entity.
    #[error("index '{index}' not found on entity")]
    IndexNotFound { index: IndexModel },

    /// Index prefix exceeds the number of indexed fields.
    #[error("index prefix length {prefix_len} exceeds index field count {field_len}")]
    IndexPrefixTooLong { prefix_len: usize, field_len: usize },

    /// Index prefix must include at least one value.
    #[error("index prefix must include at least one value")]
    IndexPrefixEmpty,

    /// Index prefix literal does not match indexed field type.
    #[error("index prefix value for field '{field}' is incompatible")]
    IndexPrefixValueMismatch { field: String },

    /// Primary key field exists but is not key-compatible.
    #[error("primary key field '{field}' is not key-compatible")]
    PrimaryKeyNotKeyable { field: String },

    /// Supplied key does not match the primary key type.
    #[error("key '{key:?}' is incompatible with primary key '{field}'")]
    PrimaryKeyMismatch { field: String, key: Value },

    /// Key range has invalid ordering.
    #[error("key range start is greater than end")]
    InvalidKeyRange,
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
/// CursorPlanError
///
/// Cursor token and continuation boundary validation failures.
///
#[derive(Debug, ThisError)]
pub enum CursorPlanError {
    /// Cursor continuation requires an explicit ordering.
    #[error("cursor pagination requires an explicit ordering")]
    CursorRequiresOrder,

    /// Cursor token could not be decoded.
    #[error("invalid continuation cursor: {reason}")]
    InvalidContinuationCursor { reason: CursorDecodeError },

    /// Cursor token payload/semantics are invalid after token decode.
    #[error("invalid continuation cursor: {reason}")]
    InvalidContinuationCursorPayload { reason: String },

    /// Cursor token version is unsupported.
    #[error("unsupported continuation cursor version: {version}")]
    ContinuationCursorVersionMismatch { version: u8 },

    /// Cursor token does not belong to this canonical query shape.
    #[error(
        "continuation cursor does not match query plan signature for '{entity_path}': expected={expected}, actual={actual}"
    )]
    ContinuationCursorSignatureMismatch {
        entity_path: &'static str,
        expected: String,
        actual: String,
    },

    /// Cursor boundary width does not match canonical order width.
    #[error("continuation cursor boundary arity mismatch: expected {expected}, found {found}")]
    ContinuationCursorBoundaryArityMismatch { expected: usize, found: usize },

    /// Cursor boundary value type mismatch for a non-primary-key ordered field.
    #[error(
        "continuation cursor boundary type mismatch for field '{field}': expected {expected}, found {value:?}"
    )]
    ContinuationCursorBoundaryTypeMismatch {
        field: String,
        expected: String,
        value: Value,
    },

    /// Cursor primary-key boundary does not match the entity key type.
    #[error(
        "continuation cursor primary key type mismatch for '{field}': expected {expected}, found {value:?}"
    )]
    ContinuationCursorPrimaryKeyTypeMismatch {
        field: String,
        expected: String,
        value: Option<Value>,
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
pub(crate) fn validate_logical_plan_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &LogicalPlan<Value>,
) -> Result<(), PlanError> {
    validate_plan_core(
        schema,
        model,
        plan,
        validate_order,
        |schema, model, plan| validate_access_plan_model(schema, model, &plan.access),
    )?;

    Ok(())
}

/// Validate plans at executor boundaries and surface invariant violations.
///
/// Ownership:
/// - defensive execution-boundary guardrail, not a semantic owner
/// - must enforce structural integrity only, never user-shape semantics
///
/// Any disagreement with logical validation indicates an internal bug and is not
/// a recoverable user-input condition.
pub(crate) fn validate_executor_plan<E: EntityKind>(
    plan: &LogicalPlan<E::Key>,
) -> Result<(), InternalError> {
    let schema = SchemaInfo::from_entity_model(E::MODEL).map_err(|err| {
        InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            format!("entity schema invalid for {}: {err}", E::PATH),
        )
    })?;

    validate_access_plan(&schema, E::MODEL, &plan.access)
        .map_err(InternalError::from_executor_plan_error)?;

    Ok(())
}

// Shared logical plan validation core owned by planner semantics.
fn validate_plan_core<K, FOrder, FAccess>(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &LogicalPlan<K>,
    validate_order_fn: FOrder,
    validate_access_fn: FAccess,
) -> Result<(), PlanError>
where
    FOrder: Fn(&SchemaInfo, &crate::db::query::plan::OrderSpec) -> Result<(), PlanError>,
    FAccess: Fn(&SchemaInfo, &EntityModel, &LogicalPlan<K>) -> Result<(), PlanError>,
{
    if let Some(predicate) = &plan.predicate {
        predicate::validate(schema, predicate)?;
    }

    if let Some(order) = &plan.order {
        validate_order_fn(schema, order)?;
        validate_primary_key_tie_break(model, order)?;
    }

    validate_access_fn(schema, model, plan)?;
    semantics::validate_plan_semantics(plan)?;

    Ok(())
}

// Map shared `PlanError` validation failures into executor-boundary invariant errors.
impl InternalError {
    fn from_executor_plan_error(err: PlanError) -> Self {
        Self::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            err.to_string(),
        )
    }
}
