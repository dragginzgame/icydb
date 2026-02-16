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
    db::query::{
        plan::LogicalPlan,
        policy::{CursorOrderPolicyError, PlanPolicyError},
        predicate::{self, SchemaInfo},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{entity::EntityModel, index::IndexModel},
    traits::EntityKind,
    value::Value,
};
use thiserror::Error as ThisError;

pub(crate) use access::{validate_access_plan, validate_access_plan_model};
pub(crate) use order::{validate_order, validate_primary_key_tie_break};
pub(crate) use pushdown::assess_secondary_order_pushdown;
pub(crate) use pushdown::{SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection};

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
    PredicateInvalid(#[from] predicate::ValidateError),

    /// ORDER BY references an unknown field.
    #[error("unknown order field '{field}'")]
    UnknownOrderField { field: String },

    /// ORDER BY references a field that cannot be ordered.
    #[error("order field '{field}' is not orderable")]
    UnorderableField { field: String },

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

    /// ORDER BY must specify at least one field.
    #[error("order specification must include at least one field")]
    EmptyOrderSpec,

    /// Ordered plans must terminate with the primary-key tie-break.
    #[error("order specification must end with primary key '{field}' as deterministic tie-break")]
    MissingPrimaryKeyTieBreak { field: String },

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

    /// Cursor continuation requires an explicit ordering.
    #[error("cursor pagination requires an explicit ordering")]
    CursorRequiresOrder,

    /// Cursor token could not be decoded.
    #[error("invalid continuation cursor: {reason}")]
    InvalidContinuationCursor { reason: String },

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

impl From<PlanPolicyError> for PlanError {
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

impl From<CursorOrderPolicyError> for PlanError {
    fn from(err: CursorOrderPolicyError) -> Self {
        match err {
            CursorOrderPolicyError::CursorRequiresOrder => Self::CursorRequiresOrder,
        }
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
    if let Some(predicate) = &plan.predicate {
        predicate::validate(schema, predicate)?;
    }

    if let Some(order) = &plan.order {
        validate_order(schema, order)?;
        validate_primary_key_tie_break(model, order)?;
    }

    // Keep pushdown eligibility diagnostics aligned with planning validation.
    let _pushdown_eligibility = assess_secondary_order_pushdown(model, plan);

    validate_access_plan_model(schema, model, &plan.access)?;
    semantics::validate_plan_semantics(plan)?;

    Ok(())
}

/// Validate plans at executor boundaries and surface invariant violations.
///
/// Ownership:
/// - defensive execution-boundary guardrail, not a semantic owner
/// - must map violations to internal invariant failures, never new user semantics
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

    if let Some(predicate) = &plan.predicate {
        predicate::validate(&schema, predicate).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                err.to_string(),
            )
        })?;
    }

    if let Some(order) = &plan.order {
        order::validate_executor_order(&schema, order).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                err.to_string(),
            )
        })?;
        validate_primary_key_tie_break(E::MODEL, order).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                err.to_string(),
            )
        })?;
    }

    validate_access_plan(&schema, E::MODEL, &plan.access).map_err(|err| {
        InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            err.to_string(),
        )
    })?;

    semantics::validate_plan_semantics(plan).map_err(|err| {
        InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            err.to_string(),
        )
    })?;

    Ok(())
}
