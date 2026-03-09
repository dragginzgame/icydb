use crate::db::{access::AccessPlanError, cursor::CursorPlanError, predicate::ValidateError};
use thiserror::Error as ThisError;

///
/// PlanError
///
/// Root plan validation taxonomy split by domain axis.
/// User-shape failures are grouped under `PlanUserError`.
/// Policy/capability failures are grouped under `PlanPolicyError`.
/// Cursor continuation failures remain in `CursorPlanError`.
///

#[derive(Debug, ThisError)]
pub enum PlanError {
    #[error("{0}")]
    User(Box<PlanUserError>),

    #[error("{0}")]
    Policy(Box<PlanPolicyError>),

    #[error("{0}")]
    Cursor(Box<CursorPlanError>),
}

///
/// PlanUserError
///
/// Planner user-shape validation failures independent of continuation cursors.
/// This axis intentionally excludes runtime routing/execution policy state and
/// release-gating capability decisions.
///

#[derive(Debug, ThisError)]
pub enum PlanUserError {
    #[error("predicate validation failed: {0}")]
    PredicateInvalid(Box<ValidateError>),

    #[error("{0}")]
    Order(Box<OrderPlanError>),

    #[error("{0}")]
    Access(Box<AccessPlanError>),

    #[error("{0}")]
    Group(Box<GroupPlanError>),

    #[error("{0}")]
    Expr(Box<ExprPlanError>),
}

///
/// PlanPolicyError
///
/// Planner policy/capability validation failures.
/// This axis captures query-shape constraints that are valid syntactically but
/// not supported by the current execution policy surface.
///

#[derive(Debug, ThisError)]
pub enum PlanPolicyError {
    #[error("{0}")]
    Policy(Box<PolicyPlanError>),

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

    /// Delete plans must not carry offsets.
    #[error("delete plans must not include OFFSET")]
    DeletePlanWithOffset,

    /// Delete plans must not carry grouped query wrappers.
    #[error("delete plans must not include GROUP BY or HAVING")]
    DeletePlanWithGrouping,

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
/// ExprPlanError
///
/// Expression-spine inference failures owned by planner semantics.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum ExprPlanError {
    /// Expression references a field that does not exist in schema.
    #[error("unknown expression field '{field}'")]
    UnknownExprField { field: String },

    /// Aggregate terminal requires a numeric target field.
    #[error("aggregate '{kind}' requires numeric target field '{field}'")]
    NonNumericAggregateTarget { kind: String, field: String },

    /// Aggregate expression requires an explicit target field.
    #[error("aggregate '{kind}' requires an explicit target field")]
    AggregateTargetRequired { kind: String },

    /// Unary operation is incompatible with inferred operand type.
    #[error("unary operator '{op}' is incompatible with operand type {found}")]
    InvalidUnaryOperand { op: String, found: String },

    /// Binary operation is incompatible with inferred operand types.
    #[error("binary operator '{op}' is incompatible with operand types ({left}, {right})")]
    InvalidBinaryOperands {
        op: String,
        left: String,
        right: String,
    },

    /// GROUP BY projections must not reference fields outside grouped keys.
    #[error(
        "grouped projection expression at index={index} references fields outside GROUP BY keys"
    )]
    GroupedProjectionReferencesNonGroupField { index: usize },
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
        Self::from(PlanUserError::from(err))
    }
}

impl From<OrderPlanError> for PlanError {
    fn from(err: OrderPlanError) -> Self {
        Self::from(PlanUserError::from(err))
    }
}

impl From<AccessPlanError> for PlanError {
    fn from(err: AccessPlanError) -> Self {
        Self::from(PlanUserError::from(err))
    }
}

impl From<PolicyPlanError> for PlanError {
    fn from(err: PolicyPlanError) -> Self {
        Self::from(PlanPolicyError::from(err))
    }
}

impl From<CursorPlanError> for PlanError {
    fn from(err: CursorPlanError) -> Self {
        Self::Cursor(Box::new(err))
    }
}

impl From<GroupPlanError> for PlanError {
    fn from(err: GroupPlanError) -> Self {
        if err.belongs_to_policy_axis() {
            return Self::from(PlanPolicyError::from(err));
        }

        Self::from(PlanUserError::from(err))
    }
}

impl From<ExprPlanError> for PlanError {
    fn from(err: ExprPlanError) -> Self {
        Self::from(PlanUserError::from(err))
    }
}

impl From<PlanUserError> for PlanError {
    fn from(err: PlanUserError) -> Self {
        Self::User(Box::new(err))
    }
}

impl From<PlanPolicyError> for PlanError {
    fn from(err: PlanPolicyError) -> Self {
        Self::Policy(Box::new(err))
    }
}

impl From<ValidateError> for PlanUserError {
    fn from(err: ValidateError) -> Self {
        Self::PredicateInvalid(Box::new(err))
    }
}

impl From<OrderPlanError> for PlanUserError {
    fn from(err: OrderPlanError) -> Self {
        Self::Order(Box::new(err))
    }
}

impl From<AccessPlanError> for PlanUserError {
    fn from(err: AccessPlanError) -> Self {
        Self::Access(Box::new(err))
    }
}

impl From<GroupPlanError> for PlanUserError {
    fn from(err: GroupPlanError) -> Self {
        Self::Group(Box::new(err))
    }
}

impl From<ExprPlanError> for PlanUserError {
    fn from(err: ExprPlanError) -> Self {
        Self::Expr(Box::new(err))
    }
}

impl From<PolicyPlanError> for PlanPolicyError {
    fn from(err: PolicyPlanError) -> Self {
        Self::Policy(Box::new(err))
    }
}

impl From<GroupPlanError> for PlanPolicyError {
    fn from(err: GroupPlanError) -> Self {
        Self::Group(Box::new(err))
    }
}

impl GroupPlanError {
    // Group-plan variants that represent release-gating/capability constraints
    // are classified under the policy axis to keep user-shape and policy
    // domains separated at the top-level `PlanError`.
    const fn belongs_to_policy_axis(&self) -> bool {
        matches!(
            self,
            Self::GlobalDistinctAggregateShapeUnsupported
                | Self::DistinctAdjacencyEligibilityRequired
                | Self::OrderPrefixNotAlignedWithGroupKeys
                | Self::OrderRequiresLimit
                | Self::DistinctHavingUnsupported
                | Self::HavingUnsupportedCompareOp { .. }
                | Self::DistinctAggregateKindUnsupported { .. }
                | Self::DistinctAggregateFieldTargetUnsupported { .. }
                | Self::FieldTargetAggregatesUnsupported { .. }
        )
    }
}
