//! Module: db::query::plan::validate::errors
//! Responsibility: own the query-plan validation error taxonomy and its
//! mapping from lower planner, cursor, and schema validation domains.
//! Does not own: the validation logic that decides which error applies.
//! Boundary: keeps query-plan validation failures under one planner-owned error surface.

use crate::db::{access::AccessPlanError, cursor::CursorPlanError, schema::ValidateError};
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

impl PlanError {
    /// Return whether this plan error is the deterministic pagination policy failure.
    #[must_use]
    pub fn is_unordered_pagination(&self) -> bool {
        matches!(
            self,
            Self::Policy(inner)
                if matches!(
                    inner.as_ref(),
                    PlanPolicyError::Policy(policy)
                        if matches!(policy.as_ref(), PolicyPlanError::UnorderedPagination)
                )
        )
    }
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

impl OrderPlanError {
    /// Construct one unorderable-field validation error.
    pub(in crate::db::query) fn unorderable_field(field: impl Into<String>) -> Self {
        Self::UnorderableField {
            field: field.into(),
        }
    }

    /// Construct one duplicate non-primary-key ORDER BY field validation error.
    pub(in crate::db::query) fn duplicate_order_field(field: impl Into<String>) -> Self {
        Self::DuplicateOrderField {
            field: field.into(),
        }
    }

    /// Construct one missing primary-key tie-break validation error.
    pub(in crate::db::query) fn missing_primary_key_tie_break(field: impl Into<String>) -> Self {
        Self::MissingPrimaryKeyTieBreak {
            field: field.into(),
        }
    }
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

    /// Delete plans must not carry grouped query wrappers.
    #[error("delete plans must not include GROUP BY or HAVING")]
    DeletePlanWithGrouping,

    /// Delete plans must not carry pagination.
    #[error("delete plans must not include pagination")]
    DeletePlanWithPagination,

    /// Load plans must not carry delete limits.
    #[error("load plans must not include delete limits")]
    LoadPlanWithDeleteLimit,

    /// Ordered delete windows require an explicit ordering.
    #[error("delete LIMIT/OFFSET requires an explicit ordering")]
    DeleteWindowRequiresOrder,

    /// Pagination requires an explicit ordering.
    #[error(
        "Unordered pagination is not allowed.\nLIMIT or OFFSET without ORDER BY is non-deterministic.\nAdd order_term(...) to make the query stable."
    )]
    UnorderedPagination,

    /// Expression ORDER BY currently requires access-satisfied ordering.
    #[error(
        "expression ORDER BY requires a matching index-backed access order for bounded execution"
    )]
    ExpressionOrderRequiresIndexSatisfiedAccess,
}

impl PolicyPlanError {
    /// Construct one empty-order-spec policy error.
    pub(in crate::db::query) const fn empty_order_spec() -> Self {
        Self::EmptyOrderSpec
    }

    /// Construct one delete-plan-with-grouping policy error.
    pub(in crate::db::query) const fn delete_plan_with_grouping() -> Self {
        Self::DeletePlanWithGrouping
    }

    /// Construct one delete-plan-with-pagination policy error.
    pub(in crate::db::query) const fn delete_plan_with_pagination() -> Self {
        Self::DeletePlanWithPagination
    }

    /// Construct one load-plan-with-delete-limit policy error.
    pub(in crate::db::query) const fn load_plan_with_delete_limit() -> Self {
        Self::LoadPlanWithDeleteLimit
    }

    /// Construct one ordered-delete-window-requires-order policy error.
    pub(in crate::db::query) const fn delete_window_requires_order() -> Self {
        Self::DeleteWindowRequiresOrder
    }

    /// Construct one unordered-pagination policy error.
    pub(in crate::db::query) const fn unordered_pagination() -> Self {
        Self::UnorderedPagination
    }

    /// Construct one expression-order-requires-index-access policy error.
    pub(in crate::db::query) const fn expression_order_requires_index_satisfied_access() -> Self {
        Self::ExpressionOrderRequiresIndexSatisfiedAccess
    }
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

impl CursorPagingPolicyError {
    /// Construct one cursor-requires-order policy error.
    pub(in crate::db::query) const fn cursor_requires_order() -> Self {
        Self::CursorRequiresOrder
    }

    /// Construct one cursor-requires-limit policy error.
    pub(in crate::db::query) const fn cursor_requires_limit() -> Self {
        Self::CursorRequiresLimit
    }
}

///
/// GroupPlanError
///
/// GROUP BY wrapper validation failures owned by query planning.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum GroupPlanError {
    /// HAVING requires GROUP BY grouped plan shape.
    #[error("HAVING requires GROUP BY")]
    HavingRequiresGroupBy,

    /// Grouped validation entrypoint received a scalar logical plan.
    #[error("group validation requires grouped plan")]
    GroupedLogicalPlanRequired,

    /// GROUP BY requires at least one declared grouping field.
    #[error("group specification must include at least one group field")]
    EmptyGroupFields,

    /// Global DISTINCT aggregate shapes without GROUP BY are restricted.
    #[error("global DISTINCT without GROUP BY requires one DISTINCT field aggregate")]
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
    #[error("grouped DISTINCT requires ordered-group adjacency proof")]
    DistinctAdjacencyEligibilityRequired,

    /// GROUP BY ORDER BY shape must start with grouped-key prefix.
    #[error("grouped ORDER BY must start with GROUP BY key prefix")]
    OrderPrefixNotAlignedWithGroupKeys,

    /// GROUP BY ORDER BY expression parses but is not order-admissible in grouped v1.
    #[error("grouped ORDER BY expression is not order-admissible in this release: '{term}'")]
    OrderExpressionNotAdmissible { term: String },

    /// Aggregate ORDER BY requires an explicit LIMIT for bounded execution.
    #[error("aggregate ORDER BY requires LIMIT for bounded execution")]
    OrderRequiresLimit,

    /// HAVING with DISTINCT is deferred until grouped DISTINCT support expands.
    #[error("grouped HAVING with DISTINCT is unsupported")]
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

impl GroupPlanError {
    /// Construct one grouped-logical-plan-required validation error.
    pub(in crate::db::query) const fn grouped_logical_plan_required() -> Self {
        Self::GroupedLogicalPlanRequired
    }

    /// Construct one unsupported global-DISTINCT aggregate shape validation error.
    pub(in crate::db::query) const fn global_distinct_aggregate_shape_unsupported() -> Self {
        Self::GlobalDistinctAggregateShapeUnsupported
    }

    /// Construct one grouped DISTINCT adjacency-eligibility-required policy error.
    pub(in crate::db::query) const fn distinct_adjacency_eligibility_required() -> Self {
        Self::DistinctAdjacencyEligibilityRequired
    }

    /// Construct one grouped DISTINCT HAVING unsupported policy error.
    pub(in crate::db::query) const fn distinct_having_unsupported() -> Self {
        Self::DistinctHavingUnsupported
    }

    /// Construct one unknown grouped-field validation error.
    pub(in crate::db::query) fn unknown_group_field(field: impl Into<String>) -> Self {
        Self::UnknownGroupField {
            field: field.into(),
        }
    }

    /// Construct one duplicate grouped-field validation error.
    pub(in crate::db::query) fn duplicate_group_field(field: impl Into<String>) -> Self {
        Self::DuplicateGroupField {
            field: field.into(),
        }
    }

    /// Construct one aggregate ORDER BY requires LIMIT validation error.
    pub(in crate::db::query) const fn order_requires_limit() -> Self {
        Self::OrderRequiresLimit
    }

    /// Construct one grouped ORDER BY prefix-alignment validation error.
    pub(in crate::db::query) const fn order_prefix_not_aligned_with_group_keys() -> Self {
        Self::OrderPrefixNotAlignedWithGroupKeys
    }

    /// Construct one grouped ORDER BY expression admission validation error.
    pub(in crate::db::query) fn order_expression_not_admissible(term: impl Into<String>) -> Self {
        Self::OrderExpressionNotAdmissible { term: term.into() }
    }

    /// Construct one empty grouped-field-set validation error.
    /// Construct one empty grouped-aggregate-set validation error.
    pub(in crate::db::query) const fn empty_aggregates() -> Self {
        Self::EmptyAggregates
    }

    /// Construct one grouped HAVING non-group-field reference validation error.
    pub(in crate::db::query) fn having_non_group_field_reference(
        index: usize,
        field: impl Into<String>,
    ) -> Self {
        Self::HavingNonGroupFieldReference {
            index,
            field: field.into(),
        }
    }

    /// Construct one grouped HAVING aggregate-index-out-of-bounds validation error.
    pub(in crate::db::query) const fn having_aggregate_index_out_of_bounds(
        index: usize,
        aggregate_index: usize,
        aggregate_count: usize,
    ) -> Self {
        Self::HavingAggregateIndexOutOfBounds {
            index,
            aggregate_index,
            aggregate_count,
        }
    }

    /// Construct one grouped HAVING unsupported-operator policy error.
    pub(in crate::db::query) fn having_unsupported_compare_op(
        index: usize,
        op: impl Into<String>,
    ) -> Self {
        Self::HavingUnsupportedCompareOp {
            index,
            op: op.into(),
        }
    }

    /// Construct one grouped DISTINCT aggregate-kind unsupported policy error.
    pub(in crate::db::query) fn distinct_aggregate_kind_unsupported(
        index: usize,
        kind: impl Into<String>,
    ) -> Self {
        Self::DistinctAggregateKindUnsupported {
            index,
            kind: kind.into(),
        }
    }

    /// Construct one grouped DISTINCT field-target unsupported policy error.
    pub(in crate::db::query) fn distinct_aggregate_field_target_unsupported(
        index: usize,
        kind: impl Into<String>,
        field: impl Into<String>,
    ) -> Self {
        Self::DistinctAggregateFieldTargetUnsupported {
            index,
            kind: kind.into(),
            field: field.into(),
        }
    }

    /// Construct one grouped field-target aggregate unsupported policy error.
    pub(in crate::db::query) fn field_target_aggregates_unsupported(
        index: usize,
        kind: impl Into<String>,
        field: impl Into<String>,
    ) -> Self {
        Self::FieldTargetAggregatesUnsupported {
            index,
            kind: kind.into(),
            field: field.into(),
        }
    }

    /// Construct one global DISTINCT SUM non-numeric-target policy error.
    pub(in crate::db::query) fn global_distinct_sum_target_not_numeric(
        index: usize,
        field: impl Into<String>,
    ) -> Self {
        Self::GlobalDistinctSumTargetNotNumeric {
            index,
            field: field.into(),
        }
    }

    /// Construct one unknown grouped aggregate-target-field validation error.
    pub(in crate::db::query) fn unknown_aggregate_target_field(
        index: usize,
        field: impl Into<String>,
    ) -> Self {
        Self::UnknownAggregateTargetField {
            index,
            field: field.into(),
        }
    }
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

    /// Function call received one incompatible argument type.
    #[error("function '{function}' argument at index={index} is incompatible with type {found}")]
    InvalidFunctionArgument {
        function: String,
        index: usize,
        found: String,
    },

    /// Unary operation is incompatible with inferred operand type.
    #[error("unary operator '{op}' is incompatible with operand type {found}")]
    InvalidUnaryOperand { op: String, found: String },

    /// CASE branch condition is not boolean-typed.
    #[error("CASE branch condition is incompatible with type {found}")]
    InvalidCaseConditionType { found: String },

    /// CASE result branches cannot agree on one shared scalar type.
    #[error("CASE result branches are incompatible with types ({left}, {right})")]
    IncompatibleCaseBranchTypes { left: String, right: String },

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

impl ExprPlanError {
    /// Construct one unknown-expression-field planner error.
    pub(in crate::db::query) fn unknown_expr_field(field: impl Into<String>) -> Self {
        Self::UnknownExprField {
            field: field.into(),
        }
    }

    /// Construct one aggregate-target-required planner error.
    pub(in crate::db::query) fn aggregate_target_required(kind: impl Into<String>) -> Self {
        Self::AggregateTargetRequired { kind: kind.into() }
    }

    /// Construct one non-numeric aggregate-target planner error.
    pub(in crate::db::query) fn non_numeric_aggregate_target(
        kind: impl Into<String>,
        field: impl Into<String>,
    ) -> Self {
        Self::NonNumericAggregateTarget {
            kind: kind.into(),
            field: field.into(),
        }
    }

    /// Construct one invalid function-argument planner error.
    pub(in crate::db::query) fn invalid_function_argument(
        function: impl Into<String>,
        index: usize,
        found: impl Into<String>,
    ) -> Self {
        Self::InvalidFunctionArgument {
            function: function.into(),
            index,
            found: found.into(),
        }
    }

    /// Construct one invalid unary-operand planner error.
    pub(in crate::db::query) fn invalid_unary_operand(
        op: impl Into<String>,
        found: impl Into<String>,
    ) -> Self {
        Self::InvalidUnaryOperand {
            op: op.into(),
            found: found.into(),
        }
    }

    /// Construct one invalid CASE-condition planner error.
    pub(in crate::db::query) fn invalid_case_condition_type(found: impl Into<String>) -> Self {
        Self::InvalidCaseConditionType {
            found: found.into(),
        }
    }

    /// Construct one incompatible CASE-branch-types planner error.
    pub(in crate::db::query) fn incompatible_case_branch_types(
        left: impl Into<String>,
        right: impl Into<String>,
    ) -> Self {
        Self::IncompatibleCaseBranchTypes {
            left: left.into(),
            right: right.into(),
        }
    }

    /// Construct one invalid binary-operands planner error.
    pub(in crate::db::query) fn invalid_binary_operands(
        op: impl Into<String>,
        left: impl Into<String>,
        right: impl Into<String>,
    ) -> Self {
        Self::InvalidBinaryOperands {
            op: op.into(),
            left: left.into(),
            right: right.into(),
        }
    }

    /// Construct one grouped projection non-group-field reference planner error.
    pub(in crate::db::query) const fn grouped_projection_references_non_group_field(
        index: usize,
    ) -> Self {
        Self::GroupedProjectionReferencesNonGroupField { index }
    }
}

///
/// CursorOrderPlanShapeError
///
/// Logical cursor-order plan-shape failures used by cursor/runtime boundary adapters.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CursorOrderPlanShapeError {
    MissingExplicitOrder,
    EmptyOrderSpec,
}

impl CursorOrderPlanShapeError {
    /// Construct one missing-explicit-order shape error.
    pub(in crate::db) const fn missing_explicit_order() -> Self {
        Self::MissingExplicitOrder
    }

    /// Construct one empty-order-spec shape error.
    pub(in crate::db) const fn empty_order_spec() -> Self {
        Self::EmptyOrderSpec
    }

    /// Map one cursor-order shape error into one cursor plan error using the
    /// caller-owned missing-order contract message.
    pub(in crate::db) fn to_cursor_plan_error(
        self,
        missing_order_message: &'static str,
    ) -> CursorPlanError {
        match self {
            Self::MissingExplicitOrder => {
                CursorPlanError::continuation_cursor_invariant(missing_order_message)
            }
            Self::EmptyOrderSpec => CursorPlanError::cursor_requires_non_empty_order(),
        }
    }
}

///
/// IntentKeyAccessKind
///
/// Key-access shape used by intent policy validation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query) enum IntentKeyAccessKind {
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
pub(in crate::db::query) enum IntentKeyAccessPolicyViolation {
    KeyAccessConflict,
    ByIdsWithPredicate,
    OnlyWithPredicate,
}

impl IntentKeyAccessPolicyViolation {
    /// Construct one conflicting-key-access policy violation.
    pub(in crate::db::query) const fn key_access_conflict() -> Self {
        Self::KeyAccessConflict
    }

    /// Construct one by-ids-with-predicate policy violation.
    pub(in crate::db::query) const fn by_ids_with_predicate() -> Self {
        Self::ByIdsWithPredicate
    }

    /// Construct one only-with-predicate policy violation.
    pub(in crate::db::query) const fn only_with_predicate() -> Self {
        Self::OnlyWithPredicate
    }
}

///
/// FluentLoadPolicyViolation
///
/// Fluent load-entry policy violations.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query) enum FluentLoadPolicyViolation {
    CursorRequiresPagedExecution,
    GroupedRequiresDirectExecute,
    CursorRequiresOrder,
    CursorRequiresLimit,
}

impl FluentLoadPolicyViolation {
    /// Construct one cursor-requires-paged-execution fluent policy violation.
    pub(in crate::db::query) const fn cursor_requires_paged_execution() -> Self {
        Self::CursorRequiresPagedExecution
    }

    /// Construct one grouped-requires-direct-execute fluent policy violation.
    pub(in crate::db::query) const fn grouped_requires_direct_execute() -> Self {
        Self::GroupedRequiresDirectExecute
    }

    /// Construct one cursor-requires-order fluent policy violation.
    pub(in crate::db::query) const fn cursor_requires_order() -> Self {
        Self::CursorRequiresOrder
    }

    /// Construct one cursor-requires-limit fluent policy violation.
    pub(in crate::db::query) const fn cursor_requires_limit() -> Self {
        Self::CursorRequiresLimit
    }
}

impl From<CursorPagingPolicyError> for FluentLoadPolicyViolation {
    fn from(err: CursorPagingPolicyError) -> Self {
        match err {
            CursorPagingPolicyError::CursorRequiresOrder => Self::cursor_requires_order(),
            CursorPagingPolicyError::CursorRequiresLimit => Self::cursor_requires_limit(),
        }
    }
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
                | Self::OrderExpressionNotAdmissible { .. }
                | Self::OrderRequiresLimit
                | Self::DistinctHavingUnsupported
                | Self::HavingUnsupportedCompareOp { .. }
                | Self::DistinctAggregateKindUnsupported { .. }
                | Self::DistinctAggregateFieldTargetUnsupported { .. }
                | Self::FieldTargetAggregatesUnsupported { .. }
        )
    }
}
