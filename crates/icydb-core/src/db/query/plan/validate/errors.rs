//! Module: db::query::plan::validate::errors
//! Responsibility: own the query-plan validation error taxonomy and its
//! mapping from lower planner, cursor, and schema validation domains.
//! Does not own: the validation logic that decides which error applies.
//! Boundary: keeps query-plan validation failures under one planner-owned error surface.

use crate::db::{
    access::AccessPlanError,
    cursor::CursorPlanError,
    predicate::CompareOp,
    query::plan::{
        AggregateKind,
        expr::{BinaryOp, ExprType, Function, UnaryOp},
    },
    schema::ValidateError,
};

///
/// PlanError
///
/// Root plan validation taxonomy split by domain axis.
/// User-shape failures are grouped under `PlanUserError`.
/// Policy/capability failures are grouped under `PlanPolicyError`.
/// Cursor continuation failures remain in `CursorPlanError`.
///

#[derive(Debug)]
pub enum PlanError {
    User(Box<PlanUserError>),

    Policy(Box<PlanPolicyError>),

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

#[derive(Debug)]
pub enum PlanUserError {
    PredicateInvalid(Box<ValidateError>),

    Order(Box<OrderPlanError>),

    Access(Box<AccessPlanError>),

    Group(Box<GroupPlanError>),

    Expr(Box<ExprPlanError>),
}

///
/// PlanPolicyError
///
/// Planner policy/capability validation failures.
/// This axis captures query-shape constraints that are valid syntactically but
/// not supported by the current execution policy surface.
///

#[derive(Debug)]
pub enum PlanPolicyError {
    Policy(Box<PolicyPlanError>),

    Group(Box<GroupPlanError>),
}

///
/// OrderPlanError
///
/// ORDER BY-specific validation failures.
///

#[derive(Debug)]
pub enum OrderPlanError {
    /// ORDER BY references an unknown field.
    UnknownField { term_index: usize },

    /// ORDER BY references a field that cannot be ordered.
    UnorderableField { term_index: usize },

    /// ORDER BY references the same non-primary-key field multiple times.
    DuplicateOrderField {
        first_term_index: usize,
        duplicate_term_index: usize,
    },

    /// Ordered plans must include every primary-key tie-break component.
    MissingPrimaryKeyTieBreak { primary_key_index: usize },
}

impl OrderPlanError {
    /// Construct one unknown-field validation error.
    pub(in crate::db::query) const fn unknown_field(term_index: usize) -> Self {
        Self::UnknownField { term_index }
    }

    /// Construct one unorderable-field validation error.
    pub(in crate::db::query) const fn unorderable_field(term_index: usize) -> Self {
        Self::UnorderableField { term_index }
    }

    /// Construct one duplicate non-primary-key ORDER BY field validation error.
    pub(in crate::db::query) const fn duplicate_order_field(
        first_term_index: usize,
        duplicate_term_index: usize,
    ) -> Self {
        Self::DuplicateOrderField {
            first_term_index,
            duplicate_term_index,
        }
    }

    /// Construct one missing primary-key tie-break validation error.
    pub(in crate::db::query) const fn missing_primary_key_tie_break(
        primary_key_index: usize,
    ) -> Self {
        Self::MissingPrimaryKeyTieBreak { primary_key_index }
    }
}

///
/// PolicyPlanError
///
/// Plan-shape policy failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyPlanError {
    /// ORDER BY must specify at least one field.
    EmptyOrderSpec,

    /// Delete plans must not carry grouped query wrappers.
    DeletePlanWithGrouping,

    /// Delete plans must not carry pagination.
    DeletePlanWithPagination,

    /// Load plans must not carry delete limits.
    LoadPlanWithDeleteLimit,

    /// Ordered delete windows require an explicit ordering.
    DeleteWindowRequiresOrder,

    /// Pagination requires an explicit ordering.
    UnorderedPagination,
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
}

///
/// CursorPagingPolicyError
///
/// Cursor pagination readiness errors shared by intent/fluent entry surfaces.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorPagingPolicyError {
    CursorRequiresOrder,

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupPlanError {
    /// HAVING requires GROUP BY grouped plan shape.
    HavingRequiresGroupBy,

    /// Grouped validation entrypoint received a scalar logical plan.
    GroupedLogicalPlanRequired,

    /// GROUP BY requires at least one declared grouping field.
    EmptyGroupFields,

    /// Global DISTINCT aggregate shapes without GROUP BY are restricted.
    GlobalDistinctAggregateShapeUnsupported,

    /// GROUP BY requires at least one aggregate terminal.
    EmptyAggregates,

    /// GROUP BY references an unknown group field.
    UnknownGroupField { field: String },

    /// GROUP BY must not repeat the same resolved group slot.
    DuplicateGroupField { field: String },

    /// GROUP BY v1 does not accept DISTINCT unless adjacency eligibility is explicit.
    DistinctAdjacencyEligibilityRequired,

    /// GROUP BY ORDER BY shape must start with grouped-key prefix.
    OrderPrefixNotAlignedWithGroupKeys,

    /// GROUP BY ORDER BY expression parses but is not order-admissible in grouped v1.
    OrderExpressionNotAdmissible { term: String },

    /// Aggregate ORDER BY requires an explicit LIMIT for bounded execution.
    OrderRequiresLimit,

    /// HAVING with DISTINCT is deferred until grouped DISTINCT support expands.
    DistinctHavingUnsupported,

    /// HAVING currently supports compare operators only.
    HavingUnsupportedCompareOp { index: usize, op: CompareOp },

    /// HAVING group-field symbols must reference declared grouped keys.
    HavingNonGroupFieldReference { index: usize, field: String },

    /// HAVING aggregate references must resolve to declared grouped terminals.
    HavingAggregateIndexOutOfBounds {
        index: usize,
        aggregate_index: usize,
        aggregate_count: usize,
    },

    /// DISTINCT grouped terminal kinds are intentionally conservative in v1.
    DistinctAggregateKindUnsupported {
        index: usize,
        kind: Option<AggregateKind>,
    },

    /// DISTINCT over grouped field-target terminals is deferred with field-target support.
    DistinctAggregateFieldTargetUnsupported {
        index: usize,
        kind: AggregateKind,
        field: String,
    },

    /// Aggregate target fields must resolve in the model schema.
    UnknownAggregateTargetField { index: usize, field: String },

    /// Global DISTINCT SUM requires a numeric field target.
    GlobalDistinctSumTargetNotNumeric { index: usize, field: String },

    /// Field-target grouped terminals are not enabled in grouped execution v1.
    FieldTargetAggregatesUnsupported {
        index: usize,
        kind: AggregateKind,
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
    pub(in crate::db::query) const fn having_unsupported_compare_op(
        index: usize,
        op: CompareOp,
    ) -> Self {
        Self::HavingUnsupportedCompareOp { index, op }
    }

    /// Construct one grouped DISTINCT aggregate-kind unsupported policy error.
    pub(in crate::db::query) const fn distinct_aggregate_kind_unsupported(
        index: usize,
        kind: Option<AggregateKind>,
    ) -> Self {
        Self::DistinctAggregateKindUnsupported { index, kind }
    }

    /// Construct one grouped DISTINCT field-target unsupported policy error.
    pub(in crate::db::query) fn distinct_aggregate_field_target_unsupported(
        index: usize,
        kind: AggregateKind,
        field: impl Into<String>,
    ) -> Self {
        Self::DistinctAggregateFieldTargetUnsupported {
            index,
            kind,
            field: field.into(),
        }
    }

    /// Construct one grouped field-target aggregate unsupported policy error.
    pub(in crate::db::query) fn field_target_aggregates_unsupported(
        index: usize,
        kind: AggregateKind,
        field: impl Into<String>,
    ) -> Self {
        Self::FieldTargetAggregatesUnsupported {
            index,
            kind,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExprPlanTypeClass {
    Blob,
    Bool,
    Collection,
    #[cfg(test)]
    Null,
    Numeric,
    Opaque,
    Structured,
    Text,
    Unknown,
}

impl ExprPlanTypeClass {
    pub(in crate::db) const fn from_expr_type(expr_type: &ExprType) -> Self {
        match expr_type {
            ExprType::Blob => Self::Blob,
            ExprType::Bool => Self::Bool,
            ExprType::Collection => Self::Collection,
            #[cfg(test)]
            ExprType::Null => Self::Null,
            ExprType::Numeric(_) => Self::Numeric,
            ExprType::Opaque => Self::Opaque,
            ExprType::Structured => Self::Structured,
            ExprType::Text => Self::Text,
            ExprType::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExprPlanUnaryOpCode(u8);

impl ExprPlanUnaryOpCode {
    pub const NOT: Self = Self(0);

    pub(in crate::db) const fn from_unary_op(op: UnaryOp) -> Self {
        match op {
            UnaryOp::Not => Self::NOT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExprPlanBinaryOpCode(u8);

impl ExprPlanBinaryOpCode {
    pub const ADD: Self = Self(0);
    pub const AND: Self = Self(1);
    pub const DIV: Self = Self(2);
    pub const EQ: Self = Self(3);
    pub const GT: Self = Self(4);
    pub const GTE: Self = Self(5);
    pub const LT: Self = Self(6);
    pub const LTE: Self = Self(7);
    pub const MUL: Self = Self(8);
    pub const NE: Self = Self(9);
    pub const OR: Self = Self(10);
    pub const SUB: Self = Self(11);

    pub(in crate::db) const fn from_binary_op(op: BinaryOp) -> Self {
        match op {
            BinaryOp::Add => Self::ADD,
            BinaryOp::And => Self::AND,
            BinaryOp::Div => Self::DIV,
            BinaryOp::Eq => Self::EQ,
            BinaryOp::Gt => Self::GT,
            BinaryOp::Gte => Self::GTE,
            BinaryOp::Lt => Self::LT,
            BinaryOp::Lte => Self::LTE,
            BinaryOp::Mul => Self::MUL,
            BinaryOp::Ne => Self::NE,
            BinaryOp::Or => Self::OR,
            BinaryOp::Sub => Self::SUB,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExprPlanFunctionCode(u8);

impl ExprPlanFunctionCode {
    pub const ABS: Self = Self(0);
    pub const CBRT: Self = Self(1);
    pub const CEILING: Self = Self(2);
    pub const COALESCE: Self = Self(3);
    pub const COLLECTION_CONTAINS: Self = Self(4);
    pub const CONTAINS: Self = Self(5);
    pub const ENDS_WITH: Self = Self(6);
    pub const EXP: Self = Self(7);
    pub const FLOOR: Self = Self(8);
    pub const IN_LIST: Self = Self(38);
    pub const IS_EMPTY: Self = Self(9);
    pub const IS_MISSING: Self = Self(10);
    pub const IS_NOT_EMPTY: Self = Self(11);
    pub const IS_NOT_NULL: Self = Self(12);
    pub const IS_NULL: Self = Self(13);
    pub const LEFT: Self = Self(14);
    pub const LENGTH: Self = Self(15);
    pub const LN: Self = Self(16);
    pub const LOG: Self = Self(17);
    pub const LOG2: Self = Self(18);
    pub const LOG10: Self = Self(19);
    pub const LOWER: Self = Self(20);
    pub const LTRIM: Self = Self(21);
    pub const MOD: Self = Self(22);
    pub const NULLIF: Self = Self(23);
    pub const OCTET_LENGTH: Self = Self(24);
    pub const POSITION: Self = Self(25);
    pub const POWER: Self = Self(26);
    pub const REPLACE: Self = Self(27);
    pub const RIGHT: Self = Self(28);
    pub const ROUND: Self = Self(29);
    pub const RTRIM: Self = Self(30);
    pub const SIGN: Self = Self(31);
    pub const SQRT: Self = Self(32);
    pub const STARTS_WITH: Self = Self(33);
    pub const SUBSTRING: Self = Self(34);
    pub const TRIM: Self = Self(35);
    pub const TRUNC: Self = Self(36);
    pub const UPPER: Self = Self(37);

    pub(in crate::db) const fn from_function(function: Function) -> Self {
        match function {
            Function::Abs => Self::ABS,
            Function::Cbrt => Self::CBRT,
            Function::Ceiling => Self::CEILING,
            Function::Coalesce => Self::COALESCE,
            Function::CollectionContains => Self::COLLECTION_CONTAINS,
            Function::Contains => Self::CONTAINS,
            Function::EndsWith => Self::ENDS_WITH,
            Function::Exp => Self::EXP,
            Function::Floor => Self::FLOOR,
            Function::InList => Self::IN_LIST,
            Function::IsEmpty => Self::IS_EMPTY,
            Function::IsMissing => Self::IS_MISSING,
            Function::IsNotEmpty => Self::IS_NOT_EMPTY,
            Function::IsNotNull => Self::IS_NOT_NULL,
            Function::IsNull => Self::IS_NULL,
            Function::Left => Self::LEFT,
            Function::Length => Self::LENGTH,
            Function::Ln => Self::LN,
            Function::Log => Self::LOG,
            Function::Log2 => Self::LOG2,
            Function::Log10 => Self::LOG10,
            Function::Lower => Self::LOWER,
            Function::Ltrim => Self::LTRIM,
            Function::Mod => Self::MOD,
            Function::NullIf => Self::NULLIF,
            Function::OctetLength => Self::OCTET_LENGTH,
            Function::Position => Self::POSITION,
            Function::Power => Self::POWER,
            Function::Replace => Self::REPLACE,
            Function::Right => Self::RIGHT,
            Function::Round => Self::ROUND,
            Function::Rtrim => Self::RTRIM,
            Function::Sign => Self::SIGN,
            Function::Sqrt => Self::SQRT,
            Function::StartsWith => Self::STARTS_WITH,
            Function::Substring => Self::SUBSTRING,
            Function::Trim => Self::TRIM,
            Function::Trunc => Self::TRUNC,
            Function::Upper => Self::UPPER,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExprPlanError {
    /// SQL lowering references a field that does not exist in schema.
    UnknownField { field: String },

    /// Expression references a field that does not exist in schema.
    UnknownExprField { field: String },

    /// Aggregate terminal requires a numeric target field.
    NonNumericAggregateTarget {
        kind: AggregateKind,
        found: ExprPlanTypeClass,
    },

    /// Aggregate expression requires an explicit target field.
    AggregateTargetRequired { kind: AggregateKind },

    /// Function call received an unsupported argument count.
    InvalidFunctionArity {
        function: ExprPlanFunctionCode,
        expected: usize,
        actual: usize,
    },

    /// Function call received one incompatible argument type.
    InvalidFunctionArgument {
        function: ExprPlanFunctionCode,
        argument_index: usize,
        found: ExprPlanTypeClass,
    },

    /// Function call received incompatible dynamic argument types.
    IncompatibleFunctionArguments {
        function: ExprPlanFunctionCode,
        left_argument_index: usize,
        right_argument_index: usize,
        left: ExprPlanTypeClass,
        right: ExprPlanTypeClass,
    },

    /// Unary operation is incompatible with inferred operand type.
    InvalidUnaryOperand {
        op: ExprPlanUnaryOpCode,
        found: ExprPlanTypeClass,
    },

    /// CASE branch condition is not boolean-typed.
    InvalidCaseConditionType {
        arm_index: usize,
        found: ExprPlanTypeClass,
    },

    /// CASE result branches cannot agree on one shared scalar type.
    IncompatibleCaseBranchTypes {
        left_branch_index: Option<usize>,
        right_branch_index: Option<usize>,
        left: ExprPlanTypeClass,
        right: ExprPlanTypeClass,
    },

    /// Binary operation is incompatible with inferred operand types.
    InvalidBinaryOperands {
        op: ExprPlanBinaryOpCode,
        left: ExprPlanTypeClass,
        right: ExprPlanTypeClass,
    },

    /// GROUP BY projections must not reference fields outside grouped keys.
    GroupedProjectionReferencesNonGroupField { index: usize },
}

impl ExprPlanError {
    /// Construct one unknown-field planner error.
    #[cfg(feature = "sql")]
    pub(in crate::db::query) fn unknown_field(field: impl Into<String>) -> Self {
        Self::UnknownField {
            field: field.into(),
        }
    }

    /// Construct one unknown-expression-field planner error.
    pub(in crate::db::query) fn unknown_expr_field(field: impl Into<String>) -> Self {
        Self::UnknownExprField {
            field: field.into(),
        }
    }

    /// Construct one aggregate-target-required planner error.
    pub(in crate::db::query) const fn aggregate_target_required(kind: AggregateKind) -> Self {
        Self::AggregateTargetRequired { kind }
    }

    /// Construct one non-numeric aggregate-target planner error.
    pub(in crate::db::query) const fn non_numeric_aggregate_target(
        kind: AggregateKind,
        found: ExprPlanTypeClass,
    ) -> Self {
        Self::NonNumericAggregateTarget { kind, found }
    }

    /// Construct one invalid function-arity planner error.
    pub(in crate::db::query) const fn invalid_function_arity(
        function: Function,
        expected: usize,
        actual: usize,
    ) -> Self {
        Self::InvalidFunctionArity {
            function: ExprPlanFunctionCode::from_function(function),
            expected,
            actual,
        }
    }

    /// Construct one invalid function-argument planner error.
    pub(in crate::db::query) const fn invalid_function_argument(
        function: Function,
        argument_index: usize,
        found: ExprPlanTypeClass,
    ) -> Self {
        Self::InvalidFunctionArgument {
            function: ExprPlanFunctionCode::from_function(function),
            argument_index,
            found,
        }
    }

    /// Construct one incompatible dynamic-function-arguments planner error.
    pub(in crate::db::query) const fn incompatible_function_arguments(
        function: Function,
        left_argument_index: usize,
        right_argument_index: usize,
        left: ExprPlanTypeClass,
        right: ExprPlanTypeClass,
    ) -> Self {
        Self::IncompatibleFunctionArguments {
            function: ExprPlanFunctionCode::from_function(function),
            left_argument_index,
            right_argument_index,
            left,
            right,
        }
    }

    /// Construct one invalid unary-operand planner error.
    pub(in crate::db::query) const fn invalid_unary_operand(
        op: UnaryOp,
        found: ExprPlanTypeClass,
    ) -> Self {
        Self::InvalidUnaryOperand {
            op: ExprPlanUnaryOpCode::from_unary_op(op),
            found,
        }
    }

    /// Construct one invalid CASE-condition planner error.
    pub(in crate::db::query) const fn invalid_case_condition_type(
        arm_index: usize,
        found: ExprPlanTypeClass,
    ) -> Self {
        Self::InvalidCaseConditionType { arm_index, found }
    }

    /// Construct one incompatible CASE-branch-types planner error.
    pub(in crate::db::query) const fn incompatible_case_branch_types(
        left_branch_index: Option<usize>,
        right_branch_index: Option<usize>,
        left: ExprPlanTypeClass,
        right: ExprPlanTypeClass,
    ) -> Self {
        Self::IncompatibleCaseBranchTypes {
            left_branch_index,
            right_branch_index,
            left,
            right,
        }
    }

    /// Construct one invalid binary-operands planner error.
    pub(in crate::db::query) const fn invalid_binary_operands(
        op: BinaryOp,
        left: ExprPlanTypeClass,
        right: ExprPlanTypeClass,
    ) -> Self {
        Self::InvalidBinaryOperands {
            op: ExprPlanBinaryOpCode::from_binary_op(op),
            left,
            right,
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

    /// Map one cursor-order shape error into one cursor plan error.
    pub(in crate::db) const fn to_cursor_plan_error(self) -> CursorPlanError {
        match self {
            Self::MissingExplicitOrder => CursorPlanError::continuation_cursor_invariant(),
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
