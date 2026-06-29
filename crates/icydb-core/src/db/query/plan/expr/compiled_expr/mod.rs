//! Module: query::plan::expr::compiled_expr
//! Responsibility: compiled expression programs, compilation, and evaluation.
//! Does not own: row loops, grouped aggregate reducer mechanics, or
//! scan/projection orchestration.
//! Boundary: expression-layer programs evaluate already-loaded slot values so
//! callers can stay on row loading, reducer updates, and LIMIT handling.
//!
//! Invariants:
//! - CompiledExpr is the single expression IR in the system.
//! - The compile submodule is the only planner tree to CompiledExpr translation boundary.
//! - All expression evaluation must go through CompiledExpr::evaluate.
//! - Readers must fail, not return NULL, for invalid access patterns.
//! - All semantics for numeric, boolean, and comparison evaluation are centralized here.
//! - Executor row shapes stay behind CompiledExprValueReader implementations.

mod compile;
mod evaluate;

use crate::{
    db::{
        numeric::NumericEvalError,
        query::plan::{
            AggregateKind,
            expr::{BinaryOp, Function, UnaryOp},
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    value::Value,
};
use icydb_diagnostic_code::QueryProjectionCode;
use std::borrow::Cow;

pub(in crate::db) use compile::{compile_grouped_projection_expr, compile_grouped_projection_plan};
pub(in crate::db) use evaluate::evaluate_grouped_having_expr;

///
/// ProjectionEvalError
///
/// ProjectionEvalError is the expression-layer failure taxonomy for compiled
/// expression evaluation.
/// It lives beside `CompiledExpr` so scalar, grouped, HAVING, and aggregate
/// input evaluation share one set of diagnostics instead of recreating error
/// boundaries in caller modules.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ProjectionAccessCode(u8);

impl ProjectionAccessCode {
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const UNKNOWN: Self = Self(0);
    pub(in crate::db) const SLOT: Self = Self(1);
    pub(in crate::db) const GROUP_KEY: Self = Self(2);
    pub(in crate::db) const FIELD_PATH: Self = Self(3);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ProjectionValueKindCode(u8);

impl ProjectionValueKindCode {
    const ACCOUNT: Self = Self(0);
    const BLOB: Self = Self(1);
    const BOOL: Self = Self(2);
    const DATE: Self = Self(3);
    const DECIMAL: Self = Self(4);
    const DURATION: Self = Self(5);
    const ENUM: Self = Self(6);
    const FLOAT32: Self = Self(7);
    const FLOAT64: Self = Self(8);
    const INT64: Self = Self(9);
    const INT128: Self = Self(10);
    const INT_BIG: Self = Self(11);
    const LIST: Self = Self(12);
    const MAP: Self = Self(13);
    const NULL: Self = Self(14);
    const PRINCIPAL: Self = Self(15);
    const SUBACCOUNT: Self = Self(16);
    const TEXT: Self = Self(17);
    const TIMESTAMP: Self = Self(18);
    const NAT64: Self = Self(19);
    const NAT128: Self = Self(20);
    const NAT_BIG: Self = Self(21);
    const ULID: Self = Self(22);
    const UNIT: Self = Self(23);

    pub(in crate::db) const fn from_value(value: &Value) -> Self {
        match value {
            Value::Account(_) => Self::ACCOUNT,
            Value::Blob(_) => Self::BLOB,
            Value::Bool(_) => Self::BOOL,
            Value::Date(_) => Self::DATE,
            Value::Decimal(_) => Self::DECIMAL,
            Value::Duration(_) => Self::DURATION,
            Value::Enum(_) => Self::ENUM,
            Value::Float32(_) => Self::FLOAT32,
            Value::Float64(_) => Self::FLOAT64,
            Value::Int64(_) => Self::INT64,
            Value::Int128(_) => Self::INT128,
            Value::IntBig(_) => Self::INT_BIG,
            Value::List(_) => Self::LIST,
            Value::Map(_) => Self::MAP,
            Value::Null => Self::NULL,
            Value::Principal(_) => Self::PRINCIPAL,
            Value::Subaccount(_) => Self::SUBACCOUNT,
            Value::Text(_) => Self::TEXT,
            Value::Timestamp(_) => Self::TIMESTAMP,
            Value::Nat64(_) => Self::NAT64,
            Value::Nat128(_) => Self::NAT128,
            Value::NatBig(_) => Self::NAT_BIG,
            Value::Ulid(_) => Self::ULID,
            Value::Unit => Self::UNIT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ProjectionUnaryOpCode(u8);

impl ProjectionUnaryOpCode {
    const NOT: Self = Self(0);

    const fn from_unary_op(op: UnaryOp) -> Self {
        match op {
            UnaryOp::Not => Self::NOT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ProjectionBinaryOpCode(u8);

impl ProjectionBinaryOpCode {
    const ADD: Self = Self(0);
    const AND: Self = Self(1);
    const DIV: Self = Self(2);
    const EQ: Self = Self(3);
    const GT: Self = Self(4);
    const GTE: Self = Self(5);
    const LT: Self = Self(6);
    const LTE: Self = Self(7);
    const MUL: Self = Self(8);
    const NE: Self = Self(9);
    const OR: Self = Self(10);
    const SUB: Self = Self(11);

    const fn from_binary_op(op: BinaryOp) -> Self {
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
pub(in crate::db) struct ProjectionFunctionCode(u8);

impl ProjectionFunctionCode {
    const ABS: Self = Self(0);
    const CBRT: Self = Self(1);
    const CEILING: Self = Self(2);
    const COALESCE: Self = Self(3);
    const COLLECTION_CONTAINS: Self = Self(4);
    const CONTAINS: Self = Self(5);
    const ENDS_WITH: Self = Self(6);
    const EXP: Self = Self(7);
    const FLOOR: Self = Self(8);
    const IN_LIST: Self = Self(38);
    const IS_EMPTY: Self = Self(9);
    const IS_MISSING: Self = Self(10);
    const IS_NOT_EMPTY: Self = Self(11);
    const IS_NOT_NULL: Self = Self(12);
    const IS_NULL: Self = Self(13);
    const LEFT: Self = Self(14);
    const LENGTH: Self = Self(15);
    const LN: Self = Self(16);
    const LOG: Self = Self(17);
    const LOG2: Self = Self(18);
    const LOG10: Self = Self(19);
    const LOWER: Self = Self(20);
    const LTRIM: Self = Self(21);
    const MOD: Self = Self(22);
    const NULLIF: Self = Self(23);
    const OCTET_LENGTH: Self = Self(24);
    const POSITION: Self = Self(25);
    const POWER: Self = Self(26);
    const REPLACE: Self = Self(27);
    const RIGHT: Self = Self(28);
    const ROUND: Self = Self(29);
    const RTRIM: Self = Self(30);
    const SIGN: Self = Self(31);
    const SQRT: Self = Self(32);
    const STARTS_WITH: Self = Self(33);
    const SUBSTRING: Self = Self(34);
    const TRIM: Self = Self(35);
    const TRUNC: Self = Self(36);
    const UPPER: Self = Self(37);

    const fn from_function(function: Function) -> Self {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ProjectionAggregateKindCode(u8);

impl ProjectionAggregateKindCode {
    const COUNT: Self = Self(0);
    const SUM: Self = Self(1);
    const AVG: Self = Self(2);
    const EXISTS: Self = Self(3);
    const MIN: Self = Self(4);
    const MAX: Self = Self(5);
    const FIRST: Self = Self(6);
    const LAST: Self = Self(7);

    const fn from_aggregate_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::COUNT,
            AggregateKind::Sum => Self::SUM,
            AggregateKind::Avg => Self::AVG,
            AggregateKind::Exists => Self::EXISTS,
            AggregateKind::Min => Self::MIN,
            AggregateKind::Max => Self::MAX,
            AggregateKind::First => Self::FIRST,
            AggregateKind::Last => Self::LAST,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ProjectionEvalError {
    UnknownField {
        access: ProjectionAccessCode,
    },

    MissingFieldValue {
        access: ProjectionAccessCode,
        index: usize,
    },

    MissingFieldPathValue {
        root_slot: usize,
    },

    FieldPathEvaluationFailed {
        class: ErrorClass,
        origin: ErrorOrigin,
    },

    ReaderFailed {
        class: ErrorClass,
        origin: ErrorOrigin,
    },

    InvalidUnaryOperand {
        op: ProjectionUnaryOpCode,
        found: ProjectionValueKindCode,
    },

    InvalidCaseCondition {
        arm_index: Option<usize>,
        found: ProjectionValueKindCode,
    },

    InvalidBinaryOperands {
        op: ProjectionBinaryOpCode,
        left: ProjectionValueKindCode,
        right: ProjectionValueKindCode,
    },

    UnknownGroupedAggregateExpression {
        kind: ProjectionAggregateKindCode,
    },

    MissingGroupedAggregateValue {
        index: usize,
    },

    InvalidFunctionCall {
        function: ProjectionFunctionCode,
        argument_count: usize,
    },

    InvalidProjection {
        reason: QueryProjectionCode,
    },

    Numeric(NumericEvalError),

    InvalidGroupedHavingResult {
        found: ProjectionValueKindCode,
    },
}

impl From<NumericEvalError> for ProjectionEvalError {
    fn from(err: NumericEvalError) -> Self {
        Self::Numeric(err)
    }
}

impl ProjectionEvalError {
    pub(in crate::db) const fn unknown_group_field() -> Self {
        Self::UnknownField {
            access: ProjectionAccessCode::GROUP_KEY,
        }
    }

    pub(in crate::db) const fn unknown_field_path() -> Self {
        Self::UnknownField {
            access: ProjectionAccessCode::FIELD_PATH,
        }
    }

    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn missing_unknown_value() -> Self {
        Self::MissingFieldValue {
            access: ProjectionAccessCode::UNKNOWN,
            index: 0,
        }
    }

    pub(in crate::db) const fn missing_slot_value(slot: usize) -> Self {
        Self::MissingFieldValue {
            access: ProjectionAccessCode::SLOT,
            index: slot,
        }
    }

    pub(in crate::db) const fn missing_group_key_value(offset: usize) -> Self {
        Self::MissingFieldValue {
            access: ProjectionAccessCode::GROUP_KEY,
            index: offset,
        }
    }

    pub(in crate::db) const fn missing_field_path_root_value(root_slot: usize) -> Self {
        Self::MissingFieldValue {
            access: ProjectionAccessCode::FIELD_PATH,
            index: root_slot,
        }
    }

    pub(in crate::db) const fn missing_field_path_value(root_slot: usize) -> Self {
        Self::MissingFieldPathValue { root_slot }
    }

    pub(in crate::db) const fn missing_grouped_aggregate_value(index: usize) -> Self {
        Self::MissingGroupedAggregateValue { index }
    }

    pub(in crate::db) const fn invalid_unary_operand(op: UnaryOp, found: &Value) -> Self {
        Self::InvalidUnaryOperand {
            op: ProjectionUnaryOpCode::from_unary_op(op),
            found: ProjectionValueKindCode::from_value(found),
        }
    }

    pub(in crate::db) const fn invalid_case_condition(
        arm_index: Option<usize>,
        found: &Value,
    ) -> Self {
        Self::InvalidCaseCondition {
            arm_index,
            found: ProjectionValueKindCode::from_value(found),
        }
    }

    pub(in crate::db) const fn invalid_binary_operands(
        op: BinaryOp,
        left: &Value,
        right: &Value,
    ) -> Self {
        Self::InvalidBinaryOperands {
            op: ProjectionBinaryOpCode::from_binary_op(op),
            left: ProjectionValueKindCode::from_value(left),
            right: ProjectionValueKindCode::from_value(right),
        }
    }

    pub(in crate::db) const fn unknown_grouped_aggregate_expression(kind: AggregateKind) -> Self {
        Self::UnknownGroupedAggregateExpression {
            kind: ProjectionAggregateKindCode::from_aggregate_kind(kind),
        }
    }

    pub(in crate::db) const fn invalid_function_call(
        function: Function,
        argument_count: usize,
    ) -> Self {
        Self::InvalidFunctionCall {
            function: ProjectionFunctionCode::from_function(function),
            argument_count,
        }
    }

    pub(in crate::db) const fn invalid_projection(reason: QueryProjectionCode) -> Self {
        Self::InvalidProjection { reason }
    }

    pub(in crate::db) const fn invalid_grouped_having_result(found: &Value) -> Self {
        Self::InvalidGroupedHavingResult {
            found: ProjectionValueKindCode::from_value(found),
        }
    }

    /// Map one projection evaluation failure into the invalid-logical-plan boundary.
    pub(in crate::db) fn into_invalid_logical_plan_internal_error(self) -> InternalError {
        self.into_internal_error()
    }

    /// Map one grouped projection evaluation failure into the grouped-output
    /// invalid-logical-plan boundary while preserving grouped context.
    pub(in crate::db) fn into_grouped_projection_internal_error(self) -> InternalError {
        self.into_internal_error()
    }

    fn into_internal_error(self) -> InternalError {
        match self {
            Self::Numeric(err) => err.into_internal_error(),
            Self::FieldPathEvaluationFailed { class, origin }
            | Self::ReaderFailed { class, origin } => InternalError::classified(class, origin),
            Self::UnknownField { access } | Self::MissingFieldValue { access, .. } => {
                let _ = access;
                InternalError::query_invalid_logical_plan()
            }
            Self::MissingFieldPathValue { root_slot } => {
                let _ = root_slot;
                InternalError::query_invalid_logical_plan()
            }
            Self::InvalidUnaryOperand { op, found } => {
                let _ = (op, found);
                InternalError::query_invalid_logical_plan()
            }
            Self::InvalidCaseCondition { arm_index, found } => {
                let _ = (arm_index, found);
                InternalError::query_invalid_logical_plan()
            }
            Self::InvalidBinaryOperands { op, left, right } => {
                let _ = (op, left, right);
                InternalError::query_invalid_logical_plan()
            }
            Self::UnknownGroupedAggregateExpression { kind } => {
                let _ = kind;
                InternalError::query_invalid_logical_plan()
            }
            Self::MissingGroupedAggregateValue { index } => {
                let _ = index;
                InternalError::query_invalid_logical_plan()
            }
            Self::InvalidFunctionCall {
                function,
                argument_count,
            } => {
                let _ = (function, argument_count);
                InternalError::query_invalid_logical_plan()
            }
            Self::InvalidProjection { reason } => {
                let _ = reason;
                InternalError::query_invalid_logical_plan()
            }
            Self::InvalidGroupedHavingResult { found } => {
                let _ = found;
                InternalError::query_invalid_logical_plan()
            }
        }
    }
}

///
/// CompiledExprValueReader
///
/// CompiledExprValueReader is the only value-access contract visible to the
/// compiled expression evaluator.
/// Row, grouped-output, and HAVING execution expose their context-specific
/// values through this trait so the expression engine depends only on resolved
/// value locations after compilation.
///

pub(in crate::db) trait CompiledExprValueReader {
    /// Borrow one row-local slot value by compiled slot index.
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>>;

    /// Read one row-local slot value, preserving reader-owned failures.
    fn read_slot_checked(
        &self,
        slot: usize,
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Ok(self.read_slot(slot))
    }

    /// Borrow one finalized grouped-key value by compiled group-field offset.
    fn read_group_key(&self, offset: usize) -> Option<Cow<'_, Value>>;

    /// Read one finalized grouped-key value, preserving reader-owned failures.
    fn read_group_key_checked(
        &self,
        offset: usize,
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Ok(self.read_group_key(offset))
    }

    /// Borrow one finalized aggregate value by compiled aggregate output index.
    fn read_aggregate(&self, index: usize) -> Option<Cow<'_, Value>>;

    /// Read one finalized aggregate value, preserving reader-owned failures.
    fn read_aggregate_checked(
        &self,
        index: usize,
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        Ok(self.read_aggregate(index))
    }

    /// Read one nested field-path value rooted at a compiled slot.
    fn read_field_path(
        &self,
        root_slot: usize,
        field: &str,
        _segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let _ = field;

        Err(ProjectionEvalError::missing_field_path_root_value(
            root_slot,
        ))
    }
}

///
/// CompiledExpr
///
/// CompiledExpr is the single executable scalar-expression IR used by row
/// evaluation, grouped aggregate input/filter evaluation, grouped output
/// projection, and HAVING.
/// Slot, grouped-key, and aggregate leaves are all resolved before this type is
/// built, keeping expression execution on resolved value locations while sharing
/// one evaluator for every runtime context.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CompiledExpr {
    Slot {
        slot: usize,
        field: String,
    },
    GroupKey {
        offset: usize,
        field: String,
    },
    Aggregate {
        index: usize,
    },
    Literal(Value),
    Add {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Sub {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Mul {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Div {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Eq {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Ne {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Lt {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Lte {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Gt {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Gte {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    BinarySlotLiteral {
        op: BinaryOp,
        slot: usize,
        field: String,
        literal: Value,
        slot_on_left: bool,
    },
    CaseSlotLiteral {
        op: BinaryOp,
        slot: usize,
        field: String,
        literal: Value,
        slot_on_left: bool,
        then_expr: Box<Self>,
        else_expr: Box<Self>,
    },
    CaseSlotBool {
        slot: usize,
        field: String,
        then_expr: Box<Self>,
        else_expr: Box<Self>,
    },
    FieldPath {
        root_slot: usize,
        field: String,
        segments: Box<[String]>,
        segment_bytes: Box<[Box<[u8]>]>,
    },
    FunctionCall {
        function: Function,
        args: Box<[Self]>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Box<[CompiledExprCaseArm]>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

///
/// CompiledExprCaseArm
///
/// CompiledExprCaseArm stores one searched-CASE condition/result pair after
/// both expressions have been compiled into the single expression IR.
/// It keeps CASE branch laziness inside the expression layer without retaining
/// pre-compilation CASE arm structures after compilation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CompiledExprCaseArm {
    condition: CompiledExpr,
    result: CompiledExpr,
}

impl CompiledExprCaseArm {
    /// Build one compiled CASE arm from already-compiled condition/result nodes.
    #[must_use]
    pub(in crate::db) const fn new(condition: CompiledExpr, result: CompiledExpr) -> Self {
        Self { condition, result }
    }
}

impl CompiledExpr {
    /// Return the direct row slot used by `OCTET_LENGTH(slot)` when the
    /// expression has that exact shape.
    ///
    /// This keeps raw-row readers from pattern matching expression internals
    /// when they can answer byte-length requests from their storage-native
    /// scalar view.
    #[must_use]
    pub(in crate::db) fn direct_octet_length_slot(&self) -> Option<(usize, &str)> {
        let Self::FunctionCall {
            function: Function::OctetLength,
            args,
        } = self
        else {
            return None;
        };
        let [Self::Slot { slot, field }] = args.as_ref() else {
            return None;
        };

        Some((*slot, field.as_str()))
    }

    /// Return whether this compiled expression contains a nested field-path leaf.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn contains_field_path(&self) -> bool {
        match self {
            Self::FieldPath { .. } => true,
            Self::FunctionCall { args, .. } => args.iter().any(Self::contains_field_path),
            Self::Unary { expr, .. } => expr.contains_field_path(),
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().any(|arm| {
                    arm.condition.contains_field_path() || arm.result.contains_field_path()
                }) || else_expr.contains_field_path()
            }
            Self::Binary { left, right, .. } => {
                left.contains_field_path() || right.contains_field_path()
            }
            Self::CaseSlotLiteral {
                then_expr,
                else_expr,
                ..
            }
            | Self::CaseSlotBool {
                then_expr,
                else_expr,
                ..
            } => then_expr.contains_field_path() || else_expr.contains_field_path(),
            Self::Slot { .. }
            | Self::GroupKey { .. }
            | Self::Aggregate { .. }
            | Self::Literal(_)
            | Self::Add { .. }
            | Self::Sub { .. }
            | Self::Mul { .. }
            | Self::Div { .. }
            | Self::Eq { .. }
            | Self::Ne { .. }
            | Self::Lt { .. }
            | Self::Lte { .. }
            | Self::Gt { .. }
            | Self::Gte { .. }
            | Self::BinarySlotLiteral { .. } => false,
        }
    }

    /// Visit every row slot referenced by this compiled expression.
    pub(in crate::db) fn for_each_referenced_slot(&self, visit: &mut impl FnMut(usize)) {
        match self {
            Self::Slot { slot, .. }
            | Self::FieldPath {
                root_slot: slot, ..
            }
            | Self::BinarySlotLiteral { slot, .. }
            | Self::CaseSlotLiteral { slot, .. }
            | Self::CaseSlotBool { slot, .. } => visit(*slot),
            Self::Add {
                left_slot,
                right_slot,
                ..
            }
            | Self::Sub {
                left_slot,
                right_slot,
                ..
            }
            | Self::Mul {
                left_slot,
                right_slot,
                ..
            }
            | Self::Div {
                left_slot,
                right_slot,
                ..
            }
            | Self::Eq {
                left_slot,
                right_slot,
                ..
            }
            | Self::Ne {
                left_slot,
                right_slot,
                ..
            }
            | Self::Lt {
                left_slot,
                right_slot,
                ..
            }
            | Self::Lte {
                left_slot,
                right_slot,
                ..
            }
            | Self::Gt {
                left_slot,
                right_slot,
                ..
            }
            | Self::Gte {
                left_slot,
                right_slot,
                ..
            } => {
                visit(*left_slot);
                visit(*right_slot);
            }
            Self::FunctionCall { args, .. } => {
                for arg in args {
                    arg.for_each_referenced_slot(visit);
                }
            }
            Self::Unary { expr, .. } => expr.for_each_referenced_slot(visit),
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    arm.condition.for_each_referenced_slot(visit);
                    arm.result.for_each_referenced_slot(visit);
                }
                else_expr.for_each_referenced_slot(visit);
            }
            Self::Binary { left, right, .. } => {
                left.for_each_referenced_slot(visit);
                right.for_each_referenced_slot(visit);
            }
            Self::GroupKey { .. } | Self::Aggregate { .. } | Self::Literal(_) => {}
        }
    }

    /// Extend one slot list with every unique row slot referenced by this expression.
    pub(in crate::db) fn extend_referenced_slots(&self, referenced: &mut Vec<usize>) {
        self.for_each_referenced_slot(&mut |slot| {
            if !referenced.contains(&slot) {
                referenced.push(slot);
            }
        });
    }

    /// Mark every row slot referenced by this expression on a caller-owned bitset.
    pub(in crate::db) fn mark_referenced_slots(&self, referenced: &mut [bool]) {
        self.for_each_referenced_slot(&mut |slot| {
            if let Some(required) = referenced.get_mut(slot) {
                *required = true;
            }
        });
    }
}

const fn missing_field_value(_field: &str, index: usize) -> ProjectionEvalError {
    ProjectionEvalError::missing_slot_value(index)
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
