//! Module: query::builder::aggregate
//! Responsibility: composable grouped/global aggregate expression builders.
//! Does not own: aggregate validation policy or executor fold semantics.
//! Boundary: fluent aggregate intent construction lowered into grouped specs.

use crate::db::query::plan::{
    AggregateKind, FieldSlot,
    expr::{BinaryOp, Expr, FieldId, Function},
};
use crate::{
    db::numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
    value::Value,
};

///
/// AggregateExpr
///
/// Composable aggregate expression used by query/fluent aggregate entrypoints.
/// This builder only carries declarative shape (`kind`, aggregate input
/// expression, `distinct`) and does not perform semantic validation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateExpr {
    kind: AggregateKind,
    input_expr: Option<Box<Expr>>,
    distinct: bool,
}

impl AggregateExpr {
    /// Construct one terminal aggregate expression with no input expression.
    const fn terminal(kind: AggregateKind) -> Self {
        Self {
            kind,
            input_expr: None,
            distinct: false,
        }
    }

    /// Construct one aggregate expression over one canonical field leaf.
    fn field_target(kind: AggregateKind, field: impl Into<String>) -> Self {
        Self {
            kind,
            input_expr: Some(Box::new(Expr::Field(FieldId::new(field.into())))),
            distinct: false,
        }
    }

    /// Construct one aggregate expression from one planner-owned input expression.
    pub(in crate::db) fn from_expression_input(kind: AggregateKind, input_expr: Expr) -> Self {
        Self {
            kind,
            input_expr: Some(Box::new(canonicalize_aggregate_input_expr(
                kind, input_expr,
            ))),
            distinct: false,
        }
    }

    /// Enable DISTINCT modifier for this aggregate expression.
    #[must_use]
    pub const fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Borrow aggregate kind.
    #[must_use]
    pub(crate) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow the aggregate input expression, if any.
    #[must_use]
    pub(crate) fn input_expr(&self) -> Option<&Expr> {
        self.input_expr.as_deref()
    }

    /// Borrow the optional target field when this aggregate input stays a plain field leaf.
    #[must_use]
    pub(crate) fn target_field(&self) -> Option<&str> {
        match self.input_expr() {
            Some(Expr::Field(field)) => Some(field.as_str()),
            _ => None,
        }
    }

    /// Return true when DISTINCT is enabled.
    #[must_use]
    pub(crate) const fn is_distinct(&self) -> bool {
        self.distinct
    }

    /// Build one aggregate expression directly from planner semantic parts.
    pub(in crate::db::query) fn from_semantic_parts(
        kind: AggregateKind,
        target_field: Option<String>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            input_expr: target_field.map(|field| Box::new(Expr::Field(FieldId::new(field)))),
            distinct,
        }
    }

    /// Build one non-field-target terminal aggregate expression from one kind.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn terminal_for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => count(),
            AggregateKind::Exists => exists(),
            AggregateKind::Min => min(),
            AggregateKind::Max => max(),
            AggregateKind::First => first(),
            AggregateKind::Last => last(),
            AggregateKind::Sum | AggregateKind::Avg => unreachable!(
                "AggregateExpr::terminal_for_kind does not support SUM/AVG field-target kinds"
            ),
        }
    }
}

// Keep aggregate input identity canonical anywhere planner-owned aggregate
// expressions are constructed so grouped/global paths do not drift on
// semantically equivalent constant subexpressions.
pub(in crate::db) fn canonicalize_aggregate_input_expr(kind: AggregateKind, expr: Expr) -> Expr {
    let folded =
        normalize_aggregate_input_numeric_literals(fold_aggregate_input_constant_expr(expr));

    match kind {
        AggregateKind::Sum | AggregateKind::Avg => match folded {
            Expr::Literal(value) => value
                .to_numeric_decimal()
                .map_or(Expr::Literal(value), |decimal| {
                    Expr::Literal(Value::Decimal(decimal.normalize()))
                }),
            other => other,
        },
        AggregateKind::Count
        | AggregateKind::Min
        | AggregateKind::Max
        | AggregateKind::Exists
        | AggregateKind::First
        | AggregateKind::Last => folded,
    }
}

// Fold literal-only aggregate-input subexpressions so semantic aggregate
// matching can treat `AVG(age + 1 * 2)` and `AVG(age + 2)` as the same input.
fn fold_aggregate_input_constant_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Field(_) | Expr::Literal(_) | Expr::Aggregate(_) => expr,
        Expr::FunctionCall { function, args } => {
            let args = args
                .into_iter()
                .map(fold_aggregate_input_constant_expr)
                .collect::<Vec<_>>();

            fold_aggregate_input_constant_function(function, args.as_slice())
                .unwrap_or(Expr::FunctionCall { function, args })
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    crate::db::query::plan::expr::CaseWhenArm::new(
                        fold_aggregate_input_constant_expr(arm.condition().clone()),
                        fold_aggregate_input_constant_expr(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(fold_aggregate_input_constant_expr(*else_expr)),
        },
        Expr::Binary { op, left, right } => {
            let left = fold_aggregate_input_constant_expr(*left);
            let right = fold_aggregate_input_constant_expr(*right);

            fold_aggregate_input_constant_binary(op, &left, &right).unwrap_or_else(|| {
                Expr::Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            })
        }
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(fold_aggregate_input_constant_expr(*expr)),
            name,
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(fold_aggregate_input_constant_expr(*expr)),
        },
    }
}

// Fold one literal-only binary aggregate-input fragment onto one decimal
// literal so aggregate identity stays stable across equivalent SQL spellings.
fn fold_aggregate_input_constant_binary(op: BinaryOp, left: &Expr, right: &Expr) -> Option<Expr> {
    let (Expr::Literal(left), Expr::Literal(right)) = (left, right) else {
        return None;
    };
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Some(Expr::Literal(Value::Null));
    }

    let arithmetic_op = match op {
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => return None,
        BinaryOp::Add => NumericArithmeticOp::Add,
        BinaryOp::Sub => NumericArithmeticOp::Sub,
        BinaryOp::Mul => NumericArithmeticOp::Mul,
        BinaryOp::Div => NumericArithmeticOp::Div,
    };
    let result = apply_numeric_arithmetic(arithmetic_op, left, right)?;

    Some(Expr::Literal(Value::Decimal(result)))
}

// Fold one admitted literal-only aggregate-input function call when the
// reduced aggregate-input family has one deterministic literal result.
fn fold_aggregate_input_constant_function(function: Function, args: &[Expr]) -> Option<Expr> {
    match function {
        Function::Round => fold_aggregate_input_constant_round(args),
        Function::Trim
        | Function::Ltrim
        | Function::Rtrim
        | Function::Lower
        | Function::Upper
        | Function::Length
        | Function::Left
        | Function::Right
        | Function::StartsWith
        | Function::EndsWith
        | Function::Contains
        | Function::Position
        | Function::Replace
        | Function::Substring => None,
    }
}

// Fold one literal-only ROUND(...) aggregate-input fragment so parenthesized
// constant arithmetic keeps the same aggregate identity as its literal result.
fn fold_aggregate_input_constant_round(args: &[Expr]) -> Option<Expr> {
    let [Expr::Literal(input), Expr::Literal(scale)] = args else {
        return None;
    };
    if matches!(input, Value::Null) || matches!(scale, Value::Null) {
        return Some(Expr::Literal(Value::Null));
    }

    let scale = match scale {
        Value::Int(value) => u32::try_from(*value).ok()?,
        Value::Uint(value) => u32::try_from(*value).ok()?,
        _ => return None,
    };
    let decimal = input.to_numeric_decimal()?;

    Some(Expr::Literal(Value::Decimal(decimal.round_dp(scale))))
}

// Normalize numeric literal leaves recursively so semantically equivalent
// aggregate inputs like `age + 2` and `age + 1 * 2` share one canonical
// planner identity after literal-only subtree folding.
fn normalize_aggregate_input_numeric_literals(expr: Expr) -> Expr {
    match expr {
        Expr::Literal(value) => value
            .to_numeric_decimal()
            .map_or(Expr::Literal(value), |decimal| {
                Expr::Literal(Value::Decimal(decimal.normalize()))
            }),
        Expr::Field(_) | Expr::Aggregate(_) => expr,
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(normalize_aggregate_input_numeric_literals)
                .collect(),
        },
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    crate::db::query::plan::expr::CaseWhenArm::new(
                        normalize_aggregate_input_numeric_literals(arm.condition().clone()),
                        normalize_aggregate_input_numeric_literals(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(normalize_aggregate_input_numeric_literals(*else_expr)),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(normalize_aggregate_input_numeric_literals(*left)),
            right: Box::new(normalize_aggregate_input_numeric_literals(*right)),
        },
        #[cfg(test)]
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(normalize_aggregate_input_numeric_literals(*expr)),
            name,
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(normalize_aggregate_input_numeric_literals(*expr)),
        },
    }
}

///
/// PreparedFluentAggregateExplainStrategy
///
/// PreparedFluentAggregateExplainStrategy is the shared explain-only
/// projection contract for fluent aggregate domains that can render one
/// `AggregateExpr`.
/// It keeps session/query explain projection generic without collapsing the
/// runtime domain boundaries that still stay family-specific.
///

pub(crate) trait PreparedFluentAggregateExplainStrategy {
    /// Return the explain-visible aggregate kind when this runtime family can
    /// project one aggregate terminal plan shape.
    fn explain_aggregate_kind(&self) -> Option<AggregateKind>;

    /// Return the explain-visible projected field label, if any.
    fn explain_projected_field(&self) -> Option<&str> {
        None
    }
}

/// PreparedFluentExistingRowsTerminalRuntimeRequest
///
/// Stable fluent existing-rows terminal runtime request projection derived
/// once at the fluent aggregate entrypoint boundary.
/// This keeps count/exists request choice aligned with the aggregate
/// expression used for explain projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentExistingRowsTerminalRuntimeRequest {
    CountRows,
    ExistsRows,
}

///
/// PreparedFluentExistingRowsTerminalStrategy
///
/// PreparedFluentExistingRowsTerminalStrategy is the single fluent
/// existing-rows behavior source for the next `0.71` slice.
/// It resolves runtime terminal request shape once and projects explain
/// aggregate metadata from that same prepared state on demand.
/// This keeps `count()` and `exists()` off the mixed id/extrema scalar
/// strategy without carrying owned explain-only aggregate expressions through
/// execution.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentExistingRowsTerminalStrategy {
    runtime_request: PreparedFluentExistingRowsTerminalRuntimeRequest,
}

impl PreparedFluentExistingRowsTerminalStrategy {
    /// Prepare one fluent `count(*)` terminal strategy.
    #[must_use]
    pub(crate) const fn count_rows() -> Self {
        Self {
            runtime_request: PreparedFluentExistingRowsTerminalRuntimeRequest::CountRows,
        }
    }

    /// Prepare one fluent `exists()` terminal strategy.
    #[must_use]
    pub(crate) const fn exists_rows() -> Self {
        Self {
            runtime_request: PreparedFluentExistingRowsTerminalRuntimeRequest::ExistsRows,
        }
    }

    /// Build the explain-visible aggregate expression projected by this
    /// prepared fluent existing-rows strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn aggregate(&self) -> AggregateExpr {
        match self.runtime_request {
            PreparedFluentExistingRowsTerminalRuntimeRequest::CountRows => count(),
            PreparedFluentExistingRowsTerminalRuntimeRequest::ExistsRows => exists(),
        }
    }

    /// Borrow the prepared runtime request projected by this fluent
    /// existing-rows strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn runtime_request(
        &self,
    ) -> &PreparedFluentExistingRowsTerminalRuntimeRequest {
        &self.runtime_request
    }

    /// Move the prepared runtime request out of this fluent existing-rows
    /// strategy so execution can consume it without cloning.
    #[must_use]
    pub(crate) const fn into_runtime_request(
        self,
    ) -> PreparedFluentExistingRowsTerminalRuntimeRequest {
        self.runtime_request
    }
}

impl PreparedFluentAggregateExplainStrategy for PreparedFluentExistingRowsTerminalStrategy {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(match self.runtime_request {
            PreparedFluentExistingRowsTerminalRuntimeRequest::CountRows => AggregateKind::Count,
            PreparedFluentExistingRowsTerminalRuntimeRequest::ExistsRows => AggregateKind::Exists,
        })
    }
}

/// PreparedFluentScalarTerminalRuntimeRequest
///
/// Stable fluent scalar terminal runtime request projection derived once at
/// the fluent aggregate entrypoint boundary.
/// This keeps id/extrema execution-side request choice aligned with the
/// same prepared metadata that explain projects on demand.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentScalarTerminalRuntimeRequest {
    IdTerminal {
        kind: AggregateKind,
    },
    IdBySlot {
        kind: AggregateKind,
        target_field: FieldSlot,
    },
}

///
/// PreparedFluentScalarTerminalStrategy
///
/// PreparedFluentScalarTerminalStrategy is the fluent scalar id/extrema
/// behavior source for the current `0.71` slice.
/// It resolves runtime terminal request shape once so the id/extrema family
/// does not rebuild those decisions through parallel branch trees.
/// Explain-visible aggregate shape is projected from that same prepared
/// metadata only when explain needs it.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentScalarTerminalStrategy {
    runtime_request: PreparedFluentScalarTerminalRuntimeRequest,
}

impl PreparedFluentScalarTerminalStrategy {
    /// Prepare one fluent id-returning scalar terminal without a field target.
    #[must_use]
    pub(crate) const fn id_terminal(kind: AggregateKind) -> Self {
        Self {
            runtime_request: PreparedFluentScalarTerminalRuntimeRequest::IdTerminal { kind },
        }
    }

    /// Prepare one fluent field-targeted extrema terminal with a resolved
    /// planner slot.
    #[must_use]
    pub(crate) const fn id_by_slot(kind: AggregateKind, target_field: FieldSlot) -> Self {
        Self {
            runtime_request: PreparedFluentScalarTerminalRuntimeRequest::IdBySlot {
                kind,
                target_field,
            },
        }
    }

    /// Move the prepared runtime request out of this fluent scalar strategy
    /// so execution can consume it without cloning.
    #[must_use]
    pub(crate) fn into_runtime_request(self) -> PreparedFluentScalarTerminalRuntimeRequest {
        self.runtime_request
    }
}

impl PreparedFluentAggregateExplainStrategy for PreparedFluentScalarTerminalStrategy {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(match self.runtime_request {
            PreparedFluentScalarTerminalRuntimeRequest::IdTerminal { kind }
            | PreparedFluentScalarTerminalRuntimeRequest::IdBySlot { kind, .. } => kind,
        })
    }

    fn explain_projected_field(&self) -> Option<&str> {
        match &self.runtime_request {
            PreparedFluentScalarTerminalRuntimeRequest::IdTerminal { .. } => None,
            PreparedFluentScalarTerminalRuntimeRequest::IdBySlot { target_field, .. } => {
                Some(target_field.field())
            }
        }
    }
}

///
/// PreparedFluentNumericFieldRuntimeRequest
///
/// Stable fluent numeric-field runtime request projection derived once at the
/// fluent aggregate entrypoint boundary.
/// This keeps numeric boundary selection aligned with the same prepared
/// metadata that runtime and explain projections share.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentNumericFieldRuntimeRequest {
    Sum,
    SumDistinct,
    Avg,
    AvgDistinct,
}

///
/// PreparedFluentNumericFieldStrategy
///
/// PreparedFluentNumericFieldStrategy is the single fluent numeric-field
/// behavior source for the next `0.71` slice.
/// It resolves target-slot ownership and runtime boundary request once so
/// `SUM/AVG` callers do not rebuild those decisions through parallel branch
/// trees.
/// Explain-visible aggregate shape is projected on demand from that prepared
/// state instead of being carried as owned execution metadata.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentNumericFieldStrategy {
    target_field: FieldSlot,
    runtime_request: PreparedFluentNumericFieldRuntimeRequest,
}

impl PreparedFluentNumericFieldStrategy {
    /// Prepare one fluent `sum(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn sum_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentNumericFieldRuntimeRequest::Sum,
        }
    }

    /// Prepare one fluent `sum(distinct field)` terminal strategy.
    #[must_use]
    pub(crate) const fn sum_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentNumericFieldRuntimeRequest::SumDistinct,
        }
    }

    /// Prepare one fluent `avg(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn avg_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentNumericFieldRuntimeRequest::Avg,
        }
    }

    /// Prepare one fluent `avg(distinct field)` terminal strategy.
    #[must_use]
    pub(crate) const fn avg_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentNumericFieldRuntimeRequest::AvgDistinct,
        }
    }

    /// Build the explain-visible aggregate expression projected by this
    /// prepared fluent numeric strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn aggregate(&self) -> AggregateExpr {
        let field = self.target_field.field();

        match self.runtime_request {
            PreparedFluentNumericFieldRuntimeRequest::Sum => sum(field),
            PreparedFluentNumericFieldRuntimeRequest::SumDistinct => sum(field).distinct(),
            PreparedFluentNumericFieldRuntimeRequest::Avg => avg(field),
            PreparedFluentNumericFieldRuntimeRequest::AvgDistinct => avg(field).distinct(),
        }
    }

    /// Return the aggregate kind projected by this prepared fluent numeric
    /// strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn aggregate_kind(&self) -> AggregateKind {
        match self.runtime_request {
            PreparedFluentNumericFieldRuntimeRequest::Sum
            | PreparedFluentNumericFieldRuntimeRequest::SumDistinct => AggregateKind::Sum,
            PreparedFluentNumericFieldRuntimeRequest::Avg
            | PreparedFluentNumericFieldRuntimeRequest::AvgDistinct => AggregateKind::Avg,
        }
    }

    /// Borrow the projected field label for this prepared fluent numeric
    /// strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn projected_field(&self) -> &str {
        self.target_field.field()
    }

    /// Borrow the resolved planner target slot owned by this prepared fluent
    /// numeric strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn target_field(&self) -> &FieldSlot {
        &self.target_field
    }

    /// Return the prepared runtime request projected by this fluent numeric
    /// strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn runtime_request(&self) -> PreparedFluentNumericFieldRuntimeRequest {
        self.runtime_request
    }

    /// Move the resolved field slot and numeric runtime request out of this
    /// strategy so execution can consume them without cloning the field slot.
    #[must_use]
    pub(crate) fn into_runtime_parts(
        self,
    ) -> (FieldSlot, PreparedFluentNumericFieldRuntimeRequest) {
        (self.target_field, self.runtime_request)
    }
}

impl PreparedFluentAggregateExplainStrategy for PreparedFluentNumericFieldStrategy {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(match self.runtime_request {
            PreparedFluentNumericFieldRuntimeRequest::Sum
            | PreparedFluentNumericFieldRuntimeRequest::SumDistinct => AggregateKind::Sum,
            PreparedFluentNumericFieldRuntimeRequest::Avg
            | PreparedFluentNumericFieldRuntimeRequest::AvgDistinct => AggregateKind::Avg,
        })
    }

    fn explain_projected_field(&self) -> Option<&str> {
        Some(self.target_field.field())
    }
}

///
/// PreparedFluentOrderSensitiveTerminalRuntimeRequest
///
/// Stable fluent order-sensitive runtime request projection derived once at
/// the fluent aggregate entrypoint boundary.
/// This keeps response-order and field-order terminal request shape aligned
/// with the prepared strategy that fluent execution consumes and explain
/// projects on demand.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentOrderSensitiveTerminalRuntimeRequest {
    ResponseOrder { kind: AggregateKind },
    NthBySlot { target_field: FieldSlot, nth: usize },
    MedianBySlot { target_field: FieldSlot },
    MinMaxBySlot { target_field: FieldSlot },
}

///
/// PreparedFluentOrderSensitiveTerminalStrategy
///
/// PreparedFluentOrderSensitiveTerminalStrategy is the single fluent
/// order-sensitive behavior source for the next `0.71` slice.
/// It resolves EXPLAIN-visible aggregate shape where applicable and the
/// runtime terminal request once so `first/last/nth_by/median_by/min_max_by`
/// do not rebuild those decisions through parallel branch trees.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentOrderSensitiveTerminalStrategy {
    runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest,
}

impl PreparedFluentOrderSensitiveTerminalStrategy {
    /// Prepare one fluent `first()` terminal strategy.
    #[must_use]
    pub(crate) const fn first() -> Self {
        Self {
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder {
                kind: AggregateKind::First,
            },
        }
    }

    /// Prepare one fluent `last()` terminal strategy.
    #[must_use]
    pub(crate) const fn last() -> Self {
        Self {
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder {
                kind: AggregateKind::Last,
            },
        }
    }

    /// Prepare one fluent `nth_by(field, nth)` terminal strategy.
    #[must_use]
    pub(crate) const fn nth_by_slot(target_field: FieldSlot, nth: usize) -> Self {
        Self {
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::NthBySlot {
                target_field,
                nth,
            },
        }
    }

    /// Prepare one fluent `median_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn median_by_slot(target_field: FieldSlot) -> Self {
        Self {
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::MedianBySlot {
                target_field,
            },
        }
    }

    /// Prepare one fluent `min_max_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn min_max_by_slot(target_field: FieldSlot) -> Self {
        Self {
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::MinMaxBySlot {
                target_field,
            },
        }
    }

    /// Build the explain-visible aggregate expression projected by this
    /// prepared order-sensitive strategy when one exists.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn explain_aggregate(&self) -> Option<AggregateExpr> {
        match self.runtime_request {
            PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder { kind } => {
                Some(AggregateExpr::terminal_for_kind(kind))
            }
            PreparedFluentOrderSensitiveTerminalRuntimeRequest::NthBySlot { .. }
            | PreparedFluentOrderSensitiveTerminalRuntimeRequest::MedianBySlot { .. }
            | PreparedFluentOrderSensitiveTerminalRuntimeRequest::MinMaxBySlot { .. } => None,
        }
    }

    /// Borrow the prepared runtime request projected by this fluent
    /// order-sensitive strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn runtime_request(
        &self,
    ) -> &PreparedFluentOrderSensitiveTerminalRuntimeRequest {
        &self.runtime_request
    }

    /// Move the prepared runtime request out of this order-sensitive strategy
    /// so execution can consume it without cloning.
    #[must_use]
    pub(crate) fn into_runtime_request(self) -> PreparedFluentOrderSensitiveTerminalRuntimeRequest {
        self.runtime_request
    }
}

impl PreparedFluentAggregateExplainStrategy for PreparedFluentOrderSensitiveTerminalStrategy {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        match self.runtime_request {
            PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder { kind } => {
                Some(kind)
            }
            PreparedFluentOrderSensitiveTerminalRuntimeRequest::NthBySlot { .. }
            | PreparedFluentOrderSensitiveTerminalRuntimeRequest::MedianBySlot { .. }
            | PreparedFluentOrderSensitiveTerminalRuntimeRequest::MinMaxBySlot { .. } => None,
        }
    }
}

///
/// PreparedFluentProjectionRuntimeRequest
///
/// Stable fluent projection/distinct runtime request projection derived once
/// at the fluent aggregate entrypoint boundary.
/// This keeps field-target projection terminal request shape aligned with the
/// prepared strategy that fluent execution consumes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentProjectionRuntimeRequest {
    Values,
    DistinctValues,
    CountDistinct,
    ValuesWithIds,
    TerminalValue { terminal_kind: AggregateKind },
}

///
/// PreparedFluentProjectionExplainDescriptor
///
/// PreparedFluentProjectionExplainDescriptor is the stable explain projection
/// surface for fluent projection/distinct terminals.
/// It carries the already-decided descriptor labels explain needs so query
/// intent does not rebuild projection terminal shape from runtime requests.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentProjectionExplainDescriptor<'a> {
    terminal: &'static str,
    field: &'a str,
    output: &'static str,
}

impl<'a> PreparedFluentProjectionExplainDescriptor<'a> {
    /// Return the stable explain terminal label.
    #[must_use]
    pub(crate) const fn terminal_label(self) -> &'static str {
        self.terminal
    }

    /// Return the stable explain field label.
    #[must_use]
    pub(crate) const fn field_label(self) -> &'a str {
        self.field
    }

    /// Return the stable explain output-shape label.
    #[must_use]
    pub(crate) const fn output_label(self) -> &'static str {
        self.output
    }
}

///
/// PreparedFluentProjectionStrategy
///
/// PreparedFluentProjectionStrategy is the single fluent projection/distinct
/// behavior source for the next `0.71` slice.
/// It resolves target-slot ownership plus runtime request shape once so
/// `values_by`/`distinct_values_by`/`count_distinct_by`/`values_by_with_ids`/
/// `first_value_by`/`last_value_by` do not rebuild those decisions inline.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentProjectionStrategy {
    target_field: FieldSlot,
    runtime_request: PreparedFluentProjectionRuntimeRequest,
}

impl PreparedFluentProjectionStrategy {
    /// Prepare one fluent `values_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn values_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::Values,
        }
    }

    /// Prepare one fluent `distinct_values_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn distinct_values_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::DistinctValues,
        }
    }

    /// Prepare one fluent `count_distinct_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn count_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::CountDistinct,
        }
    }

    /// Prepare one fluent `values_by_with_ids(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn values_by_with_ids_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::ValuesWithIds,
        }
    }

    /// Prepare one fluent `first_value_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn first_value_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::TerminalValue {
                terminal_kind: AggregateKind::First,
            },
        }
    }

    /// Prepare one fluent `last_value_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn last_value_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::TerminalValue {
                terminal_kind: AggregateKind::Last,
            },
        }
    }

    /// Borrow the resolved planner target slot owned by this prepared fluent
    /// projection strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn target_field(&self) -> &FieldSlot {
        &self.target_field
    }

    /// Return the prepared runtime request projected by this fluent
    /// projection strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn runtime_request(&self) -> PreparedFluentProjectionRuntimeRequest {
        self.runtime_request
    }

    /// Move the resolved field slot and projection runtime request out of
    /// this strategy so execution can consume them without cloning the field
    /// slot.
    #[must_use]
    pub(crate) fn into_runtime_parts(self) -> (FieldSlot, PreparedFluentProjectionRuntimeRequest) {
        (self.target_field, self.runtime_request)
    }

    /// Return the stable projection explain descriptor for this prepared
    /// strategy.
    #[must_use]
    pub(crate) fn explain_descriptor(&self) -> PreparedFluentProjectionExplainDescriptor<'_> {
        let terminal_label = match self.runtime_request {
            PreparedFluentProjectionRuntimeRequest::Values => "values_by",
            PreparedFluentProjectionRuntimeRequest::DistinctValues => "distinct_values_by",
            PreparedFluentProjectionRuntimeRequest::CountDistinct => "count_distinct_by",
            PreparedFluentProjectionRuntimeRequest::ValuesWithIds => "values_by_with_ids",
            PreparedFluentProjectionRuntimeRequest::TerminalValue {
                terminal_kind: AggregateKind::First,
            } => "first_value_by",
            PreparedFluentProjectionRuntimeRequest::TerminalValue {
                terminal_kind: AggregateKind::Last,
            } => "last_value_by",
            PreparedFluentProjectionRuntimeRequest::TerminalValue { .. } => {
                unreachable!("projection terminal value explain requires FIRST/LAST kind")
            }
        };
        let output_label = match self.runtime_request {
            PreparedFluentProjectionRuntimeRequest::Values
            | PreparedFluentProjectionRuntimeRequest::DistinctValues => "values",
            PreparedFluentProjectionRuntimeRequest::CountDistinct => "count",
            PreparedFluentProjectionRuntimeRequest::ValuesWithIds => "values_with_ids",
            PreparedFluentProjectionRuntimeRequest::TerminalValue { .. } => "terminal_value",
        };

        PreparedFluentProjectionExplainDescriptor {
            terminal: terminal_label,
            field: self.target_field.field(),
            output: output_label,
        }
    }
}

/// Build `count(*)`.
#[must_use]
pub const fn count() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Count)
}

/// Build `count(field)`.
#[must_use]
pub fn count_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Count, field.as_ref().to_string())
}

/// Build `sum(field)`.
#[must_use]
pub fn sum(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Sum, field.as_ref().to_string())
}

/// Build `avg(field)`.
#[must_use]
pub fn avg(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Avg, field.as_ref().to_string())
}

/// Build `exists`.
#[must_use]
pub const fn exists() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Exists)
}

/// Build `first`.
#[must_use]
pub const fn first() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::First)
}

/// Build `last`.
#[must_use]
pub const fn last() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Last)
}

/// Build `min`.
#[must_use]
pub const fn min() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Min)
}

/// Build `min(field)`.
#[must_use]
pub fn min_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Min, field.as_ref().to_string())
}

/// Build `max`.
#[must_use]
pub const fn max() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Max)
}

/// Build `max(field)`.
#[must_use]
pub fn max_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Max, field.as_ref().to_string())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::query::{
        builder::{
            PreparedFluentExistingRowsTerminalRuntimeRequest,
            PreparedFluentExistingRowsTerminalStrategy, PreparedFluentNumericFieldRuntimeRequest,
            PreparedFluentNumericFieldStrategy, PreparedFluentOrderSensitiveTerminalRuntimeRequest,
            PreparedFluentOrderSensitiveTerminalStrategy, PreparedFluentProjectionRuntimeRequest,
            PreparedFluentProjectionStrategy,
        },
        plan::{AggregateKind, FieldSlot},
    };

    #[test]
    fn prepared_fluent_numeric_field_strategy_sum_distinct_preserves_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy = PreparedFluentNumericFieldStrategy::sum_distinct_by_slot(rank_slot.clone());

        assert_eq!(
            strategy.aggregate_kind(),
            AggregateKind::Sum,
            "sum(distinct field) should preserve SUM aggregate kind",
        );
        assert_eq!(
            strategy.projected_field(),
            "rank",
            "sum(distinct field) should preserve projected field labels",
        );
        assert!(
            strategy.aggregate().is_distinct(),
            "sum(distinct field) should preserve DISTINCT aggregate shape",
        );
        assert_eq!(
            strategy.target_field(),
            &rank_slot,
            "sum(distinct field) should preserve the resolved planner field slot",
        );
        assert_eq!(
            strategy.runtime_request(),
            PreparedFluentNumericFieldRuntimeRequest::SumDistinct,
            "sum(distinct field) should project the numeric DISTINCT runtime request",
        );
    }

    #[test]
    fn prepared_fluent_existing_rows_strategy_count_preserves_runtime_shape() {
        let strategy = PreparedFluentExistingRowsTerminalStrategy::count_rows();

        assert_eq!(
            strategy.aggregate().kind(),
            AggregateKind::Count,
            "count() should preserve the explain-visible aggregate kind",
        );
        assert_eq!(
            strategy.runtime_request(),
            &PreparedFluentExistingRowsTerminalRuntimeRequest::CountRows,
            "count() should project the existing-rows count runtime request",
        );
    }

    #[test]
    fn prepared_fluent_existing_rows_strategy_exists_preserves_runtime_shape() {
        let strategy = PreparedFluentExistingRowsTerminalStrategy::exists_rows();

        assert_eq!(
            strategy.aggregate().kind(),
            AggregateKind::Exists,
            "exists() should preserve the explain-visible aggregate kind",
        );
        assert_eq!(
            strategy.runtime_request(),
            &PreparedFluentExistingRowsTerminalRuntimeRequest::ExistsRows,
            "exists() should project the existing-rows exists runtime request",
        );
    }

    #[test]
    fn prepared_fluent_numeric_field_strategy_avg_preserves_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy = PreparedFluentNumericFieldStrategy::avg_by_slot(rank_slot.clone());

        assert_eq!(
            strategy.aggregate_kind(),
            AggregateKind::Avg,
            "avg(field) should preserve AVG aggregate kind",
        );
        assert_eq!(
            strategy.projected_field(),
            "rank",
            "avg(field) should preserve projected field labels",
        );
        assert!(
            !strategy.aggregate().is_distinct(),
            "avg(field) should stay non-distinct unless requested explicitly",
        );
        assert_eq!(
            strategy.target_field(),
            &rank_slot,
            "avg(field) should preserve the resolved planner field slot",
        );
        assert_eq!(
            strategy.runtime_request(),
            PreparedFluentNumericFieldRuntimeRequest::Avg,
            "avg(field) should project the numeric AVG runtime request",
        );
    }

    #[test]
    fn prepared_fluent_order_sensitive_strategy_first_preserves_explain_and_runtime_shape() {
        let strategy = PreparedFluentOrderSensitiveTerminalStrategy::first();

        assert_eq!(
            strategy
                .explain_aggregate()
                .map(|aggregate| aggregate.kind()),
            Some(AggregateKind::First),
            "first() should preserve the explain-visible aggregate kind",
        );
        assert_eq!(
            strategy.runtime_request(),
            &PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder {
                kind: AggregateKind::First,
            },
            "first() should project the response-order runtime request",
        );
    }

    #[test]
    fn prepared_fluent_order_sensitive_strategy_nth_preserves_field_order_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy =
            PreparedFluentOrderSensitiveTerminalStrategy::nth_by_slot(rank_slot.clone(), 2);

        assert_eq!(
            strategy.explain_aggregate(),
            None,
            "nth_by(field, nth) should stay off the current explain aggregate surface",
        );
        assert_eq!(
            strategy.runtime_request(),
            &PreparedFluentOrderSensitiveTerminalRuntimeRequest::NthBySlot {
                target_field: rank_slot,
                nth: 2,
            },
            "nth_by(field, nth) should preserve the resolved field-order runtime request",
        );
    }

    #[test]
    fn prepared_fluent_projection_strategy_count_distinct_preserves_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy = PreparedFluentProjectionStrategy::count_distinct_by_slot(rank_slot.clone());
        let explain = strategy.explain_descriptor();

        assert_eq!(
            strategy.target_field(),
            &rank_slot,
            "count_distinct_by(field) should preserve the resolved planner field slot",
        );
        assert_eq!(
            strategy.runtime_request(),
            PreparedFluentProjectionRuntimeRequest::CountDistinct,
            "count_distinct_by(field) should project the distinct-count runtime request",
        );
        assert_eq!(
            explain.terminal_label(),
            "count_distinct_by",
            "count_distinct_by(field) should project the stable explain terminal label",
        );
        assert_eq!(
            explain.field_label(),
            "rank",
            "count_distinct_by(field) should project the stable explain field label",
        );
        assert_eq!(
            explain.output_label(),
            "count",
            "count_distinct_by(field) should project the stable explain output label",
        );
    }

    #[test]
    fn prepared_fluent_projection_strategy_terminal_value_preserves_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy = PreparedFluentProjectionStrategy::last_value_by_slot(rank_slot.clone());
        let explain = strategy.explain_descriptor();

        assert_eq!(
            strategy.target_field(),
            &rank_slot,
            "last_value_by(field) should preserve the resolved planner field slot",
        );
        assert_eq!(
            strategy.runtime_request(),
            PreparedFluentProjectionRuntimeRequest::TerminalValue {
                terminal_kind: AggregateKind::Last,
            },
            "last_value_by(field) should project the terminal-value runtime request",
        );
        assert_eq!(
            explain.terminal_label(),
            "last_value_by",
            "last_value_by(field) should project the stable explain terminal label",
        );
        assert_eq!(
            explain.field_label(),
            "rank",
            "last_value_by(field) should project the stable explain field label",
        );
        assert_eq!(
            explain.output_label(),
            "terminal_value",
            "last_value_by(field) should project the stable explain output label",
        );
    }
}
