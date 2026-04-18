//! Module: db::sql::parser::model
//! Responsibility: reduced SQL parser-owned statement and projection model types.
//! Does not own: cursor movement, clause sequencing, or execution semantics.
//! Boundary: defines the parser output contracts re-exported by the parser root.

use crate::value::Value;

///
/// SqlStatement
///
/// Reduced SQL statement contract accepted by the current parser baseline.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlStatement {
    Select(SqlSelectStatement),
    Delete(SqlDeleteStatement),
    Insert(SqlInsertStatement),
    Update(SqlUpdateStatement),
    Explain(SqlExplainStatement),
    Describe(SqlDescribeStatement),
    ShowIndexes(SqlShowIndexesStatement),
    ShowColumns(SqlShowColumnsStatement),
    ShowEntities(SqlShowEntitiesStatement),
}

///
/// SqlProjection
///
/// Projection shape parsed from one `SELECT` statement.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlProjection {
    All,
    Items(Vec<SqlSelectItem>),
}

///
/// SqlSelectItem
///
/// One projection item parsed from one `SELECT` list.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlSelectItem {
    Field(String),
    Aggregate(SqlAggregateCall),
    TextFunction(SqlTextFunctionCall),
    Arithmetic(SqlArithmeticProjectionCall),
    Round(SqlRoundProjectionCall),
    Expr(SqlExpr),
}

///
/// SqlExprUnaryOp
///
/// Parser-owned unary SQL expression operator taxonomy.
/// This keeps searched-CASE conditions and future scalar-expression widening
/// on one frontend boundary before planner lowering maps onto `Expr`.
///

#[allow(
    dead_code,
    reason = "0.91 introduces the SQL expression boundary before searched CASE parser admission"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlExprUnaryOp {
    Not,
}

///
/// SqlExprBinaryOp
///
/// Parser-owned binary SQL expression operator taxonomy.
/// This unifies arithmetic, comparison, and boolean operators on the SQL-side
/// expression boundary instead of scattering clause-local operator enums.
///

#[allow(
    dead_code,
    reason = "0.91 introduces the SQL expression boundary before searched CASE parser admission"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlExprBinaryOp {
    Or,
    And,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
}

///
/// SqlCaseArm
///
/// Parser-owned searched-CASE branch pairing one boolean condition with the
/// value expression selected when that condition evaluates true.
/// Missing ELSE stays optional at this boundary so lowering can canonicalize
/// it to one explicit planner-owned NULL fallback.
///

#[allow(
    dead_code,
    reason = "0.91 introduces the SQL expression boundary before searched CASE parser admission"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlCaseArm {
    pub(crate) condition: SqlExpr,
    pub(crate) result: SqlExpr,
}

///
/// SqlExpr
///
/// Parser-owned SQL scalar expression tree shared across existing scalar
/// positions before planner lowering maps onto canonical planner expressions.
/// This keeps clause-specific parsing models from becoming the semantic owner
/// for CASE or future scalar-expression widening.
///

#[allow(
    dead_code,
    reason = "0.91 introduces the SQL expression boundary before searched CASE parser admission"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlExpr {
    Field(String),
    Aggregate(SqlAggregateCall),
    Literal(Value),
    TextFunction(SqlTextFunctionCall),
    Membership {
        expr: Box<Self>,
        values: Vec<Value>,
        negated: bool,
    },
    NullTest {
        expr: Box<Self>,
        negated: bool,
    },
    FunctionCall {
        function: SqlTextFunction,
        args: Vec<Self>,
    },
    Round(SqlRoundProjectionCall),
    Unary {
        op: SqlExprUnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: SqlExprBinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    Case {
        arms: Vec<SqlCaseArm>,
        else_expr: Option<Box<Self>>,
    },
}

impl SqlExpr {
    /// Convert one parsed select item into the shared SQL expression tree.
    #[must_use]
    pub(crate) fn from_select_item(item: &SqlSelectItem) -> Self {
        match item {
            SqlSelectItem::Field(field) => Self::Field(field.clone()),
            SqlSelectItem::Aggregate(aggregate) => Self::Aggregate(aggregate.clone()),
            SqlSelectItem::TextFunction(call) => Self::TextFunction(call.clone()),
            SqlSelectItem::Arithmetic(call) => Self::from_arithmetic_call(call),
            SqlSelectItem::Round(call) => Self::Round(call.clone()),
            SqlSelectItem::Expr(expr) => expr.clone(),
        }
    }

    /// Convert one projection operand into the shared SQL expression tree.
    #[must_use]
    pub(crate) fn from_projection_operand(operand: &SqlProjectionOperand) -> Self {
        match operand {
            SqlProjectionOperand::Field(field) => Self::Field(field.clone()),
            SqlProjectionOperand::Aggregate(aggregate) => Self::Aggregate(aggregate.clone()),
            SqlProjectionOperand::Literal(literal) => Self::Literal(literal.clone()),
            SqlProjectionOperand::Arithmetic(call) => Self::from_arithmetic_call(call.as_ref()),
        }
    }

    /// Convert one aggregate-input expression into the shared SQL expression tree.
    #[must_use]
    pub(crate) fn from_aggregate_input_expr(expr: &SqlAggregateInputExpr) -> Self {
        match expr {
            SqlAggregateInputExpr::Field(field) => Self::Field(field.clone()),
            SqlAggregateInputExpr::Literal(literal) => Self::Literal(literal.clone()),
            SqlAggregateInputExpr::Arithmetic(call) => Self::from_arithmetic_call(call),
            SqlAggregateInputExpr::Round(call) => Self::Round(call.clone()),
            SqlAggregateInputExpr::Expr(expr) => expr.clone(),
        }
    }

    /// Return true when one SQL expression tree contains any aggregate leaf.
    #[must_use]
    pub(crate) fn contains_aggregate(&self) -> bool {
        match self {
            Self::Aggregate(_) => true,
            Self::Field(_) | Self::Literal(_) | Self::TextFunction(_) => false,
            Self::Membership { expr, .. }
            | Self::NullTest { expr, .. }
            | Self::Unary { expr, .. } => expr.contains_aggregate(),
            Self::FunctionCall { args, .. } => args.iter().any(Self::contains_aggregate),
            Self::Round(call) => match &call.input {
                SqlRoundProjectionInput::Operand(operand) => Self::from_projection_operand(operand),
                SqlRoundProjectionInput::Arithmetic(call) => Self::from_arithmetic_call(call),
            }
            .contains_aggregate(),
            Self::Binary { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::Case { arms, else_expr } => {
                arms.iter().any(|arm| {
                    arm.condition.contains_aggregate() || arm.result.contains_aggregate()
                }) || else_expr
                    .as_ref()
                    .is_some_and(|else_expr| else_expr.contains_aggregate())
            }
        }
    }

    fn from_arithmetic_call(call: &SqlArithmeticProjectionCall) -> Self {
        Self::Binary {
            op: match call.op {
                SqlArithmeticProjectionOp::Add => SqlExprBinaryOp::Add,
                SqlArithmeticProjectionOp::Sub => SqlExprBinaryOp::Sub,
                SqlArithmeticProjectionOp::Mul => SqlExprBinaryOp::Mul,
                SqlArithmeticProjectionOp::Div => SqlExprBinaryOp::Div,
            },
            left: Box::new(Self::from_projection_operand(&call.left)),
            right: Box::new(Self::from_projection_operand(&call.right)),
        }
    }
}

///
/// SqlArithmeticProjectionOp
///
/// Reduced scalar arithmetic operator taxonomy admitted in projection
/// position.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlArithmeticProjectionOp {
    Add,
    Sub,
    Mul,
    Div,
}

///
/// SqlArithmeticProjectionCall
///
/// Parsed bounded scalar arithmetic projection item.
/// Reduced SQL keeps this narrow to one binary operation over admitted scalar
/// operands so grouped widening can add aggregate leaves without reopening a
/// full generic SQL expression parser.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlArithmeticProjectionCall {
    pub(crate) left: SqlProjectionOperand,
    pub(crate) op: SqlArithmeticProjectionOp,
    pub(crate) right: SqlProjectionOperand,
}

///
/// SqlProjectionOperand
///
/// Bounded scalar operand admitted in grouped/scalar projection expression
/// position.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlProjectionOperand {
    Field(String),
    Aggregate(SqlAggregateCall),
    Literal(Value),
    Arithmetic(Box<SqlArithmeticProjectionCall>),
}

///
/// SqlRoundProjectionInput
///
/// Parsed bounded `ROUND` source expression admitted in scalar projection
/// position.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlRoundProjectionInput {
    Operand(SqlProjectionOperand),
    Arithmetic(SqlArithmeticProjectionCall),
}

///
/// SqlRoundProjectionCall
///
/// Parsed bounded `ROUND(expr, scale)` projection item.
/// Reduced SQL keeps this to one field or one admitted arithmetic expression
/// plus one non-negative integer literal scale.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlRoundProjectionCall {
    pub(crate) input: SqlRoundProjectionInput,
    pub(crate) scale: Value,
}

///
/// SqlAggregateKind
///
/// Aggregate operator taxonomy accepted by the reduced parser.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlAggregateKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

///
/// SqlAggregateCall
///
/// Parsed aggregate call projection item.
/// `input = None` is only valid for `COUNT(*)`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlAggregateCall {
    pub(crate) kind: SqlAggregateKind,
    pub(crate) input: Option<Box<SqlAggregateInputExpr>>,
    pub(crate) filter_expr: Option<Box<SqlExpr>>,
    pub(crate) distinct: bool,
}

///
/// SqlAggregateInputExpr
///
/// Parser-owned aggregate input expression admitted inside one aggregate call.
/// This intentionally stays narrower than the full planner expression model
/// and only carries the reduced scalar expression family already admitted by
/// the SQL frontend.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlAggregateInputExpr {
    Field(String),
    Literal(Value),
    Arithmetic(SqlArithmeticProjectionCall),
    Round(SqlRoundProjectionCall),
    Expr(SqlExpr),
}

///
/// SqlTextFunction
///
/// Reduced text-function taxonomy accepted in scalar SQL projection position.
/// This remains intentionally narrow and only carries the small staged `0.66`
/// projection batches.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlTextFunction {
    Trim,
    Ltrim,
    Rtrim,
    Lower,
    Upper,
    Length,
    Left,
    Right,
    StartsWith,
    EndsWith,
    Contains,
    Position,
    Replace,
    Substring,
}

impl SqlTextFunction {
    /// Resolve one reduced SQL function identifier into one supported unary text function.
    #[must_use]
    pub(crate) fn from_identifier(identifier: &str) -> Option<Self> {
        const SUPPORTED_TEXT_FUNCTIONS: [(&str, SqlTextFunction); 14] = [
            ("trim", SqlTextFunction::Trim),
            ("ltrim", SqlTextFunction::Ltrim),
            ("rtrim", SqlTextFunction::Rtrim),
            ("lower", SqlTextFunction::Lower),
            ("upper", SqlTextFunction::Upper),
            ("length", SqlTextFunction::Length),
            ("left", SqlTextFunction::Left),
            ("right", SqlTextFunction::Right),
            ("starts_with", SqlTextFunction::StartsWith),
            ("ends_with", SqlTextFunction::EndsWith),
            ("contains", SqlTextFunction::Contains),
            ("position", SqlTextFunction::Position),
            ("replace", SqlTextFunction::Replace),
            ("substring", SqlTextFunction::Substring),
        ];

        for (name, function) in SUPPORTED_TEXT_FUNCTIONS {
            if identifier.eq_ignore_ascii_case(name) {
                return Some(function);
            }
        }

        None
    }
}

///
/// SqlTextFunctionCall
///
/// Parsed narrow text-function projection item.
/// Reduced SQL keeps this to one field plus a small fixed literal envelope so
/// the parser does not open a broad nested expression surface.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlTextFunctionCall {
    pub(crate) function: SqlTextFunction,
    pub(crate) field: String,
    pub(crate) literal: Option<Value>,
    pub(crate) literal2: Option<Value>,
    pub(crate) literal3: Option<Value>,
}

///
/// SqlOrderDirection
///
/// Parsed order direction for one `ORDER BY` item.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlOrderDirection {
    Asc,
    Desc,
}

///
/// SqlOrderTerm
///
/// Parsed `ORDER BY` expression and direction pair.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlOrderTerm {
    pub(crate) field: SqlExpr,
    pub(crate) direction: SqlOrderDirection,
}

///
/// SqlSelectStatement
///
/// Canonical parsed `SELECT` statement shape for reduced SQL.
///
/// This contract is frontend-only and intentionally schema-agnostic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlSelectStatement {
    pub(crate) entity: String,
    pub(crate) projection: SqlProjection,
    pub(crate) projection_aliases: Vec<Option<String>>,
    pub(crate) predicate: Option<SqlExpr>,
    pub(crate) distinct: bool,
    pub(crate) group_by: Vec<String>,
    pub(crate) having: Vec<SqlExpr>,
    pub(crate) order_by: Vec<SqlOrderTerm>,
    pub(crate) limit: Option<u32>,
    pub(crate) offset: Option<u32>,
}

///
/// SqlReturningProjection
///
/// Narrow write-lane `RETURNING` contract accepted by reduced SQL.
/// This intentionally keeps returning projections on field lists or `*` only
/// so write-result shaping does not reopen the broader computed or aggregate
/// SELECT projection surface.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlReturningProjection {
    All,
    Fields(Vec<String>),
}

///
/// SqlDeleteStatement
///
/// Canonical parsed `DELETE` statement shape for reduced SQL.
///
/// This contract keeps delete-mode clause policy explicit.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlDeleteStatement {
    pub(crate) entity: String,
    pub(crate) predicate: Option<SqlExpr>,
    pub(crate) order_by: Vec<SqlOrderTerm>,
    pub(crate) limit: Option<u32>,
    pub(crate) offset: Option<u32>,
    pub(crate) returning: Option<SqlReturningProjection>,
}

///
/// SqlInsertSource
///
/// Canonical parsed reduced-SQL `INSERT` source.
///
/// This keeps the current write lane narrow while still distinguishing between
/// literal tuple inserts and session-owned `INSERT ... SELECT` follow-ups.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlInsertSource {
    Values(Vec<Vec<Value>>),
    Select(Box<SqlSelectStatement>),
}

///
/// SqlInsertStatement
///
/// Canonical parsed `INSERT` statement shape for reduced SQL.
///
/// This stays intentionally narrow in the current slice: one explicit column
/// list plus either one or more literal `VALUES` tuples or one scalar
/// `SELECT` source handled later at the session boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlInsertStatement {
    pub(crate) entity: String,
    pub(crate) columns: Vec<String>,
    pub(crate) source: SqlInsertSource,
    pub(crate) returning: Option<SqlReturningProjection>,
}

///
/// SqlAssignment
///
/// One parsed `UPDATE ... SET field = literal` assignment.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlAssignment {
    pub(crate) field: String,
    pub(crate) value: Value,
}

///
/// SqlUpdateStatement
///
/// Canonical parsed `UPDATE` statement shape for reduced SQL.
///
/// This stays intentionally narrow in the current slice: one `SET` list plus
/// one optional reduced predicate and one bounded ordered window that later
/// session policy constrains further.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlUpdateStatement {
    pub(crate) entity: String,
    pub(crate) assignments: Vec<SqlAssignment>,
    pub(crate) predicate: Option<SqlExpr>,
    pub(crate) order_by: Vec<SqlOrderTerm>,
    pub(crate) limit: Option<u32>,
    pub(crate) offset: Option<u32>,
    pub(crate) returning: Option<SqlReturningProjection>,
}

///
/// SqlExplainMode
///
/// Reduced EXPLAIN render mode selector.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlExplainMode {
    Plan,
    Execution,
    Json,
}

///
/// SqlExplainTarget
///
/// Statement forms accepted behind one `EXPLAIN` prefix.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlExplainTarget {
    Select(SqlSelectStatement),
    Delete(SqlDeleteStatement),
}

///
/// SqlExplainStatement
///
/// Canonical parsed `EXPLAIN` statement.
///
/// Explain remains a wrapper over one executable reduced SQL statement.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlExplainStatement {
    pub(crate) mode: SqlExplainMode,
    pub(crate) statement: SqlExplainTarget,
}

///
/// SqlDescribeStatement
///
/// Canonical parsed `DESCRIBE` statement.
/// Carries one schema entity identifier for typed session introspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlDescribeStatement {
    pub(crate) entity: String,
}

///
/// SqlShowIndexesStatement
///
/// Canonical parsed `SHOW INDEXES` statement.
/// Carries one schema entity identifier for typed session introspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlShowIndexesStatement {
    pub(crate) entity: String,
}

///
/// SqlShowColumnsStatement
///
/// Canonical parsed `SHOW COLUMNS` statement.
/// Carries one schema entity identifier for typed session introspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlShowColumnsStatement {
    pub(crate) entity: String,
}

///
/// SqlShowEntitiesStatement
///
/// Canonical parsed `SHOW ENTITIES` statement.
/// This lane carries no entity identifier and targets SQL helper introspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlShowEntitiesStatement;
