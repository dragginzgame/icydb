//! Module: db::sql::parser::model
//! Responsibility: reduced SQL parser-owned statement and projection model types.
//! Does not own: cursor movement, clause sequencing, or execution semantics.
//! Boundary: defines the parser output contracts re-exported by the parser root.

use crate::{
    db::{
        query::plan::{AggregateKind, expr::Function},
        sql::identifier::split_qualified_identifier,
    },
    value::Value,
};

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

impl SqlProjection {
    /// Return whether this parsed projection already stays within the local
    /// scalar normalization fast path.
    #[must_use]
    pub(in crate::db) fn is_already_local_scalar(&self) -> bool {
        match self {
            Self::All => true,
            Self::Items(items) => items.iter().all(SqlSelectItem::is_already_local_projection),
        }
    }
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
    Expr(SqlExpr),
}

impl SqlSelectItem {
    /// Return whether one parsed select item contains any aggregate leaf.
    #[must_use]
    pub(crate) fn contains_aggregate(&self) -> bool {
        match self {
            Self::Field(_) => false,
            Self::Aggregate(_) => true,
            Self::Expr(expr) => expr.contains_aggregate(),
        }
    }

    /// Return whether this parsed select item already stays within the local
    /// projection normalization fast path.
    #[must_use]
    pub(in crate::db) fn is_already_local_projection(&self) -> bool {
        match self {
            Self::Field(field) => SqlExpr::identifier_is_already_local(field.as_str()),
            Self::Aggregate(aggregate) => aggregate.is_already_local_scalar(),
            Self::Expr(_) => false,
        }
    }
}

///
/// SqlExprUnaryOp
///
/// Parser-owned unary SQL expression operator taxonomy.
/// This keeps searched-CASE conditions and future scalar-expression widening
/// on one frontend boundary before planner lowering maps onto `Expr`.
///

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlExpr {
    Field(String),
    Aggregate(SqlAggregateCall),
    Literal(Value),
    Param {
        index: usize,
    },
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
        function: SqlScalarFunction,
        args: Vec<Self>,
    },
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
            SqlSelectItem::Expr(expr) => expr.clone(),
        }
    }

    /// Return true when one SQL expression tree contains any aggregate leaf.
    #[must_use]
    pub(crate) fn contains_aggregate(&self) -> bool {
        self.any_tree_expr(&mut |expr| matches!(expr, Self::Aggregate(_)))
    }

    /// Return whether this SQL expression already fits the local scalar
    /// normalization fast path without identifier rescoping.
    #[must_use]
    pub(in crate::db) fn is_already_local_scalar(&self) -> bool {
        self.all_tree_expr(&mut |expr| match expr {
            Self::Field(field) => Self::identifier_is_already_local(field.as_str()),
            Self::Literal(_)
            | Self::Param { .. }
            | Self::NullTest { .. }
            | Self::Unary { .. }
            | Self::Binary { .. } => true,
            Self::Membership { values, .. } => values
                .iter()
                .all(|value| !matches!(value, Value::List(_) | Value::Map(_))),
            Self::Aggregate(_) | Self::FunctionCall { .. } | Self::Case { .. } => false,
        })
    }

    /// Return whether every field leaf in this expression is already a local
    /// bare identifier.
    #[must_use]
    pub(in crate::db) fn fields_are_already_local(&self) -> bool {
        self.all_tree_expr(&mut |expr| match expr {
            Self::Field(field) => Self::identifier_is_already_local(field.as_str()),
            Self::Aggregate(aggregate) => aggregate.is_already_local_scalar(),
            Self::Literal(_)
            | Self::Param { .. }
            | Self::Membership { .. }
            | Self::NullTest { .. }
            | Self::FunctionCall { .. }
            | Self::Unary { .. }
            | Self::Binary { .. }
            | Self::Case { .. } => true,
        })
    }

    /// Return whether this expression is exactly one `LOWER(field)` or
    /// `UPPER(field)` wrapper over one direct field leaf.
    #[must_use]
    pub(in crate::db) fn is_casefold_field_wrapper(&self) -> bool {
        matches!(
            self,
            Self::FunctionCall {
                function,
                args,
            } if function.is_casefold_transform() && matches!(args.as_slice(), [Self::Field(_)])
        )
    }

    /// Return whether this SQL expression tree contains any searched `CASE`
    /// arm with an omitted `ELSE`.
    #[must_use]
    pub(in crate::db) fn contains_omitted_else_case(&self) -> bool {
        self.any_tree_expr(
            &mut |expr| matches!(expr, Self::Case { else_expr, .. } if else_expr.is_none()),
        )
    }

    /// Visit every SQL expression node through the owner-local parser
    /// traversal contract.
    pub(in crate::db) fn for_each_tree_expr(&self, visit: &mut impl FnMut(&Self)) {
        visit(self);

        match self {
            Self::Field(_) | Self::Aggregate(_) | Self::Literal(_) | Self::Param { .. } => {}
            Self::Membership { expr, .. }
            | Self::NullTest { expr, .. }
            | Self::Unary { expr, .. } => expr.for_each_tree_expr(visit),
            Self::FunctionCall { args, .. } => {
                for arg in args {
                    arg.for_each_tree_expr(visit);
                }
            }
            Self::Binary { left, right, .. } => {
                left.for_each_tree_expr(visit);
                right.for_each_tree_expr(visit);
            }
            Self::Case { arms, else_expr } => {
                for arm in arms {
                    arm.condition.for_each_tree_expr(visit);
                    arm.result.for_each_tree_expr(visit);
                }
                if let Some(else_expr) = else_expr.as_ref() {
                    else_expr.for_each_tree_expr(visit);
                }
            }
        }
    }

    /// Visit every aggregate leaf owned by this SQL expression tree through
    /// the canonical parser traversal contract.
    pub(in crate::db) fn for_each_tree_aggregate(&self, visit: &mut impl FnMut(&SqlAggregateCall)) {
        self.for_each_tree_expr(&mut |expr| {
            if let Self::Aggregate(aggregate) = expr {
                visit(aggregate);
            }
        });
    }

    // Local identifiers are already in the parser/planner leaf form and do
    // not need entity-scope reduction.
    fn identifier_is_already_local(identifier: &str) -> bool {
        split_qualified_identifier(identifier).is_none()
    }

    // Walk the whole SQL expression tree and return true as soon as one node
    // matches the supplied predicate. This keeps aggregate and omitted-ELSE
    // detection on one traversal shape instead of repeating the same tree walk.
    fn any_tree_expr(&self, predicate: &mut impl FnMut(&Self) -> bool) -> bool {
        if predicate(self) {
            return true;
        }

        match self {
            Self::Field(_) | Self::Aggregate(_) | Self::Literal(_) | Self::Param { .. } => false,
            Self::Membership { expr, .. }
            | Self::NullTest { expr, .. }
            | Self::Unary { expr, .. } => expr.any_tree_expr(predicate),
            Self::FunctionCall { args, .. } => args.iter().any(|arg| arg.any_tree_expr(predicate)),
            Self::Binary { left, right, .. } => {
                left.any_tree_expr(predicate) || right.any_tree_expr(predicate)
            }
            Self::Case { arms, else_expr } => {
                arms.iter().any(|arm| {
                    arm.condition.any_tree_expr(predicate) || arm.result.any_tree_expr(predicate)
                }) || else_expr
                    .as_ref()
                    .is_some_and(|else_expr| else_expr.any_tree_expr(predicate))
            }
        }
    }

    // Walk the whole SQL expression tree and require every visited node to
    // satisfy the supplied admission rule. This keeps the local-scalar and
    // local-field checks on one recursive traversal while still letting each
    // caller define its own leaf policy.
    fn all_tree_expr(&self, predicate: &mut impl FnMut(&Self) -> bool) -> bool {
        if !predicate(self) {
            return false;
        }

        match self {
            Self::Field(_) | Self::Aggregate(_) | Self::Literal(_) | Self::Param { .. } => true,
            Self::Membership { expr, .. }
            | Self::NullTest { expr, .. }
            | Self::Unary { expr, .. } => expr.all_tree_expr(predicate),
            Self::FunctionCall { args, .. } => args.iter().all(|arg| arg.all_tree_expr(predicate)),
            Self::Binary { left, right, .. } => {
                left.all_tree_expr(predicate) && right.all_tree_expr(predicate)
            }
            Self::Case { arms, else_expr } => {
                arms.iter().all(|arm| {
                    arm.condition.all_tree_expr(predicate) && arm.result.all_tree_expr(predicate)
                }) && else_expr
                    .as_ref()
                    .is_none_or(|else_expr| else_expr.all_tree_expr(predicate))
            }
        }
    }
}

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

impl SqlAggregateKind {
    /// Return whether this parsed aggregate kind admits `*` as its input.
    #[must_use]
    pub(crate) const fn supports_star_input(self) -> bool {
        matches!(self, Self::Count)
    }

    /// Return whether this parsed aggregate kind lowers one field input into
    /// the shared field-target aggregate shape.
    #[must_use]
    pub(in crate::db) const fn lowers_shared_field_target_shape(self) -> bool {
        !matches!(self, Self::Count)
    }

    /// Return the canonical planner aggregate kind for this parsed SQL kind.
    #[must_use]
    pub(in crate::db) const fn aggregate_kind(self) -> AggregateKind {
        match self {
            Self::Count => AggregateKind::Count,
            Self::Sum => AggregateKind::Sum,
            Self::Avg => AggregateKind::Avg,
            Self::Min => AggregateKind::Min,
            Self::Max => AggregateKind::Max,
        }
    }
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
    pub(crate) input: Option<Box<SqlExpr>>,
    pub(crate) filter_expr: Option<Box<SqlExpr>>,
    pub(crate) distinct: bool,
}

impl SqlAggregateCall {
    /// Return whether this aggregate call already stays within the local
    /// aggregate input normalization fast path.
    #[must_use]
    pub(in crate::db) fn is_already_local_scalar(&self) -> bool {
        let input_is_local = self
            .input
            .as_deref()
            .is_none_or(SqlExpr::is_already_local_scalar);

        input_is_local
            && self
                .filter_expr
                .as_deref()
                .is_none_or(SqlExpr::is_already_local_scalar)
    }
}

///
/// SqlScalarFunction
///
/// Reduced scalar-function taxonomy accepted in parsed SQL expression position.
/// This remains intentionally narrow and only carries the supported scalar
/// function family that lowers into the shared planner expression surface.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub(crate) enum SqlScalarFunction {
    Abs,
    Ceiling,
    Coalesce,
    Contains,
    EndsWith,
    Floor,
    Left,
    Length,
    Lower,
    Ltrim,
    NullIf,
    Position,
    Replace,
    Right,
    Round,
    Rtrim,
    StartsWith,
    Substring,
    Trim,
    Upper,
}

///
/// SqlScalarFunctionCallShape
///
/// Parser-owned call-shape family for one supported SQL scalar function.
/// This keeps parser dispatch on one enum-owned contract so projection, WHERE,
/// and ORDER parsing do not each re-derive the same function-family ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlScalarFunctionCallShape {
    BinaryExprArgs,
    FieldPlusLiteral,
    Position,
    Replace,
    RoundSpecial,
    SharedScalarCall,
    Substring,
    UnaryExpr,
    VariadicExprArgs,
    WherePredicateExprPair,
}

impl SqlScalarFunction {
    /// Return whether this parsed SQL scalar function is one of the admitted
    /// casefold text transforms that preserve shared LOWER/UPPER wrapper
    /// semantics across parser and lowering seams.
    #[must_use]
    pub(crate) const fn is_casefold_transform(self) -> bool {
        matches!(self, Self::Lower | Self::Upper)
    }

    /// Return whether this parsed SQL scalar function uses the dedicated
    /// `ROUND(...)` parser/lowering path instead of the general scalar call
    /// surface.
    #[must_use]
    pub(crate) const fn uses_round_special_case(self) -> bool {
        matches!(self, Self::Round)
    }

    /// Return the canonical planner-owned scalar function identity for this
    /// parsed SQL scalar function.
    #[must_use]
    pub(in crate::db) const fn planner_function(self) -> Function {
        match self {
            Self::Abs => Function::Abs,
            Self::Ceiling => Function::Ceiling,
            Self::Coalesce => Function::Coalesce,
            Self::Contains => Function::Contains,
            Self::EndsWith => Function::EndsWith,
            Self::Floor => Function::Floor,
            Self::Left => Function::Left,
            Self::Length => Function::Length,
            Self::Lower => Function::Lower,
            Self::Ltrim => Function::Ltrim,
            Self::NullIf => Function::NullIf,
            Self::Position => Function::Position,
            Self::Replace => Function::Replace,
            Self::Right => Function::Right,
            Self::Round => Function::Round,
            Self::Rtrim => Function::Rtrim,
            Self::StartsWith => Function::StartsWith,
            Self::Substring => Function::Substring,
            Self::Trim => Function::Trim,
            Self::Upper => Function::Upper,
        }
    }

    /// Return the parser call-shape used by non-WHERE scalar function parsing.
    #[must_use]
    pub(in crate::db) const fn non_where_call_shape(self) -> SqlScalarFunctionCallShape {
        match self {
            Self::Round => SqlScalarFunctionCallShape::RoundSpecial,
            Self::Coalesce => SqlScalarFunctionCallShape::VariadicExprArgs,
            Self::NullIf => SqlScalarFunctionCallShape::BinaryExprArgs,
            Self::Trim
            | Self::Ltrim
            | Self::Rtrim
            | Self::Abs
            | Self::Ceiling
            | Self::Floor
            | Self::Lower
            | Self::Upper
            | Self::Length => SqlScalarFunctionCallShape::UnaryExpr,
            Self::Left | Self::Right | Self::StartsWith | Self::EndsWith | Self::Contains => {
                SqlScalarFunctionCallShape::FieldPlusLiteral
            }
            Self::Position => SqlScalarFunctionCallShape::Position,
            Self::Replace => SqlScalarFunctionCallShape::Replace,
            Self::Substring => SqlScalarFunctionCallShape::Substring,
        }
    }

    /// Return the parser call-shape used by WHERE expression parsing.
    #[must_use]
    pub(in crate::db) const fn where_call_shape(self) -> SqlScalarFunctionCallShape {
        match self.non_where_call_shape() {
            SqlScalarFunctionCallShape::RoundSpecial => SqlScalarFunctionCallShape::RoundSpecial,
            SqlScalarFunctionCallShape::VariadicExprArgs => {
                SqlScalarFunctionCallShape::VariadicExprArgs
            }
            SqlScalarFunctionCallShape::BinaryExprArgs => {
                SqlScalarFunctionCallShape::BinaryExprArgs
            }
            SqlScalarFunctionCallShape::FieldPlusLiteral
                if self
                    .planner_function()
                    .boolean_text_predicate_kind()
                    .is_some() =>
            {
                SqlScalarFunctionCallShape::WherePredicateExprPair
            }
            SqlScalarFunctionCallShape::UnaryExpr
            | SqlScalarFunctionCallShape::FieldPlusLiteral
            | SqlScalarFunctionCallShape::Position
            | SqlScalarFunctionCallShape::Replace
            | SqlScalarFunctionCallShape::Substring => SqlScalarFunctionCallShape::SharedScalarCall,
            SqlScalarFunctionCallShape::SharedScalarCall
            | SqlScalarFunctionCallShape::WherePredicateExprPair => {
                SqlScalarFunctionCallShape::SharedScalarCall
            }
        }
    }

    /// Resolve one parsed SQL identifier into one supported scalar function.
    #[must_use]
    pub(crate) fn from_identifier(identifier: &str) -> Option<Self> {
        const SUPPORTED_SCALAR_FUNCTIONS: [(&str, SqlScalarFunction); 21] = [
            ("trim", SqlScalarFunction::Trim),
            ("ltrim", SqlScalarFunction::Ltrim),
            ("rtrim", SqlScalarFunction::Rtrim),
            ("round", SqlScalarFunction::Round),
            ("coalesce", SqlScalarFunction::Coalesce),
            ("nullif", SqlScalarFunction::NullIf),
            ("abs", SqlScalarFunction::Abs),
            ("ceil", SqlScalarFunction::Ceiling),
            ("ceiling", SqlScalarFunction::Ceiling),
            ("floor", SqlScalarFunction::Floor),
            ("lower", SqlScalarFunction::Lower),
            ("upper", SqlScalarFunction::Upper),
            ("length", SqlScalarFunction::Length),
            ("left", SqlScalarFunction::Left),
            ("right", SqlScalarFunction::Right),
            ("starts_with", SqlScalarFunction::StartsWith),
            ("ends_with", SqlScalarFunction::EndsWith),
            ("contains", SqlScalarFunction::Contains),
            ("position", SqlScalarFunction::Position),
            ("replace", SqlScalarFunction::Replace),
            ("substring", SqlScalarFunction::Substring),
        ];

        for (name, function) in SUPPORTED_SCALAR_FUNCTIONS {
            if identifier.eq_ignore_ascii_case(name) {
                return Some(function);
            }
        }

        None
    }
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

impl SqlOrderTerm {
    /// Return whether this parsed order term already stays within the local
    /// supported-order fast path.
    #[must_use]
    pub(in crate::db) fn is_already_local_supported(&self) -> bool {
        self.field.fields_are_already_local()
    }

    /// Return one direct field name when this order term still targets one
    /// bare SQL field leaf.
    #[must_use]
    pub(in crate::db) const fn direct_field_name(&self) -> Option<&str> {
        match &self.field {
            SqlExpr::Field(field) => Some(field.as_str()),
            SqlExpr::Aggregate(_)
            | SqlExpr::Literal(_)
            | SqlExpr::Param { .. }
            | SqlExpr::Membership { .. }
            | SqlExpr::NullTest { .. }
            | SqlExpr::FunctionCall { .. }
            | SqlExpr::Unary { .. }
            | SqlExpr::Binary { .. }
            | SqlExpr::Case { .. } => None,
        }
    }
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

impl SqlSelectStatement {
    /// Return whether this parsed `SELECT` already stays in the local
    /// canonical shape expected by lowering.
    #[must_use]
    pub(in crate::db) fn is_already_local_canonical(&self) -> bool {
        if !self.projection_aliases.iter().all(Option::is_none) {
            return false;
        }
        if !self.having.is_empty() {
            return false;
        }
        if !self
            .group_by
            .iter()
            .all(|field| SqlExpr::identifier_is_already_local(field.as_str()))
        {
            return false;
        }
        if !self.projection.is_already_local_scalar() {
            return false;
        }
        if self
            .predicate
            .as_ref()
            .is_some_and(|predicate| !predicate.is_already_local_scalar())
        {
            return false;
        }

        self.order_by
            .iter()
            .all(SqlOrderTerm::is_already_local_supported)
    }
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
    pub(crate) verbose: bool,
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
