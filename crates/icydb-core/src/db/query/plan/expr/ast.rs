//! Module: query::plan::expr::ast
//! Responsibility: planner expression AST domain types and field/operator identifiers.
//! Does not own: expression type inference policy or runtime expression evaluation.
//! Boundary: defines canonical expression tree structures consumed by planner validation/lowering.

use crate::db::query::builder::aggregate::AggregateExpr;
use crate::value::Value;

///
/// FieldId
///
/// Canonical planner-owned field identity token for expression trees.
/// This wrapper carries the declared field name and avoids ad-hoc string use.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct FieldId(String);

impl FieldId {
    /// Build one field-id token from a field name.
    #[must_use]
    pub(crate) fn new(field: impl Into<String>) -> Self {
        Self(field.into())
    }

    /// Borrow the canonical field name.
    #[must_use]
    pub(crate) const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for FieldId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for FieldId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

///
/// Alias
///
/// Canonical planner-owned alias token attached to expression projections.
/// Alias remains presentation metadata and does not affect semantic identity.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Alias(String);

impl Alias {
    /// Build one alias token from owned/borrowed text.
    #[must_use]
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Borrow the alias as text.
    #[must_use]
    pub(crate) const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for Alias {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Alias {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

///
/// UnaryOp
///
/// Canonical unary expression operator taxonomy.
///

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnaryOp {
    Not,
}

///
/// BinaryOp
///
/// Canonical binary expression operator taxonomy.
///

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BinaryOp {
    Add,
    Mul,
    And,
    Eq,
}

///
/// Function
///
/// Canonical bounded function taxonomy admitted by planner-owned projection
/// expressions.
/// This intentionally stays limited to the shipped text-function surface.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Function {
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

impl Function {
    /// Return the stable uppercase SQL label for this bounded function.
    #[must_use]
    pub(crate) const fn sql_label(self) -> &'static str {
        match self {
            Self::Trim => "TRIM",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
            Self::Length => "LENGTH",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::StartsWith => "STARTS_WITH",
            Self::EndsWith => "ENDS_WITH",
            Self::Contains => "CONTAINS",
            Self::Position => "POSITION",
            Self::Replace => "REPLACE",
            Self::Substring => "SUBSTRING",
        }
    }
}

/// Parse one supported canonical `ORDER BY` function term into the canonical
/// expression tree.
#[must_use]
pub(in crate::db) fn parse_supported_order_expr(term: &str) -> Option<Expr> {
    let open_index = term.find('(')?;
    if !term.ends_with(')') {
        return None;
    }

    let function = match &term[..open_index] {
        name if name.eq_ignore_ascii_case("LOWER") => Function::Lower,
        name if name.eq_ignore_ascii_case("UPPER") => Function::Upper,
        _ => return None,
    };
    let field = &term[open_index.saturating_add(1)..term.len().saturating_sub(1)];

    Some(Expr::FunctionCall {
        function,
        args: vec![Expr::Field(FieldId::new(field))],
    })
}

/// Borrow the referenced field when one expression is an admitted `ORDER BY`
/// function term.
#[must_use]
pub(in crate::db) fn supported_order_expr_field(expr: &Expr) -> Option<&FieldId> {
    match expr {
        Expr::FunctionCall {
            function: Function::Lower | Function::Upper,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => Some(field),
            _ => None,
        },
        _ => None,
    }
}

/// Rebuild one admitted canonical `ORDER BY` function term with a replacement
/// field identifier.
#[must_use]
pub(in crate::db) fn rewrite_supported_order_expr_field(
    expr: &Expr,
    field: impl Into<String>,
) -> Option<Expr> {
    let function = match expr {
        Expr::FunctionCall {
            function: function @ (Function::Lower | Function::Upper),
            args,
        } if matches!(args.as_slice(), [Expr::Field(_)]) => *function,
        _ => return None,
    };

    Some(Expr::FunctionCall {
        function,
        args: vec![Expr::Field(FieldId::new(field))],
    })
}

/// Render one admitted canonical `ORDER BY` function term back into its stable
/// text form.
#[must_use]
pub(in crate::db) fn render_supported_order_expr(expr: &Expr) -> Option<String> {
    let function = match expr {
        Expr::FunctionCall {
            function: function @ (Function::Lower | Function::Upper),
            args,
        } if matches!(args.as_slice(), [Expr::Field(_)]) => *function,
        _ => return None,
    };
    let field = supported_order_expr_field(expr)?;

    Some(format!("{}({})", function.sql_label(), field.as_str()))
}

///
/// Expr
///
/// Canonical planner-owned expression tree for projection semantics.
/// This model is semantic-only and intentionally excludes execution logic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Expr {
    Field(FieldId),
    Literal(Value),
    FunctionCall {
        function: Function,
        args: Vec<Self>,
    },
    #[cfg(test)]
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    #[cfg(test)]
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    Aggregate(AggregateExpr),
    #[cfg(test)]
    Alias {
        expr: Box<Self>,
        name: Alias,
    },
}
