//! Module: query::expr
//! Responsibility: schema-agnostic filter/order expression wrappers and lowering.
//! Does not own: planner route selection or executor evaluation.
//! Boundary: intent boundary lowers these to validated predicate/order forms.

use crate::db::query::{
    builder::FieldRef,
    builder::{AggregateExpr, NumericProjectionExpr, RoundProjectionExpr, TextProjectionExpr},
    plan::{
        OrderDirection, OrderTerm as PlannedOrderTerm,
        expr::{BinaryOp, Expr, FieldId, Function, UnaryOp},
    },
};
use crate::{traits::FieldValue, value::Value};
use candid::CandidType;
use serde::Deserialize;

///
/// FilterExpr
///
/// Serialized, planner-agnostic filter language.
/// This is the shared typed filter input model for fluent callers and lowers
/// directly onto planner-owned boolean expressions at the intent boundary.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum FilterExpr {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Eq {
        field: String,
        value: Value,
    },
    EqCi {
        field: String,
        value: Value,
    },
    Ne {
        field: String,
        value: Value,
    },
    Lt {
        field: String,
        value: Value,
    },
    Lte {
        field: String,
        value: Value,
    },
    Gt {
        field: String,
        value: Value,
    },
    Gte {
        field: String,
        value: Value,
    },
    EqField {
        left_field: String,
        right_field: String,
    },
    NeField {
        left_field: String,
        right_field: String,
    },
    LtField {
        left_field: String,
        right_field: String,
    },
    LteField {
        left_field: String,
        right_field: String,
    },
    GtField {
        left_field: String,
        right_field: String,
    },
    GteField {
        left_field: String,
        right_field: String,
    },
    In {
        field: String,
        values: Vec<Value>,
    },
    NotIn {
        field: String,
        values: Vec<Value>,
    },
    Contains {
        field: String,
        value: Value,
    },
    TextContains {
        field: String,
        value: Value,
    },
    TextContainsCi {
        field: String,
        value: Value,
    },
    StartsWith {
        field: String,
        value: Value,
    },
    StartsWithCi {
        field: String,
        value: Value,
    },
    EndsWith {
        field: String,
        value: Value,
    },
    EndsWithCi {
        field: String,
        value: Value,
    },
    IsNull {
        field: String,
    },
    IsNotNull {
        field: String,
    },
    IsMissing {
        field: String,
    },
    IsEmpty {
        field: String,
    },
    IsNotEmpty {
        field: String,
    },
}

impl FilterExpr {
    /// Lower this typed filter expression into the shared planner-owned boolean expression model.
    #[must_use]
    pub(in crate::db) fn lower_bool_expr(&self) -> Expr {
        match self {
            Self::True => Expr::Literal(Value::Bool(true)),
            Self::False => Expr::Literal(Value::Bool(false)),
            Self::And(xs) => fold_filter_bool_chain(BinaryOp::And, xs),
            Self::Or(xs) => fold_filter_bool_chain(BinaryOp::Or, xs),
            Self::Not(x) => Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(x.lower_bool_expr()),
            },
            Self::Eq { field, value } => field_compare_expr(BinaryOp::Eq, field, value.clone()),
            Self::EqCi { field, value } => Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(casefold_field_expr(field)),
                right: Box::new(Expr::Literal(value.clone())),
            },
            Self::Ne { field, value } => field_compare_expr(BinaryOp::Ne, field, value.clone()),
            Self::Lt { field, value } => field_compare_expr(BinaryOp::Lt, field, value.clone()),
            Self::Lte { field, value } => field_compare_expr(BinaryOp::Lte, field, value.clone()),
            Self::Gt { field, value } => field_compare_expr(BinaryOp::Gt, field, value.clone()),
            Self::Gte { field, value } => field_compare_expr(BinaryOp::Gte, field, value.clone()),
            Self::EqField {
                left_field,
                right_field,
            } => field_compare_field_expr(BinaryOp::Eq, left_field, right_field),
            Self::NeField {
                left_field,
                right_field,
            } => field_compare_field_expr(BinaryOp::Ne, left_field, right_field),
            Self::LtField {
                left_field,
                right_field,
            } => field_compare_field_expr(BinaryOp::Lt, left_field, right_field),
            Self::LteField {
                left_field,
                right_field,
            } => field_compare_field_expr(BinaryOp::Lte, left_field, right_field),
            Self::GtField {
                left_field,
                right_field,
            } => field_compare_field_expr(BinaryOp::Gt, left_field, right_field),
            Self::GteField {
                left_field,
                right_field,
            } => field_compare_field_expr(BinaryOp::Gte, left_field, right_field),
            Self::In { field, values } => membership_expr(field, values.as_slice(), false),
            Self::NotIn { field, values } => membership_expr(field, values.as_slice(), true),
            Self::Contains { field, value } => Expr::FunctionCall {
                function: Function::CollectionContains,
                args: vec![
                    Expr::Field(FieldId::new(field.clone())),
                    Expr::Literal(value.clone()),
                ],
            },
            Self::TextContains { field, value } => text_function_expr(
                Function::Contains,
                Expr::Field(FieldId::new(field.clone())),
                value.clone(),
            ),
            Self::TextContainsCi { field, value } => text_function_expr(
                Function::Contains,
                casefold_field_expr(field),
                value.clone(),
            ),
            Self::StartsWith { field, value } => text_function_expr(
                Function::StartsWith,
                Expr::Field(FieldId::new(field.clone())),
                value.clone(),
            ),
            Self::StartsWithCi { field, value } => text_function_expr(
                Function::StartsWith,
                casefold_field_expr(field),
                value.clone(),
            ),
            Self::EndsWith { field, value } => text_function_expr(
                Function::EndsWith,
                Expr::Field(FieldId::new(field.clone())),
                value.clone(),
            ),
            Self::EndsWithCi { field, value } => text_function_expr(
                Function::EndsWith,
                casefold_field_expr(field),
                value.clone(),
            ),
            Self::IsNull { field } => field_function_expr(Function::IsNull, field),
            Self::IsNotNull { field } => field_function_expr(Function::IsNotNull, field),
            Self::IsMissing { field } => field_function_expr(Function::IsMissing, field),
            Self::IsEmpty { field } => field_function_expr(Function::IsEmpty, field),
            Self::IsNotEmpty { field } => field_function_expr(Function::IsNotEmpty, field),
        }
    }

    /// Build an `And` expression from a list of child expressions.
    #[must_use]
    pub const fn and(exprs: Vec<Self>) -> Self {
        Self::And(exprs)
    }

    /// Build an `Or` expression from a list of child expressions.
    #[must_use]
    pub const fn or(exprs: Vec<Self>) -> Self {
        Self::Or(exprs)
    }

    /// Negate one child expression.
    #[must_use]
    #[expect(clippy::should_implement_trait)]
    pub fn not(expr: Self) -> Self {
        Self::Not(Box::new(expr))
    }

    /// Compare `field == value`.
    #[must_use]
    pub fn eq(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field != value`.
    #[must_use]
    pub fn ne(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Ne {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field < value`.
    #[must_use]
    pub fn lt(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Lt {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field <= value`.
    #[must_use]
    pub fn lte(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Lte {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field > value`.
    #[must_use]
    pub fn gt(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Gt {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field >= value`.
    #[must_use]
    pub fn gte(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Gte {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field == value` with casefolded text equality.
    #[must_use]
    pub fn eq_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::EqCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `left_field == right_field`.
    #[must_use]
    pub fn eq_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::EqField {
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field != right_field`.
    #[must_use]
    pub fn ne_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::NeField {
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field < right_field`.
    #[must_use]
    pub fn lt_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::LtField {
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field <= right_field`.
    #[must_use]
    pub fn lte_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::LteField {
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field > right_field`.
    #[must_use]
    pub fn gt_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::GtField {
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field >= right_field`.
    #[must_use]
    pub fn gte_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::GteField {
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `field IN values`.
    #[must_use]
    pub fn in_list(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl FieldValue>,
    ) -> Self {
        Self::In {
            field: field.into(),
            values: values.into_iter().map(|value| value.to_value()).collect(),
        }
    }

    /// Compare `field NOT IN values`.
    #[must_use]
    pub fn not_in(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl FieldValue>,
    ) -> Self {
        Self::NotIn {
            field: field.into(),
            values: values.into_iter().map(|value| value.to_value()).collect(),
        }
    }

    /// Compare collection `field CONTAINS value`.
    #[must_use]
    pub fn contains(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Contains {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-sensitive substring containment.
    #[must_use]
    pub fn text_contains(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::TextContains {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-insensitive substring containment.
    #[must_use]
    pub fn text_contains_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::TextContainsCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-sensitive prefix match.
    #[must_use]
    pub fn starts_with(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::StartsWith {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-insensitive prefix match.
    #[must_use]
    pub fn starts_with_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::StartsWithCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-sensitive suffix match.
    #[must_use]
    pub fn ends_with(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::EndsWith {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-insensitive suffix match.
    #[must_use]
    pub fn ends_with_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::EndsWithCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Match rows where `field` is present and null.
    #[must_use]
    pub fn is_null(field: impl Into<String>) -> Self {
        Self::IsNull {
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and non-null.
    #[must_use]
    pub fn is_not_null(field: impl Into<String>) -> Self {
        Self::IsNotNull {
            field: field.into(),
        }
    }

    /// Match rows where `field` is absent.
    #[must_use]
    pub fn is_missing(field: impl Into<String>) -> Self {
        Self::IsMissing {
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and empty.
    #[must_use]
    pub fn is_empty(field: impl Into<String>) -> Self {
        Self::IsEmpty {
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and non-empty.
    #[must_use]
    pub fn is_not_empty(field: impl Into<String>) -> Self {
        Self::IsNotEmpty {
            field: field.into(),
        }
    }
}

fn fold_filter_bool_chain(op: BinaryOp, exprs: &[FilterExpr]) -> Expr {
    let mut exprs = exprs.iter();
    let Some(first) = exprs.next() else {
        return Expr::Literal(Value::Bool(matches!(op, BinaryOp::And)));
    };

    let first = first.lower_bool_expr();

    exprs.fold(first, |left, expr| Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(expr.lower_bool_expr()),
    })
}

fn field_compare_expr(op: BinaryOp, field: &str, value: Value) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(Expr::Field(FieldId::new(field.to_string()))),
        right: Box::new(Expr::Literal(value)),
    }
}

fn field_compare_field_expr(op: BinaryOp, left_field: &str, right_field: &str) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(Expr::Field(FieldId::new(left_field.to_string()))),
        right: Box::new(Expr::Field(FieldId::new(right_field.to_string()))),
    }
}

fn membership_expr(field: &str, values: &[Value], negated: bool) -> Expr {
    let compare_op = if negated { BinaryOp::Ne } else { BinaryOp::Eq };
    let join_op = if negated { BinaryOp::And } else { BinaryOp::Or };
    let mut values = values.iter();
    let Some(first) = values.next() else {
        return Expr::Literal(Value::Bool(negated));
    };

    let field = Expr::Field(FieldId::new(field.to_string()));
    let mut expr = Expr::Binary {
        op: compare_op,
        left: Box::new(field.clone()),
        right: Box::new(Expr::Literal(first.clone())),
    };

    for value in values {
        expr = Expr::Binary {
            op: join_op,
            left: Box::new(expr),
            right: Box::new(Expr::Binary {
                op: compare_op,
                left: Box::new(field.clone()),
                right: Box::new(Expr::Literal(value.clone())),
            }),
        };
    }

    expr
}

fn field_function_expr(function: Function, field: &str) -> Expr {
    Expr::FunctionCall {
        function,
        args: vec![Expr::Field(FieldId::new(field.to_string()))],
    }
}

fn text_function_expr(function: Function, left: Expr, value: Value) -> Expr {
    Expr::FunctionCall {
        function,
        args: vec![left, Expr::Literal(value)],
    }
}

fn casefold_field_expr(field: &str) -> Expr {
    Expr::FunctionCall {
        function: Function::Lower,
        args: vec![Expr::Field(FieldId::new(field.to_string()))],
    }
}

///
/// OrderExpr
///
/// Typed fluent ORDER BY expression wrapper.
/// This exists so fluent code can construct planner-owned ORDER BY
/// semantics directly at the query boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderExpr {
    expr: Expr,
}

impl OrderExpr {
    /// Build one direct field ORDER BY expression.
    #[must_use]
    pub fn field(field: impl Into<String>) -> Self {
        let field = field.into();

        Self {
            expr: Expr::Field(FieldId::new(field)),
        }
    }

    // Freeze one typed fluent order expression onto the planner-owned
    // semantic expression now that labels are derived only at explain/hash
    // edges instead of being stored in fluent order shells.
    const fn new(expr: Expr) -> Self {
        Self { expr }
    }

    // Lower one typed fluent order expression into the planner-owned order
    // contract now that ordering is expression-based end to end.
    pub(in crate::db) fn lower(&self, direction: OrderDirection) -> PlannedOrderTerm {
        PlannedOrderTerm::new(self.expr.clone(), direction)
    }
}

impl From<&str> for OrderExpr {
    fn from(value: &str) -> Self {
        Self::field(value)
    }
}

impl From<String> for OrderExpr {
    fn from(value: String) -> Self {
        Self::field(value)
    }
}

impl From<FieldRef> for OrderExpr {
    fn from(value: FieldRef) -> Self {
        Self::field(value.as_str())
    }
}

impl From<TextProjectionExpr> for OrderExpr {
    fn from(value: TextProjectionExpr) -> Self {
        Self::new(value.expr().clone())
    }
}

impl From<NumericProjectionExpr> for OrderExpr {
    fn from(value: NumericProjectionExpr) -> Self {
        Self::new(value.expr().clone())
    }
}

impl From<RoundProjectionExpr> for OrderExpr {
    fn from(value: RoundProjectionExpr) -> Self {
        Self::new(value.expr().clone())
    }
}

impl From<AggregateExpr> for OrderExpr {
    fn from(value: AggregateExpr) -> Self {
        Self::new(Expr::Aggregate(value))
    }
}

///
/// OrderTerm
///
/// Typed fluent ORDER BY term.
/// Carries one typed ORDER BY expression plus direction so fluent builders can
/// express deterministic ordering directly at the query boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderTerm {
    expr: OrderExpr,
    direction: OrderDirection,
}

impl OrderTerm {
    /// Build one ascending ORDER BY term from one typed expression.
    #[must_use]
    pub fn asc(expr: impl Into<OrderExpr>) -> Self {
        Self {
            expr: expr.into(),
            direction: OrderDirection::Asc,
        }
    }

    /// Build one descending ORDER BY term from one typed expression.
    #[must_use]
    pub fn desc(expr: impl Into<OrderExpr>) -> Self {
        Self {
            expr: expr.into(),
            direction: OrderDirection::Desc,
        }
    }

    // Lower one typed fluent order term directly into the planner-owned
    // `OrderTerm` contract.
    pub(in crate::db) fn lower(&self) -> PlannedOrderTerm {
        self.expr.lower(self.direction)
    }
}

/// Build one typed direct-field ORDER BY expression.
#[must_use]
pub fn field(field: impl Into<String>) -> OrderExpr {
    OrderExpr::field(field)
}

/// Build one ascending typed ORDER BY term.
#[must_use]
pub fn asc(expr: impl Into<OrderExpr>) -> OrderTerm {
    OrderTerm::asc(expr)
}

/// Build one descending typed ORDER BY term.
#[must_use]
pub fn desc(expr: impl Into<OrderExpr>) -> OrderTerm {
    OrderTerm::desc(expr)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::FilterExpr;
    use candid::types::{CandidType, Label, Type, TypeInner};

    fn expect_record_fields(ty: Type) -> Vec<String> {
        match ty.as_ref() {
            TypeInner::Record(fields) => fields
                .iter()
                .map(|field| match field.id.as_ref() {
                    Label::Named(name) => name.clone(),
                    other => panic!("expected named record field, got {other:?}"),
                })
                .collect(),
            other => panic!("expected candid record, got {other:?}"),
        }
    }

    fn expect_variant_labels(ty: Type) -> Vec<String> {
        match ty.as_ref() {
            TypeInner::Variant(fields) => fields
                .iter()
                .map(|field| match field.id.as_ref() {
                    Label::Named(name) => name.clone(),
                    other => panic!("expected named variant label, got {other:?}"),
                })
                .collect(),
            other => panic!("expected candid variant, got {other:?}"),
        }
    }

    fn expect_variant_field_type(ty: Type, variant_name: &str) -> Type {
        match ty.as_ref() {
            TypeInner::Variant(fields) => fields
                .iter()
                .find_map(|field| match field.id.as_ref() {
                    Label::Named(name) if name == variant_name => Some(field.ty.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("expected variant label `{variant_name}`")),
            other => panic!("expected candid variant, got {other:?}"),
        }
    }

    #[test]
    fn filter_expr_eq_candid_payload_shape_is_stable() {
        let fields = expect_record_fields(expect_variant_field_type(FilterExpr::ty(), "Eq"));

        for field in ["field", "value"] {
            assert!(
                fields.iter().any(|candidate| candidate == field),
                "Eq payload must keep `{field}` field key in Candid shape",
            );
        }
    }

    #[test]
    fn filter_expr_and_candid_payload_shape_is_stable() {
        match expect_variant_field_type(FilterExpr::ty(), "And").as_ref() {
            TypeInner::Vec(_) => {}
            other => panic!("And payload must remain a Candid vec payload, got {other:?}"),
        }
    }

    #[test]
    fn filter_expr_text_contains_ci_candid_payload_shape_is_stable() {
        let fields = expect_record_fields(expect_variant_field_type(
            FilterExpr::ty(),
            "TextContainsCi",
        ));

        for field in ["field", "value"] {
            assert!(
                fields.iter().any(|candidate| candidate == field),
                "TextContainsCi payload must keep `{field}` field key in Candid shape",
            );
        }
    }

    #[test]
    fn filter_expr_not_payload_shape_is_stable() {
        match expect_variant_field_type(FilterExpr::ty(), "Not").as_ref() {
            TypeInner::Var(_) | TypeInner::Knot(_) | TypeInner::Variant(_) => {}
            other => panic!("Not payload must keep nested predicate payload, got {other:?}"),
        }
    }

    #[test]
    fn filter_expr_variant_labels_are_stable() {
        let labels = expect_variant_labels(FilterExpr::ty());

        for label in ["Eq", "And", "Not", "TextContainsCi", "IsMissing"] {
            assert!(
                labels.iter().any(|candidate| candidate == label),
                "FilterExpr must keep `{label}` variant label",
            );
        }
    }

    #[test]
    fn query_expr_fixture_constructors_stay_usable() {
        let expr = FilterExpr::and(vec![
            FilterExpr::is_null("deleted_at"),
            FilterExpr::not(FilterExpr::is_missing("name")),
        ]);

        match expr {
            FilterExpr::And(items) => assert_eq!(items.len(), 2),
            other => panic!("expected And fixture, got {other:?}"),
        }
    }
}
