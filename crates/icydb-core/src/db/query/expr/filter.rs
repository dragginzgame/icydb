//! Module: db::query::expr::filter
//! Responsibility: frontend-safe filter expression DTOs and planner lowering.
//! Does not own: query route planning or executor predicate evaluation.
//! Boundary: converts serialized filter input into planner-owned boolean expressions.

use crate::{
    db::{
        codec::hex::encode_hex_lower,
        query::plan::expr::{BinaryOp, Expr, FieldId, Function, UnaryOp},
        schema::SchemaInfo,
    },
    value::{InputValue, PublicValue, Value},
};
use candid::CandidType;
use serde::Deserialize;

/// Serialized frontend-safe filter literal payload.
///
/// This keeps the public filter wire surface narrow and string-backed while
/// the intent boundary still rehydrates typed runtime values from schema.

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum FilterValue {
    String(String),
    Bool(bool),
    Null,
    List(Vec<Self>),
}

impl FilterValue {
    /// Lower one public wire literal back onto the runtime value model before
    /// adjacent schema-aware callers optionally canonicalize it to the target
    /// field kind.
    fn lower_value(&self) -> Value {
        match self {
            Self::String(value) => Value::Text(value.clone()),
            Self::Bool(value) => Value::Bool(*value),
            Self::Null => Value::Null,
            Self::List(values) => Value::List(values.iter().map(Self::lower_value).collect()),
        }
    }

    fn from_input_value(value: InputValue) -> Self {
        fn from_public_value(value: PublicValue) -> FilterValue {
            match value {
                PublicValue::Bool(value) => FilterValue::Bool(value),
                PublicValue::List(values) => {
                    FilterValue::List(values.into_iter().map(from_public_value).collect())
                }
                PublicValue::Null | PublicValue::Unit => FilterValue::Null,
                PublicValue::Text(value) => FilterValue::String(value),
                PublicValue::Enum(value) => FilterValue::String(value.variant().to_string()),
                PublicValue::Account(value) => FilterValue::String(value.to_string()),
                PublicValue::Blob(value) => FilterValue::String(encode_hex_lower(value.as_slice())),
                PublicValue::Date(value) => FilterValue::String(value.to_string()),
                PublicValue::Decimal(value) => FilterValue::String(value.to_string()),
                PublicValue::Duration(value) => FilterValue::String(value.as_millis().to_string()),
                PublicValue::Float32(value) => FilterValue::String(value.to_string()),
                PublicValue::Float64(value) => FilterValue::String(value.to_string()),
                PublicValue::Int64(value) => FilterValue::String(value.to_string()),
                PublicValue::Int128(value) => FilterValue::String(value.to_string()),
                PublicValue::IntBig(value) => FilterValue::String(value.to_string()),
                PublicValue::Map(value) => FilterValue::String(format!("{value:?}")),
                PublicValue::Principal(value) => FilterValue::String(value.to_string()),
                PublicValue::Subaccount(value) => FilterValue::String(value.to_string()),
                PublicValue::Timestamp(value) => FilterValue::String(value.to_string()),
                PublicValue::Nat64(value) => FilterValue::String(value.to_string()),
                PublicValue::Nat128(value) => FilterValue::String(value.to_string()),
                PublicValue::NatBig(value) => FilterValue::String(value.to_string()),
                PublicValue::Ulid(value) => FilterValue::String(value.to_string()),
                PublicValue::U256(value) => FilterValue::String(value.to_string()),
            }
        }

        from_public_value(value.into_public())
    }
}

impl<T> From<T> for FilterValue
where
    T: Into<InputValue>,
{
    fn from(value: T) -> Self {
        Self::from_input_value(value.into())
    }
}

/// Boolean junction applied to a group of filter expressions.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum JunctionOperator {
    /// Every child expression must match.
    And,
    /// At least one child expression must match.
    Or,
}

impl JunctionOperator {
    const fn binary_op(self) -> BinaryOp {
        match self {
            Self::And => BinaryOp::And,
            Self::Or => BinaryOp::Or,
        }
    }
}

/// Comparison between one field and one literal value.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum CompareOperator {
    /// Exact equality.
    Eq,
    /// Case-insensitive text equality.
    EqCi,
    /// Inequality.
    Ne,
    /// Strictly less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// Strictly greater than.
    Gt,
    /// Greater than or equal.
    Gte,
}

impl CompareOperator {
    const fn binary_op(self) -> BinaryOp {
        match self {
            Self::Eq | Self::EqCi => BinaryOp::Eq,
            Self::Ne => BinaryOp::Ne,
            Self::Lt => BinaryOp::Lt,
            Self::Lte => BinaryOp::Lte,
            Self::Gt => BinaryOp::Gt,
            Self::Gte => BinaryOp::Gte,
        }
    }
}

/// Comparison between two fields.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum FieldCompareOperator {
    /// Exact equality.
    Eq,
    /// Inequality.
    Ne,
    /// Strictly less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// Strictly greater than.
    Gt,
    /// Greater than or equal.
    Gte,
}

impl FieldCompareOperator {
    const fn binary_op(self) -> BinaryOp {
        match self {
            Self::Eq => BinaryOp::Eq,
            Self::Ne => BinaryOp::Ne,
            Self::Lt => BinaryOp::Lt,
            Self::Lte => BinaryOp::Lte,
            Self::Gt => BinaryOp::Gt,
            Self::Gte => BinaryOp::Gte,
        }
    }
}

/// Membership operation applied to one field and a set of literal values.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SetOperator {
    /// Match any listed value.
    In,
    /// Reject every listed value.
    NotIn,
}

impl SetOperator {
    const fn is_negated(self) -> bool {
        matches!(self, Self::NotIn)
    }
}

/// Collection or text-matching operation applied to one field and literal.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum CollectionOperator {
    /// Match a collection containing the literal value.
    Contains,
    /// Match a case-sensitive text substring.
    TextContains,
    /// Match a case-insensitive text substring.
    TextContainsCi,
    /// Match a case-sensitive text prefix.
    StartsWith,
    /// Match a case-insensitive text prefix.
    StartsWithCi,
    /// Match a case-sensitive text suffix.
    EndsWith,
    /// Match a case-insensitive text suffix.
    EndsWithCi,
}

/// Presence or emptiness operation applied to one field.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum StateOperator {
    /// Match a present null value.
    IsNull,
    /// Match a present non-null value.
    IsNotNull,
    /// Match an absent field.
    IsMissing,
    /// Match a present empty value.
    IsEmpty,
    /// Match a present non-empty value.
    IsNotEmpty,
}

impl StateOperator {
    const fn function(self) -> Function {
        match self {
            Self::IsNull => Function::IsNull,
            Self::IsNotNull => Function::IsNotNull,
            Self::IsMissing => Function::IsMissing,
            Self::IsEmpty => Function::IsEmpty,
            Self::IsNotEmpty => Function::IsNotEmpty,
        }
    }
}

/// Serialized, planner-agnostic filter language.
///
/// This is the shared frontend-facing filter input model for fluent callers
/// and lowers onto planner-owned boolean expressions at the intent boundary.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum FilterExpr {
    /// A constant boolean predicate.
    Constant(bool),
    /// A conjunction or disjunction of child predicates.
    Junction {
        /// Boolean operator joining the children.
        operator: JunctionOperator,
        /// Child predicates in caller-provided order.
        filters: Vec<Self>,
    },
    /// Negation of one child predicate.
    Not(Box<Self>),
    /// Comparison between one field and one literal value.
    Compare {
        /// Comparison operation.
        operator: CompareOperator,
        /// Field name.
        field: String,
        /// Literal value.
        value: FilterValue,
    },
    /// Comparison between two fields.
    CompareFields {
        /// Comparison operation.
        operator: FieldCompareOperator,
        /// Left-hand field name.
        left_field: String,
        /// Right-hand field name.
        right_field: String,
    },
    /// Set-membership comparison.
    Set {
        /// Membership operation.
        operator: SetOperator,
        /// Field name.
        field: String,
        /// Candidate literal values.
        values: Vec<FilterValue>,
    },
    /// Collection containment or text matching.
    Collection {
        /// Collection or text operation.
        operator: CollectionOperator,
        /// Field name.
        field: String,
        /// Literal operand.
        value: FilterValue,
    },
    /// Presence or emptiness predicate.
    State {
        /// State operation.
        operator: StateOperator,
        /// Field name.
        field: String,
    },
}

impl FilterExpr {
    /// Lower this dynamic filter expression against accepted schema authority.
    #[must_use]
    pub(in crate::db::query) fn lower_bool_expr_for_schema(&self, schema: &SchemaInfo) -> Expr {
        self.lower_bool_expr_with_schema(schema)
    }

    fn lower_bool_expr_with_schema(&self, schema: &SchemaInfo) -> Expr {
        match self {
            Self::Constant(value) => Expr::Literal(Value::Bool(*value)),
            Self::Junction { operator, filters } => {
                fold_filter_bool_chain(operator.binary_op(), filters, schema)
            }
            Self::Not(filter) => Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(filter.lower_bool_expr_with_schema(schema)),
            },
            Self::Compare {
                operator,
                field,
                value,
            } => lower_field_value_compare(*operator, schema, field, value),
            Self::CompareFields {
                operator,
                left_field,
                right_field,
            } => field_compare_field_expr(operator.binary_op(), left_field, right_field),
            Self::Set {
                operator,
                field,
                values,
            } => membership_expr(
                field,
                lower_membership(schema, field, values).as_slice(),
                operator.is_negated(),
            ),
            Self::Collection {
                operator,
                field,
                value,
            } => lower_collection_compare(*operator, schema, field, value),
            Self::State { operator, field } => field_function_expr(operator.function(), field),
        }
    }

    /// Build an `And` expression from a list of child expressions.
    #[must_use]
    pub const fn and(exprs: Vec<Self>) -> Self {
        Self::Junction {
            operator: JunctionOperator::And,
            filters: exprs,
        }
    }

    /// Build an `Or` expression from a list of child expressions.
    #[must_use]
    pub const fn or(exprs: Vec<Self>) -> Self {
        Self::Junction {
            operator: JunctionOperator::Or,
            filters: exprs,
        }
    }

    /// Negate one child expression.
    #[must_use]
    #[expect(clippy::should_implement_trait)]
    pub fn not(expr: Self) -> Self {
        Self::Not(Box::new(expr))
    }

    /// Compare `field == value`.
    #[must_use]
    pub fn eq(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Eq,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field != value`.
    #[must_use]
    pub fn ne(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Ne,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field < value`.
    #[must_use]
    pub fn lt(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Lt,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field <= value`.
    #[must_use]
    pub fn lte(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Lte,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field > value`.
    #[must_use]
    pub fn gt(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Gt,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field >= value`.
    #[must_use]
    pub fn gte(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Gte,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field == value` with casefolded text equality.
    #[must_use]
    pub fn eq_ci(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::EqCi,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `left_field == right_field`.
    #[must_use]
    pub fn eq_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::CompareFields {
            operator: FieldCompareOperator::Eq,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field != right_field`.
    #[must_use]
    pub fn ne_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::CompareFields {
            operator: FieldCompareOperator::Ne,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field < right_field`.
    #[must_use]
    pub fn lt_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::CompareFields {
            operator: FieldCompareOperator::Lt,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field <= right_field`.
    #[must_use]
    pub fn lte_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::CompareFields {
            operator: FieldCompareOperator::Lte,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field > right_field`.
    #[must_use]
    pub fn gt_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::CompareFields {
            operator: FieldCompareOperator::Gt,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `left_field >= right_field`.
    #[must_use]
    pub fn gte_field(left_field: impl Into<String>, right_field: impl Into<String>) -> Self {
        Self::CompareFields {
            operator: FieldCompareOperator::Gte,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }

    /// Compare `field IN values`.
    #[must_use]
    pub fn in_list(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<FilterValue>>,
    ) -> Self {
        Self::Set {
            operator: SetOperator::In,
            field: field.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    /// Compare `field NOT IN values`.
    #[must_use]
    pub fn not_in(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<FilterValue>>,
    ) -> Self {
        Self::Set {
            operator: SetOperator::NotIn,
            field: field.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    /// Compare collection `field CONTAINS value`.
    #[must_use]
    pub fn contains(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::Contains,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive substring containment.
    #[must_use]
    pub fn text_contains(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::TextContains,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive substring containment.
    #[must_use]
    pub fn text_contains_ci(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::TextContainsCi,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive prefix match.
    #[must_use]
    pub fn starts_with(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::StartsWith,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive prefix match.
    #[must_use]
    pub fn starts_with_ci(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::StartsWithCi,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive suffix match.
    #[must_use]
    pub fn ends_with(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::EndsWith,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive suffix match.
    #[must_use]
    pub fn ends_with_ci(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::EndsWithCi,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Match rows where `field` is present and null.
    #[must_use]
    pub fn is_null(field: impl Into<String>) -> Self {
        Self::State {
            operator: StateOperator::IsNull,
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and non-null.
    #[must_use]
    pub fn is_not_null(field: impl Into<String>) -> Self {
        Self::State {
            operator: StateOperator::IsNotNull,
            field: field.into(),
        }
    }

    /// Match rows where `field` is absent.
    #[must_use]
    pub fn is_missing(field: impl Into<String>) -> Self {
        Self::State {
            operator: StateOperator::IsMissing,
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and empty.
    #[must_use]
    pub fn is_empty(field: impl Into<String>) -> Self {
        Self::State {
            operator: StateOperator::IsEmpty,
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and non-empty.
    #[must_use]
    pub fn is_not_empty(field: impl Into<String>) -> Self {
        Self::State {
            operator: StateOperator::IsNotEmpty,
            field: field.into(),
        }
    }
}

fn lower_field_value_compare(
    operator: CompareOperator,
    schema: &SchemaInfo,
    field: &str,
    value: &FilterValue,
) -> Expr {
    if operator == CompareOperator::EqCi {
        return Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(casefold_field_expr(field)),
            right: Box::new(Expr::Literal(value.lower_value())),
        };
    }

    field_compare_expr(
        operator.binary_op(),
        field,
        lower_compare(schema, field, value),
    )
}

fn lower_collection_compare(
    operator: CollectionOperator,
    schema: &SchemaInfo,
    field: &str,
    value: &FilterValue,
) -> Expr {
    match operator {
        CollectionOperator::Contains => Expr::FunctionCall {
            function: Function::CollectionContains,
            args: vec![
                Expr::Field(FieldId::new(field.to_string())),
                Expr::Literal(lower_collection_element(schema, field, value)),
            ],
        },
        CollectionOperator::TextContains => text_function_expr(
            Function::Contains,
            Expr::Field(FieldId::new(field.to_string())),
            value.lower_value(),
        ),
        CollectionOperator::TextContainsCi => text_function_expr(
            Function::Contains,
            casefold_field_expr(field),
            value.lower_value(),
        ),
        CollectionOperator::StartsWith => text_function_expr(
            Function::StartsWith,
            Expr::Field(FieldId::new(field.to_string())),
            value.lower_value(),
        ),
        CollectionOperator::StartsWithCi => text_function_expr(
            Function::StartsWith,
            casefold_field_expr(field),
            value.lower_value(),
        ),
        CollectionOperator::EndsWith => text_function_expr(
            Function::EndsWith,
            Expr::Field(FieldId::new(field.to_string())),
            value.lower_value(),
        ),
        CollectionOperator::EndsWithCi => text_function_expr(
            Function::EndsWith,
            casefold_field_expr(field),
            value.lower_value(),
        ),
    }
}

fn lower_compare(schema: &SchemaInfo, field: &str, value: &FilterValue) -> Value {
    let raw = value.lower_value();
    schema
        .canonicalize_filter_literal(field, &raw)
        .unwrap_or(raw)
}

fn lower_membership(schema: &SchemaInfo, field: &str, values: &[FilterValue]) -> Vec<Value> {
    values
        .iter()
        .map(|value| lower_compare(schema, field, value))
        .collect()
}

fn lower_collection_element(schema: &SchemaInfo, field: &str, value: &FilterValue) -> Value {
    let raw = value.lower_value();
    schema
        .canonicalize_filter_collection_element(field, &raw)
        .unwrap_or(raw)
}

fn fold_filter_bool_chain(op: BinaryOp, exprs: &[FilterExpr], schema: &SchemaInfo) -> Expr {
    let mut exprs = exprs.iter();
    let Some(first) = exprs.next() else {
        return Expr::Literal(Value::Bool(matches!(op, BinaryOp::And)));
    };

    let first = first.lower_bool_expr_with_schema(schema);

    exprs.fold(first, |left, expr| Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(expr.lower_bool_expr_with_schema(schema)),
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

#[cfg(test)]
mod tests {
    use super::{
        CollectionOperator, CompareOperator, FieldCompareOperator, FilterExpr, FilterValue,
        JunctionOperator, SetOperator, StateOperator,
    };
    use crate::db::query::plan::expr::{BinaryOp, Function};
    use crate::types::{Date, Duration, Subaccount, Timestamp};

    #[test]
    fn typed_filter_atoms_use_reversible_string_representations() {
        assert_eq!(
            FilterValue::from(vec![0x00, 0x0a, 0xff]),
            FilterValue::String("000aff".to_string()),
        );
        assert_eq!(
            FilterValue::from(Date::try_new(2026, 8, 5).expect("test date should be valid")),
            FilterValue::String("2026-08-05".to_string()),
        );
        assert_eq!(
            FilterValue::from(Duration::from_millis(12_345)),
            FilterValue::String("12345".to_string()),
        );
        assert_eq!(
            FilterValue::from(Subaccount::from_array([0xab; 32])),
            FilterValue::String("ab".repeat(32)),
        );
        assert_eq!(
            FilterValue::from(Timestamp::from_millis(-42)),
            FilterValue::String("-42".to_string()),
        );
    }

    #[test]
    fn grouped_filter_candid_round_trips_every_supported_operation() {
        let value = FilterValue::String("value".to_string());
        let comparisons = [
            CompareOperator::Eq,
            CompareOperator::EqCi,
            CompareOperator::Ne,
            CompareOperator::Lt,
            CompareOperator::Lte,
            CompareOperator::Gt,
            CompareOperator::Gte,
        ]
        .map(|operator| FilterExpr::Compare {
            operator,
            field: "left".to_string(),
            value: value.clone(),
        });
        let field_comparisons = [
            FieldCompareOperator::Eq,
            FieldCompareOperator::Ne,
            FieldCompareOperator::Lt,
            FieldCompareOperator::Lte,
            FieldCompareOperator::Gt,
            FieldCompareOperator::Gte,
        ]
        .map(|operator| FilterExpr::CompareFields {
            operator,
            left_field: "left".to_string(),
            right_field: "right".to_string(),
        });
        let sets = [SetOperator::In, SetOperator::NotIn].map(|operator| FilterExpr::Set {
            operator,
            field: "set".to_string(),
            values: vec![value.clone()],
        });
        let collections = [
            CollectionOperator::Contains,
            CollectionOperator::TextContains,
            CollectionOperator::TextContainsCi,
            CollectionOperator::StartsWith,
            CollectionOperator::StartsWithCi,
            CollectionOperator::EndsWith,
            CollectionOperator::EndsWithCi,
        ]
        .map(|operator| FilterExpr::Collection {
            operator,
            field: "collection".to_string(),
            value: value.clone(),
        });
        let states = [
            StateOperator::IsNull,
            StateOperator::IsNotNull,
            StateOperator::IsMissing,
            StateOperator::IsEmpty,
            StateOperator::IsNotEmpty,
        ]
        .map(|operator| FilterExpr::State {
            operator,
            field: "state".to_string(),
        });
        let mut filters = vec![
            FilterExpr::Constant(true),
            FilterExpr::Constant(false),
            FilterExpr::Junction {
                operator: JunctionOperator::And,
                filters: Vec::new(),
            },
            FilterExpr::Junction {
                operator: JunctionOperator::Or,
                filters: Vec::new(),
            },
            FilterExpr::Not(Box::new(FilterExpr::Constant(true))),
        ];
        filters.extend(comparisons);
        filters.extend(field_comparisons);
        filters.extend(sets);
        filters.extend(collections);
        filters.extend(states);

        for filter in filters {
            let encoded = candid::encode_one(&filter).expect("filter should encode");
            let decoded = candid::decode_one::<FilterExpr>(&encoded).expect("filter should decode");
            assert_eq!(decoded, filter);
        }
    }

    #[test]
    fn constructors_build_the_grouped_filter_families_directly() {
        assert!(matches!(
            FilterExpr::and(vec![FilterExpr::Constant(true)]),
            FilterExpr::Junction {
                operator: JunctionOperator::And,
                ..
            }
        ));
        assert!(matches!(
            FilterExpr::eq("field", 1_u64),
            FilterExpr::Compare {
                operator: CompareOperator::Eq,
                ..
            }
        ));
        assert!(matches!(
            FilterExpr::eq_field("left", "right"),
            FilterExpr::CompareFields {
                operator: FieldCompareOperator::Eq,
                ..
            }
        ));
        assert!(matches!(
            FilterExpr::not_in("field", [1_u64]),
            FilterExpr::Set {
                operator: SetOperator::NotIn,
                ..
            }
        ));
        assert!(matches!(
            FilterExpr::contains("field", 1_u64),
            FilterExpr::Collection {
                operator: CollectionOperator::Contains,
                ..
            }
        ));
        assert!(matches!(
            FilterExpr::is_missing("field"),
            FilterExpr::State {
                operator: StateOperator::IsMissing,
                ..
            }
        ));
    }

    #[test]
    fn grouped_operator_lowering_retains_the_existing_planner_operations() {
        assert_eq!(JunctionOperator::And.binary_op(), BinaryOp::And);
        assert_eq!(JunctionOperator::Or.binary_op(), BinaryOp::Or);
        assert_eq!(CompareOperator::Eq.binary_op(), BinaryOp::Eq);
        assert_eq!(CompareOperator::EqCi.binary_op(), BinaryOp::Eq);
        assert_eq!(CompareOperator::Ne.binary_op(), BinaryOp::Ne);
        assert_eq!(CompareOperator::Lt.binary_op(), BinaryOp::Lt);
        assert_eq!(CompareOperator::Lte.binary_op(), BinaryOp::Lte);
        assert_eq!(CompareOperator::Gt.binary_op(), BinaryOp::Gt);
        assert_eq!(CompareOperator::Gte.binary_op(), BinaryOp::Gte);
        assert_eq!(FieldCompareOperator::Eq.binary_op(), BinaryOp::Eq);
        assert_eq!(FieldCompareOperator::Ne.binary_op(), BinaryOp::Ne);
        assert_eq!(FieldCompareOperator::Lt.binary_op(), BinaryOp::Lt);
        assert_eq!(FieldCompareOperator::Lte.binary_op(), BinaryOp::Lte);
        assert_eq!(FieldCompareOperator::Gt.binary_op(), BinaryOp::Gt);
        assert_eq!(FieldCompareOperator::Gte.binary_op(), BinaryOp::Gte);
        assert!(!SetOperator::In.is_negated());
        assert!(SetOperator::NotIn.is_negated());
        assert_eq!(StateOperator::IsNull.function(), Function::IsNull);
        assert_eq!(StateOperator::IsNotNull.function(), Function::IsNotNull);
        assert_eq!(StateOperator::IsMissing.function(), Function::IsMissing);
        assert_eq!(StateOperator::IsEmpty.function(), Function::IsEmpty);
        assert_eq!(StateOperator::IsNotEmpty.function(), Function::IsNotEmpty);
    }
}
