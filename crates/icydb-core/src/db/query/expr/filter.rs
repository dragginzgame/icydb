//! Module: query::expr::filter
//! Responsibility: frontend-safe filter expression DTOs and planner lowering.
//! Does not own: query route planning or executor predicate evaluation.
//! Boundary: converts serialized filter input into planner-owned boolean expressions.

use crate::{
    db::query::plan::{
        canonicalize_filter_literal_for_kind,
        expr::{BinaryOp, Expr, FieldId, Function, UnaryOp},
    },
    model::EntityModel,
    value::{InputValue, Value},
};
use candid::CandidType;
use serde::Deserialize;

// FilterValue
//
// Serialized frontend-safe filter literal payload.
// This keeps the public filter wire surface narrow and string-backed while
// the intent boundary still rehydrates typed runtime values from schema.

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum FilterValue {
    String(String),
    Bool(bool),
    Null,
    List(Vec<Self>),
}

impl FilterValue {
    // Convert one typed runtime value onto the narrowed public filter wire
    // contract. Non-bool scalar values travel as canonical strings so the
    // schema-aware intent boundary can rehydrate the exact field kind later.
    fn from_typed_value(value: Value) -> Self {
        match value {
            Value::Bool(value) => Self::Bool(value),
            Value::List(values) => {
                Self::List(values.into_iter().map(Self::from_typed_value).collect())
            }
            Value::Null | Value::Unit => Self::Null,
            Value::Text(value) => Self::String(value),
            Value::Enum(value) => Self::String(value.variant().to_string()),
            Value::Account(value) => Self::String(value.to_string()),
            Value::Blob(value) => Self::String(format!("{value:?}")),
            Value::Date(value) => Self::String(value.to_string()),
            Value::Decimal(value) => Self::String(value.to_string()),
            Value::Duration(value) => Self::String(format!("{value:?}")),
            Value::Float32(value) => Self::String(value.to_string()),
            Value::Float64(value) => Self::String(value.to_string()),
            Value::Int(value) => Self::String(value.to_string()),
            Value::Int128(value) => Self::String(value.to_string()),
            Value::IntBig(value) => Self::String(value.to_string()),
            Value::Map(value) => Self::String(format!("{value:?}")),
            Value::Principal(value) => Self::String(value.to_string()),
            Value::Subaccount(value) => Self::String(value.to_string()),
            Value::Timestamp(value) => Self::String(value.to_string()),
            Value::Uint(value) => Self::String(value.to_string()),
            Value::Uint128(value) => Self::String(value.to_string()),
            Value::UintBig(value) => Self::String(value.to_string()),
            Value::Ulid(value) => Self::String(value.to_string()),
        }
    }

    // Lower one public wire literal back onto the runtime value model before
    // adjacent schema-aware callers optionally canonicalize it to the target field kind.
    fn lower_value(&self) -> Value {
        match self {
            Self::String(value) => Value::Text(value.clone()),
            Self::Bool(value) => Value::Bool(*value),
            Self::Null => Value::Null,
            Self::List(values) => Value::List(values.iter().map(Self::lower_value).collect()),
        }
    }

    fn from_input_value(value: InputValue) -> Self {
        Self::from_typed_value(Value::from(value))
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

// FilterExpr
//
// Serialized, planner-agnostic filter language.
// This is the shared frontend-facing filter input model for fluent callers
// and lowers onto planner-owned boolean expressions at the intent boundary.

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum FilterExpr {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Eq {
        field: String,
        value: FilterValue,
    },
    EqCi {
        field: String,
        value: FilterValue,
    },
    Ne {
        field: String,
        value: FilterValue,
    },
    Lt {
        field: String,
        value: FilterValue,
    },
    Lte {
        field: String,
        value: FilterValue,
    },
    Gt {
        field: String,
        value: FilterValue,
    },
    Gte {
        field: String,
        value: FilterValue,
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
        values: Vec<FilterValue>,
    },
    NotIn {
        field: String,
        values: Vec<FilterValue>,
    },
    Contains {
        field: String,
        value: FilterValue,
    },
    TextContains {
        field: String,
        value: FilterValue,
    },
    TextContainsCi {
        field: String,
        value: FilterValue,
    },
    StartsWith {
        field: String,
        value: FilterValue,
    },
    StartsWithCi {
        field: String,
        value: FilterValue,
    },
    EndsWith {
        field: String,
        value: FilterValue,
    },
    EndsWithCi {
        field: String,
        value: FilterValue,
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
    #[expect(clippy::too_many_lines)]
    pub(in crate::db) fn lower_bool_expr_for_model(&self, model: &EntityModel) -> Expr {
        match self {
            Self::True => Expr::Literal(Value::Bool(true)),
            Self::False => Expr::Literal(Value::Bool(false)),
            Self::And(xs) => fold_filter_bool_chain(BinaryOp::And, xs, model),
            Self::Or(xs) => fold_filter_bool_chain(BinaryOp::Or, xs, model),
            Self::Not(x) => Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(x.lower_bool_expr_for_model(model)),
            },
            Self::Eq { field, value } => field_compare_expr(
                BinaryOp::Eq,
                field,
                lower_compare_filter_value_for_field(model, field, value),
            ),
            Self::EqCi { field, value } => Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(casefold_field_expr(field)),
                right: Box::new(Expr::Literal(value.lower_value())),
            },
            Self::Ne { field, value } => field_compare_expr(
                BinaryOp::Ne,
                field,
                lower_compare_filter_value_for_field(model, field, value),
            ),
            Self::Lt { field, value } => field_compare_expr(
                BinaryOp::Lt,
                field,
                lower_compare_filter_value_for_field(model, field, value),
            ),
            Self::Lte { field, value } => field_compare_expr(
                BinaryOp::Lte,
                field,
                lower_compare_filter_value_for_field(model, field, value),
            ),
            Self::Gt { field, value } => field_compare_expr(
                BinaryOp::Gt,
                field,
                lower_compare_filter_value_for_field(model, field, value),
            ),
            Self::Gte { field, value } => field_compare_expr(
                BinaryOp::Gte,
                field,
                lower_compare_filter_value_for_field(model, field, value),
            ),
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
            Self::In { field, values } => membership_expr(
                field,
                lower_membership_filter_values_for_field(model, field, values).as_slice(),
                false,
            ),
            Self::NotIn { field, values } => membership_expr(
                field,
                lower_membership_filter_values_for_field(model, field, values).as_slice(),
                true,
            ),
            Self::Contains { field, value } => Expr::FunctionCall {
                function: Function::CollectionContains,
                args: vec![
                    Expr::Field(FieldId::new(field.clone())),
                    Expr::Literal(lower_contains_filter_value_for_field(model, field, value)),
                ],
            },
            Self::TextContains { field, value } => text_function_expr(
                Function::Contains,
                Expr::Field(FieldId::new(field.clone())),
                value.lower_value(),
            ),
            Self::TextContainsCi { field, value } => text_function_expr(
                Function::Contains,
                casefold_field_expr(field),
                value.lower_value(),
            ),
            Self::StartsWith { field, value } => text_function_expr(
                Function::StartsWith,
                Expr::Field(FieldId::new(field.clone())),
                value.lower_value(),
            ),
            Self::StartsWithCi { field, value } => text_function_expr(
                Function::StartsWith,
                casefold_field_expr(field),
                value.lower_value(),
            ),
            Self::EndsWith { field, value } => text_function_expr(
                Function::EndsWith,
                Expr::Field(FieldId::new(field.clone())),
                value.lower_value(),
            ),
            Self::EndsWithCi { field, value } => text_function_expr(
                Function::EndsWith,
                casefold_field_expr(field),
                value.lower_value(),
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
    pub fn eq(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field != value`.
    #[must_use]
    pub fn ne(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Ne {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field < value`.
    #[must_use]
    pub fn lt(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Lt {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field <= value`.
    #[must_use]
    pub fn lte(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Lte {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field > value`.
    #[must_use]
    pub fn gt(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Gt {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field >= value`.
    #[must_use]
    pub fn gte(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Gte {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field == value` with casefolded text equality.
    #[must_use]
    pub fn eq_ci(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::EqCi {
            field: field.into(),
            value: value.into(),
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
        values: impl IntoIterator<Item = impl Into<FilterValue>>,
    ) -> Self {
        Self::In {
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
        Self::NotIn {
            field: field.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    /// Compare collection `field CONTAINS value`.
    #[must_use]
    pub fn contains(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::Contains {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive substring containment.
    #[must_use]
    pub fn text_contains(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::TextContains {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive substring containment.
    #[must_use]
    pub fn text_contains_ci(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::TextContainsCi {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive prefix match.
    #[must_use]
    pub fn starts_with(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::StartsWith {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive prefix match.
    #[must_use]
    pub fn starts_with_ci(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::StartsWithCi {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive suffix match.
    #[must_use]
    pub fn ends_with(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::EndsWith {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive suffix match.
    #[must_use]
    pub fn ends_with_ci(field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        Self::EndsWithCi {
            field: field.into(),
            value: value.into(),
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

fn fold_filter_bool_chain(op: BinaryOp, exprs: &[FilterExpr], model: &EntityModel) -> Expr {
    let mut exprs = exprs.iter();
    let Some(first) = exprs.next() else {
        return Expr::Literal(Value::Bool(matches!(op, BinaryOp::And)));
    };

    let first = first.lower_bool_expr_for_model(model);

    exprs.fold(first, |left, expr| Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(expr.lower_bool_expr_for_model(model)),
    })
}

fn lower_compare_filter_value_for_field(
    model: &EntityModel,
    field: &str,
    value: &FilterValue,
) -> Value {
    lower_filter_value_for_field_kind(model, field, value, false)
}

fn lower_contains_filter_value_for_field(
    model: &EntityModel,
    field: &str,
    value: &FilterValue,
) -> Value {
    lower_filter_value_for_field_kind(model, field, value, true)
}

fn lower_filter_value_for_field_kind(
    model: &EntityModel,
    field: &str,
    value: &FilterValue,
    collection_element: bool,
) -> Value {
    let raw = value.lower_value();
    let Some(field_slot) = model.resolve_field_slot(field) else {
        return raw;
    };

    let mut kind = model.fields()[field_slot].kind();
    if collection_element {
        kind = match kind {
            crate::model::field::FieldKind::List(inner)
            | crate::model::field::FieldKind::Set(inner) => *inner,
            _ => kind,
        };
    }

    canonicalize_filter_literal_for_kind(&kind, &raw).unwrap_or(raw)
}

fn lower_membership_filter_values_for_field(
    model: &EntityModel,
    field: &str,
    values: &[FilterValue],
) -> Vec<Value> {
    values
        .iter()
        .map(|value| lower_compare_filter_value_for_field(model, field, value))
        .collect()
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
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::query::{
            expr::{FilterExpr, FilterValue},
            plan::expr::{BinaryOp, Expr, FieldId},
        },
        model::{EntityModel, field::FieldKind, field::FieldModel},
        types::Ulid,
        value::Value,
    };
    use candid::types::{CandidType, Label, Type, TypeInner};

    static FILTER_TEST_FIELDS: [FieldModel; 3] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("rank", FieldKind::Uint),
        FieldModel::generated("active", FieldKind::Bool),
    ];
    static FILTER_TEST_MODEL: EntityModel = EntityModel::generated(
        "tests::FilterEntity",
        "FilterEntity",
        &FILTER_TEST_FIELDS[0],
        0,
        &FILTER_TEST_FIELDS,
        &[],
    );

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
    fn filter_value_variant_labels_are_stable() {
        let labels = expect_variant_labels(FilterValue::ty());

        for label in ["String", "Bool", "Null", "List"] {
            assert!(
                labels.iter().any(|candidate| candidate == label),
                "FilterValue must keep `{label}` variant label",
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
                "TextContainsCi payload must keep `{field}` field key",
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

    #[test]
    fn filter_expr_model_lowering_rehydrates_string_ulid_literal() {
        let ulid = Ulid::default();
        let expr =
            FilterExpr::eq("id", ulid.to_string()).lower_bool_expr_for_model(&FILTER_TEST_MODEL);

        assert_eq!(
            expr,
            Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("id".to_string()))),
                right: Box::new(Expr::Literal(Value::Ulid(ulid))),
            }
        );
    }

    #[test]
    fn filter_expr_model_lowering_rehydrates_numeric_membership_literals() {
        let expr = FilterExpr::in_list("rank", [7_u64, 9_u64])
            .lower_bool_expr_for_model(&FILTER_TEST_MODEL);

        assert_eq!(
            expr,
            Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Field(FieldId::new("rank".to_string()))),
                    right: Box::new(Expr::Literal(Value::Uint(7))),
                }),
                right: Box::new(Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Field(FieldId::new("rank".to_string()))),
                    right: Box::new(Expr::Literal(Value::Uint(9))),
                }),
            }
        );
    }
}
