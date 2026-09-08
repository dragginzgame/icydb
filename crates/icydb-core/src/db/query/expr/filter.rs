//! Module: db::query::expr::filter
//! Responsibility: frontend-safe filter expression DTOs and planner lowering.
//! Does not own: query route planning or executor predicate evaluation.
//! Boundary: converts serialized filter input into planner-owned boolean expressions.

use crate::{
    db::{
        QueryError,
        codec::hex::encode_hex_lower,
        query::plan::expr::{BinaryOp, Expr, FieldId, Function, UnaryOp},
        query::preparation::PreparationWork,
        schema::SchemaInfo,
    },
    value::{InputValue, PublicValue, Value},
};
use candid::CandidType;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use serde::{Deserialize, Deserializer, de::Error as _};
use std::borrow::Cow;

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
    fn lower_value(&self, work: &PreparationWork<'_>) -> Result<Value, QueryError> {
        work.charge(DiagnosticExecutionBudgetResource::NestedValueSteps, 1)?;
        Ok(match self {
            Self::String(value) => {
                work.charge(
                    DiagnosticExecutionBudgetResource::TemporaryBytes,
                    value.len() as u64,
                )?;
                Value::Text(value.clone())
            }
            Self::Bool(value) => Value::Bool(*value),
            Self::Null => Value::Null,
            Self::List(values) => {
                work.charge(
                    DiagnosticExecutionBudgetResource::TemporaryBytes,
                    (values.len() as u64).saturating_mul(size_of::<Value>() as u64),
                )?;
                let mut lowered = Vec::with_capacity(values.len());
                for value in values {
                    lowered.push(value.lower_value(work)?);
                }
                Value::List(lowered)
            }
        })
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
/// Supplied expressions must decode successfully even inside a Candid optional
/// argument; an invalid predicate must never become an absent filter.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(remote = "Self")]
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
    pub(in crate::db::query) fn lower_bool_expr_for_schema(
        &self,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<Expr, QueryError> {
        // One step per frontend expression actually visited, before descent.
        work.charge(
            DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
            1,
        )?;
        Ok(match self {
            Self::Constant(value) => Expr::Literal(Value::Bool(*value)),
            Self::Junction { operator, filters } => {
                fold_filter_bool_chain(operator.binary_op(), filters, schema, work)?
            }
            Self::Not(filter) => {
                let child = filter.lower_bool_expr_for_schema(schema, work)?;
                charge_expr_slots(1, work)?;
                Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(child),
                }
            }
            Self::Compare {
                operator,
                field,
                value,
            } => lower_field_value_compare(*operator, schema, field, value, work)?,
            Self::CompareFields {
                operator,
                left_field,
                right_field,
            } => field_compare_field_expr(operator.binary_op(), left_field, right_field, work)?,
            Self::Set {
                operator,
                field,
                values,
            } => membership_expr(
                field,
                lower_membership(schema, field, values, work)?,
                operator.is_negated(),
                work,
            )?,
            Self::Collection {
                operator,
                field,
                value,
            } => lower_collection_compare(*operator, schema, field, value, work)?,
            Self::State { operator, field } => {
                field_function_expr(operator.function(), field, work)?
            }
        })
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

impl<'de> Deserialize<'de> for FilterExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Keep the derived visitor on this canonical enum, but make its failures
        // terminal: Candid otherwise recovers subtype errors in opt as None.
        // The contextual error also prevents Candid from reclassifying a bare
        // subtype diagnostic as recoverable through serde::de::Error::custom.
        Self::deserialize(deserializer)
            .map_err(|error| D::Error::custom(format_args!("invalid filter expression: {error}")))
    }
}

fn lower_field_value_compare(
    operator: CompareOperator,
    schema: &SchemaInfo,
    field: &str,
    value: &FilterValue,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    if operator == CompareOperator::EqCi {
        return binary_expr(
            BinaryOp::Eq,
            field_function_expr(Function::Lower, field, work)?,
            Expr::Literal(value.lower_value(work)?),
            work,
        );
    }

    field_compare_expr(
        operator.binary_op(),
        field,
        lower_compare(schema, field, value, work)?,
        work,
    )
}

fn lower_collection_compare(
    operator: CollectionOperator,
    schema: &SchemaInfo,
    field: &str,
    value: &FilterValue,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    let (function, casefold) = match operator {
        CollectionOperator::Contains => (Function::CollectionContains, false),
        CollectionOperator::TextContains => (Function::Contains, false),
        CollectionOperator::TextContainsCi => (Function::Contains, true),
        CollectionOperator::StartsWith => (Function::StartsWith, false),
        CollectionOperator::StartsWithCi => (Function::StartsWith, true),
        CollectionOperator::EndsWith => (Function::EndsWith, false),
        CollectionOperator::EndsWithCi => (Function::EndsWith, true),
    };
    let left = if casefold {
        field_function_expr(Function::Lower, field, work)?
    } else {
        field_expr(field, work)?
    };
    let value = if operator == CollectionOperator::Contains {
        lower_collection_element(schema, field, value, work)?
    } else {
        value.lower_value(work)?
    };
    function_expr(function, [left, Expr::Literal(value)], work)
}

fn lower_compare(
    schema: &SchemaInfo,
    field: &str,
    value: &FilterValue,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    let raw = value.lower_value(work)?;
    Ok(
        match schema.canonicalize_filter_literal(field, &raw, work)? {
            Some(Cow::Owned(canonical)) => canonical,
            // Unchanged text/blob storage already belongs to this invocation.
            Some(Cow::Borrowed(_)) | None => raw,
        },
    )
}

fn lower_membership(
    schema: &SchemaInfo,
    field: &str,
    values: &[FilterValue],
    work: &PreparationWork<'_>,
) -> Result<Vec<Value>, QueryError> {
    work.charge(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        (values.len() as u64).saturating_mul(size_of::<Value>() as u64),
    )?;
    let mut lowered = Vec::with_capacity(values.len());
    for value in values {
        lowered.push(lower_compare(schema, field, value, work)?);
    }

    Ok(lowered)
}

fn lower_collection_element(
    schema: &SchemaInfo,
    field: &str,
    value: &FilterValue,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    let raw = value.lower_value(work)?;
    Ok(
        match schema.canonicalize_filter_collection_element(field, &raw, work)? {
            Some(Cow::Owned(canonical)) => canonical,
            Some(Cow::Borrowed(_)) | None => raw,
        },
    )
}

fn fold_filter_bool_chain(
    op: BinaryOp,
    exprs: &[FilterExpr],
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    // Split before lowering so flat width adds logarithmic depth, visiting every
    // leaf in source order. Canonical sorting/simplification belongs to the planner.
    Ok(match exprs {
        [] => Expr::Literal(Value::Bool(matches!(op, BinaryOp::And))),
        [expr] => expr.lower_bool_expr_for_schema(schema, work)?,
        _ => {
            let (left, right) = exprs.split_at(exprs.len() / 2);
            binary_expr(
                op,
                fold_filter_bool_chain(op, left, schema, work)?,
                fold_filter_bool_chain(op, right, schema, work)?,
                work,
            )?
        }
    })
}

// These constructors own introduced backing storage; precharge immediately
// before the copy/allocation, not via a separate expression-size estimator.
fn charge_expr_slots(slots: usize, work: &PreparationWork<'_>) -> Result<(), QueryError> {
    work.charge(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        (slots as u64).saturating_mul(size_of::<Expr>() as u64),
    )
}

fn field_expr(field: &str, work: &PreparationWork<'_>) -> Result<Expr, QueryError> {
    work.charge(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        field.len() as u64,
    )?;
    Ok(Expr::Field(FieldId::new(field)))
}

fn field_compare_expr(
    op: BinaryOp,
    field: &str,
    value: Value,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    binary_expr(op, field_expr(field, work)?, Expr::Literal(value), work)
}

fn field_compare_field_expr(
    op: BinaryOp,
    left_field: &str,
    right_field: &str,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    binary_expr(
        op,
        field_expr(left_field, work)?,
        field_expr(right_field, work)?,
        work,
    )
}

fn membership_expr(
    field: &str,
    values: Vec<Value>,
    negated: bool,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    // Typed empty sets are constants even for NULL or absent fields. Keep this
    // frontend contract before constructing the ordinary nonempty membership.
    if values.is_empty() {
        return Ok(Expr::Literal(Value::Bool(negated)));
    }

    let field = field_expr(field, work)?;
    // Shared membership owns two argument slots and an optional NOT box.
    // The value vector was already charged while lowering its elements.
    charge_expr_slots(2 + usize::from(negated), work)?;
    Ok(Expr::membership(field, values, negated))
}

fn field_function_expr(
    function: Function,
    field: &str,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    function_expr(function, [field_expr(field, work)?], work)
}

fn function_expr<const N: usize>(
    function: Function,
    args: [Expr; N],
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    charge_expr_slots(N, work)?;
    Ok(Expr::FunctionCall {
        function,
        args: Vec::from(args),
    })
}

fn binary_expr(
    op: BinaryOp,
    left: Expr,
    right: Expr,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    charge_expr_slots(2, work)?;
    Ok(Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        CollectionOperator, CompareOperator, FieldCompareOperator, FilterExpr, FilterValue,
        JunctionOperator, SetOperator, StateOperator, field_compare_expr, membership_expr,
    };
    use crate::{
        db::{
            predicate::PredicateProgram,
            query::plan::expr::{
                BinaryOp, Expr, Function, UnaryOp, derive_normalized_bool_expr_predicate_subset,
                eval_builder_expr_for_value_preview, normalize_bool_expr,
            },
            schema::{
                AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
                AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId, FieldStorageDecode,
                LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot, ScalarCodec,
                SchemaFieldSlot, SchemaInfo, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
                empty_accepted_enum_catalog_for_tests,
            },
        },
        types::{Date, Duration, Subaccount, Timestamp},
        value::Value,
    };

    fn lower_filter(filter: &FilterExpr, schema: &SchemaInfo) -> Expr {
        let root = crate::db::RequestExecutionRoot::__new_runtime_root();
        crate::db::query::preparation::PreparationWork::run(
            &root.scope(),
            icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead,
            |work| filter.lower_bool_expr_for_schema(schema, work),
        )
        .expect("fixture lowering fits the production request budget")
    }

    #[test]
    fn nested_filter_value_lowering_charges_before_descent_and_retains_failed_work() {
        use crate::db::{
            RequestExecutionRoot,
            executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            query::preparation::PreparationWork,
        };
        use icydb_diagnostic_code::{
            DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
        };

        let schema = membership_schema();
        let filter = FilterExpr::in_list(
            "id",
            [
                FilterValue::String("1".into()),
                FilterValue::List(vec![FilterValue::Bool(true)]),
            ],
        );
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::NestedValueSteps, 2),
        );
        for expected in [3, 4] {
            let error =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    filter.lower_bool_expr_for_schema(&schema, work)
                })
                .expect_err("reject the child visit before materializing it");
            assert_eq!(root.observed(Resource::NestedValueSteps), expected);
            assert!(error.diagnostic_facts().contains(&(
                icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                Resource::NestedValueSteps.raw()
            )));
        }
    }

    fn filter_construction_cases() -> Vec<(FilterExpr, u64)> {
        let slot = size_of::<Expr>() as u64;
        // NULL avoids variable raw-literal storage here. This tests construction;
        // operation/type admissibility still belongs to downstream validation.
        let mut cases = vec![
            (FilterExpr::Constant(true), 0),
            (FilterExpr::and(vec![]), 0),
            (FilterExpr::or(vec![]), 0),
            (FilterExpr::not(FilterExpr::Constant(true)), slot),
            (
                FilterExpr::and(vec![
                    FilterExpr::Constant(true),
                    FilterExpr::Constant(false),
                ]),
                2 * slot,
            ),
            (
                FilterExpr::or(vec![
                    FilterExpr::Constant(true),
                    FilterExpr::Constant(false),
                ]),
                2 * slot,
            ),
            (FilterExpr::in_list("id", Vec::<FilterValue>::new()), 0),
            (FilterExpr::not_in("id", Vec::<FilterValue>::new()), 0),
            (FilterExpr::eq("id", FilterValue::Null), 2 + 2 * slot),
            (FilterExpr::eq_ci("id", FilterValue::Null), 2 + 3 * slot),
            (FilterExpr::eq_field("id", "id"), 4 + 2 * slot),
            (
                FilterExpr::in_list("id", [FilterValue::Null]),
                2 + size_of::<Value>() as u64 + 2 * slot,
            ),
            (
                FilterExpr::not_in("id", [FilterValue::Null]),
                2 + size_of::<Value>() as u64 + 3 * slot,
            ),
        ];
        for (operator, slots) in [
            (CollectionOperator::Contains, 2),
            (CollectionOperator::TextContains, 2),
            (CollectionOperator::TextContainsCi, 3),
            (CollectionOperator::StartsWith, 2),
            (CollectionOperator::StartsWithCi, 3),
            (CollectionOperator::EndsWith, 2),
            (CollectionOperator::EndsWithCi, 3),
        ] {
            cases.push((
                FilterExpr::Collection {
                    operator,
                    field: "id".into(),
                    value: FilterValue::Null,
                },
                2 + slots * slot,
            ));
        }
        for operator in [
            StateOperator::IsNull,
            StateOperator::IsNotNull,
            StateOperator::IsMissing,
            StateOperator::IsEmpty,
            StateOperator::IsNotEmpty,
        ] {
            cases.push((
                FilterExpr::State {
                    operator,
                    field: "id".into(),
                },
                2 + slot,
            ));
        }
        cases
    }

    #[test]
    fn typed_filter_constructors_charge_names_and_introduced_storage() {
        use crate::db::{
            RequestExecutionRoot,
            executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            query::preparation::PreparationWork,
        };
        use icydb_diagnostic_code::{
            DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
            DiagnosticFactTag,
        };

        let schema = membership_schema();
        for (filter, expected) in filter_construction_cases() {
            for reject in [false, true] {
                if reject && expected == 0 {
                    continue;
                }
                let limit = expected - u64::from(reject);
                let root = RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(Resource::TemporaryBytes, limit),
                );
                let result = PreparationWork::run(
                    &root.scope(),
                    DiagnosticExecutionLane::PublicRead,
                    |work| filter.lower_bool_expr_for_schema(&schema, work),
                );
                if reject {
                    let error = result.expect_err("one byte short rejects construction");
                    assert!(error.diagnostic_facts().contains(&(
                        DiagnosticFactTag::BudgetResource,
                        Resource::TemporaryBytes.raw()
                    )));
                } else {
                    result.expect("exact construction allowance fits");
                }
                assert_eq!(
                    root.observed(Resource::TemporaryBytes),
                    expected,
                    "{filter:?}"
                );
            }
        }
    }

    fn membership_schema() -> SchemaInfo {
        let id = FieldId::new(1);
        let slot = SchemaFieldSlot::new(0);
        let snapshot = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "filter::tests::Entity".to_string(),
            "Entity".to_string(),
            id,
            SchemaRowLayout::initial(vec![(id, slot)]),
            vec![PersistedFieldSnapshot::new_initial(
                id,
                "id".to_string(),
                slot,
                AcceptedFieldKind::Int32,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Int64),
            )],
        ))
        .expect("test schema should be accepted");
        let catalog = AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );

        SchemaInfo::from_accepted_snapshot_and_catalog(&snapshot, catalog, true)
    }

    // Inspect junction depth and leaf order without imposing a recursive walk
    // on the test itself. Comparisons are leaves for this measurement.
    fn bool_chain_leaves(expr: &Expr, join: BinaryOp) -> (usize, Vec<&Expr>) {
        let mut pending = vec![(expr, 0)];
        let mut leaves = Vec::new();
        let mut depth = 0;
        while let Some((expr, level)) = pending.pop() {
            depth = depth.max(level);
            if let Expr::Binary { op, left, right } = expr
                && *op == join
            {
                pending.push((right, level + 1));
                pending.push((left, level + 1));
            } else {
                leaves.push(expr);
            }
        }

        (depth, leaves)
    }

    #[test]
    fn flat_junctions_lower_with_logarithmic_depth_in_source_order() {
        crate::db::query::preparation::with_preparation_work(|work| {
            let schema = membership_schema();
            for count in [0_usize, 1, 2, 3, 127, 128, 129, 1024, 4096] {
                let values = (0..count)
                    .map(|value| FilterValue::String(value.to_string()))
                    .collect::<Vec<_>>();
                for negated in [false, true] {
                    let compare = if negated { BinaryOp::Ne } else { BinaryOp::Eq };
                    let join = if negated { BinaryOp::And } else { BinaryOp::Or };
                    let filters = values
                        .iter()
                        .map(|value| FilterExpr::Compare {
                            operator: if negated {
                                CompareOperator::Ne
                            } else {
                                CompareOperator::Eq
                            },
                            field: "id".to_string(),
                            value: value.clone(),
                        })
                        .collect();
                    let junction = FilterExpr::Junction {
                        operator: if negated {
                            JunctionOperator::And
                        } else {
                            JunctionOperator::Or
                        },
                        filters,
                    };
                    let junction = lower_filter(&junction, &schema);
                    if count == 0 {
                        assert_eq!(junction, Expr::Literal(Value::Bool(negated)));
                        continue;
                    }

                    let (depth, leaves) = bool_chain_leaves(&junction, join);
                    assert_eq!(depth, count.next_power_of_two().ilog2() as usize);
                    assert_eq!(leaves.len(), count);
                    for (index, leaf) in leaves.into_iter().enumerate() {
                        let value = i64::try_from(index).expect("test integer fits");
                        assert_eq!(
                            *leaf,
                            field_compare_expr(compare, "id", Value::Int64(value), work)
                                .expect("bounded fixture comparison")
                        );
                    }
                    // Canonical identity must not depend on associative grouping.
                    // Keep this control below the separate wide-lowering stress sizes.
                    if count <= 129 {
                        let associated = bool_chain_leaves(&junction, join)
                            .1
                            .into_iter()
                            .cloned()
                            .reduce(|left, right| Expr::Binary {
                                op: join,
                                left: Box::new(left),
                                right: Box::new(right),
                            })
                            .expect("nonempty fixture");
                        assert_eq!(
                            normalize_bool_expr(junction, work).expect("canonical preparation"),
                            normalize_bool_expr(associated, work).expect("canonical preparation")
                        );
                    }
                }
            }
        });
    }

    #[test]
    fn compact_membership_preserves_admitted_values_without_comparison_expansion() {
        crate::db::query::preparation::with_preparation_work(|work| {
            let schema = membership_schema();
            let values = vec![
                FilterValue::String("3".to_string()),
                FilterValue::Null,
                FilterValue::String("1".to_string()),
                FilterValue::String("3".to_string()),
            ];
            for negated in [false, true] {
                let lowered = FilterExpr::Set {
                    operator: if negated {
                        SetOperator::NotIn
                    } else {
                        SetOperator::In
                    },
                    field: "id".to_string(),
                    values: values.clone(),
                };
                let lowered = lower_filter(&lowered, &schema);
                let expected = vec![
                    Value::Int64(3),
                    Value::Null,
                    Value::Int64(1),
                    Value::Int64(3),
                ];
                assert_eq!(
                    lowered,
                    membership_expr("id", expected, negated, work)
                        .expect("bounded fixture membership")
                );
                assert_eq!(
                    normalize_bool_expr(lowered.clone(), work).expect("canonical preparation"),
                    lowered
                );
            }
        });
    }

    #[test]
    fn compact_membership_moves_the_list_and_keeps_constant_depth() {
        crate::db::query::preparation::with_preparation_work(|work| {
            for count in [1, 2, 128, 4096] {
                for negated in [false, true] {
                    let values = (0..count).map(Value::Int64).collect::<Vec<_>>();
                    let allocation = values.as_ptr();
                    let expr = membership_expr("id", values, negated, work)
                        .expect("bounded fixture membership");
                    let membership = if negated {
                        let Expr::Unary {
                            op: UnaryOp::Not,
                            expr,
                        } = &expr
                        else {
                            panic!("negated membership wraps one compact operation");
                        };
                        expr.as_ref()
                    } else {
                        &expr
                    };
                    let Expr::FunctionCall {
                        function: Function::InList,
                        args,
                    } = membership
                    else {
                        panic!("membership remains compact at every width");
                    };
                    let [Expr::Field(field), Expr::Literal(Value::List(values))] = args.as_slice()
                    else {
                        panic!("membership owns a target and a value list");
                    };
                    assert_eq!(field.as_str(), "id");
                    assert_eq!(values.as_ptr(), allocation);
                    assert_eq!(
                        values.len(),
                        usize::try_from(count).expect("fixture count fits")
                    );
                }
            }
        });
    }

    #[test]
    fn compact_membership_matches_explicit_comparisons_and_empty_identities() {
        crate::db::query::preparation::with_preparation_work(|work| {
            let fixtures = [
                vec![],
                vec![Value::Null],
                vec![Value::Int64(1)],
                vec![Value::Int64(1), Value::Null, Value::Int64(1)],
                vec![Value::Int64(2), Value::Int64(3)],
                vec![Value::Int64(1), Value::Nat64(1)],
                vec![Value::Text("one".into()), Value::Text("two".into())],
                vec![Value::Bool(true), Value::Bool(false)],
                vec![Value::Blob(vec![1]), Value::Blob(vec![2])],
                vec![Value::List(vec![Value::Int64(1)])],
                vec![Value::Unit],
            ];
            let targets = [
                Value::Null,
                Value::Int64(1),
                Value::Int64(4),
                Value::Nat64(1),
                Value::Text("one".into()),
                Value::Bool(false),
                Value::Blob(vec![1]),
                Value::List(vec![Value::Int64(1)]),
                Value::Unit,
            ];
            for values in fixtures {
                for negated in [false, true] {
                    let compare = if negated { BinaryOp::Ne } else { BinaryOp::Eq };
                    let join = if negated { BinaryOp::And } else { BinaryOp::Or };
                    let explicit = values
                        .iter()
                        .cloned()
                        .map(|value| {
                            field_compare_expr(compare, "id", value, work)
                                .expect("bounded fixture comparison")
                        })
                        .reduce(|left, right| Expr::Binary {
                            op: join,
                            left: Box::new(left),
                            right: Box::new(right),
                        })
                        .unwrap_or(Expr::Literal(Value::Bool(negated)));
                    let compact = membership_expr("id", values.clone(), negated, work)
                        .expect("bounded fixture membership");
                    if values.is_empty() {
                        assert_eq!(compact, Expr::Literal(Value::Bool(negated)));
                    }
                    for target in &targets {
                        let actual = eval_builder_expr_for_value_preview(&compact, "id", target);
                        let expected = eval_builder_expr_for_value_preview(&explicit, "id", target);
                        match (actual, expected) {
                            (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
                            (Err(_), Err(_)) => {}
                            other => panic!("membership evaluation parity failed: {other:?}"),
                        }
                    }
                    let compact_predicate = derive_normalized_bool_expr_predicate_subset(
                        &normalize_bool_expr(compact, work).expect("canonical preparation"),
                    );
                    let explicit_predicate = derive_normalized_bool_expr_predicate_subset(
                        &normalize_bool_expr(explicit, work).expect("canonical preparation"),
                    );
                    assert_eq!(compact_predicate.is_some(), explicit_predicate.is_some());
                    if let (Some(compact), Some(explicit)) = (compact_predicate, explicit_predicate)
                    {
                        let schema = membership_schema();
                        let compact = PredicateProgram::compile_with_schema_info(&schema, &compact);
                        let explicit =
                            PredicateProgram::compile_with_schema_info(&schema, &explicit);
                        for target in &targets {
                            assert_eq!(
                                compact.eval_with_slot_value_cow_reader(&mut |_| Some(
                                    std::borrow::Cow::Borrowed(target)
                                )),
                                explicit.eval_with_slot_value_cow_reader(&mut |_| Some(
                                    std::borrow::Cow::Borrowed(target)
                                )),
                            );
                        }
                    }
                }
            }
        });
    }

    // Manual native preparation probe: includes accepted-value lowering,
    // canonicalization, predicate extraction and output disposal, not row reads.
    #[test]
    #[ignore = "manual native typed-membership preparation microbenchmark"]
    fn typed_membership_native_timing() {
        use std::{hint::black_box, time::Instant};

        let schema = membership_schema();
        let root = crate::db::RequestExecutionRoot::__new_runtime_root();
        let scope = root.scope();
        for count in [4, 16, 64, 128] {
            let filter = FilterExpr::Set {
                operator: SetOperator::In,
                field: "id".to_string(),
                values: (0..count)
                    .rev()
                    .map(|value| FilterValue::String(value.to_string()))
                    .collect(),
            };
            let prepare = || {
                let expr = crate::db::query::preparation::PreparationWork::run(
                    &scope,
                    icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead,
                    |work| {
                        normalize_bool_expr(filter.lower_bool_expr_for_schema(&schema, work)?, work)
                    },
                )
                .expect("timing workload fits request budget");
                let predicate = derive_normalized_bool_expr_predicate_subset(&expr);
                drop(black_box((expr, predicate)));
            };
            prepare();
            let mut samples = Vec::new();
            for _ in 0..7 {
                let start = Instant::now();
                for _ in 0..64 {
                    prepare();
                }
                samples.push(start.elapsed().as_nanos() / 64);
            }
            samples.sort_unstable();
            println!("membership_native count={count} median_ns={}", samples[3]);
        }
    }

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
        filters.push(FilterExpr::not(FilterExpr::and(vec![
            FilterExpr::eq("collection_id", "collection"),
            FilterExpr::eq("stage", "Draft"),
        ])));

        for filter in filters {
            let encoded = candid::encode_one(&filter).expect("filter should encode");
            let decoded = candid::decode_one::<FilterExpr>(&encoded).expect("filter should decode");
            assert_eq!(decoded, filter);

            let optional = Some(filter);
            let encoded = candid::encode_one(&optional).expect("optional filter should encode");
            let decoded = candid::decode_one::<Option<FilterExpr>>(&encoded)
                .expect("supplied filter should decode");
            assert_eq!(decoded, optional);
        }
    }

    #[test]
    fn optional_filter_candid_preserves_explicit_absence() {
        let encoded =
            candid::encode_one(Option::<FilterExpr>::None).expect("absent filter should encode");
        assert_eq!(
            candid::decode_one::<Option<FilterExpr>>(&encoded)
                .expect("absent filter should decode"),
            None,
        );
    }

    #[test]
    fn optional_filter_candid_rejects_unknown_expression_family() {
        #[derive(candid::CandidType)]
        enum InvalidFilter {
            Unsupported,
        }

        let encoded = candid::encode_one(Some(InvalidFilter::Unsupported))
            .expect("invalid fixture should encode");
        assert!(candid::decode_one::<Option<FilterExpr>>(&encoded).is_err());
    }

    #[test]
    fn optional_filter_candid_rejects_malformed_current_payloads() {
        #[derive(candid::CandidType)]
        enum InvalidFilter {
            Constant(String),
            Compare {
                operator: String,
                field: String,
                value: FilterValue,
            },
            Not(Box<Self>),
            Junction {
                operator: JunctionOperator,
                filters: Vec<Self>,
            },
        }

        let invalid = [
            InvalidFilter::Constant("true".to_string()),
            InvalidFilter::Compare {
                operator: "Eq".to_string(),
                field: "stage".to_string(),
                value: FilterValue::String("Draft".to_string()),
            },
            InvalidFilter::Not(Box::new(InvalidFilter::Constant("false".to_string()))),
            InvalidFilter::Junction {
                operator: JunctionOperator::And,
                filters: vec![InvalidFilter::Constant("true".to_string())],
            },
        ];
        for filter in invalid {
            let encoded = candid::encode_one(Some(filter)).expect("invalid fixture should encode");
            assert!(candid::decode_one::<Option<FilterExpr>>(&encoded).is_err());
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
