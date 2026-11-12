//! Module: db::query::expr::filter
//! Responsibility: frontend-safe filter expression DTOs and planner lowering.
//! Does not own: query route planning or executor predicate evaluation.
//! Boundary: converts serialized filter input into planner-owned boolean expressions.

use crate::{
    db::{
        QueryError,
        query::plan::expr::{BinaryOp, Expr, FieldId, Function, UnaryOp},
        query::preparation::PreparationWork,
        schema::SchemaInfo,
    },
    value::{InputValue, PublicValue, Value},
};

use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use serde::Deserialize;
use std::borrow::Cow;

/// Boolean junction applied to a group of filter expressions.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
/// Supplied expressions must decode successfully even inside a Wire optional
/// argument; an invalid predicate must never become an absent filter.
/// Decode untrusted bytes through IC Wire's bounded native codec before lowering.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
        value: InputValue,
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
        values: Vec<InputValue>,
    },
    /// Collection containment or text matching.
    Collection {
        /// Collection or text operation.
        operator: CollectionOperator,
        /// Field name.
        field: String,
        /// Literal operand.
        value: InputValue,
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
    pub fn eq(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Eq,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field != value`.
    #[must_use]
    pub fn ne(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Ne,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field < value`.
    #[must_use]
    pub fn lt(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Lt,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field <= value`.
    #[must_use]
    pub fn lte(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Lte,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field > value`.
    #[must_use]
    pub fn gt(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Gt,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field >= value`.
    #[must_use]
    pub fn gte(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Compare {
            operator: CompareOperator::Gte,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare `field == value` with casefolded text equality.
    #[must_use]
    pub fn eq_ci(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
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
        values: impl IntoIterator<Item = impl Into<InputValue>>,
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
        values: impl IntoIterator<Item = impl Into<InputValue>>,
    ) -> Self {
        Self::Set {
            operator: SetOperator::NotIn,
            field: field.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    /// Compare collection `field CONTAINS value`.
    #[must_use]
    pub fn contains(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::Contains,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive substring containment.
    #[must_use]
    pub fn text_contains(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::TextContains,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive substring containment.
    #[must_use]
    pub fn text_contains_ci(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::TextContainsCi,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive prefix match.
    #[must_use]
    pub fn starts_with(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::StartsWith,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive prefix match.
    #[must_use]
    pub fn starts_with_ci(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::StartsWithCi,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-sensitive suffix match.
    #[must_use]
    pub fn ends_with(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
        Self::Collection {
            operator: CollectionOperator::EndsWith,
            field: field.into(),
            value: value.into(),
        }
    }

    /// Compare case-insensitive suffix match.
    #[must_use]
    pub fn ends_with_ci(field: impl Into<String>, value: impl Into<InputValue>) -> Self {
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
    value: &InputValue,
    work: &PreparationWork<'_>,
) -> Result<Expr, QueryError> {
    if operator == CompareOperator::EqCi {
        return binary_expr(
            BinaryOp::Eq,
            field_function_expr(Function::Lower, field, work)?,
            Expr::Literal(lower_input_value(value, None, work)?),
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
    value: &InputValue,
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
        lower_input_value(value, None, work)?
    };
    function_expr(function, [left, Expr::Literal(value)], work)
}

fn lower_compare(
    schema: &SchemaInfo,
    field: &str,
    value: &InputValue,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    lower_canonical_filter_value(value, Some((schema, field, false)), work, |raw| {
        schema.canonicalize_filter_literal(field, raw, work)
    })
}

fn lower_membership(
    schema: &SchemaInfo,
    field: &str,
    values: &[InputValue],
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
    value: &InputValue,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    lower_canonical_filter_value(value, Some((schema, field, true)), work, |raw| {
        schema.canonicalize_filter_collection_element(field, raw, work)
    })
}

// Keep already-owned storage when canonicalization borrows or declines it.
fn lower_canonical_filter_value(
    value: &InputValue,
    expected: Option<(&SchemaInfo, &str, bool)>,
    work: &PreparationWork<'_>,
    canonicalize: impl for<'a> FnOnce(&'a Value) -> Result<Option<Cow<'a, Value>>, QueryError>,
) -> Result<Value, QueryError> {
    let raw = lower_input_value(value, expected, work)?;
    Ok(match canonicalize(&raw)? {
        Some(Cow::Owned(canonical)) => canonical,
        Some(Cow::Borrowed(_)) | None => raw,
    })
}

// Charge every copied node and both public/runtime container backings before
// cloning input. Enum resolution must use accepted authority, never display text.
fn admit_input_copy(value: &PublicValue, work: &PreparationWork<'_>) -> Result<bool, QueryError> {
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
    work.charge(Resource::NestedValueSteps, 1)?;
    let mut contains_enum = false;
    let bytes = match value {
        PublicValue::Text(text) => text.len(),
        PublicValue::Blob(bytes) => bytes.len(),
        PublicValue::List(values) => {
            work.charge(
                Resource::TemporaryBytes,
                (values.len() * (size_of::<PublicValue>() + size_of::<Value>())) as u64,
            )?;
            for value in values {
                contains_enum |= admit_input_copy(value, work)?;
            }
            0
        }
        PublicValue::Map(entries) => {
            let comparisons = crate::db::resource::budget::sort_comparison_allowance(entries.len())
                .saturating_add(entries.len().saturating_sub(1) as u64);
            let key_bytes = entries.iter().fold(0_u64, |bytes, (key, _)| {
                bytes.saturating_add(match key {
                    PublicValue::Text(text) => text.len() as u64,
                    PublicValue::Blob(blob) => blob.len() as u64,
                    _ => 0,
                })
            });
            work.charge(Resource::SortEntries, entries.len() as u64)?;
            work.charge(Resource::SortComparisons, comparisons)?;
            work.charge(
                Resource::PredicateExpressionSteps,
                key_bytes.saturating_mul(comparisons),
            )?;

            work.charge(
                Resource::TemporaryBytes,
                (entries.len()
                    * (size_of::<(PublicValue, PublicValue)>() + size_of::<(Value, Value)>()))
                    as u64,
            )?;
            for (key, value) in entries {
                contains_enum |= admit_input_copy(key, work)?;
                contains_enum |= admit_input_copy(value, work)?;
            }
            0
        }
        PublicValue::Enum(value) => {
            contains_enum = true;
            if let Some(payload) = value.payload() {
                work.charge(
                    Resource::TemporaryBytes,
                    (size_of::<PublicValue>() + size_of::<Value>()) as u64,
                )?;
                admit_input_copy(payload, work)?;
            }
            value.variant().len() + value.path().map_or(0, str::len)
        }
        _ => 0,
    };
    work.charge(Resource::TemporaryBytes, bytes as u64)?;
    Ok(contains_enum)
}

fn lower_input_value(
    value: &InputValue,
    expected: Option<(&SchemaInfo, &str, bool)>,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    use crate::db::{
        predicate::predicate_admission_error,
        schema::enum_catalog::{ValueAdmissionBudget, ValueAdmissionError},
    };
    let contains_enum = admit_input_copy(value.as_public(), work)?;
    let field = expected.map_or("", |(_, field, _)| field);
    let invalid = || {
        QueryError::from(predicate_admission_error(
            field,
            ValueAdmissionError::TypeMismatch,
        ))
    };
    if contains_enum {
        let (schema, field, element) = expected.ok_or_else(invalid)?;
        let contract = schema.accepted_field_contract(field).ok_or_else(invalid)?;
        let contract = if element {
            contract.collection_element_contract().ok_or_else(invalid)?
        } else {
            contract
        };
        return contract
            .normalize_input_to_runtime(value.clone(), &mut ValueAdmissionBudget::standard())
            .map_err(|error| QueryError::from(predicate_admission_error(field, error)));
    }
    value
        .clone()
        .try_into_runtime_non_enum()
        .ok_or_else(invalid)
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
mod tests;
