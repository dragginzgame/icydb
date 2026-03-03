//! Module: query::plan::expr
//! Responsibility: planner-owned expression and projection semantic contracts.
//! Does not own: expression execution, fingerprinting, or continuation wiring.
//! Boundary: additive semantic spine introduced without changing executor behavior.

use crate::{
    db::{
        numeric::field_kind_supports_expr_numeric,
        predicate::SchemaInfo,
        query::{
            builder::aggregate::AggregateExpr,
            plan::{AggregateKind, PlanError, validate::ExprPlanError},
        },
    },
    model::field::FieldKind,
    value::Value,
};
use std::collections::HashSet;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnaryOp {
    Neg,
    Not,
}

///
/// BinaryOp
///
/// Canonical binary expression operator taxonomy.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
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
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    Aggregate(AggregateExpr),
    Alias {
        expr: Box<Self>,
        name: Alias,
    },
}

///
/// ProjectionField
///
/// One canonical projection output field in declaration order.
/// This remains planner-owned semantic shape and is executor-agnostic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionField {
    Scalar { expr: Expr, alias: Option<Alias> },
}

///
/// ProjectionSpec
///
/// Canonical projection semantic contract emitted by planner.
/// Construction remains planner-only; consumers borrow read-only views.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProjectionSpec {
    fields: Vec<ProjectionField>,
}

impl ProjectionSpec {
    /// Build one projection semantic contract from planner-lowered fields.
    #[must_use]
    pub(in crate::db::query::plan) const fn new(fields: Vec<ProjectionField>) -> Self {
        Self { fields }
    }

    /// Return true when projection has no declared output fields.
    #[must_use]
    pub(crate) const fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Return the declared output field count.
    #[must_use]
    pub(crate) const fn len(&self) -> usize {
        self.fields.len()
    }

    /// Borrow declared projection fields in canonical order.
    pub(crate) fn fields(&self) -> std::slice::Iter<'_, ProjectionField> {
        self.fields.iter()
    }

    /// Build one projection semantic contract for tests outside planner modules.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn from_fields_for_test(fields: Vec<ProjectionField>) -> Self {
        Self { fields }
    }
}

/// Return true when one expression references only fields in one allowed set.
///
/// Semantic contract:
/// - field leaves must be present in `allowed`
/// - aggregate/literal leaves are always admissible
/// - alias and unary wrappers recurse into inner expression
/// - binary expressions require both sides to be admissible
#[must_use]
pub(crate) fn expr_references_only_fields(expr: &Expr, allowed: &HashSet<&str>) -> bool {
    match expr {
        Expr::Field(field) => allowed.contains(field.as_str()),
        Expr::Literal(_) | Expr::Aggregate(_) => true,
        Expr::Alias { expr, .. } | Expr::Unary { expr, .. } => {
            expr_references_only_fields(expr.as_ref(), allowed)
        }
        Expr::Binary { left, right, .. } => {
            expr_references_only_fields(left.as_ref(), allowed)
                && expr_references_only_fields(right.as_ref(), allowed)
        }
    }
}

///
/// ExprType
///
/// Minimal deterministic expression type classification for planner inference.
/// This intentionally remains coarse in the bootstrap phase.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExprType {
    Bool,
    Numeric(NumericSubtype),
    Text,
    Null,
    Collection,
    Structured,
    Opaque,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NumericSubtype {
    Integer,
    Float,
    Decimal,
    Unknown,
}

impl ExprType {
    // Eligibility answers "can this participate in numeric-only operators?".
    // Subtype answers "which numeric family?" and may remain unresolved.
    const fn is_numeric_eligible(&self) -> bool {
        matches!(self, Self::Numeric(_))
    }

    const fn numeric_subtype(&self) -> Option<NumericSubtype> {
        match self {
            Self::Numeric(subtype) => Some(*subtype),
            _ => None,
        }
    }
}

/// Infer expression type deterministically from canonical expression shape.
pub(crate) fn infer_expr_type(expr: &Expr, schema: &SchemaInfo) -> Result<ExprType, PlanError> {
    match expr {
        Expr::Field(field) => infer_field_expr_type(field, schema),
        Expr::Literal(value) => Ok(infer_literal_type(value)),
        Expr::Aggregate(aggregate) => infer_aggregate_expr_type(aggregate, schema),
        Expr::Alias { expr, .. } => infer_expr_type(expr.as_ref(), schema),
        Expr::Unary { op, expr } => infer_unary_expr_type(*op, expr.as_ref(), schema),
        Expr::Binary { op, left, right } => {
            infer_binary_expr_type(*op, left.as_ref(), right.as_ref(), schema)
        }
    }
}

fn infer_field_expr_type(field: &FieldId, schema: &SchemaInfo) -> Result<ExprType, PlanError> {
    let field_name = field.as_str();
    let Some(field_kind) = schema.field_kind(field_name) else {
        return Err(PlanError::from(ExprPlanError::UnknownExprField {
            field: field_name.to_string(),
        }));
    };

    Ok(expr_type_from_field_kind(field_kind))
}

fn infer_aggregate_expr_type(
    aggregate: &AggregateExpr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let kind = aggregate.kind();
    let target_field = aggregate.target_field();

    match kind {
        AggregateKind::Count => Ok(ExprType::Numeric(NumericSubtype::Integer)),
        AggregateKind::Exists => Ok(ExprType::Bool),
        AggregateKind::Sum => infer_sum_aggregate_type(target_field, schema),
        AggregateKind::Min | AggregateKind::Max | AggregateKind::First | AggregateKind::Last => {
            infer_target_field_aggregate_type(kind, target_field, schema)
        }
    }
}

fn infer_sum_aggregate_type(
    target_field: Option<&str>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(field_name) = target_field else {
        return Err(PlanError::from(ExprPlanError::AggregateTargetRequired {
            kind: "sum".to_string(),
        }));
    };

    let Some(field_kind) = schema.field_kind(field_name) else {
        return Err(PlanError::from(ExprPlanError::UnknownExprField {
            field: field_name.to_string(),
        }));
    };

    if !field_kind_supports_expr_numeric(field_kind) {
        return Err(PlanError::from(ExprPlanError::NonNumericAggregateTarget {
            kind: "sum".to_string(),
            field: field_name.to_string(),
        }));
    }

    Ok(expr_type_from_field_kind(field_kind))
}

fn infer_target_field_aggregate_type(
    kind: AggregateKind,
    target_field: Option<&str>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(field_name) = target_field else {
        // Bootstrap behavior: target-less extrema/value terminals stay unresolved.
        return Ok(ExprType::Unknown);
    };

    let Some(field_kind) = schema.field_kind(field_name) else {
        return Err(PlanError::from(ExprPlanError::UnknownExprField {
            field: field_name.to_string(),
        }));
    };

    let _ = kind;
    Ok(expr_type_from_field_kind(field_kind))
}

fn infer_unary_expr_type(
    op: UnaryOp,
    expr: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let inner = infer_expr_type(expr, schema)?;

    match op {
        UnaryOp::Neg => {
            if !inner.is_numeric_eligible() {
                return Err(PlanError::from(ExprPlanError::InvalidUnaryOperand {
                    op: "neg".to_string(),
                    found: format!("{inner:?}"),
                }));
            }

            Ok(ExprType::Numeric(
                inner.numeric_subtype().unwrap_or(NumericSubtype::Unknown),
            ))
        }
        UnaryOp::Not => {
            if !matches!(inner, ExprType::Bool) {
                return Err(PlanError::from(ExprPlanError::InvalidUnaryOperand {
                    op: "not".to_string(),
                    found: format!("{inner:?}"),
                }));
            }

            Ok(ExprType::Bool)
        }
    }
}

fn infer_binary_expr_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let left_ty = infer_expr_type(left, schema)?;
    let right_ty = infer_expr_type(right, schema)?;

    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            if !binary_numeric_compatible(&left_ty, &right_ty) {
                return Err(PlanError::from(ExprPlanError::InvalidBinaryOperands {
                    op: binary_op_name(op).to_string(),
                    left: format!("{left_ty:?}"),
                    right: format!("{right_ty:?}"),
                }));
            }

            Ok(ExprType::Numeric(infer_numeric_result_subtype(
                op, &left_ty, &right_ty,
            )))
        }
        BinaryOp::And | BinaryOp::Or => {
            if !matches!(left_ty, ExprType::Bool) || !matches!(right_ty, ExprType::Bool) {
                return Err(PlanError::from(ExprPlanError::InvalidBinaryOperands {
                    op: binary_op_name(op).to_string(),
                    left: format!("{left_ty:?}"),
                    right: format!("{right_ty:?}"),
                }));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Eq | BinaryOp::Ne => {
            if !binary_equality_comparable(&left_ty, &right_ty) {
                return Err(PlanError::from(ExprPlanError::InvalidBinaryOperands {
                    op: binary_op_name(op).to_string(),
                    left: format!("{left_ty:?}"),
                    right: format!("{right_ty:?}"),
                }));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
            if !binary_order_comparable(&left_ty, &right_ty) {
                return Err(PlanError::from(ExprPlanError::InvalidBinaryOperands {
                    op: binary_op_name(op).to_string(),
                    left: format!("{left_ty:?}"),
                    right: format!("{right_ty:?}"),
                }));
            }

            Ok(ExprType::Bool)
        }
    }
}

const fn binary_numeric_compatible(left: &ExprType, right: &ExprType) -> bool {
    left.is_numeric_eligible() && right.is_numeric_eligible()
}

const fn binary_equality_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    matches!(
        (left, right),
        (ExprType::Bool, ExprType::Bool)
            | (ExprType::Text, ExprType::Text)
            | (ExprType::Null, ExprType::Null)
            | (ExprType::Collection, ExprType::Collection)
            | (ExprType::Structured, ExprType::Structured)
            | (ExprType::Opaque, ExprType::Opaque)
    )
}

const fn binary_order_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    matches!(
        (left, right),
        (ExprType::Bool, ExprType::Bool) | (ExprType::Text, ExprType::Text)
    )
}

const fn infer_numeric_result_subtype(
    _op: BinaryOp,
    left: &ExprType,
    right: &ExprType,
) -> NumericSubtype {
    let (Some(left_subtype), Some(right_subtype)) =
        (left.numeric_subtype(), right.numeric_subtype())
    else {
        return NumericSubtype::Unknown;
    };

    match (left_subtype, right_subtype) {
        (NumericSubtype::Integer, NumericSubtype::Integer) => NumericSubtype::Integer,
        (NumericSubtype::Float, NumericSubtype::Float) => NumericSubtype::Float,
        (NumericSubtype::Decimal, NumericSubtype::Decimal) => NumericSubtype::Decimal,
        _ => NumericSubtype::Unknown,
    }
}

const fn infer_literal_type(value: &Value) -> ExprType {
    match value {
        Value::Bool(_) => ExprType::Bool,
        Value::Text(_) | Value::Enum(_) => ExprType::Text,
        Value::Int(_)
        | Value::Int128(_)
        | Value::IntBig(_)
        | Value::Uint(_)
        | Value::Uint128(_)
        | Value::UintBig(_)
        | Value::Duration(_)
        | Value::Timestamp(_) => ExprType::Numeric(NumericSubtype::Integer),
        Value::Float32(_) | Value::Float64(_) => ExprType::Numeric(NumericSubtype::Float),
        Value::Decimal(_) => ExprType::Numeric(NumericSubtype::Decimal),
        Value::List(_) | Value::Map(_) => ExprType::Collection,
        Value::Null => ExprType::Null,
        Value::Account(_)
        | Value::Blob(_)
        | Value::Date(_)
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Ulid(_)
        | Value::Unit => ExprType::Opaque,
    }
}

fn expr_type_from_field_kind(kind: &FieldKind) -> ExprType {
    match kind {
        FieldKind::Bool => ExprType::Bool,
        FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Duration
        | FieldKind::Timestamp => ExprType::Numeric(NumericSubtype::Integer),
        FieldKind::Float32 | FieldKind::Float64 => ExprType::Numeric(NumericSubtype::Float),
        FieldKind::Decimal { .. } => ExprType::Numeric(NumericSubtype::Decimal),
        FieldKind::Text | FieldKind::Enum { .. } => ExprType::Text,
        FieldKind::List(_) | FieldKind::Set(_) | FieldKind::Map { .. } => ExprType::Collection,
        FieldKind::Structured { .. } => ExprType::Structured,
        FieldKind::Relation { key_kind, .. } => expr_type_from_field_kind(key_kind),
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Date
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Ulid
        | FieldKind::Unit => ExprType::Opaque,
    }
}

const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Eq => "eq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            predicate::SchemaInfo,
            query::{
                builder::aggregate::{AggregateExpr, min, min_by, sum},
                plan::{GroupAggregateKind, PlanError, PlanUserError, validate::ExprPlanError},
            },
        },
        model::{entity::EntityModel, field::FieldKind, index::IndexModel},
        value::Value,
    };

    use super::{BinaryOp, Expr, ExprType, FieldId, NumericSubtype, UnaryOp, infer_expr_type};

    const EMPTY_INDEX_FIELDS: [&str; 0] = [];
    const EMPTY_INDEX: IndexModel = IndexModel::new(
        "query::plan::expr::idx_empty",
        "query::plan::expr::Store",
        &EMPTY_INDEX_FIELDS,
        false,
    );

    crate::test_entity! {
        ident = ExprInferenceEntity,
        id = crate::types::Ulid,
        entity_name = "ExprInferenceEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("rank", FieldKind::Uint),
            ("flag", FieldKind::Bool),
            ("label", FieldKind::Text),
            ("created_on", FieldKind::Date),
        ],
        indexes = [&EMPTY_INDEX],
    }

    fn schema() -> SchemaInfo {
        let model: &'static EntityModel =
            <ExprInferenceEntity as crate::traits::EntitySchema>::MODEL;
        SchemaInfo::from_entity_model(model).expect("schema should validate")
    }

    fn is_expr_plan_error(err: &PlanError, predicate: impl FnOnce(&ExprPlanError) -> bool) -> bool {
        matches!(
            err,
            PlanError::User(inner)
                if matches!(
                    inner.as_ref(),
                    PlanUserError::Expr(inner) if predicate(inner.as_ref())
                )
        )
    }

    #[test]
    fn infer_field_type_uses_schema_field_kind() {
        let schema = schema();
        let expr = Expr::Field(FieldId::new("rank"));

        let inferred = infer_expr_type(&expr, &schema).expect("field should infer");

        assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Integer));
    }

    #[test]
    fn infer_literal_type_is_deterministic() {
        let schema = schema();
        let expr = Expr::Literal(Value::Bool(true));
        let duration_expr = Expr::Literal(Value::Duration(crate::types::Duration::from_millis(5)));

        let inferred = infer_expr_type(&expr, &schema).expect("literal should infer");
        let duration_inferred =
            infer_expr_type(&duration_expr, &schema).expect("duration literal should infer");

        assert_eq!(inferred, ExprType::Bool);
        assert_eq!(
            duration_inferred,
            ExprType::Numeric(NumericSubtype::Integer)
        );
    }

    #[test]
    fn infer_binary_numeric_expr_requires_numeric_operands() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Uint(7))),
        };

        let inferred = infer_expr_type(&expr, &schema).expect("numeric addition should infer");

        assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Integer));
    }

    #[test]
    fn infer_binary_numeric_expr_rejects_decidable_non_numeric_schema_operand() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("label"))),
        };

        let err = infer_expr_type(&expr, &schema)
            .expect_err("numeric operators must reject schema-known non-numeric fields");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
        ));
    }

    #[test]
    fn infer_binary_numeric_expr_rejects_decidable_non_numeric_bool_field_operand() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("flag"))),
        };

        let err = infer_expr_type(&expr, &schema)
            .expect_err("numeric operators must reject schema-known bool fields");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
        ));
    }

    #[test]
    fn infer_binary_numeric_expr_rejects_decidable_non_numeric_date_field_operand() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("created_on"))),
        };

        let err = infer_expr_type(&expr, &schema)
            .expect_err("numeric operators must reject schema-known date fields");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
        ));
    }

    #[test]
    fn infer_binary_numeric_expr_rejects_decidable_non_numeric_literal_operand() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Bool(true))),
            right: Box::new(Expr::Literal(Value::Int(5))),
        };

        let err = infer_expr_type(&expr, &schema)
            .expect_err("numeric operators must reject non-numeric literal operands");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
        ));
    }

    #[test]
    fn infer_binary_numeric_expr_keeps_numeric_with_unknown_subtype_for_mixed_operands() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Decimal(
                crate::types::Decimal::from_num(7_u64).expect("decimal literal"),
            ))),
        };

        let inferred =
            infer_expr_type(&expr, &schema).expect("mixed numeric addition should stay numeric");

        assert_eq!(inferred, ExprType::Numeric(NumericSubtype::Unknown));
    }

    #[test]
    fn infer_binary_numeric_expr_rejects_unknown_non_eligible_operands() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(min())),
            right: Box::new(Expr::Literal(Value::Int(1))),
        };

        let err = infer_expr_type(&expr, &schema)
            .expect_err("unknown type does not imply numeric eligibility");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "add")
        ));
    }

    #[test]
    fn infer_sum_aggregate_rejects_decidable_non_numeric_bool_target() {
        let schema = schema();
        let expr = Expr::Aggregate(sum("flag"));

        let err = infer_expr_type(&expr, &schema).expect_err("sum over bool should fail");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::NonNumericAggregateTarget { field, .. } if field == "flag")
        ));
    }

    #[test]
    fn infer_min_by_aggregate_keeps_existing_non_numeric_semantics() {
        let schema = schema();
        let expr = Expr::Aggregate(min_by("label"));

        let inferred = infer_expr_type(&expr, &schema).expect("min_by(text) should remain valid");

        assert_eq!(inferred, ExprType::Text);
    }

    #[test]
    fn infer_sum_aggregate_requires_numeric_target() {
        let schema = schema();
        let expr = Expr::Aggregate(sum("label"));

        let err = infer_expr_type(&expr, &schema).expect_err("sum over text should fail");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::NonNumericAggregateTarget { field, .. } if field == "label")
        ));
    }

    #[test]
    fn infer_sum_aggregate_without_target_rejects_missing_target() {
        let schema = schema();
        let expr = Expr::Aggregate(AggregateExpr::from_semantic_parts(
            GroupAggregateKind::Sum,
            None,
            false,
        ));

        let err = infer_expr_type(&expr, &schema).expect_err("sum without target should fail");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::AggregateTargetRequired { kind } if kind == "sum")
        ));
    }

    #[test]
    fn infer_unary_bool_not_rejects_non_bool_operands() {
        let schema = schema();
        let expr = Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Field(FieldId::new("rank"))),
        };

        let err = infer_expr_type(&expr, &schema).expect_err("not over numeric field should fail");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::InvalidUnaryOperand { op, .. } if op == "not")
        ));
    }

    #[test]
    fn infer_binary_compare_rejects_incompatible_operand_types() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("label"))),
        };

        let err = infer_expr_type(&expr, &schema)
            .expect_err("numeric/text comparison should fail deterministic type inference");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "eq")
        ));
    }

    #[test]
    fn infer_binary_compare_rejects_unknown_operands_fail_closed() {
        let schema = schema();
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Aggregate(AggregateExpr::from_semantic_parts(
                GroupAggregateKind::Min,
                None,
                false,
            ))),
            right: Box::new(Expr::Aggregate(AggregateExpr::from_semantic_parts(
                GroupAggregateKind::Max,
                None,
                false,
            ))),
        };

        let err = infer_expr_type(&expr, &schema)
            .expect_err("unknown aggregate operand comparison should fail closed");
        assert!(is_expr_plan_error(
            &err,
            |inner| matches!(inner, ExprPlanError::InvalidBinaryOperands { op, .. } if op == "eq")
        ));
    }
}
