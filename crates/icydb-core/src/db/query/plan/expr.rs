//! Module: query::plan::expr
//! Responsibility: planner-owned expression and projection semantic contracts.
//! Does not own: expression execution, fingerprinting, or continuation wiring.
//! Boundary: additive semantic spine introduced without changing executor behavior.

use crate::{
    db::{
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
    Numeric,
    Text,
    Null,
    Collection,
    Structured,
    Opaque,
    Unknown,
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
        AggregateKind::Count => Ok(ExprType::Numeric),
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
        // Bootstrap behavior: target-less SUM remains unresolved in this phase.
        return Ok(ExprType::Unknown);
    };

    let Some(field_kind) = schema.field_kind(field_name) else {
        return Err(PlanError::from(ExprPlanError::UnknownExprField {
            field: field_name.to_string(),
        }));
    };

    if !field_kind_is_numeric(field_kind) {
        return Err(PlanError::from(ExprPlanError::NonNumericAggregateTarget {
            kind: "sum".to_string(),
            field: field_name.to_string(),
        }));
    }

    Ok(ExprType::Numeric)
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
            if !matches!(inner, ExprType::Numeric) {
                return Err(PlanError::from(ExprPlanError::InvalidUnaryOperand {
                    op: "neg".to_string(),
                    found: format!("{inner:?}"),
                }));
            }

            Ok(ExprType::Numeric)
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

            Ok(ExprType::Numeric)
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
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => {
            if !binary_comparable(&left_ty, &right_ty) {
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
    matches!(left, ExprType::Numeric) && matches!(right, ExprType::Numeric)
}

const fn binary_comparable(left: &ExprType, right: &ExprType) -> bool {
    if matches!(left, ExprType::Numeric) && matches!(right, ExprType::Numeric) {
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
            | (ExprType::Unknown, ExprType::Unknown)
    )
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
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::Decimal(_) => ExprType::Numeric,
        Value::List(_) | Value::Map(_) => ExprType::Collection,
        Value::Null => ExprType::Null,
        Value::Account(_)
        | Value::Blob(_)
        | Value::Date(_)
        | Value::Duration(_)
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Timestamp(_)
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
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Decimal { .. } => ExprType::Numeric,
        FieldKind::Text | FieldKind::Enum { .. } => ExprType::Text,
        FieldKind::List(_) | FieldKind::Set(_) | FieldKind::Map { .. } => ExprType::Collection,
        FieldKind::Structured { .. } => ExprType::Structured,
        FieldKind::Relation { key_kind, .. } => expr_type_from_field_kind(key_kind),
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Date
        | FieldKind::Duration
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Timestamp
        | FieldKind::Ulid
        | FieldKind::Unit => ExprType::Opaque,
    }
}

const fn field_kind_is_numeric(kind: &FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::Int
            | FieldKind::Int128
            | FieldKind::IntBig
            | FieldKind::Uint
            | FieldKind::Uint128
            | FieldKind::UintBig
            | FieldKind::Float32
            | FieldKind::Float64
            | FieldKind::Decimal { .. }
    )
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
            query::{builder::aggregate::sum, plan::validate::ExprPlanError},
        },
        model::{entity::EntityModel, field::FieldKind, index::IndexModel},
        value::Value,
    };

    use super::{BinaryOp, Expr, ExprType, FieldId, UnaryOp, infer_expr_type};

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
        ],
        indexes = [&EMPTY_INDEX],
    }

    fn schema() -> SchemaInfo {
        let model: &'static EntityModel =
            <ExprInferenceEntity as crate::traits::EntitySchema>::MODEL;
        SchemaInfo::from_entity_model(model).expect("schema should validate")
    }

    #[test]
    fn infer_field_type_uses_schema_field_kind() {
        let schema = schema();
        let expr = Expr::Field(FieldId::new("rank"));

        let inferred = infer_expr_type(&expr, &schema).expect("field should infer");

        assert_eq!(inferred, ExprType::Numeric);
    }

    #[test]
    fn infer_literal_type_is_deterministic() {
        let schema = schema();
        let expr = Expr::Literal(Value::Bool(true));

        let inferred = infer_expr_type(&expr, &schema).expect("literal should infer");

        assert_eq!(inferred, ExprType::Bool);
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

        assert_eq!(inferred, ExprType::Numeric);
    }

    #[test]
    fn infer_sum_aggregate_requires_numeric_target() {
        let schema = schema();
        let expr = Expr::Aggregate(sum("label"));

        let err = infer_expr_type(&expr, &schema).expect_err("sum over text should fail");
        assert!(matches!(
            err,
            crate::db::query::plan::PlanError::Expr(inner)
                if matches!(inner.as_ref(), ExprPlanError::NonNumericAggregateTarget { field, .. } if field == "label")
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
        assert!(matches!(
            err,
            crate::db::query::plan::PlanError::Expr(inner)
                if matches!(inner.as_ref(), ExprPlanError::InvalidUnaryOperand { op, .. } if op == "not")
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
        assert!(matches!(
            err,
            crate::db::query::plan::PlanError::Expr(inner)
                if matches!(inner.as_ref(), ExprPlanError::InvalidBinaryOperands { op, .. } if op == "eq")
        ));
    }
}
