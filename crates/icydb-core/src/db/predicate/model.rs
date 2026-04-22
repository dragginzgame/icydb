//! Module: predicate::model
//! Responsibility: public predicate AST and construction helpers.
//! Does not own: schema validation or runtime slot resolution.
//! Boundary: user/query-facing predicate model.

use crate::{
    db::{
        QueryError,
        predicate::coercion::{CoercionId, CoercionSpec},
        sql::lowering::{
            PreparedSqlPredicateTemplateShape, sql_expr_prepared_predicate_template_shape,
        },
    },
    value::Value,
};
use std::ops::{BitAnd, BitOr};
use thiserror::Error as ThisError;

#[cfg_attr(doc, doc = "Predicate")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ComparePredicate),
    CompareFields(CompareFieldsPredicate),
    IsNull { field: String },
    IsNotNull { field: String },
    IsMissing { field: String },
    IsEmpty { field: String },
    IsNotEmpty { field: String },
    TextContains { field: String, value: Value },
    TextContainsCi { field: String, value: Value },
}

///
/// PreparedSqlScalarCompareSlotTemplate
///
/// Frozen compare slot metadata for one symbolic scalar prepared predicate.
/// The template preserves the planner-owned compare field/operator/coercion
/// shape while deferring only the compared literal value to one binding slot.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct PreparedSqlScalarCompareSlotTemplate {
    field: String,
    op: CompareOp,
    coercion: CoercionId,
    slot_index: usize,
}

///
/// PreparedSqlScalarPredicateTemplate
///
/// Predicate-owned symbolic scalar prepared template tree.
/// This keeps prepared scalar predicate structure under the predicate owner
/// while the session layer supplies only the lowering-owned SQL shape facts.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum PreparedSqlScalarPredicateTemplate {
    Compare(PreparedSqlScalarCompareSlotTemplate),
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
}

impl Predicate {
    /// Build an `And` predicate from child predicates.
    #[must_use]
    pub const fn and(preds: Vec<Self>) -> Self {
        Self::And(preds)
    }

    /// Build an `Or` predicate from child predicates.
    #[must_use]
    pub const fn or(preds: Vec<Self>) -> Self {
        Self::Or(preds)
    }

    /// Negate one predicate.
    #[must_use]
    #[expect(clippy::should_implement_trait)]
    pub fn not(pred: Self) -> Self {
        Self::Not(Box::new(pred))
    }

    /// Compare `field == value`.
    #[must_use]
    pub fn eq(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::eq(field, value))
    }

    /// Compare `field != value`.
    #[must_use]
    pub fn ne(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::ne(field, value))
    }

    /// Compare `field < value`.
    #[must_use]
    pub fn lt(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::lt(field, value))
    }

    /// Compare `field <= value`.
    #[must_use]
    pub fn lte(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::lte(field, value))
    }

    /// Compare `field > value`.
    #[must_use]
    pub fn gt(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::gt(field, value))
    }

    /// Compare `field >= value`.
    #[must_use]
    pub fn gte(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::gte(field, value))
    }

    /// Compare `left_field == right_field`.
    #[must_use]
    pub fn eq_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::eq(left_field, right_field))
    }

    /// Compare `left_field != right_field`.
    #[must_use]
    pub fn ne_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::ne(left_field, right_field))
    }

    /// Compare `left_field < right_field`.
    #[must_use]
    pub fn lt_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            CompareOp::Lt,
            right_field,
            CoercionId::NumericWiden,
        ))
    }

    /// Compare `left_field <= right_field`.
    #[must_use]
    pub fn lte_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            CompareOp::Lte,
            right_field,
            CoercionId::NumericWiden,
        ))
    }

    /// Compare `left_field > right_field`.
    #[must_use]
    pub fn gt_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            CompareOp::Gt,
            right_field,
            CoercionId::NumericWiden,
        ))
    }

    /// Compare `left_field >= right_field`.
    #[must_use]
    pub fn gte_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            CompareOp::Gte,
            right_field,
            CoercionId::NumericWiden,
        ))
    }

    /// Compare `field IN values`.
    #[must_use]
    pub fn in_(field: String, values: Vec<Value>) -> Self {
        Self::Compare(ComparePredicate::in_(field, values))
    }

    /// Compare `field NOT IN values`.
    #[must_use]
    pub fn not_in(field: String, values: Vec<Value>) -> Self {
        Self::Compare(ComparePredicate::not_in(field, values))
    }

    /// Compare `field IS NOT NULL`.
    #[must_use]
    pub const fn is_not_null(field: String) -> Self {
        Self::IsNotNull { field }
    }

    /// Compare `field BETWEEN lower AND upper`.
    #[must_use]
    pub fn between(field: String, lower: Value, upper: Value) -> Self {
        Self::And(vec![
            Self::gte(field.clone(), lower),
            Self::lte(field, upper),
        ])
    }

    /// Compare `field NOT BETWEEN lower AND upper`.
    #[must_use]
    pub fn not_between(field: String, lower: Value, upper: Value) -> Self {
        Self::Or(vec![Self::lt(field.clone(), lower), Self::gt(field, upper)])
    }

    /// Return whether this predicate still carries any literal equal to one of
    /// the supplied runtime candidate values.
    #[must_use]
    pub(in crate::db) fn contains_any_runtime_values(&self, candidates: &[Value]) -> bool {
        match self {
            Self::True
            | Self::False
            | Self::CompareFields(_)
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::IsMissing { .. }
            | Self::IsEmpty { .. }
            | Self::IsNotEmpty { .. } => false,
            Self::Compare(compare) => candidates.contains(compare.value()),
            Self::And(children) | Self::Or(children) => children
                .iter()
                .any(|child| child.contains_any_runtime_values(candidates)),
            Self::Not(child) => child.contains_any_runtime_values(candidates),
            Self::TextContains { value, .. } | Self::TextContainsCi { value, .. } => {
                candidates.contains(value)
            }
        }
    }

    /// Rebind any template sentinel literals in this predicate to their
    /// runtime bound values without changing predicate structure.
    #[must_use]
    pub(in crate::db) fn bind_template_values(self, replacements: &[(Value, Value)]) -> Self {
        match self {
            Self::True => Self::True,
            Self::False => Self::False,
            Self::And(children) => Self::And(
                children
                    .into_iter()
                    .map(|child| child.bind_template_values(replacements))
                    .collect(),
            ),
            Self::Or(children) => Self::Or(
                children
                    .into_iter()
                    .map(|child| child.bind_template_values(replacements))
                    .collect(),
            ),
            Self::Not(child) => Self::Not(Box::new(child.bind_template_values(replacements))),
            Self::Compare(compare) => Self::Compare(ComparePredicate::with_coercion(
                compare.field,
                compare.op,
                bind_template_value(compare.value, replacements),
                compare.coercion.id,
            )),
            Self::CompareFields(compare) => Self::CompareFields(compare),
            Self::IsNull { field } => Self::IsNull { field },
            Self::IsNotNull { field } => Self::IsNotNull { field },
            Self::IsMissing { field } => Self::IsMissing { field },
            Self::IsEmpty { field } => Self::IsEmpty { field },
            Self::IsNotEmpty { field } => Self::IsNotEmpty { field },
            Self::TextContains { field, value } => Self::TextContains {
                field,
                value: bind_template_value(value, replacements),
            },
            Self::TextContainsCi { field, value } => Self::TextContainsCi {
                field,
                value: bind_template_value(value, replacements),
            },
        }
    }

    /// Build one symbolic scalar prepared predicate template from one
    /// lowering-owned SQL predicate shape plus this predicate tree.
    #[must_use]
    pub(in crate::db) fn build_prepared_template(
        &self,
        shape: PreparedSqlPredicateTemplateShape<'_>,
    ) -> Option<PreparedSqlScalarPredicateTemplate> {
        match (shape, self) {
            (PreparedSqlPredicateTemplateShape::And { left, right }, Self::And(children))
                if children.len() == 2 =>
            {
                Self::build_prepared_binary_children_template(
                    left,
                    right,
                    &children[0],
                    &children[1],
                    PreparedSqlScalarPredicateTemplate::And,
                )
            }
            (PreparedSqlPredicateTemplateShape::Or { left, right }, Self::Or(children))
                if children.len() == 2 =>
            {
                Self::build_prepared_binary_children_template(
                    left,
                    right,
                    &children[0],
                    &children[1],
                    PreparedSqlScalarPredicateTemplate::Or,
                )
            }
            (PreparedSqlPredicateTemplateShape::Not { expr }, Self::Not(child)) => {
                Some(PreparedSqlScalarPredicateTemplate::Not(Box::new(
                    child.build_prepared_template(sql_expr_prepared_predicate_template_shape(
                        expr,
                    )?)?,
                )))
            }
            (
                PreparedSqlPredicateTemplateShape::CompareWithParamRhs { slot_index },
                Self::Compare(compare),
            ) => Some(PreparedSqlScalarPredicateTemplate::Compare(
                PreparedSqlScalarCompareSlotTemplate {
                    field: compare.field.clone(),
                    op: compare.op,
                    coercion: compare.coercion.id,
                    slot_index,
                },
            )),
            _ => None,
        }
    }

    fn build_prepared_binary_children_template(
        left_sql: &crate::db::sql::parser::SqlExpr,
        right_sql: &crate::db::sql::parser::SqlExpr,
        first_child: &Self,
        second_child: &Self,
        ctor: fn(Vec<PreparedSqlScalarPredicateTemplate>) -> PreparedSqlScalarPredicateTemplate,
    ) -> Option<PreparedSqlScalarPredicateTemplate> {
        if let (Some(left), Some(right)) = (
            first_child
                .build_prepared_template(sql_expr_prepared_predicate_template_shape(left_sql)?),
            second_child
                .build_prepared_template(sql_expr_prepared_predicate_template_shape(right_sql)?),
        ) {
            return Some(ctor(vec![left, right]));
        }

        let (Some(left), Some(right)) = (
            second_child
                .build_prepared_template(sql_expr_prepared_predicate_template_shape(left_sql)?),
            first_child
                .build_prepared_template(sql_expr_prepared_predicate_template_shape(right_sql)?),
        ) else {
            return None;
        };

        Some(ctor(vec![left, right]))
    }
}

impl PreparedSqlScalarPredicateTemplate {
    /// Instantiate one symbolic scalar predicate template with runtime
    /// bindings and rebuild the planner-owned predicate tree.
    pub(in crate::db) fn instantiate(&self, bindings: &[Value]) -> Result<Predicate, QueryError> {
        match self {
            Self::Compare(compare) => {
                let binding = bindings
                    .get(compare.slot_index)
                    .ok_or_else(|| {
                        QueryError::unsupported_query(format!(
                            "missing prepared SQL binding at index={}",
                            compare.slot_index,
                        ))
                    })?
                    .clone();

                Ok(Predicate::Compare(ComparePredicate::with_coercion(
                    compare.field.clone(),
                    compare.op,
                    binding,
                    compare.coercion,
                )))
            }
            Self::And(children) => Ok(Predicate::And(
                children
                    .iter()
                    .map(|child| child.instantiate(bindings))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Self::Or(children) => Ok(Predicate::Or(
                children
                    .iter()
                    .map(|child| child.instantiate(bindings))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Self::Not(child) => Ok(Predicate::Not(Box::new(child.instantiate(bindings)?))),
        }
    }
}

fn bind_template_value(value: Value, replacements: &[(Value, Value)]) -> Value {
    replacements
        .iter()
        .find(|(template, _)| *template == value)
        .map_or(value, |(_, bound)| bound.clone())
}

impl BitAnd for Predicate {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self::And(vec![self, rhs])
    }
}

impl BitAnd for &Predicate {
    type Output = Predicate;

    fn bitand(self, rhs: Self) -> Self::Output {
        Predicate::And(vec![self.clone(), rhs.clone()])
    }
}

impl BitOr for Predicate {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self::Or(vec![self, rhs])
    }
}

impl BitOr for &Predicate {
    type Output = Predicate;

    fn bitor(self, rhs: Self) -> Self::Output {
        Predicate::Or(vec![self.clone(), rhs.clone()])
    }
}

#[cfg_attr(doc, doc = "CompareOp")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CompareOp {
    Eq = 0x01,
    Ne = 0x02,
    Lt = 0x03,
    Lte = 0x04,
    Gt = 0x05,
    Gte = 0x06,
    In = 0x07,
    NotIn = 0x08,
    Contains = 0x09,
    StartsWith = 0x0a,
    EndsWith = 0x0b,
}

impl CompareOp {
    /// Return the stable wire tag for this compare operator.
    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }

    /// Return the operator that preserves semantics when the two operands are swapped.
    #[must_use]
    pub const fn flipped(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Lte => Self::Gte,
            Self::Gt => Self::Lt,
            Self::Gte => Self::Lte,
            Self::In => Self::In,
            Self::NotIn => Self::NotIn,
            Self::Contains => Self::Contains,
            Self::StartsWith => Self::StartsWith,
            Self::EndsWith => Self::EndsWith,
        }
    }
}

#[cfg_attr(doc, doc = "ComparePredicate")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparePredicate {
    pub(crate) field: String,
    pub(crate) op: CompareOp,
    pub(crate) value: Value,
    pub(crate) coercion: CoercionSpec,
}

impl ComparePredicate {
    fn new(field: String, op: CompareOp, value: Value) -> Self {
        Self {
            field,
            op,
            value,
            coercion: CoercionSpec::default(),
        }
    }

    /// Construct a comparison predicate with an explicit coercion policy.
    #[must_use]
    pub fn with_coercion(
        field: impl Into<String>,
        op: CompareOp,
        value: Value,
        coercion: CoercionId,
    ) -> Self {
        Self {
            field: field.into(),
            op,
            value,
            coercion: CoercionSpec::new(coercion),
        }
    }

    /// Build `Eq` comparison.
    #[must_use]
    pub fn eq(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Eq, value)
    }

    /// Build `Ne` comparison.
    #[must_use]
    pub fn ne(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Ne, value)
    }

    /// Build `Lt` comparison.
    #[must_use]
    pub fn lt(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Lt, value)
    }

    /// Build `Lte` comparison.
    #[must_use]
    pub fn lte(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Lte, value)
    }

    /// Build `Gt` comparison.
    #[must_use]
    pub fn gt(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Gt, value)
    }

    /// Build `Gte` comparison.
    #[must_use]
    pub fn gte(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Gte, value)
    }

    /// Build `In` comparison.
    #[must_use]
    pub fn in_(field: String, values: Vec<Value>) -> Self {
        Self::new(field, CompareOp::In, Value::List(values))
    }

    /// Build `NotIn` comparison.
    #[must_use]
    pub fn not_in(field: String, values: Vec<Value>) -> Self {
        Self::new(field, CompareOp::NotIn, Value::List(values))
    }

    /// Borrow the compared field name.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Return the compare operator.
    #[must_use]
    pub const fn op(&self) -> CompareOp {
        self.op
    }

    /// Borrow the compared literal value.
    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Borrow the comparison coercion policy.
    #[must_use]
    pub const fn coercion(&self) -> &CoercionSpec {
        &self.coercion
    }
}

///
/// CompareFieldsPredicate
///
/// Canonical predicate-owned field-to-field comparison leaf.
/// This keeps bounded compare expressions on the predicate authority seam
/// instead of routing them through projection-expression ownership.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompareFieldsPredicate {
    pub(crate) left_field: String,
    pub(crate) op: CompareOp,
    pub(crate) right_field: String,
    pub(crate) coercion: CoercionSpec,
}

impl CompareFieldsPredicate {
    fn canonicalize_symmetric_fields(
        op: CompareOp,
        left_field: String,
        right_field: String,
    ) -> (String, String) {
        if matches!(op, CompareOp::Eq | CompareOp::Ne) && left_field < right_field {
            (right_field, left_field)
        } else {
            (left_field, right_field)
        }
    }

    fn new(left_field: String, op: CompareOp, right_field: String) -> Self {
        let (left_field, right_field) =
            Self::canonicalize_symmetric_fields(op, left_field, right_field);

        Self {
            left_field,
            op,
            right_field,
            coercion: CoercionSpec::default(),
        }
    }

    /// Construct a field-to-field comparison predicate with an explicit
    /// coercion policy.
    #[must_use]
    pub fn with_coercion(
        left_field: impl Into<String>,
        op: CompareOp,
        right_field: impl Into<String>,
        coercion: CoercionId,
    ) -> Self {
        let (left_field, right_field) =
            Self::canonicalize_symmetric_fields(op, left_field.into(), right_field.into());

        Self {
            left_field,
            op,
            right_field,
            coercion: CoercionSpec::new(coercion),
        }
    }

    /// Build `Eq` field-to-field comparison.
    #[must_use]
    pub fn eq(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Eq, right_field)
    }

    /// Build `Ne` field-to-field comparison.
    #[must_use]
    pub fn ne(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Ne, right_field)
    }

    /// Build `Lt` field-to-field comparison.
    #[must_use]
    pub fn lt(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Lt, right_field)
    }

    /// Build `Lte` field-to-field comparison.
    #[must_use]
    pub fn lte(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Lte, right_field)
    }

    /// Build `Gt` field-to-field comparison.
    #[must_use]
    pub fn gt(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Gt, right_field)
    }

    /// Build `Gte` field-to-field comparison.
    #[must_use]
    pub fn gte(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Gte, right_field)
    }

    /// Borrow the left compared field name.
    #[must_use]
    pub fn left_field(&self) -> &str {
        &self.left_field
    }

    /// Return the compare operator.
    #[must_use]
    pub const fn op(&self) -> CompareOp {
        self.op
    }

    /// Borrow the right compared field name.
    #[must_use]
    pub fn right_field(&self) -> &str {
        &self.right_field
    }

    /// Borrow the comparison coercion policy.
    #[must_use]
    pub const fn coercion(&self) -> &CoercionSpec {
        &self.coercion
    }
}

#[cfg_attr(
    doc,
    doc = "UnsupportedQueryFeature\n\nPolicy-level query features intentionally rejected by the engine."
)]
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum UnsupportedQueryFeature {
    #[error("map field '{field}' is not queryable; use scalar/indexed fields or list entries")]
    MapPredicate { field: String },
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::sql::lowering::PreparedSqlPredicateTemplateShape;

    #[test]
    fn bind_template_values_rebinds_nested_literal_leaves_without_changing_shape() {
        let template = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gt,
                Value::Uint(999),
                CoercionId::NumericWiden,
            )),
            Predicate::Not(Box::new(Predicate::TextContains {
                field: "name".to_string(),
                value: Value::Text("__template__".to_string()),
            })),
            Predicate::CompareFields(CompareFieldsPredicate::eq(
                "strength".to_string(),
                "dexterity".to_string(),
            )),
        ]);

        let rebound = template.bind_template_values(&[
            (Value::Uint(999), Value::Uint(21)),
            (
                Value::Text("__template__".to_string()),
                Value::Text("Ada".to_string()),
            ),
        ]);

        assert_eq!(
            rebound,
            Predicate::And(vec![
                Predicate::Compare(ComparePredicate::with_coercion(
                    "age",
                    CompareOp::Gt,
                    Value::Uint(21),
                    CoercionId::NumericWiden,
                )),
                Predicate::Not(Box::new(Predicate::TextContains {
                    field: "name".to_string(),
                    value: Value::Text("Ada".to_string()),
                })),
                Predicate::CompareFields(CompareFieldsPredicate::eq(
                    "strength".to_string(),
                    "dexterity".to_string(),
                )),
            ]),
            "predicate-owned template rebinding should replace only literal leaves and keep the predicate structure intact",
        );
    }

    #[test]
    fn build_prepared_template_rebinds_compare_slot_owned_literal_leaves() {
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "age",
            CompareOp::Gt,
            Value::Uint(999),
            CoercionId::NumericWiden,
        ));

        let template = predicate
            .build_prepared_template(PreparedSqlPredicateTemplateShape::CompareWithParamRhs {
                slot_index: 0,
            })
            .expect("prepared predicate template should build");
        let rebound = template
            .instantiate(&[Value::Uint(21)])
            .expect("prepared predicate template should instantiate");

        assert_eq!(
            rebound,
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gt,
                Value::Uint(21),
                CoercionId::NumericWiden,
            )),
            "predicate-owned prepared templates should preserve compare structure and rebind only the slot-owned literal",
        );
    }
}
