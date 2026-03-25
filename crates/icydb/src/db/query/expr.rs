use crate::{
    traits::{EntityKind, FieldValue},
    value::Value,
};
use candid::CandidType;
use icydb_core::db::{
    CoercionId, CompareOp, ComparePredicate, FilterExpr as CoreFilterExpr,
    OrderDirection as CoreOrderDirection, Predicate, QueryError, SortExpr as CoreSortExpr,
};
use serde::Deserialize;

///
/// FilterExpr
///
/// Serialized, planner-agnostic predicate language.
///
/// This enum is intentionally isomorphic to the subset of core::Predicate that is:
/// - deterministic
/// - schema-visible
/// - safe across API boundaries
///
/// No planner hints, no implicit semantics, no overloaded operators.
/// Any new Predicate variant must be explicitly reviewed for exposure here.
///

#[derive(CandidType, Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum FilterExpr {
    /// Always true.
    True,
    /// Always false.
    False,

    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),

    // ─────────────────────────────────────────────────────────────
    // Scalar comparisons
    // ─────────────────────────────────────────────────────────────
    Eq {
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

    In {
        field: String,
        values: Vec<Value>,
    },
    NotIn {
        field: String,
        values: Vec<Value>,
    },

    // ─────────────────────────────────────────────────────────────
    // Collection predicates
    // ─────────────────────────────────────────────────────────────
    /// Collection contains value.
    Contains {
        field: String,
        value: Value,
    },

    // ─────────────────────────────────────────────────────────────
    // Text predicates (explicit, no overloading)
    // ─────────────────────────────────────────────────────────────
    /// Case-sensitive substring match.
    TextContains {
        field: String,
        value: Value,
    },

    /// Case-insensitive substring match.
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

    // ─────────────────────────────────────────────────────────────
    // Presence / nullability
    // ─────────────────────────────────────────────────────────────
    /// Field is present and explicitly null.
    IsNull {
        field: String,
    },

    /// Field is present and not null.
    /// Equivalent to: NOT IsNull AND NOT IsMissing
    IsNotNull {
        field: String,
    },

    /// Field is not present at all.
    IsMissing {
        field: String,
    },

    /// Field is present but empty (collection or string).
    IsEmpty {
        field: String,
    },

    /// Field is present and non-empty.
    IsNotEmpty {
        field: String,
    },
}

impl FilterExpr {
    // ─────────────────────────────────────────────────────────────
    // Lowering
    // ─────────────────────────────────────────────────────────────

    /// Lower this API-level filter expression into core predicate IR.
    ///
    /// Lowering applies explicit coercion policies so execution semantics are stable.
    #[expect(clippy::too_many_lines)]
    pub fn lower<E: EntityKind>(&self) -> Result<CoreFilterExpr, QueryError> {
        let lower_pred =
            |expr: &Self| -> Result<Predicate, QueryError> { Ok(expr.lower::<E>()?.0) };

        let pred = match self {
            Self::True => Predicate::True,
            Self::False => Predicate::False,

            Self::And(xs) => {
                Predicate::and(xs.iter().map(lower_pred).collect::<Result<Vec<_>, _>>()?)
            }
            Self::Or(xs) => {
                Predicate::or(xs.iter().map(lower_pred).collect::<Result<Vec<_>, _>>()?)
            }
            Self::Not(x) => Predicate::not(lower_pred(x)?),

            Self::Eq { field, value } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::Eq,
                value.clone(),
                CoercionId::Strict,
            )),

            Self::Ne { field, value } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::Ne,
                value.clone(),
                CoercionId::Strict,
            )),

            Self::Lt { field, value } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::Lt,
                value.clone(),
                CoercionId::NumericWiden,
            )),

            Self::Lte { field, value } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::Lte,
                value.clone(),
                CoercionId::NumericWiden,
            )),

            Self::Gt { field, value } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::Gt,
                value.clone(),
                CoercionId::NumericWiden,
            )),

            Self::Gte { field, value } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::Gte,
                value.clone(),
                CoercionId::NumericWiden,
            )),

            Self::In { field, values } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::In,
                Value::List(values.clone()),
                CoercionId::Strict,
            )),

            Self::NotIn { field, values } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::NotIn,
                Value::List(values.clone()),
                CoercionId::Strict,
            )),

            Self::Contains { field, value } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::Contains,
                value.clone(),
                CoercionId::Strict,
            )),

            Self::TextContains { field, value } => Predicate::TextContains {
                field: field.clone(),
                value: value.clone(),
            },

            Self::TextContainsCi { field, value } => Predicate::TextContainsCi {
                field: field.clone(),
                value: value.clone(),
            },

            Self::StartsWith { field, value } => {
                Predicate::Compare(ComparePredicate::with_coercion(
                    field.as_str(),
                    CompareOp::StartsWith,
                    value.clone(),
                    CoercionId::Strict,
                ))
            }

            Self::StartsWithCi { field, value } => {
                Predicate::Compare(ComparePredicate::with_coercion(
                    field.as_str(),
                    CompareOp::StartsWith,
                    value.clone(),
                    CoercionId::TextCasefold,
                ))
            }

            Self::EndsWith { field, value } => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str(),
                CompareOp::EndsWith,
                value.clone(),
                CoercionId::Strict,
            )),

            Self::EndsWithCi { field, value } => {
                Predicate::Compare(ComparePredicate::with_coercion(
                    field.as_str(),
                    CompareOp::EndsWith,
                    value.clone(),
                    CoercionId::TextCasefold,
                ))
            }

            Self::IsNull { field } => Predicate::IsNull {
                field: field.clone(),
            },

            Self::IsNotNull { field } => Predicate::and(vec![
                Predicate::not(Predicate::IsNull {
                    field: field.clone(),
                }),
                Predicate::not(Predicate::IsMissing {
                    field: field.clone(),
                }),
            ]),

            Self::IsMissing { field } => Predicate::IsMissing {
                field: field.clone(),
            },

            Self::IsEmpty { field } => Predicate::IsEmpty {
                field: field.clone(),
            },

            Self::IsNotEmpty { field } => Predicate::IsNotEmpty {
                field: field.clone(),
            },
        };

        Ok(CoreFilterExpr(pred))
    }

    // ─────────────────────────────────────────────────────────────
    // Boolean
    // ─────────────────────────────────────────────────────────────

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

    // ─────────────────────────────────────────────────────────────
    // Scalar comparisons
    // ─────────────────────────────────────────────────────────────

    /// Compare `field == value`.
    pub fn eq(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field != value`.
    pub fn ne(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Ne {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field < value`.
    pub fn lt(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Lt {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field <= value`.
    pub fn lte(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Lte {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field > value`.
    pub fn gt(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Gt {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field >= value`.
    pub fn gte(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Gte {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare `field IN values`.
    pub fn in_list(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl FieldValue>,
    ) -> Self {
        Self::In {
            field: field.into(),
            values: values.into_iter().map(|v| v.to_value()).collect(),
        }
    }

    /// Compare `field NOT IN values`.
    pub fn not_in(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl FieldValue>,
    ) -> Self {
        Self::NotIn {
            field: field.into(),
            values: values.into_iter().map(|v| v.to_value()).collect(),
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Collection
    // ─────────────────────────────────────────────────────────────

    /// Compare collection `field CONTAINS value`.
    pub fn contains(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Contains {
            field: field.into(),
            value: value.to_value(),
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Text predicates
    // ─────────────────────────────────────────────────────────────

    /// Compare case-sensitive substring containment.
    pub fn text_contains(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::TextContains {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-insensitive substring containment.
    pub fn text_contains_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::TextContainsCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-sensitive prefix match.
    pub fn starts_with(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::StartsWith {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-insensitive prefix match.
    pub fn starts_with_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::StartsWithCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-sensitive suffix match.
    pub fn ends_with(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::EndsWith {
            field: field.into(),
            value: value.to_value(),
        }
    }

    /// Compare case-insensitive suffix match.
    pub fn ends_with_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::EndsWithCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Presence / nullability
    // ─────────────────────────────────────────────────────────────

    /// Match rows where `field` is present and null.
    pub fn is_null(field: impl Into<String>) -> Self {
        Self::IsNull {
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and non-null.
    pub fn is_not_null(field: impl Into<String>) -> Self {
        Self::IsNotNull {
            field: field.into(),
        }
    }

    /// Match rows where `field` is absent.
    pub fn is_missing(field: impl Into<String>) -> Self {
        Self::IsMissing {
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and empty.
    pub fn is_empty(field: impl Into<String>) -> Self {
        Self::IsEmpty {
            field: field.into(),
        }
    }

    /// Match rows where `field` is present and non-empty.
    pub fn is_not_empty(field: impl Into<String>) -> Self {
        Self::IsNotEmpty {
            field: field.into(),
        }
    }
}

///
/// SortExpr
///

#[derive(CandidType, Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SortExpr {
    fields: Vec<(String, OrderDirection)>,
}

impl SortExpr {
    /// Build a sort specification from ordered `(field, direction)` pairs.
    #[must_use]
    pub const fn new(fields: Vec<(String, OrderDirection)>) -> Self {
        Self { fields }
    }

    /// Borrow the ordered sort fields.
    #[must_use]
    pub fn fields(&self) -> &[(String, OrderDirection)] {
        &self.fields
    }

    /// Lower this API-level sort expression into core sort IR.
    #[must_use]
    pub fn lower(&self) -> CoreSortExpr {
        let fields = self
            .fields()
            .iter()
            .map(|(field, dir)| {
                let dir = match dir {
                    OrderDirection::Asc => CoreOrderDirection::Asc,
                    OrderDirection::Desc => CoreOrderDirection::Desc,
                };
                (field.clone(), dir)
            })
            .collect();

        CoreSortExpr::new(fields)
    }
}

///
/// OrderDirection
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum OrderDirection {
    Asc,
    Desc,
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{FilterExpr, OrderDirection, SortExpr};
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
    fn sort_expr_candid_field_name_is_stable() {
        let fields = expect_record_fields(SortExpr::ty());

        assert!(
            fields.iter().any(|candidate| candidate == "fields"),
            "SortExpr must keep `fields` as Candid field key",
        );
    }

    #[test]
    fn order_direction_variant_labels_are_stable() {
        let mut labels = expect_variant_labels(OrderDirection::ty());
        labels.sort_unstable();
        assert_eq!(labels, vec!["Asc".to_string(), "Desc".to_string()]);
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
        let sort = SortExpr::new(vec![("created_at".to_string(), OrderDirection::Desc)]);

        match expr {
            FilterExpr::And(items) => assert_eq!(items.len(), 2),
            other => panic!("expected And fixture, got {other:?}"),
        }

        assert_eq!(sort.fields().len(), 1);
        assert!(matches!(sort.fields()[0].1, OrderDirection::Desc));
    }
}
