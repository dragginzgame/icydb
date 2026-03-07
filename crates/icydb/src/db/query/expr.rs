use crate::{
    traits::{EntityKind, FieldValue},
    value::Value,
};
use candid::CandidType;
use icydb_core::db::{
    CoercionId, CompareOp, ComparePredicate, FilterExpr as CoreFilterExpr,
    OrderDirection as CoreOrderDirection, Predicate, QueryError, SortExpr as CoreSortExpr,
};
use serde::{Deserialize, Serialize};

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

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
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

    #[must_use]
    pub const fn and(exprs: Vec<Self>) -> Self {
        Self::And(exprs)
    }

    #[must_use]
    pub const fn or(exprs: Vec<Self>) -> Self {
        Self::Or(exprs)
    }

    #[must_use]
    #[expect(clippy::should_implement_trait)]
    pub fn not(expr: Self) -> Self {
        Self::Not(Box::new(expr))
    }

    // ─────────────────────────────────────────────────────────────
    // Scalar comparisons
    // ─────────────────────────────────────────────────────────────

    pub fn eq(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn ne(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Ne {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn lt(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Lt {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn lte(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Lte {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn gt(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Gt {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn gte(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Gte {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn in_list(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl FieldValue>,
    ) -> Self {
        Self::In {
            field: field.into(),
            values: values.into_iter().map(|v| v.to_value()).collect(),
        }
    }

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

    pub fn contains(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::Contains {
            field: field.into(),
            value: value.to_value(),
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Text predicates
    // ─────────────────────────────────────────────────────────────

    pub fn text_contains(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::TextContains {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn text_contains_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::TextContainsCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn starts_with(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::StartsWith {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn starts_with_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::StartsWithCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn ends_with(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::EndsWith {
            field: field.into(),
            value: value.to_value(),
        }
    }

    pub fn ends_with_ci(field: impl Into<String>, value: impl FieldValue) -> Self {
        Self::EndsWithCi {
            field: field.into(),
            value: value.to_value(),
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Presence / nullability
    // ─────────────────────────────────────────────────────────────

    pub fn is_null(field: impl Into<String>) -> Self {
        Self::IsNull {
            field: field.into(),
        }
    }

    pub fn is_not_null(field: impl Into<String>) -> Self {
        Self::IsNotNull {
            field: field.into(),
        }
    }

    pub fn is_missing(field: impl Into<String>) -> Self {
        Self::IsMissing {
            field: field.into(),
        }
    }

    pub fn is_empty(field: impl Into<String>) -> Self {
        Self::IsEmpty {
            field: field.into(),
        }
    }

    pub fn is_not_empty(field: impl Into<String>) -> Self {
        Self::IsNotEmpty {
            field: field.into(),
        }
    }
}

///
/// SortExpr
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SortExpr {
    fields: Vec<(String, OrderDirection)>,
}

impl SortExpr {
    #[must_use]
    pub const fn new(fields: Vec<(String, OrderDirection)>) -> Self {
        Self { fields }
    }

    #[must_use]
    pub fn fields(&self) -> &[(String, OrderDirection)] {
        &self.fields
    }

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

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Serialize)]
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
    use serde::Serialize;
    use serde_cbor::Value as CborValue;
    use std::collections::BTreeMap;

    fn to_cbor_value<T: Serialize>(value: &T) -> CborValue {
        let bytes =
            serde_cbor::to_vec(value).expect("test fixtures must serialize into CBOR payloads");
        serde_cbor::from_slice::<CborValue>(&bytes)
            .expect("test fixtures must deserialize into CBOR value trees")
    }

    fn expect_cbor_map(value: &CborValue) -> &BTreeMap<CborValue, CborValue> {
        match value {
            CborValue::Map(map) => map,
            other => panic!("expected CBOR map, got {other:?}"),
        }
    }

    fn map_field<'a>(map: &'a BTreeMap<CborValue, CborValue>, key: &str) -> Option<&'a CborValue> {
        map.get(&CborValue::Text(key.to_string()))
    }

    #[test]
    fn filter_expr_eq_serialization_shape_is_stable() {
        let encoded = to_cbor_value(&FilterExpr::eq("rank", 42_u64));
        let root = expect_cbor_map(&encoded);
        let eq_payload = map_field(root, "Eq").expect("expected external enum variant key");
        let payload = expect_cbor_map(eq_payload);

        assert!(
            map_field(payload, "field").is_some(),
            "Eq payload must keep `field` key in serialized shape",
        );
        assert!(
            map_field(payload, "value").is_some(),
            "Eq payload must keep `value` key in serialized shape",
        );
    }

    #[test]
    fn filter_expr_and_serialization_shape_is_stable() {
        let encoded = to_cbor_value(&FilterExpr::and(vec![
            FilterExpr::is_null("deleted_at"),
            FilterExpr::not(FilterExpr::is_missing("name")),
        ]));
        let root = expect_cbor_map(&encoded);
        let and_payload = map_field(root, "And").expect("expected external enum variant key");
        match and_payload {
            CborValue::Array(items) => {
                assert_eq!(items.len(), 2, "And payload must remain an array payload");
            }
            other => panic!("expected And payload array, got {other:?}"),
        }
    }

    #[test]
    fn sort_expr_serialization_field_name_is_stable() {
        let encoded = to_cbor_value(&SortExpr::new(vec![(
            "created_at".to_string(),
            OrderDirection::Desc,
        )]));
        let root = expect_cbor_map(&encoded);
        assert!(
            map_field(root, "fields").is_some(),
            "SortExpr must keep `fields` as serialized field key",
        );
    }

    #[test]
    fn order_direction_variant_labels_are_stable() {
        assert_eq!(
            to_cbor_value(&OrderDirection::Asc),
            CborValue::Text("Asc".to_string())
        );
        assert_eq!(
            to_cbor_value(&OrderDirection::Desc),
            CborValue::Text("Desc".to_string())
        );
    }
}
