use crate::{traits::EntityKind, value::Value};
use candid::CandidType;
use icydb_core::{
    self as core,
    db::query::{
        QueryError,
        predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
    },
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
/// No planner hints, no legacy semantics, no overloaded operators.
/// Any new Predicate variant must be explicitly reviewed for exposure here.
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
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

    // ─────────────────────────────────────────────────────────────
    // Map predicates (strict only)
    // ─────────────────────────────────────────────────────────────
    MapContainsKey {
        field: String,
        key: Value,
    },
    MapContainsValue {
        field: String,
        value: Value,
    },
    MapContainsEntry {
        field: String,
        key: Value,
        value: Value,
    },
}

impl FilterExpr {
    // ─────────────────────────────────────────────────────────────
    // Lowering
    // ─────────────────────────────────────────────────────────────

    pub fn lower<E: EntityKind>(&self) -> Result<core::db::query::expr::FilterExpr, QueryError> {
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

            Self::Eq { field, value } => compare(field, CompareOp::Eq, value.clone()),

            Self::Ne { field, value } => compare(field, CompareOp::Ne, value.clone()),

            Self::Lt { field, value } => compare(field, CompareOp::Lt, value.clone()),

            Self::Lte { field, value } => compare(field, CompareOp::Lte, value.clone()),

            Self::Gt { field, value } => compare(field, CompareOp::Gt, value.clone()),

            Self::Gte { field, value } => compare(field, CompareOp::Gte, value.clone()),

            Self::In { field, values } => compare_list(field, CompareOp::In, values),

            Self::NotIn { field, values } => compare_list(field, CompareOp::NotIn, values),

            Self::Contains { field, value } => compare(field, CompareOp::Contains, value.clone()),

            Self::TextContains { field, value } => Predicate::TextContains {
                field: field.clone(),
                value: value.clone(),
            },

            Self::TextContainsCi { field, value } => Predicate::TextContainsCi {
                field: field.clone(),
                value: value.clone(),
            },

            Self::StartsWith { field, value } => {
                compare(field, CompareOp::StartsWith, value.clone())
            }

            Self::StartsWithCi { field, value } => {
                compare_ci(field, CompareOp::StartsWith, value.clone())
            }

            Self::EndsWith { field, value } => compare(field, CompareOp::EndsWith, value.clone()),

            Self::EndsWithCi { field, value } => {
                compare_ci(field, CompareOp::EndsWith, value.clone())
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

            Self::MapContainsKey { field, key } => Predicate::MapContainsKey {
                field: field.clone(),
                key: key.clone(),
                coercion: CoercionSpec::new(CoercionId::Strict),
            },

            Self::MapContainsValue { field, value } => Predicate::MapContainsValue {
                field: field.clone(),
                value: value.clone(),
                coercion: CoercionSpec::new(CoercionId::Strict),
            },

            Self::MapContainsEntry { field, key, value } => Predicate::MapContainsEntry {
                field: field.clone(),
                key: key.clone(),
                value: value.clone(),
                coercion: CoercionSpec::new(CoercionId::Strict),
            },
        };

        Ok(core::db::query::expr::FilterExpr(pred))
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
    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: Self) -> Self {
        Self::Not(Box::new(expr))
    }

    // ─────────────────────────────────────────────────────────────
    // Scalar comparisons
    // ─────────────────────────────────────────────────────────────

    pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn ne(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Ne {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn lt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Lt {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn lte(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Lte {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn gt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Gt {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn gte(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Gte {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn in_list(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<Value>>,
    ) -> Self {
        Self::In {
            field: field.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    pub fn not_in(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<Value>>,
    ) -> Self {
        Self::NotIn {
            field: field.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Collection
    // ─────────────────────────────────────────────────────────────

    pub fn contains(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Contains {
            field: field.into(),
            value: value.into(),
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Text predicates
    // ─────────────────────────────────────────────────────────────

    pub fn text_contains(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::TextContains {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn text_contains_ci(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::TextContainsCi {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn starts_with(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::StartsWith {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn starts_with_ci(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::StartsWithCi {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn ends_with(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::EndsWith {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn ends_with_ci(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::EndsWithCi {
            field: field.into(),
            value: value.into(),
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

    // ─────────────────────────────────────────────────────────────
    // Map predicates
    // ─────────────────────────────────────────────────────────────

    pub fn map_contains_key(field: impl Into<String>, key: impl Into<Value>) -> Self {
        Self::MapContainsKey {
            field: field.into(),
            key: key.into(),
        }
    }

    pub fn map_contains_value(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::MapContainsValue {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn map_contains_entry(
        field: impl Into<String>,
        key: impl Into<Value>,
        value: impl Into<Value>,
    ) -> Self {
        Self::MapContainsEntry {
            field: field.into(),
            key: key.into(),
            value: value.into(),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────

fn compare(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate {
        field: field.to_string(),
        op,
        value,
        coercion: CoercionSpec::new(CoercionId::Strict),
    })
}

fn compare_ci(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate {
        field: field.to_string(),
        op,
        value,
        coercion: CoercionSpec::new(CoercionId::TextCasefold),
    })
}

fn compare_list(field: &str, op: CompareOp, values: &[Value]) -> Predicate {
    Predicate::Compare(ComparePredicate {
        field: field.to_string(),
        op,
        value: Value::List(values.to_vec()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    })
}

///
/// SortExpr
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct SortExpr {
    pub fields: Vec<(String, OrderDirection)>,
}

impl SortExpr {
    #[must_use]
    pub fn lower(&self) -> core::db::query::expr::SortExpr {
        let fields = self
            .fields
            .iter()
            .map(|(field, dir)| {
                let dir = match dir {
                    OrderDirection::Asc => core::db::query::plan::OrderDirection::Asc,
                    OrderDirection::Desc => core::db::query::plan::OrderDirection::Desc,
                };
                (field.clone(), dir)
            })
            .collect();

        core::db::query::expr::SortExpr { fields }
    }
}

///
/// OrderDirection
///

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Serialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}
