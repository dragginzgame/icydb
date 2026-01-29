use crate::{traits::EntityKind, value::Value};
use candid::CandidType;
use icydb_core::{self as core, db::query::QueryError};
use serde::{Deserialize, Serialize};

///
/// FilterExpr
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub enum FilterExpr {
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),

    Eq { field: String, value: Value },
    Ne { field: String, value: Value },
    Lt { field: String, value: Value },
    Lte { field: String, value: Value },
    Gt { field: String, value: Value },
    Gte { field: String, value: Value },
    In { field: String, values: Vec<Value> },
}

impl FilterExpr {
    // ─────────────────────────────────────────────────────────────
    // Lowering
    // ─────────────────────────────────────────────────────────────

    pub fn lower<E: EntityKind>(&self) -> Result<core::db::query::expr::FilterExpr, QueryError> {
        use core::db::query::predicate::Predicate;

        let lower_pred =
            |expr: &Self| -> Result<Predicate, QueryError> { Ok(expr.lower::<E>()?.0) };

        let pred = match self {
            Self::And(xs) => {
                Predicate::and(xs.iter().map(lower_pred).collect::<Result<Vec<_>, _>>()?)
            }
            Self::Or(xs) => {
                Predicate::or(xs.iter().map(lower_pred).collect::<Result<Vec<_>, _>>()?)
            }
            Self::Not(x) => Predicate::not(lower_pred(x)?),

            Self::Eq { field, value } => Predicate::eq(field.clone(), value.clone()),
            Self::Ne { field, value } => Predicate::ne(field.clone(), value.clone()),
            Self::Lt { field, value } => Predicate::lt(field.clone(), value.clone()),
            Self::Lte { field, value } => Predicate::lte(field.clone(), value.clone()),
            Self::Gt { field, value } => Predicate::gt(field.clone(), value.clone()),
            Self::Gte { field, value } => Predicate::gte(field.clone(), value.clone()),
            Self::In { field, values } => Predicate::in_(field.clone(), values.clone()),
        };

        Ok(core::db::query::expr::FilterExpr(pred))
    }

    // ─────────────────────────────────────────────────────────────
    // Comparison constructors
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

    pub fn in_list<V>(field: impl Into<String>, values: V) -> Self
    where
        V: IntoIterator,
        V::Item: Into<Value>,
    {
        Self::In {
            field: field.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Logical constructors
    // ─────────────────────────────────────────────────────────────

    pub fn and_all(exprs: impl Into<Vec<Self>>) -> Self {
        Self::And(exprs.into())
    }

    pub fn or_all(exprs: impl Into<Vec<Self>>) -> Self {
        Self::Or(exprs.into())
    }

    #[allow(clippy::should_implement_trait)]
    #[must_use]
    pub fn not(expr: Self) -> Self {
        Self::Not(Box::new(expr))
    }

    // ─────────────────────────────────────────────────────────────
    // Fluent combinators (flattening)
    // ─────────────────────────────────────────────────────────────

    #[must_use]
    pub fn and(self, other: Self) -> Self {
        match self {
            Self::And(mut xs) => {
                xs.push(other);
                Self::And(xs)
            }
            lhs => Self::And(vec![lhs, other]),
        }
    }

    #[must_use]
    pub fn or(self, other: Self) -> Self {
        match self {
            Self::Or(mut xs) => {
                xs.push(other);
                Self::Or(xs)
            }
            lhs => Self::Or(vec![lhs, other]),
        }
    }
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
