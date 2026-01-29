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
    pub fn lower<E: EntityKind>(&self) -> Result<core::db::query::expr::FilterExpr, QueryError> {
        use core::db::query::predicate::Predicate;

        // Helper: lower and extract the inner Predicate
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
