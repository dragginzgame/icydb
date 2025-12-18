use crate::{
    db::{
        primitives::{FilterExpr, FilterSlot, LimitExpr, LimitSlot, SortExpr, SortSlot},
        query::{QueryError, QueryValidate, prelude::*},
    },
    traits::{EntityKind, FieldValue},
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// LoadQuery
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct LoadQuery {
    pub filter: Option<FilterExpr>,
    pub limit: Option<LimitExpr>,
    pub sort: Option<SortExpr>,
}

impl LoadQuery {
    /// Construct an empty load query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.filter.is_none() && self.limit.is_none() && self.sort.is_none()
    }

    ///
    /// CONSTRUCTORS
    ///

    /// Filter by a single primary key value.
    #[must_use]
    pub fn one<E: EntityKind>(self, value: impl FieldValue) -> Self {
        self.filter(|f| f.eq(E::PRIMARY_KEY, value))
    }

    /// Filter by a set of primary key values.
    #[must_use]
    pub fn many<E: EntityKind>(self, values: impl IntoIterator<Item = impl FieldValue>) -> Self {
        self.filter(move |f| f.in_iter(E::PRIMARY_KEY, values))
    }

    /// Read all rows (alias for `LoadQuery::default()`).
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    ///
    /// Convenience
    ///

    /// Set offset=0, limit=1 (useful for existence checks / fast-paths).
    #[must_use]
    pub fn limit_1(self) -> Self {
        self.offset(0).limit(1)
    }
}

impl FilterSlot for LoadQuery {
    fn filter_slot(&mut self) -> &mut Option<FilterExpr> {
        &mut self.filter
    }
}

impl LimitSlot for LoadQuery {
    fn limit_slot(&mut self) -> &mut Option<LimitExpr> {
        &mut self.limit
    }
}

impl SortSlot for LoadQuery {
    fn sort_slot(&mut self) -> &mut Option<SortExpr> {
        &mut self.sort
    }
}

impl<E: EntityKind> QueryValidate<E> for LoadQuery {
    fn validate(&self) -> Result<(), QueryError> {
        if let Some(filter) = &self.filter {
            QueryValidate::<E>::validate(filter)?;
        }
        if let Some(limit) = &self.limit {
            QueryValidate::<E>::validate(limit)?;
        }
        if let Some(sort) = &self.sort {
            QueryValidate::<E>::validate(sort)?;
        }

        Ok(())
    }
}
