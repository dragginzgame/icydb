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
    // ─────────────────────────────────────────────
    // CONSTRUCTORS
    // ─────────────────────────────────────────────

    /// Construct an empty load query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.filter.is_none() && self.limit.is_none() && self.sort.is_none()
    }

    // ─────────────────────────────────────────────
    // ENTITY CONVENIENCE HELPERS
    // ─────────────────────────────────────────────

    /// Filter by a single primary key value.
    #[must_use]
    pub fn one<E: EntityKind>(self, value: impl FieldValue) -> Self {
        self.one_by_field(E::PRIMARY_KEY, value)
    }

    /// Read all rows (alias for default).
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Filter by a set of field values.
    #[must_use]
    pub fn many<E, I, V>(self, values: I) -> Self
    where
        E: EntityKind,
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.filter(|f| f.in_iter(E::PRIMARY_KEY, values))
    }

    // ─────────────────────────────────────────────
    // FIELD-BASED PRIMITIVES
    // ─────────────────────────────────────────────

    /// Filter by a single field value.
    #[must_use]
    pub fn one_by_field(self, field: impl AsRef<str>, value: impl FieldValue) -> Self {
        self.filter(|f| f.eq(field, value))
    }

    /// Filter by a set of field values.
    #[must_use]
    pub fn many_by_field<I, V>(self, field: impl AsRef<str>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.filter(|f| f.in_iter(field, values))
    }

    // ─────────────────────────────────────────────
    // CONVENIENCE
    // ─────────────────────────────────────────────

    /// Set offset=0, limit=1 (useful for existence checks / fast-paths).
    #[must_use]
    pub fn limit_1(self) -> Self {
        self.offset(0).limit(1)
    }
}

// ─────────────────────────────────────────────
// TRAIT IMPLEMENTATIONS
// ─────────────────────────────────────────────

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
