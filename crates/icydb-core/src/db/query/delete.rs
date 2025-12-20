use crate::{
    db::{
        primitives::{FilterExpr, FilterExt, FilterSlot, LimitExpr, LimitSlot},
        query::{QueryError, QueryValidate},
    },
    traits::{EntityKind, FieldValue},
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// DeleteQuery
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct DeleteQuery {
    pub filter: Option<FilterExpr>,
    pub limit: Option<LimitExpr>,
}

impl DeleteQuery {
    // ─────────────────────────────────────────────
    // CONSTRUCTORS
    // ─────────────────────────────────────────────

    /// Construct an empty delete query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    // ─────────────────────────────────────────────
    // ENTITY CONVENIENCE HELPERS
    // ─────────────────────────────────────────────

    /// Delete a single row by primary key value.
    ///
    /// Convenience wrapper; entity knowledge lives here only for ergonomics.
    #[must_use]
    pub fn one<E: EntityKind>(self, value: impl FieldValue) -> Self {
        self.one_by_field(E::PRIMARY_KEY, value)
    }

    /// Delete a single row where the primary key is unit.
    #[must_use]
    pub fn only<E: EntityKind>(self) -> Self {
        self.one_by_field(E::PRIMARY_KEY, ())
    }

    /// Delete multiple rows by an arbitrary field.
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
    // FIELD-BASED HELPERS (PRIMITIVES)
    // ─────────────────────────────────────────────

    /// Delete a single row by an arbitrary field value.
    #[must_use]
    pub fn one_by_field(self, field: impl AsRef<str>, value: impl FieldValue) -> Self {
        self.filter(|f| f.eq(field, value))
    }

    /// Delete multiple rows by an arbitrary field.
    #[must_use]
    pub fn many_by_field<I, V>(self, field: impl AsRef<str>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.filter(|f| f.in_iter(field, values))
    }
}

// ─────────────────────────────────────────────
// TRAIT IMPLEMENTATIONS
// ─────────────────────────────────────────────

impl FilterSlot for DeleteQuery {
    fn filter_slot(&mut self) -> &mut Option<FilterExpr> {
        &mut self.filter
    }
}

impl LimitSlot for DeleteQuery {
    fn limit_slot(&mut self) -> &mut Option<LimitExpr> {
        &mut self.limit
    }
}

impl<E: EntityKind> QueryValidate<E> for DeleteQuery {
    fn validate(&self) -> Result<(), QueryError> {
        if let Some(filter) = &self.filter {
            QueryValidate::<E>::validate(filter)?;
        }

        if let Some(limit) = &self.limit {
            QueryValidate::<E>::validate(limit)?;
        }

        Ok(())
    }
}
