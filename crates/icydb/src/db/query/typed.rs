//! Module: db::query::typed
//!
//! Responsibility: typed read ergonomics over the accepted dynamic-query lane.
//! Does not own: schema identity, planning, admission, execution, or row decoding.
//! Boundary: generated adapters supply an opaque binding; accepted authority
//! resolves and executes the structural query before generated output decoding.

use crate::{
    db::{
        DbSession, DynamicQuery, TypedBindingError, TypedEntityAdapter, TypedEntityBinding,
        TypedRowError,
    },
    traits::CanisterKind,
};
use icydb_core::db::{FilterExpr, OrderTerm};
use std::{error::Error as StdError, fmt, marker::PhantomData};

/// Failure while constructing or executing one accepted-schema-bound typed query.
#[derive(Debug)]
pub enum TypedQueryError {
    /// The generated adapter could not bind to current accepted authority.
    Binding(TypedBindingError),
    /// The accepted dynamic read rejected or failed.
    Database(crate::Error),
    /// A returned accepted row could not be projected through the binding.
    Row(TypedRowError),
}

impl fmt::Display for TypedQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(error) => error.fmt(formatter),
            Self::Database(error) => error.fmt(formatter),
            Self::Row(error) => error.fmt(formatter),
        }
    }
}

impl StdError for TypedQueryError {}

///
/// Query
///
/// Typed application projection over one accepted-schema-driven dynamic read.
/// Query planning, admission, and execution never consume generated model
/// metadata. The generated type participates only in binding and output decode.
///
pub struct Query<'session, C, E>
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    session: &'session DbSession<C>,
    binding: TypedEntityBinding,
    request: DynamicQuery,
    entity: PhantomData<fn() -> E>,
}

impl<'session, C, E> Query<'session, C, E>
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    pub(crate) fn new(session: &'session DbSession<C>) -> Result<Self, TypedBindingError> {
        let binding = E::typed_binding(session)?;
        let request = DynamicQuery::new(binding.entity());
        Ok(Self {
            session,
            binding,
            request,
            entity: PhantomData,
        })
    }

    /// Add one accepted-field filter expression.
    #[must_use]
    pub fn filter(mut self, filter: impl Into<FilterExpr>) -> Self {
        self.request = self.request.filter(filter);
        self
    }

    /// Append one deterministic accepted-field ordering term.
    #[must_use]
    pub fn order_by(mut self, order: OrderTerm) -> Self {
        self.request = self.request.order_by(order);
        self
    }

    /// Select explicit accepted fields in output order.
    #[must_use]
    pub fn select<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.request = self.request.select(fields);
        self
    }

    /// Bound the maximum number of returned rows.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.request = self.request.limit(limit);
        self
    }

    /// Execute through ordinary bounded public-read admission and decode rows.
    pub fn execute_rows(self) -> Result<Vec<E::Row>, TypedQueryError> {
        let result = self
            .session
            .execute_public_typed_dynamic_query(&self.binding, &self.request)
            .map_err(TypedQueryError::Database)?
            .ok_or({
                TypedQueryError::Row(TypedRowError::Adapter(
                    crate::db::TypedAdapterError::StaleBinding,
                ))
            })?;
        let mut rows = Vec::with_capacity(result.rows.len());
        for row_index in 0..result.rows.len() {
            let row = self
                .session
                .typed_query_row(&self.binding, &result, row_index)
                .map_err(TypedQueryError::Row)?;
            rows.push(
                E::decode_row(&self.binding, row)
                    .map_err(|error| TypedQueryError::Row(TypedRowError::Adapter(error)))?,
            );
        }
        Ok(rows)
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Start one typed read bound to current accepted schema authority.
    pub fn query<E>(&self) -> Result<Query<'_, C, E>, TypedBindingError>
    where
        E: TypedEntityAdapter,
    {
        Query::new(self)
    }
}
