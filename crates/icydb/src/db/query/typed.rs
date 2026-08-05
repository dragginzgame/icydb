//! Module: db::query::typed
//!
//! Responsibility: typed read ergonomics over the accepted dynamic-query lane.
//! Does not own: schema identity, planning, admission, execution, or row decoding.
//! Boundary: generated adapters supply an opaque binding; accepted authority
//! resolves and executes the structural query before generated output decoding.

use crate::{
    db::{
        DbSession, DynamicQuery, GroupedQueryOutput, TypedBindingError, TypedEntityAdapter,
        TypedEntityBinding, TypedRowError,
    },
    traits::{CanisterKind, EntityKey},
    types::Id,
};
use icydb_core::db::{AggregateExpr, FilterExpr, OrderTerm};
use std::{error::Error as StdError, fmt, marker::PhantomData};

/// Failure while executing one accepted-schema-bound typed query.
#[derive(Debug)]
pub enum TypedQueryError {
    /// The accepted dynamic read rejected or failed.
    Database(crate::Error),
    /// A returned accepted row could not be projected through the binding.
    Row(TypedRowError),
}

impl fmt::Display for TypedQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => error.fmt(formatter),
            Self::Row(error) => error.fmt(formatter),
        }
    }
}

impl StdError for TypedQueryError {}

fn typed_query_error_from_binding(error: TypedBindingError) -> TypedQueryError {
    match error {
        TypedBindingError::Adapter(error) => TypedQueryError::Row(TypedRowError::Adapter(error)),
        TypedBindingError::Database(error) => TypedQueryError::Database(error),
    }
}

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

    /// Select explicit accepted fields in scalar output order.
    ///
    /// Grouped execution rejects an explicit scalar selection because group
    /// keys and aggregates define its output contract.
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

    /// Append one accepted field to the grouped key in declaration order.
    #[must_use]
    pub fn group_by(mut self, field: impl Into<String>) -> Self {
        self.request = self.request.group_by(field);
        self
    }

    /// Append one grouped aggregate in declaration order.
    #[must_use]
    pub fn aggregate(mut self, aggregate: AggregateExpr) -> Self {
        self.request = self.request.aggregate(aggregate);
        self
    }

    /// Set explicit hard limits for grouped execution.
    #[must_use]
    pub fn grouped_limits(mut self, max_groups: u32, max_group_bytes: u32) -> Self {
        self.request = self.request.grouped_limits(max_groups, max_group_bytes);
        self
    }

    /// Continue from one opaque grouped cursor returned by IcyDB.
    #[must_use]
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.request = self.request.cursor(cursor);
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

    /// Execute through ordinary bounded grouped-read admission.
    ///
    /// Group keys and aggregate outputs preserve their declaration order. The
    /// accepted schema, shared query planner, and grouped executor remain the
    /// sole runtime authorities; `E` supplies only the source-bound entity
    /// binding used to reject stale adapters.
    pub fn execute_grouped(self) -> Result<GroupedQueryOutput, TypedQueryError> {
        self.session
            .execute_public_typed_dynamic_grouped_query(&self.binding, &self.request)
            .map_err(TypedQueryError::Database)?
            .ok_or({
                TypedQueryError::Row(TypedRowError::Adapter(
                    crate::db::TypedAdapterError::StaleBinding,
                ))
            })
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

    /// Read one generated entity directly by its typed entity identifier.
    ///
    /// This path validates one current accepted binding and performs one
    /// bounded store lookup without constructing or caching a dynamic plan.
    pub fn get<E>(&self, id: Id<E>) -> Result<Option<E::Row>, TypedQueryError>
    where
        E: EntityKey + TypedEntityAdapter,
        E::Row: Clone,
    {
        let mut rows = self.get_many::<E>(&[id])?;
        rows.pop().ok_or({
            TypedQueryError::Row(TypedRowError::Adapter(
                crate::db::TypedAdapterError::RowShapeMismatch,
            ))
        })
    }

    /// Read generated entities directly by typed entity identifier.
    ///
    /// The result has exactly one position per input identifier in request
    /// order. Missing identifiers produce `None`; duplicates preserve positions
    /// while sharing one physical lookup and persisted-row decode. One batch is
    /// bounded by [`MAX_TYPED_EXACT_KEY_BATCH_ITEMS`], encoded key bytes,
    /// distinct stored-row bytes, and logical result bytes.
    pub fn get_many<E>(&self, ids: &[Id<E>]) -> Result<Vec<Option<E::Row>>, TypedQueryError>
    where
        E: EntityKey + TypedEntityAdapter,
        E::Row: Clone,
    {
        let binding = E::typed_binding(self).map_err(typed_query_error_from_binding)?;
        let result = self
            .execute_public_typed_exact_key_batch(&binding, ids)
            .map_err(TypedQueryError::Database)?
            .ok_or({
                TypedQueryError::Row(TypedRowError::Adapter(
                    crate::db::TypedAdapterError::StaleBinding,
                ))
            })?;
        let mut distinct_rows = Vec::with_capacity(result.distinct_rows.len());
        for values in result.distinct_rows {
            let row = values
                .map(|values| {
                    let row = Self::typed_exact_key_row(
                        &binding,
                        result.entity.as_str(),
                        result.columns.as_slice(),
                        values,
                    )
                    .map_err(TypedQueryError::Row)?;
                    E::decode_row(&binding, row)
                        .map_err(|error| TypedQueryError::Row(TypedRowError::Adapter(error)))
                })
                .transpose()?;
            distinct_rows.push(row);
        }
        result
            .positions
            .into_iter()
            .map(|position| {
                let index = usize::try_from(position).map_err(|_| {
                    TypedQueryError::Row(TypedRowError::Adapter(
                        crate::db::TypedAdapterError::RowShapeMismatch,
                    ))
                })?;
                distinct_rows.get(index).cloned().ok_or({
                    TypedQueryError::Row(TypedRowError::Adapter(
                        crate::db::TypedAdapterError::RowShapeMismatch,
                    ))
                })
            })
            .collect()
    }
}

/// Maximum input positions admitted by one typed exact-key batch.
pub const MAX_TYPED_EXACT_KEY_BATCH_ITEMS: usize = icydb_core::db::MAX_TYPED_EXACT_KEY_BATCH_ITEMS;

/// Maximum encoded stored-key bytes admitted before input deduplication.
pub const MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES: usize =
    icydb_core::db::MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES;

/// Maximum raw row bytes admitted across distinct stored keys.
pub const MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES: usize =
    icydb_core::db::MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES;

/// Maximum logical projection bytes admitted across original input positions.
pub const MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES: usize =
    icydb_core::db::MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES;
