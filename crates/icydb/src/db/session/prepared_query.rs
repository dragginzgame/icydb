//! Module: db::session::prepared_query
//! Responsibility: non-entity typed-query state and prepared live-page traversal.
//! Does not own: accepted schema, planning, storage execution, or typed row decoding.
//! Boundary: one accepted binding plus dynamic request -> prepared output rows.

use crate::{
    db::{
        AttributedRead, DbSession, DynamicQuery, OutputRow, PreparedOutputRows, PrimaryKeyValue,
        TypedAdapterError, TypedEntityBinding, TypedOperationError,
        session::{OutputRowProjection, live_page::prepare_live_page_step},
    },
    traits::CanisterKind,
};

/// One consumed live-page output prepared for downstream typed decoding.
#[doc(hidden)]
pub struct PreparedLivePageOutput {
    /// Owned rows sharing one accepted typed projection.
    pub rows: PreparedOutputRows,
    /// Authenticated continuation moved from the dynamic page.
    pub continuation: Option<String>,
    /// Bounded work moved from the dynamic page.
    pub work: crate::db::ScalarPageWork,
}

/// One prepared exact-key batch awaiting only typed row decoding.
#[doc(hidden)]
pub struct PreparedExactKeyOutput {
    /// One bound optional row for each distinct canonical input key.
    pub distinct_rows: Vec<Option<OutputRow>>,
    /// Distinct-row index for each original caller position.
    pub positions: Vec<u32>,
}

/// Entity-erased typed-query state and prepared live-page cursor.
///
/// Generated adapters retain only final typed row decoding. Single-page users
/// supply caller-owned continuation to [`Self::execute_page`]. All-page users
/// may instead call [`Self::next_page`], which owns continuation internally and
/// must be dropped if downstream row decoding fails.
#[doc(hidden)]
pub struct PreparedLivePageCursor<'session, C>
where
    C: CanisterKind,
{
    session: &'session DbSession<C>,
    binding: TypedEntityBinding,
    request: DynamicQuery,
    continuation: Option<String>,
    exhausted: bool,
}

impl<'session, C> PreparedLivePageCursor<'session, C>
where
    C: CanisterKind,
{
    const fn new(
        session: &'session DbSession<C>,
        binding: TypedEntityBinding,
        request: DynamicQuery,
    ) -> Self {
        Self {
            session,
            binding,
            request,
            continuation: None,
            exhausted: false,
        }
    }

    /// Borrow the accepted binding required by the final typed decode leaf.
    #[must_use]
    pub const fn binding(&self) -> &TypedEntityBinding {
        &self.binding
    }

    /// Execute one page without changing cursor-owned traversal state.
    ///
    /// The supplied continuation remains caller-owned. Move the returned
    /// continuation into caller state only after every row decodes.
    pub fn execute_page(
        &self,
        continuation: Option<&str>,
    ) -> Result<PreparedLivePageOutput, TypedOperationError> {
        let page = self
            .session
            .inner
            .execute_public_live_page_for_typed_binding(
                self.binding.inner(),
                &self.request,
                continuation,
            )
            .map_err(|error| TypedOperationError::Database(crate::Error::from(error)))?
            .ok_or_else(stale_binding_error)?;
        self.prepare_page(page, continuation)
    }

    /// Execute one explicitly authorized trusted page without changing
    /// cursor-owned traversal state.
    pub fn execute_trusted_page(
        &self,
        continuation: Option<&str>,
    ) -> Result<PreparedLivePageOutput, TypedOperationError> {
        let page = self
            .session
            .inner
            .execute_trusted_live_page(&self.request, continuation)
            .map_err(|error| TypedOperationError::Database(crate::Error::from(error)))?;
        self.prepare_page(page, continuation)
    }

    /// Execute and adopt the next cursor-owned page.
    ///
    /// A decoding failure after this method returns must discard the cursor.
    /// This keeps retry semantics explicit without retaining page traversal in
    /// each entity-generic adapter.
    #[inline(never)]
    pub fn next_page(&mut self) -> Result<Option<PreparedOutputRows>, TypedOperationError> {
        if self.exhausted {
            return Ok(None);
        }
        let prepared = self.execute_page(self.continuation.as_deref())?;
        Ok(Some(self.adopt_page(prepared)))
    }

    /// Execute and adopt the next explicitly authorized trusted page.
    ///
    /// A decoding failure after this method returns must discard the cursor.
    #[inline(never)]
    pub fn next_trusted_page(&mut self) -> Result<Option<PreparedOutputRows>, TypedOperationError> {
        if self.exhausted {
            return Ok(None);
        }
        let prepared = self.execute_trusted_page(self.continuation.as_deref())?;
        Ok(Some(self.adopt_page(prepared)))
    }

    fn adopt_page(&mut self, prepared: PreparedLivePageOutput) -> PreparedOutputRows {
        self.continuation = prepared.continuation;
        self.exhausted = self.continuation.is_none();
        prepared.rows
    }

    pub(crate) fn execute_public_page_with_attribution(
        &self,
        continuation: Option<&str>,
    ) -> Result<AttributedRead<PreparedLivePageOutput>, TypedOperationError> {
        let attributed = self
            .session
            .inner
            .execute_public_live_page_with_attribution_for_typed_binding(
                self.binding.inner(),
                &self.request,
                continuation,
            )
            .map_err(|error| TypedOperationError::Database(crate::Error::from(error)))?
            .ok_or_else(stale_binding_error)?;
        let prepare_start = read_operation_local_instruction_counter();
        let result = self.prepare_page(attributed.result, continuation)?;
        let response_decode_local_instructions =
            read_operation_local_instruction_counter().saturating_sub(prepare_start);
        let mut attribution = attributed.attribution;
        attribution.response_decode_local_instructions = response_decode_local_instructions;
        Ok(AttributedRead {
            result,
            attribution,
        })
    }

    fn prepare_page(
        &self,
        page: crate::db::LiveQueryPageOutput,
        continuation: Option<&str>,
    ) -> Result<PreparedLivePageOutput, TypedOperationError> {
        let crate::db::LiveQueryPageOutput {
            entity,
            columns,
            rows,
            row_count: _,
            continuation,
            work,
        } = prepare_live_page_step(page, continuation)?.into_page();
        let rows = self
            .session
            .prepare_typed_output_rows(&self.binding, entity, columns, rows)?;
        Ok(PreparedLivePageOutput {
            rows,
            continuation,
            work,
        })
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one bounded exact-key batch and bind its distinct output rows.
    #[doc(hidden)]
    pub fn execute_public_prepared_exact_key_batch(
        &self,
        binding: &TypedEntityBinding,
        keys: &[PrimaryKeyValue],
    ) -> Result<PreparedExactKeyOutput, TypedOperationError> {
        let output = self
            .inner
            .execute_public_exact_key_batch_for_typed_binding(binding.inner(), keys)
            .map_err(|error| TypedOperationError::Database(crate::Error::from(error)))?
            .ok_or_else(stale_binding_error)?;
        let icydb_core::db::ExactKeyBatchProjectionOutput {
            entity,
            columns,
            distinct_rows,
            positions,
        } = output;
        let projection = OutputRowProjection::new(binding, entity, columns.as_slice())
            .map_err(TypedOperationError::Adapter)?;
        let distinct_rows = distinct_rows
            .into_iter()
            .map(|values| {
                values
                    .map(|values| projection.project(values))
                    .transpose()
                    .map_err(TypedOperationError::Adapter)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PreparedExactKeyOutput {
            distinct_rows,
            positions,
        })
    }

    /// Start one caller-authorized public prepared-page cursor.
    #[doc(hidden)]
    #[must_use]
    pub const fn prepare_live_page_cursor(
        &self,
        binding: TypedEntityBinding,
        request: DynamicQuery,
    ) -> PreparedLivePageCursor<'_, C> {
        PreparedLivePageCursor::new(self, binding, request)
    }
}

const fn stale_binding_error() -> TypedOperationError {
    TypedOperationError::Adapter(TypedAdapterError::StaleBinding)
}

#[must_use]
#[cfg(target_arch = "wasm32")]
fn read_operation_local_instruction_counter() -> u64 {
    ic_cdk::api::performance_counter(1)
}

#[must_use]
#[cfg(not(target_arch = "wasm32"))]
const fn read_operation_local_instruction_counter() -> u64 {
    0
}
