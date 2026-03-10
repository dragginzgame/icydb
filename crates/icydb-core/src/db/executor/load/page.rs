//! Module: executor::load::page
//! Responsibility: materialize ordered key streams into cursor-paged load rows.
//! Does not own: access-path selection, route precedence, or query planning.
//! Boundary: shared row materialization helper used by load execution paths.

use crate::{
    db::{
        Context,
        executor::load::{CursorPage, LoadExecutor, PageCursor},
        executor::{
            BudgetedOrderedKeyStream, ExecutionKernel, OrderedKeyStream, ScalarContinuationBindings,
        },
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::AccessPlannedQuery,
        response::{EntityResponse, ProjectedRow},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

///
/// PageMaterializationRequest
///
/// Request contract for one ordered key-stream to cursor-page materialization
/// pass. Bundles logical, physical, paging, and continuation inputs so the
/// page materialization boundary is explicit and stable.
///

pub(in crate::db::executor) struct PageMaterializationRequest<'a, E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db::executor) ctx: &'a Context<'a, E>,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery<E::Key>,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) stream_order_contract_safe: bool,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Run shared load phases for an already-produced ordered key stream.
    pub(in crate::db::executor) fn materialize_key_stream_into_page(
        request: PageMaterializationRequest<'_, E>,
    ) -> Result<(CursorPage<E>, usize, usize), InternalError> {
        let PageMaterializationRequest {
            ctx,
            plan,
            predicate_slots,
            key_stream,
            scan_budget_hint,
            stream_order_contract_safe,
            consistency,
            continuation,
        } = request;

        // Phase 1: validate continuation-owned scan-budget hint preconditions.
        continuation
            .validate_load_scan_budget_hint(scan_budget_hint, stream_order_contract_safe)?;

        // Phase 2: read rows from the ordered key stream, with optional budget guard.
        let data_rows = if let Some(scan_budget) = scan_budget_hint {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
            ctx.rows_from_ordered_key_stream(&mut budgeted, consistency)?
        } else {
            ctx.rows_from_ordered_key_stream(key_stream, consistency)?
        };
        let rows_scanned = data_rows.len();
        let mut rows = Context::deserialize_rows(data_rows)?;

        // Phase 3: apply post-access pipeline and emit one cursor page.
        let page = Self::finalize_rows_into_page(plan, predicate_slots, &mut rows, continuation)?;
        let post_access_rows = page.items.len();

        Ok((page, rows_scanned, post_access_rows))
    }

    // Apply canonical post-access phases to scanned rows and assemble the cursor page.
    fn finalize_rows_into_page(
        plan: &AccessPlannedQuery<E::Key>,
        predicate_slots: Option<&PredicateProgram>,
        rows: &mut Vec<(Id<E>, E)>,
        continuation: ScalarContinuationBindings<'_>,
    ) -> Result<CursorPage<E>, InternalError> {
        let stats = ExecutionKernel::apply_post_access_with_cursor_and_compiled_predicate::<E, _, _>(
            plan,
            rows,
            continuation.post_access_cursor_boundary(),
            predicate_slots,
        )?;
        let next_cursor =
            ExecutionKernel::next_cursor_for_materialized_rows(plan, rows, &stats, continuation)?
                .map(PageCursor::Scalar);
        let projected_rows = Self::project_materialized_rows_if_needed(plan, rows.as_slice())?;
        Self::validate_projection_alignment(rows.as_slice(), projected_rows.as_deref())?;
        let items = EntityResponse::from_rows(std::mem::take(rows));

        Ok(CursorPage { items, next_cursor })
    }

    // Projection materialization must remain a pure row-wise mapping over the
    // already-ordered post-access row domain. Any cardinality/id drift can
    // corrupt continuation semantics, so fail closed on mismatches.
    pub(in crate::db::executor) fn validate_projection_alignment(
        rows: &[(Id<E>, E)],
        projected_rows: Option<&[ProjectedRow<E>]>,
    ) -> Result<(), InternalError> {
        let Some(projected_rows) = projected_rows else {
            return Ok(());
        };

        if projected_rows.len() != rows.len() {
            return Err(InternalError::query_executor_invariant(
                "projection materialization cardinality mismatch against post-access rows",
            ));
        }

        for ((row_id, _), projected_row) in rows.iter().zip(projected_rows.iter()) {
            if projected_row.id() != *row_id {
                return Err(InternalError::query_executor_invariant(
                    "projection materialization id alignment mismatch against post-access rows",
                ));
            }
        }

        Ok(())
    }
}
