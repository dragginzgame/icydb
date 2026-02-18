use crate::{
    db::{
        Context,
        executor::load::{FastLoadResult, LoadExecutor},
        index::RawIndexKey,
        query::plan::{
            ContinuationSignature, CursorBoundary, Direction, LogicalPlan, OrderDirection,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

/// IndexRangeLimitSpec
/// Canonical LIMIT pushdown sizing for `IndexRange` execution.
/// Centralizes fetch math so ASC/DESC work can reuse one policy point.
struct IndexRangeLimitSpec {
    effective_fetch: usize,
    needs_extra_row: bool,
    is_cursor_mode: bool,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Limited IndexRange pushdown for semantically safe plan shapes.
    pub(super) fn try_execute_index_range_limit_pushdown_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Result<Option<FastLoadResult<E>>, InternalError> {
        let Some(limit_spec) = Self::assess_index_range_limit_pushdown(plan, cursor_boundary)
        else {
            return Ok(None);
        };
        if limit_spec.is_cursor_mode && index_range_anchor.is_none() {
            return Ok(None);
        }
        debug_assert!(!limit_spec.needs_extra_row || limit_spec.effective_fetch > 0);

        let Some((index, prefix, lower, upper)) = plan.access.as_index_range_path() else {
            return Ok(None);
        };

        // Phase 1: resolve candidate keys via bounded range traversal with early termination.
        let ordered_keys = ctx.db.with_store_registry(|reg| {
            reg.try_get_store(index.store).and_then(|store| {
                store.with_index(|index_store| {
                    index_store.resolve_data_values_in_range_limited::<E>(
                        index,
                        prefix,
                        (lower, upper),
                        index_range_anchor,
                        direction,
                        limit_spec.effective_fetch,
                    )
                })
            })
        })?;
        let rows_scanned = ordered_keys.len();

        // Phase 2: load rows preserving traversal order.
        let data_rows = ctx.rows_from_ordered_data_keys(&ordered_keys, plan.consistency)?;
        let mut rows = Context::deserialize_rows(data_rows)?;

        // Phase 3: apply canonical post-access semantics and derive continuation.
        let page = Self::finalize_rows_into_page(
            plan,
            &mut rows,
            cursor_boundary,
            direction,
            continuation_signature,
        )?;

        Ok(Some(FastLoadResult {
            post_access_rows: page.items.0.len(),
            page,
            rows_scanned,
        }))
    }

    fn assess_index_range_limit_pushdown(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Option<IndexRangeLimitSpec> {
        let (index_fields, prefix_len) = plan
            .access
            .as_index_range_path()
            .map(|(index, prefix, _, _)| (index.fields, prefix.len()))?;
        if plan.predicate.is_some() {
            return None;
        }

        if let Some(order) = plan.order.as_ref()
            && !order.fields.is_empty()
        {
            if order
                .fields
                .iter()
                .any(|(_, direction)| !matches!(direction, OrderDirection::Asc))
            {
                return None;
            }

            let mut expected =
                Vec::with_capacity(index_fields.len().saturating_sub(prefix_len) + 1);
            expected.extend(index_fields.iter().skip(prefix_len).copied());
            expected.push(E::MODEL.primary_key.name);
            if order.fields.len() != expected.len() {
                return None;
            }
            if !order
                .fields
                .iter()
                .map(|(field, _)| field.as_str())
                .eq(expected)
            {
                return None;
            }
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        let is_cursor_mode = cursor_boundary.is_some();
        if limit == 0 {
            return Some(IndexRangeLimitSpec {
                effective_fetch: 0,
                needs_extra_row: false,
                is_cursor_mode,
            });
        }

        let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let page_end = offset.saturating_add(limit);
        let needs_extra_row = true;
        let effective_fetch = page_end.saturating_add(usize::from(needs_extra_row));

        Some(IndexRangeLimitSpec {
            effective_fetch,
            needs_extra_row,
            is_cursor_mode,
        })
    }
}
