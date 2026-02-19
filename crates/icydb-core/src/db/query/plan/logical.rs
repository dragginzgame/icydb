//! Executor contract for a fully resolved logical plan; must not plan or validate.
use crate::{
    db::query::{
        ReadConsistency,
        intent::QueryMode,
        plan::{
            AccessPlan, CursorBoundary, CursorBoundarySlot, DeleteLimitSpec, OrderDirection,
            OrderSpec, PageSpec,
        },
        predicate::{Predicate, coercion::canonical_cmp, eval as eval_predicate},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::cmp::Ordering;

#[cfg(test)]
use crate::db::query::{intent::LoadSpec, plan::AccessPath};

///
/// LogicalPlan
///
/// Executor-ready query plan produced by the planner.
///
/// A `LogicalPlan` represents the *complete, linearized execution intent*
/// for a query. All schema validation, predicate normalization, coercion
/// checks, and access-path selection have already occurred by the time a
/// `LogicalPlan` is constructed.
///
/// Design notes:
/// - Access may be a single path or a composite (union/intersection) of paths
/// - Predicates are applied *after* data access
/// - Ordering is applied after filtering
/// - Pagination is applied after ordering (load only)
/// - Delete limits are applied after ordering (delete only)
/// - Missing-row policy is explicit and must not depend on access path
///
/// This struct is the explicit contract between the planner and executors.
/// Executors must be able to execute any valid `LogicalPlan` without
/// additional planning or schema access.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalPlan<K> {
    /// Load vs delete intent.
    pub(crate) mode: QueryMode,

    /// Storage access strategy (single path or composite).
    pub(crate) access: AccessPlan<K>,

    /// Optional residual predicate applied after access.
    pub(crate) predicate: Option<Predicate>,

    /// Optional ordering specification.
    pub(crate) order: Option<OrderSpec>,

    /// Optional delete bound (delete intents only).
    pub(crate) delete_limit: Option<DeleteLimitSpec>,

    /// Optional pagination specification.
    pub(crate) page: Option<PageSpec>,

    /// Missing-row policy for execution.
    pub(crate) consistency: ReadConsistency,
}

///
/// PlanRow
/// Row abstraction for applying plan semantics to executor rows.
///

pub(crate) trait PlanRow<E: EntityKind> {
    fn entity(&self) -> &E;
}

impl<E: EntityKind> PlanRow<E> for (Id<E>, E) {
    fn entity(&self) -> &E {
        &self.1
    }
}

///
/// PostAccessStats
///
/// Post-access execution statistics.
///
/// Runtime currently consumes only:
/// - `rows_after_cursor` for continuation decisions
/// - `delete_was_limited` for delete diagnostics
///
/// Additional phase-level fields are compiled in tests for structural assertions.
///

#[cfg_attr(test, allow(dead_code))]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct PostAccessStats {
    pub(crate) delete_was_limited: bool,
    pub(crate) rows_after_cursor: usize,
    #[cfg(test)]
    pub(crate) filtered: bool,
    #[cfg(test)]
    pub(crate) ordered: bool,
    #[cfg(test)]
    pub(crate) paged: bool,
    #[cfg(test)]
    pub(crate) rows_after_filter: usize,
    #[cfg(test)]
    pub(crate) rows_after_order: usize,
    #[cfg(test)]
    pub(crate) rows_after_page: usize,
    #[cfg(test)]
    pub(crate) rows_after_delete_limit: usize,
}

impl<K> LogicalPlan<K> {
    /// Construct a minimal logical plan with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(access: AccessPath<K>, consistency: ReadConsistency) -> Self {
        Self {
            mode: QueryMode::Load(LoadSpec::new()),
            access: AccessPlan::path(access),
            predicate: None,
            order: None,
            delete_limit: None,
            page: None,
            consistency,
        }
    }

    /// Apply predicate, ordering, and pagination in plan order.
    pub(crate) fn apply_post_access<E, R>(
        &self,
        rows: &mut Vec<R>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        self.apply_post_access_with_cursor::<E, R>(rows, None)
    }

    /// Apply predicate, ordering, cursor boundary, and pagination in plan order.
    pub(crate) fn apply_post_access_with_cursor<E, R>(
        &self,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        self.validate_post_access_invariants()?;
        self.validate_cursor_mode(cursor)?;

        // Phase 1: predicate filtering.
        let (filtered, rows_after_filter) = self.apply_filter_phase::<E, R>(rows);

        // Phase 2: ordering.
        let (ordered, rows_after_order) = self.apply_order_phase::<E, R>(rows, cursor, filtered)?;

        // Phase 3: continuation boundary.
        let (_cursor_skipped, rows_after_cursor) =
            self.apply_cursor_phase::<E, R>(rows, cursor, ordered, rows_after_order)?;

        // Phase 4: load pagination.
        let (paged, rows_after_page) = self.apply_page_phase(rows, ordered)?;

        // Phase 5: delete limiting.
        let (delete_was_limited, rows_after_delete_limit) =
            self.apply_delete_limit_phase(rows, ordered)?;

        #[cfg(not(test))]
        let _ = rows_after_filter;
        #[cfg(not(test))]
        let _ = paged;
        #[cfg(not(test))]
        let _ = rows_after_page;
        #[cfg(not(test))]
        let _ = rows_after_delete_limit;

        Ok(PostAccessStats {
            delete_was_limited,
            rows_after_cursor,
            #[cfg(test)]
            filtered,
            #[cfg(test)]
            ordered,
            #[cfg(test)]
            paged,
            #[cfg(test)]
            rows_after_filter,
            #[cfg(test)]
            rows_after_order,
            #[cfg(test)]
            rows_after_page,
            #[cfg(test)]
            rows_after_delete_limit,
        })
    }

    // Guard post-access execution with internal plan-shape invariants.
    // Planner owns user-facing validation; this only catches internal misuse.
    fn validate_post_access_invariants(&self) -> Result<(), InternalError> {
        if self.mode.is_load() && self.delete_limit.is_some() {
            return Err(InternalError::query_invariant(
                "executor invariant violated: load plans must not carry delete limits",
            ));
        }

        if self.mode.is_delete() && self.page.is_some() {
            return Err(InternalError::query_invariant(
                "executor invariant violated: delete plans must not carry pagination",
            ));
        }

        let has_explicit_order = self
            .order
            .as_ref()
            .is_some_and(|order| !order.fields.is_empty());
        if self.page.is_some() && !has_explicit_order {
            return Err(InternalError::query_invariant(
                "executor invariant violated: pagination requires explicit ordering",
            ));
        }

        if self.delete_limit.is_some() && !has_explicit_order {
            return Err(InternalError::query_invariant(
                "executor invariant violated: delete limit requires explicit ordering",
            ));
        }

        Ok(())
    }

    // Enforce load/delete cursor compatibility before execution phases.
    fn validate_cursor_mode(&self, cursor: Option<&CursorBoundary>) -> Result<(), InternalError> {
        if cursor.is_some() && !self.mode.is_load() {
            return Err(InternalError::query_invariant(
                "invalid logical plan: delete plans must not carry cursor boundaries",
            ));
        }

        Ok(())
    }

    // Predicate phase (already normalized and validated during planning).
    fn apply_filter_phase<E, R>(&self, rows: &mut Vec<R>) -> (bool, usize)
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        let filtered = if let Some(predicate) = self.predicate.as_ref() {
            rows.retain(|row| eval_predicate(row.entity(), predicate));
            true
        } else {
            false
        };

        (filtered, rows.len())
    }

    // Ordering phase with bounded-load optimization for first-page load paths.
    fn apply_order_phase<E, R>(
        &self,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
        filtered: bool,
    ) -> Result<(bool, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        let bounded_order_keep = self.bounded_order_keep_count(cursor);
        if let Some(order) = &self.order
            && !order.fields.is_empty()
        {
            if self.predicate.is_some() && !filtered {
                return Err(InternalError::query_invariant(
                    "executor invariant violated: ordering must run after filtering",
                ));
            }

            let ordered_total = rows.len();
            if rows.len() > 1 {
                if let Some(keep_count) = bounded_order_keep {
                    apply_order_spec_bounded::<E, R>(rows, order, keep_count);
                } else {
                    apply_order_spec::<E, R>(rows, order);
                }
            }

            // Keep logical post-order cardinality even when bounded ordering
            // trims the working set for load-page execution.
            return Ok((true, ordered_total));
        }

        Ok((false, rows.len()))
    }

    // Continuation phase (strictly after ordering, before pagination).
    fn apply_cursor_phase<E, R>(
        &self,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
        ordered: bool,
        rows_after_order: usize,
    ) -> Result<(bool, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        if self.mode.is_load()
            && let Some(boundary) = cursor
        {
            let Some(order) = self.order.as_ref() else {
                return Err(InternalError::query_invariant(
                    "executor invariant violated: cursor boundary requires ordering",
                ));
            };

            if !ordered {
                return Err(InternalError::query_invariant(
                    "executor invariant violated: cursor boundary must run after ordering",
                ));
            }

            apply_cursor_boundary::<E, R>(rows, order, boundary);
            return Ok((true, rows.len()));
        }

        // No cursor boundary; preserve post-order cardinality for continuation
        // decisions and diagnostics.
        Ok((false, rows_after_order))
    }

    // Load pagination phase (offset/limit).
    fn apply_page_phase<R>(
        &self,
        rows: &mut Vec<R>,
        ordered: bool,
    ) -> Result<(bool, usize), InternalError> {
        let paged = if self.mode.is_load()
            && let Some(page) = &self.page
        {
            if self.order.is_some() && !ordered {
                return Err(InternalError::query_invariant(
                    "executor invariant violated: pagination must run after ordering",
                ));
            }
            apply_pagination(rows, page.offset, page.limit);
            true
        } else {
            false
        };

        Ok((paged, rows.len()))
    }

    // Delete-limit phase (after ordering).
    fn apply_delete_limit_phase<R>(
        &self,
        rows: &mut Vec<R>,
        ordered: bool,
    ) -> Result<(bool, usize), InternalError> {
        let delete_was_limited = if self.mode.is_delete()
            && let Some(limit) = &self.delete_limit
        {
            if self.order.is_some() && !ordered {
                return Err(InternalError::query_invariant(
                    "executor invariant violated: delete limit must run after ordering",
                ));
            }
            apply_delete_limit(rows, limit.max_rows);
            true
        } else {
            false
        };

        Ok((delete_was_limited, rows.len()))
    }

    // Return the bounded working-set size for ordered loads without a
    // continuation boundary. This keeps canonical semantics while avoiding a
    // full sort when only one page window (+1 to detect continuation) is
    // needed.
    fn bounded_order_keep_count(&self, cursor: Option<&CursorBoundary>) -> Option<usize> {
        if !self.mode.is_load() || cursor.is_some() {
            return None;
        }

        let page = self.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return None;
        }

        let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);

        Some(offset.saturating_add(limit).saturating_add(1))
    }

    /// Build a cursor boundary from one materialized entity using this plan's canonical ordering.
    pub(crate) fn cursor_boundary_from_entity<E>(
        &self,
        entity: &E,
    ) -> Result<CursorBoundary, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let Some(order) = self.order.as_ref() else {
            return Err(InternalError::query_invariant(
                "executor invariant violated: cannot build cursor boundary without ordering",
            ));
        };

        Ok(CursorBoundary {
            slots: order
                .fields
                .iter()
                .map(|(field, _)| field_slot(entity, field))
                .collect(),
        })
    }
}

// Sort rows by the configured order spec, using entity field values.
fn apply_order_spec<E, R>(rows: &mut [R], order: &OrderSpec)
where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    // Canonical order already includes the PK tie-break; comparator equality should only occur
    // for semantically equal rows. Avoid positional tie-breakers so cursor-boundary comparison can
    // share this exact ordering contract.
    rows.sort_by(|left, right| compare_entities::<E>(left.entity(), right.entity(), order));
}

// Bounded ordering for first-page loads.
// We select the smallest `keep_count` rows under canonical order and then sort
// only that prefix. This preserves output and continuation behavior.
fn apply_order_spec_bounded<E, R>(rows: &mut Vec<R>, order: &OrderSpec, keep_count: usize)
where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    if keep_count == 0 {
        rows.clear();
        return;
    }

    if rows.len() > keep_count {
        // Partition around the last element we want to keep.
        // After this call, `0..keep_count` contains the canonical top-k set
        // (unsorted), which we then sort deterministically.
        rows.select_nth_unstable_by(keep_count - 1, |left, right| {
            compare_entities::<E>(left.entity(), right.entity(), order)
        });
        rows.truncate(keep_count);
    }

    apply_order_spec::<E, R>(rows, order);
}

// Compare two entities according to the order spec, returning the first non-equal field ordering.
fn compare_entities<E: EntityKind + EntityValue>(
    left: &E,
    right: &E,
    order: &OrderSpec,
) -> Ordering {
    for (field, direction) in &order.fields {
        let left_slot = field_slot(left, field);
        let right_slot = field_slot(right, field);
        let ordering = compare_order_slots(&left_slot, &right_slot);

        let ordering = match direction {
            OrderDirection::Asc => ordering,
            OrderDirection::Desc => ordering.reverse(),
        };

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

// Convert one field value into the explicit ordering slot used for deterministic comparisons.
fn field_slot<E: EntityKind + EntityValue>(entity: &E, field: &str) -> CursorBoundarySlot {
    match entity.get_value(field) {
        Some(value) => CursorBoundarySlot::Present(value),
        None => CursorBoundarySlot::Missing,
    }
}

// Compare ordering slots using the same semantics used by query ordering:
// - Missing values sort lower than present values in ascending order
// - Present values use canonical value ordering
fn compare_order_slots(left: &CursorBoundarySlot, right: &CursorBoundarySlot) -> Ordering {
    match (left, right) {
        (CursorBoundarySlot::Missing, CursorBoundarySlot::Missing) => Ordering::Equal,
        (CursorBoundarySlot::Missing, CursorBoundarySlot::Present(_)) => Ordering::Less,
        (CursorBoundarySlot::Present(_), CursorBoundarySlot::Missing) => Ordering::Greater,
        (CursorBoundarySlot::Present(left_value), CursorBoundarySlot::Present(right_value)) => {
            canonical_cmp(left_value, right_value)
        }
    }
}

// Apply a strict continuation boundary using the canonical order comparator.
fn apply_cursor_boundary<E, R>(rows: &mut Vec<R>, order: &OrderSpec, boundary: &CursorBoundary)
where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    debug_assert_eq!(
        boundary.slots.len(),
        order.fields.len(),
        "continuation boundary arity is validated by the cursor spine",
    );

    // Strict continuation: keep only rows greater than the boundary under canonical order.
    rows.retain(|row| compare_entity_with_boundary::<E>(row.entity(), order, boundary).is_gt());
}

// Compare an entity with a continuation boundary using the exact canonical ordering semantics.
fn compare_entity_with_boundary<E: EntityKind + EntityValue>(
    entity: &E,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Ordering {
    for ((field, direction), boundary_slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let entity_slot = field_slot(entity, field);
        let ordering = compare_order_slots(&entity_slot, boundary_slot);
        let ordering = match direction {
            OrderDirection::Asc => ordering,
            OrderDirection::Desc => ordering.reverse(),
        };

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

/// Apply offset/limit pagination to an in-memory vector, in-place.
///
/// - `offset` and `limit` are logical (u32) pagination parameters
/// - Conversion to `usize` happens only at the indexing boundary
#[expect(clippy::cast_possible_truncation)]
fn apply_pagination<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let total: u32 = rows.len() as u32;

    // If offset is past the end, clear everything.
    if offset >= total {
        rows.clear();
        return;
    }

    let start = offset;
    let end = match limit {
        Some(limit) => start.saturating_add(limit).min(total),
        None => total,
    };

    // Convert once, at the boundary.
    let start_usize = start as usize;
    let end_usize = end as usize;

    // Drop leading rows, then truncate to window size.
    rows.drain(..start_usize);
    rows.truncate(end_usize - start_usize);
}

// Apply a delete limit to an in-memory vector, in-place.
fn apply_delete_limit<T>(rows: &mut Vec<T>, max_rows: u32) {
    let limit = usize::min(rows.len(), max_rows as usize);
    rows.truncate(limit);
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_order_keep_count_includes_offset_for_non_cursor_page() {
        let mut plan = LogicalPlan::new(AccessPath::<u64>::FullScan, ReadConsistency::MissingOk);
        plan.page = Some(PageSpec {
            limit: Some(5),
            offset: 3,
        });

        assert_eq!(
            plan.bounded_order_keep_count(None),
            Some(9),
            "bounded ordering should keep offset + limit + 1 rows"
        );
    }

    #[test]
    fn bounded_order_keep_count_disabled_when_cursor_present() {
        let mut plan = LogicalPlan::new(AccessPath::<u64>::FullScan, ReadConsistency::MissingOk);
        plan.page = Some(PageSpec {
            limit: Some(5),
            offset: 0,
        });
        let cursor = CursorBoundary { slots: Vec::new() };

        assert_eq!(
            plan.bounded_order_keep_count(Some(&cursor)),
            None,
            "bounded ordering should be disabled for continuation requests"
        );
    }
}
