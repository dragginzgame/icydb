//! Executor contract for a fully resolved logical plan; must not plan or validate.
#[cfg_attr(not(test), expect(unused_imports))]
use crate::{
    db::query::{
        LoadSpec, QueryMode, ReadConsistency,
        plan::{
            AccessPath, AccessPlan, CursorBoundary, CursorBoundarySlot, DeleteLimitSpec,
            OrderDirection, OrderSpec, PageSpec,
        },
        policy,
        predicate::{Predicate, coercion::canonical_cmp, eval as eval_predicate},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::cmp::Ordering;

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
pub struct LogicalPlan<K> {
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

/// Row abstraction for applying plan semantics to executor rows.
pub trait PlanRow<E: EntityKind> {
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
/// Diagnostic-only execution statistics.
/// Fields are populated but not currently consumed.
///

#[allow(dead_code)]
#[allow(clippy::struct_excessive_bools)]
pub struct PostAccessStats {
    pub(crate) filtered: bool,
    pub(crate) ordered: bool,
    pub(crate) cursor_skipped: bool,
    pub(crate) paged: bool,
    pub(crate) delete_was_limited: bool,
    pub(crate) rows_after_filter: usize,
    pub(crate) rows_after_order: usize,
    pub(crate) rows_after_cursor: usize,
    pub(crate) rows_after_page: usize,
    pub(crate) rows_after_delete_limit: usize,
}

impl<K> LogicalPlan<K> {
    /// Construct a minimal logical plan with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub const fn new(access: AccessPath<K>, consistency: ReadConsistency) -> Self {
        Self {
            mode: QueryMode::Load(LoadSpec::new()),
            access: AccessPlan::Path(access),
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
    #[expect(clippy::too_many_lines)]
    pub(crate) fn apply_post_access_with_cursor<E, R>(
        &self,
        rows: &mut Vec<R>,
        cursor: Option<&CursorBoundary>,
    ) -> Result<PostAccessStats, InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        policy::validate_plan_shape(self).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                err.invariant_message(),
            )
        })?;

        if cursor.is_some() && !self.mode.is_load() {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                "invalid logical plan: delete plans must not carry cursor boundaries".to_string(),
            ));
        }

        // Predicate (already normalized during planning).
        let filtered = if let Some(predicate) = self.predicate.as_ref() {
            // CONTRACT: predicates are validated before reaching the executor.
            rows.retain(|row| eval_predicate(row.entity(), predicate));
            true
        } else {
            false
        };
        let rows_after_filter = rows.len();

        // Ordering.
        let rows_after_order;
        let bounded_order_keep = self.bounded_order_keep_count(cursor);
        let ordered = if let Some(order) = &self.order
            && !order.fields.is_empty()
        {
            if self.predicate.is_some() && !filtered {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Query,
                    "executor invariant violated: ordering must run after filtering".to_string(),
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

            // Diagnostics keep the logical post-order cardinality, even when
            // bounded ordering trims the working set for load-page execution.
            rows_after_order = ordered_total;
            true
        } else {
            rows_after_order = rows.len();
            false
        };

        // Cursor boundary (applied after ordering, before page slicing).
        let rows_after_cursor;
        let cursor_skipped = if self.mode.is_load()
            && let Some(boundary) = cursor
        {
            let Some(order) = self.order.as_ref() else {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Query,
                    "executor invariant violated: cursor boundary requires ordering".to_string(),
                ));
            };

            if !ordered {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Query,
                    "executor invariant violated: cursor boundary must run after ordering"
                        .to_string(),
                ));
            }

            apply_cursor_boundary::<E, R>(rows, order, boundary)?;
            rows_after_cursor = rows.len();
            true
        } else {
            // No cursor boundary for this request; preserve the logical
            // post-order cardinality for stats/continuation decisions.
            rows_after_cursor = rows_after_order;
            false
        };

        // Offset/limit pagination after cursor boundary.
        let paged = if self.mode.is_load()
            && let Some(page) = &self.page
        {
            if self.order.is_some() && !ordered {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Query,
                    "executor invariant violated: pagination must run after ordering".to_string(),
                ));
            }
            apply_pagination(rows, page.offset, page.limit);
            true
        } else {
            false
        };
        let rows_after_page = rows.len();

        // Delete limit (applied after ordering).
        let delete_was_limited = if self.mode.is_delete()
            && let Some(limit) = &self.delete_limit
        {
            if self.order.is_some() && !ordered {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Query,
                    "executor invariant violated: delete limit must run after ordering".to_string(),
                ));
            }
            apply_delete_limit(rows, limit.max_rows);
            true
        } else {
            false
        };
        let rows_after_delete_limit = rows.len();

        Ok(PostAccessStats {
            filtered,
            ordered,
            cursor_skipped,
            paged,
            delete_was_limited,
            rows_after_filter,
            rows_after_order,
            rows_after_cursor,
            rows_after_page,
            rows_after_delete_limit,
        })
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
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                "executor invariant violated: cannot build cursor boundary without ordering"
                    .to_string(),
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
fn apply_cursor_boundary<E, R>(
    rows: &mut Vec<R>,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    if boundary.slots.len() != order.fields.len() {
        return Err(InternalError::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Query,
            format!(
                "invalid continuation boundary arity: expected {}, found {}",
                order.fields.len(),
                boundary.slots.len()
            ),
        ));
    }

    // Strict continuation: keep only rows greater than the boundary under canonical order.
    let mut filtered = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let ordering = compare_entity_with_boundary::<E>(row.entity(), order, boundary);
        if ordering == Ordering::Greater {
            filtered.push(row);
        }
    }
    *rows = filtered;

    Ok(())
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
#[allow(clippy::cast_possible_truncation)]
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
#[allow(clippy::cast_possible_truncation)]
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
