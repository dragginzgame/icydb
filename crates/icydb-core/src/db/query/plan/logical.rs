//! Executor contract for a fully resolved logical plan; must not plan or validate.
#[cfg_attr(not(test), expect(unused_imports))]
use crate::db::query::{
    LoadSpec, QueryMode, ReadConsistency,
    plan::{AccessPath, AccessPlan, DeleteLimitSpec, OrderSpec, PageSpec},
    predicate::{Predicate, eval as eval_predicate},
};
use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    key::Key,
    traits::EntityKind,
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
pub struct LogicalPlan {
    /// Load vs delete intent.
    pub(crate) mode: QueryMode,

    /// Storage access strategy (single path or composite).
    pub(crate) access: AccessPlan,

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

impl<E: EntityKind> PlanRow<E> for (Key, E) {
    fn entity(&self) -> &E {
        &self.1
    }
}

///
/// PostAccessStats
/// Result flags and row counts for post-access plan application.
///

#[allow(clippy::struct_excessive_bools)]
pub struct PostAccessStats {
    pub(crate) filtered: bool,
    pub(crate) ordered: bool,
    pub(crate) paged: bool,
    pub(crate) delete_limited: bool,
    pub(crate) rows_after_filter: usize,
    pub(crate) rows_after_order: usize,
    pub(crate) rows_after_page: usize,
    pub(crate) rows_after_delete_limit: usize,
}

impl LogicalPlan {
    /// Construct a minimal logical plan with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub const fn new(access: AccessPath, consistency: ReadConsistency) -> Self {
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
        E: EntityKind,
        R: PlanRow<E>,
    {
        if self.mode.is_delete() && self.page.is_some() {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                "invalid logical plan: delete plans must not carry pagination".to_string(),
            ));
        }
        if self.mode.is_load() && self.delete_limit.is_some() {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                "invalid logical plan: load plans must not carry delete limits".to_string(),
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
            if rows.len() > 1 {
                apply_order_spec::<E, R>(rows, order);
            }
            true
        } else {
            false
        };
        let rows_after_order = rows.len();

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
        let delete_limited = if self.mode.is_delete()
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
            paged,
            delete_limited,
            rows_after_filter,
            rows_after_order,
            rows_after_page,
            rows_after_delete_limit,
        })
    }
}

// Sort rows by the configured order spec, using entity field values.
fn apply_order_spec<E, R>(rows: &mut Vec<R>, order: &OrderSpec)
where
    E: EntityKind,
    R: PlanRow<E>,
{
    // Phase 1: tag rows with their original position to preserve stability.
    let mut indexed = Vec::with_capacity(rows.len());
    for (idx, row) in rows.drain(..).enumerate() {
        indexed.push((idx, row));
    }

    // Phase 2: stable ordering via position tie-breaker.
    indexed.sort_by(|(left_idx, left), (right_idx, right)| {
        let ordering = compare_entities::<E>(left.entity(), right.entity(), order);
        if ordering == Ordering::Equal {
            left_idx.cmp(right_idx)
        } else {
            ordering
        }
    });

    // Phase 3: restore the ordered rows.
    rows.extend(indexed.into_iter().map(|(_, row)| row));
}

// Compare two entities according to the order spec, returning the first non-equal field ordering.
fn compare_entities<E: EntityKind>(left: &E, right: &E, order: &OrderSpec) -> Ordering {
    for (field, direction) in &order.fields {
        let left_value = left.get_value(field);
        let right_value = right.get_value(field);

        // NOTE: Incomparable values are treated as equal so that stable sorting
        // preserves input order. This matches SQL-style ORDER BY semantics.
        let ordering = match (left_value, right_value) {
            (None, None) => continue,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left_value), Some(right_value)) => match left_value.partial_cmp(&right_value) {
                Some(ordering) => ordering,
                // Preserve relative order for incomparable values.
                None => Ordering::Equal,
            },
        };

        let ordering = match direction {
            crate::db::query::plan::OrderDirection::Asc => ordering,
            crate::db::query::plan::OrderDirection::Desc => ordering.reverse(),
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

    // If offset is past the end, clear everything
    if offset >= total {
        rows.clear();
        return;
    }

    let start = offset;
    let end = match limit {
        Some(limit) => start.saturating_add(limit).min(total),
        None => total,
    };

    // Convert once, at the boundary
    let start_usize = start as usize;
    let end_usize = end as usize;

    // Drop leading rows, then truncate to window size
    rows.drain(..start_usize);
    rows.truncate(end_usize - start_usize);
}

/// Apply a delete limit to an in-memory vector, in-place.
fn apply_delete_limit<T>(rows: &mut Vec<T>, max_rows: u32) {
    let limit = usize::min(rows.len(), max_rows as usize);
    rows.truncate(limit);
}
