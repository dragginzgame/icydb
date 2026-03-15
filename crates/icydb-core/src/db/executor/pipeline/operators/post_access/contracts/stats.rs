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

#[cfg_attr(test, expect(dead_code, clippy::struct_excessive_bools))]
pub(in crate::db::executor) struct PostAccessStats {
    pub(in crate::db::executor) delete_was_limited: bool,
    pub(in crate::db::executor) rows_after_cursor: usize,
    #[cfg(test)]
    pub(in crate::db::executor) filtered: bool,
    #[cfg(test)]
    pub(in crate::db::executor) ordered: bool,
    #[cfg(test)]
    pub(in crate::db::executor) paged: bool,
    #[cfg(test)]
    pub(in crate::db::executor) rows_after_filter: usize,
    #[cfg(test)]
    pub(in crate::db::executor) rows_after_order: usize,
    #[cfg(test)]
    pub(in crate::db::executor) rows_after_page: usize,
    #[cfg(test)]
    pub(in crate::db::executor) rows_after_delete_limit: usize,
}

///
/// BudgetSafetyMetadata
///
/// Executor-facing plan metadata for guarded scan-budget eligibility checks.
/// This metadata keeps budget-safety predicates explicit at the plan boundary.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(in crate::db::executor) struct BudgetSafetyMetadata {
    pub(in crate::db::executor) has_residual_filter: bool,
    pub(in crate::db::executor) access_order_satisfied_by_path: bool,
    pub(in crate::db::executor) requires_post_access_sort: bool,
}
