//! Test-only constructors and adapters for the parent module.

use super::*;

impl AccessPlannedQuery {
    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[must_use]
    pub(in crate::db) fn new(access: AccessPath<Value>, consistency: MissingRowPolicy) -> Self {
        let access = AccessPlan::path(access);
        let logical = LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency,
        });

        Self::seeded_unfinalized(
            logical,
            access.clone(),
            ProjectionSelection::All,
            if access.has_selected_index_access_path() {
                AccessChoiceExplainSnapshot::selected_index_not_projected()
            } else {
                non_index_access_choice_snapshot_for_access_plan(&access)
            },
        )
    }

    /// Construct one minimal full-scan access-planned query under the access
    /// boundary for runtime tests that only need grouped execution shells.
    #[must_use]
    pub(in crate::db) fn full_scan_for_test(consistency: MissingRowPolicy) -> Self {
        Self::new(AccessPath::<Value>::FullScan, consistency)
    }
}
