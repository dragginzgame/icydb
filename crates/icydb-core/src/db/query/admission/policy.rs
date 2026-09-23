//! Module: db::query::admission::policy
//! Responsibility: read-admission policies and budget evaluation.
//! Does not own: planner summary extraction, diagnostics DTOs, or text render.
//! Boundary: applies policy to an already-built admission summary.

use std::num::NonZeroU32;

use super::{
    QueryAdmissionAccessKind, QueryAdmissionLane, QueryAdmissionRejection, QueryAdmissionSummary,
    QueryBoundKind, plan_summary,
};

pub(in crate::db::query) const DEFAULT_BOUNDED_READ_MAX_ROWS: u32 = 100;
const DEFAULT_BOUNDED_READ_MAX_GROUPS: u32 = 100;
const DEFAULT_BOUNDED_READ_MAX_GROUP_BYTES: u32 = 64 * 1024;
const DEFAULT_BOUNDED_READ_MAX_PRIMARY_KEY_INPUT_TERMS: u32 = 1024;
const DEFAULT_BOUNDED_READ_MAX_PRIMARY_KEY_INPUT_BYTES: u32 = 64 * 1024;

const fn non_zero_default(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => NonZeroU32::MIN,
    }
}

/// Grouped/aggregate read admission budgets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedAdmissionPolicy {
    groups: Option<NonZeroU32>,
    group_bytes: Option<NonZeroU32>,
}

impl GroupedAdmissionPolicy {
    /// Build a policy that rejects grouped reads.
    #[must_use]
    pub(in crate::db) const fn disabled() -> Self {
        Self {
            groups: None,
            group_bytes: None,
        }
    }

    /// Build a grouped policy with explicit group and memory budgets.
    #[must_use]
    pub(in crate::db) const fn bounded(
        max_groups: NonZeroU32,
        max_group_bytes: NonZeroU32,
    ) -> Self {
        Self {
            groups: Some(max_groups),
            group_bytes: Some(max_group_bytes),
        }
    }

    /// Build the default grouped budget used by ordinary public reads.
    ///
    /// Grouped query execution still needs matching query-owned hard limits
    /// via `grouped_limits(...)`; this policy defines the maximum values those
    /// limits may carry on the default read path.
    #[must_use]
    pub(in crate::db) const fn default_bounded_read() -> Self {
        Self::bounded(
            non_zero_default(DEFAULT_BOUNDED_READ_MAX_GROUPS),
            non_zero_default(DEFAULT_BOUNDED_READ_MAX_GROUP_BYTES),
        )
    }

    /// Return the maximum allowed output groups.
    #[must_use]
    pub(in crate::db) const fn max_groups(self) -> Option<NonZeroU32> {
        self.groups
    }

    /// Return the maximum allowed total accounted live grouped-state bytes.
    #[must_use]
    pub(in crate::db) const fn max_group_bytes(self) -> Option<NonZeroU32> {
        self.group_bytes
    }
}

/// Physical access requirements attached to one read-admission surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AccessAdmissionPolicy {
    index_required: bool,
    full_scan_allowed: bool,
    materialized_sort_allowed: bool,
}

impl AccessAdmissionPolicy {
    const BOUNDED_PUBLIC_READ: Self = Self {
        index_required: true,
        full_scan_allowed: false,
        materialized_sort_allowed: false,
    };

    #[cfg(feature = "sql")]
    const DIAGNOSTIC_EXPLAIN: Self = Self {
        index_required: false,
        full_scan_allowed: true,
        materialized_sort_allowed: true,
    };
}

/// Read-admission policy attached to one query surface.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct QueryAdmissionPolicy {
    lane: QueryAdmissionLane,
    limit_required: bool,
    max_returned_rows: Option<NonZeroU32>,
    max_primary_key_input_terms: Option<NonZeroU32>,
    max_primary_key_input_bytes: Option<NonZeroU32>,
    access: AccessAdmissionPolicy,
    grouped: GroupedAdmissionPolicy,
}

impl QueryAdmissionPolicy {
    /// Build the safe default policy for caller-facing bounded read endpoints.
    #[must_use]
    pub(in crate::db) const fn public_read(max_returned_rows: NonZeroU32) -> Self {
        Self {
            lane: QueryAdmissionLane::PublicRead,
            limit_required: true,
            max_returned_rows: Some(max_returned_rows),
            max_primary_key_input_terms: Some(non_zero_default(
                DEFAULT_BOUNDED_READ_MAX_PRIMARY_KEY_INPUT_TERMS,
            )),
            max_primary_key_input_bytes: Some(non_zero_default(
                DEFAULT_BOUNDED_READ_MAX_PRIMARY_KEY_INPUT_BYTES,
            )),
            access: AccessAdmissionPolicy::BOUNDED_PUBLIC_READ,
            grouped: GroupedAdmissionPolicy::disabled(),
        }
    }

    /// Build the default bounded policy used by ordinary typed/dynamic reads.
    ///
    /// The policy rejects full scans and queries without a proven row bound.
    /// Materialized sorts require exact bounded primary-key candidates. Scalar
    /// pages supply their envelope; authored limits cap the whole traversal.
    #[must_use]
    pub(in crate::db) const fn default_bounded_read() -> Self {
        Self::public_read(non_zero_default(DEFAULT_BOUNDED_READ_MAX_ROWS))
            .with_grouped_policy(GroupedAdmissionPolicy::default_bounded_read())
    }

    /// Return this policy with explicit grouped execution budgets attached.
    ///
    /// Public read policies still reject grouped queries unless the selected
    /// plan is executed with matching group-count and total live-state byte caps.
    #[must_use]
    pub(in crate::db) const fn with_grouped_policy(
        mut self,
        grouped: GroupedAdmissionPolicy,
    ) -> Self {
        self.grouped = grouped;
        self
    }

    /// Build an EXPLAIN-only policy that cannot execute rows.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) const fn diagnostic_explain() -> Self {
        Self {
            lane: QueryAdmissionLane::DiagnosticExplain,
            limit_required: false,
            max_returned_rows: None,
            max_primary_key_input_terms: None,
            max_primary_key_input_bytes: None,
            access: AccessAdmissionPolicy::DIAGNOSTIC_EXPLAIN,
            grouped: GroupedAdmissionPolicy::disabled(),
        }
    }

    /// Return the lane this policy governs.
    #[must_use]
    pub(in crate::db) const fn lane(&self) -> QueryAdmissionLane {
        self.lane
    }

    /// Return whether admission requires a limit or another proven row bound.
    #[must_use]
    pub(in crate::db) const fn require_limit(&self) -> bool {
        self.limit_required
    }

    /// Return whether the selected plan must use an index-backed path.
    #[must_use]
    pub(in crate::db) const fn require_index(&self) -> bool {
        self.access.index_required
    }

    /// Return whether a full entity scan may execute.
    #[must_use]
    pub(in crate::db) const fn allow_full_scan(&self) -> bool {
        self.access.full_scan_allowed
    }

    /// Return whether this surface permits materialized ORDER BY execution.
    #[must_use]
    pub(in crate::db) const fn allow_materialized_sort(&self) -> bool {
        self.access.materialized_sort_allowed
    }

    /// Apply this policy to one already-summarized plan.
    #[must_use]
    pub(in crate::db) fn evaluate(
        &self,
        mut summary: QueryAdmissionSummary,
    ) -> QueryAdmissionSummary {
        summary.lane = self.lane;

        match self.rejection_for_summary(&summary) {
            Some(rejection) => summary.reject(rejection),
            None => summary.admit(),
        }
    }

    fn rejection_for_summary(
        &self,
        summary: &QueryAdmissionSummary,
    ) -> Option<QueryAdmissionRejection> {
        if !self.lane.executes_rows() {
            return Some(QueryAdmissionRejection::DiagnosticLaneDoesNotExecute);
        }

        if let Some(rejection) = self.grouped_rejection(summary) {
            return Some(rejection);
        }

        if !self.allow_full_scan() && summary.selected_access().is_full_scan() {
            return Some(QueryAdmissionRejection::UnboundedFullScanRejected);
        }

        if self.require_index()
            && !plan_summary::access_satisfies_index_requirement(
                summary.selected_access(),
                summary.scan_bound(),
            )
        {
            return Some(QueryAdmissionRejection::PublicQueryRequiresIndex);
        }

        if self.require_limit()
            && summary.limit().is_none()
            && summary.grouped().is_none()
            && !summary.returned_row_bound_kind().admits_public_read()
        {
            return Some(QueryAdmissionRejection::PublicQueryRequiresLimit);
        }

        if let Some(rejection) = self.returned_row_bound_rejection(summary) {
            return Some(rejection);
        }

        if let Some(rejection) = self.primary_key_input_rejection(summary) {
            return Some(rejection);
        }

        self.materialization_rejection(summary)
    }

    fn grouped_rejection(
        &self,
        summary: &QueryAdmissionSummary,
    ) -> Option<QueryAdmissionRejection> {
        let grouped = summary.grouped()?;
        let Some(max_groups) = self.grouped.max_groups() else {
            return Some(QueryAdmissionRejection::GroupedQueryRequiresLimits);
        };
        let Some(max_group_bytes) = self.grouped.max_group_bytes() else {
            return Some(QueryAdmissionRejection::GroupedQueryRequiresLimits);
        };

        if grouped.max_groups() == u64::MAX || grouped.max_group_bytes() == u64::MAX {
            return Some(QueryAdmissionRejection::GroupedQueryRequiresLimits);
        }

        if grouped.max_groups() > u64::from(max_groups.get())
            || grouped.max_group_bytes() > u64::from(max_group_bytes.get())
        {
            return Some(QueryAdmissionRejection::GroupedQueryExceedsBudget);
        }

        None
    }

    fn returned_row_bound_rejection(
        &self,
        summary: &QueryAdmissionSummary,
    ) -> Option<QueryAdmissionRejection> {
        let max_returned_rows = self.max_returned_rows?;

        let Some(returned_row_bound) = summary
            .returned_row_bound()
            .filter(|_| summary.returned_row_bound_kind().admits_public_read())
        else {
            return Some(QueryAdmissionRejection::PublicQueryRequiresLimit);
        };

        if returned_row_bound > max_returned_rows.get() {
            return Some(QueryAdmissionRejection::ReturnedRowBoundExceedsPolicy);
        }

        None
    }

    const fn primary_key_input_rejection(
        &self,
        summary: &QueryAdmissionSummary,
    ) -> Option<QueryAdmissionRejection> {
        if let (Some(bound), Some(max)) = (
            summary.primary_key_input_terms(),
            self.max_primary_key_input_terms,
        ) && bound > max.get()
        {
            return Some(QueryAdmissionRejection::PrimaryKeyInputExceedsPolicy);
        }

        if let (Some(bound), Some(max)) = (
            summary.primary_key_input_payload_bytes(),
            self.max_primary_key_input_bytes,
        ) && bound > max.get()
        {
            return Some(QueryAdmissionRejection::PrimaryKeyInputExceedsPolicy);
        }

        None
    }

    fn materialization_rejection(
        &self,
        summary: &QueryAdmissionSummary,
    ) -> Option<QueryAdmissionRejection> {
        if !self.allow_materialized_sort()
            && summary.materialization().materialized_sort()
            && !primary_key_materialized_sort_has_exact_candidate_bound(summary)
        {
            return Some(QueryAdmissionRejection::SortRequiresMaterialization);
        }

        None
    }
}

fn primary_key_materialized_sort_has_exact_candidate_bound(
    summary: &QueryAdmissionSummary,
) -> bool {
    if !matches!(
        summary.selected_access(),
        QueryAdmissionAccessKind::ByKey | QueryAdmissionAccessKind::ByKeys
    ) {
        return false;
    }
    if !matches!(summary.scan_bound_kind(), QueryBoundKind::Exact) {
        return false;
    }
    if !summary
        .materialization()
        .row_bound_kind()
        .admits_public_read()
    {
        return false;
    }

    match (
        summary.scan_bound(),
        summary.materialization().materialized_rows(),
    ) {
        (Some(scan_bound), Some(materialized_rows)) => u64::from(materialized_rows) == scan_bound,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::QueryAdmissionPolicy;
    use crate::db::query::admission::input::{
        MAX_QUERY_INPUT_BYTES, MAX_QUERY_INPUT_DEPTH, MAX_QUERY_INPUT_NODES,
    };
    use std::{collections::BTreeMap, num::NonZeroU32};

    #[test]
    fn documentation_resource_limits_match_compiled_owners() {
        // Read checkout data at test time; library builds do not embed docs.
        // Only keys and numbers are contractual, not table order or prose.
        let document = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/contracts/RESOURCE_MODEL.md"
        ))
        .expect("repository resource model");
        let section = document
            .split_once("<!-- icydb-read-resource-limits:start -->")
            .expect("resource data start")
            .1
            .split_once("<!-- icydb-read-resource-limits:end -->")
            .expect("resource data end")
            .0;
        let mut documented = BTreeMap::new();
        for line in section.lines() {
            let mut cells = line.split('|').skip(1).map(str::trim);
            let Some(key) = cells
                .next()
                .and_then(|cell| cell.strip_prefix('`'))
                .and_then(|cell| cell.strip_suffix('`'))
            else {
                continue;
            };
            let value = cells
                .next()
                .expect("numeric data cell")
                .parse::<u64>()
                .expect("numeric resource limit");
            assert!(documented.insert(key, value).is_none(), "duplicate limit");
        }
        let policy = QueryAdmissionPolicy::default_bounded_read();
        let value = |limit: Option<NonZeroU32>| u64::from(limit.expect("bounded policy").get());
        let expected = BTreeMap::from([
            ("max_returned_rows", value(policy.max_returned_rows)),
            (
                "max_primary_key_input_terms",
                value(policy.max_primary_key_input_terms),
            ),
            (
                "max_primary_key_input_bytes",
                value(policy.max_primary_key_input_bytes),
            ),
            ("max_groups", value(policy.grouped.max_groups())),
            ("max_group_bytes", value(policy.grouped.max_group_bytes())),
            (
                "max_input_depth",
                u64::try_from(MAX_QUERY_INPUT_DEPTH).expect("input depth"),
            ),
            (
                "max_input_nodes",
                u64::try_from(MAX_QUERY_INPUT_NODES).expect("input nodes"),
            ),
            (
                "max_input_bytes",
                u64::try_from(MAX_QUERY_INPUT_BYTES).expect("input bytes"),
            ),
        ]);
        assert_eq!(documented, expected);
    }

    #[test]
    fn public_read_keeps_bounded_access_requirements() {
        let policy = QueryAdmissionPolicy::public_read(NonZeroU32::MIN);

        assert!(policy.require_limit());
        assert!(policy.require_index());
        assert!(!policy.allow_full_scan());
        assert!(!policy.allow_materialized_sort());
    }

    #[cfg(feature = "sql")]
    #[test]
    fn diagnostic_explain_keeps_non_executing_access_permissions() {
        let policy = QueryAdmissionPolicy::diagnostic_explain();

        assert!(!policy.require_limit());
        assert!(!policy.require_index());
        assert!(policy.allow_full_scan());
        assert!(policy.allow_materialized_sort());
    }
}
