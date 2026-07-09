//! Module: db::query::admission::policy
//! Responsibility: read-admission policies and budget evaluation.
//! Does not own: planner summary extraction, diagnostics DTOs, or text render.
//! Boundary: applies policy to an already-built admission summary.

use std::num::{NonZeroU32, NonZeroU64};

use super::{
    QueryAdmissionAccessKind, QueryAdmissionLane, QueryAdmissionPlanShape, QueryAdmissionRejection,
    QueryAdmissionSummary, QueryBoundKind, plan_summary,
};

pub(in crate::db::query) const DEFAULT_BOUNDED_READ_MAX_ROWS: u32 = 100;
pub(in crate::db::query) const DEFAULT_BOUNDED_READ_RESPONSE_BYTES: u32 = 128 * 1024;
const DEFAULT_BOUNDED_READ_MAX_GROUPS: u32 = 100;
const DEFAULT_BOUNDED_READ_MAX_GROUP_BYTES: u32 = 64 * 1024;
const DEFAULT_BOUNDED_READ_MAX_DISTINCT_ENTRIES: u32 = 1024;
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
    distinct_entries: Option<NonZeroU32>,
}

impl GroupedAdmissionPolicy {
    /// Build a policy that rejects grouped reads unless a later slice enables them.
    #[must_use]
    pub(in crate::db) const fn disabled() -> Self {
        Self {
            groups: None,
            group_bytes: None,
            distinct_entries: None,
        }
    }

    /// Build a grouped policy with explicit group and memory budgets.
    #[must_use]
    pub(in crate::db) const fn bounded(
        max_groups: NonZeroU32,
        max_group_bytes: NonZeroU32,
        max_distinct_entries: Option<NonZeroU32>,
    ) -> Self {
        Self {
            groups: Some(max_groups),
            group_bytes: Some(max_group_bytes),
            distinct_entries: max_distinct_entries,
        }
    }

    /// Build the default grouped budget used by ordinary typed/fluent reads.
    ///
    /// Grouped query execution still needs matching query-owned hard limits
    /// via `grouped_limits(...)`; this policy defines the maximum values those
    /// limits may carry on the default read path.
    #[must_use]
    pub(in crate::db) const fn default_bounded_read() -> Self {
        Self::bounded(
            non_zero_default(DEFAULT_BOUNDED_READ_MAX_GROUPS),
            non_zero_default(DEFAULT_BOUNDED_READ_MAX_GROUP_BYTES),
            Some(non_zero_default(DEFAULT_BOUNDED_READ_MAX_DISTINCT_ENTRIES)),
        )
    }

    /// Return the maximum allowed output groups.
    #[must_use]
    pub(in crate::db) const fn max_groups(&self) -> Option<NonZeroU32> {
        self.groups
    }

    /// Return the maximum allowed bytes per group accumulator.
    #[must_use]
    pub(in crate::db) const fn max_group_bytes(&self) -> Option<NonZeroU32> {
        self.group_bytes
    }

    /// Return the maximum allowed distinct entries for distinct-style aggregates.
    #[must_use]
    pub(in crate::db) const fn max_distinct_entries(&self) -> Option<NonZeroU32> {
        self.distinct_entries
    }

    /// Return whether grouped execution has the minimum hard budgets admission needs.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn has_hard_limits(&self) -> bool {
        self.groups.is_some() && self.group_bytes.is_some()
    }

    /// Project this admission policy into grouped execution caps.
    #[must_use]
    #[cfg(all(test, feature = "sql"))]
    pub(in crate::db) const fn execution_config(
        &self,
    ) -> Option<crate::db::query::plan::GroupedExecutionConfig> {
        match (self.groups, self.group_bytes) {
            (Some(groups), Some(group_bytes)) => Some(
                crate::db::query::plan::GroupedExecutionConfig::with_hard_limits(
                    groups.get() as u64,
                    group_bytes.get() as u64,
                ),
            ),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LimitRequirement {
    Required,
    Optional,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndexRequirement {
    Required,
    Optional,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FullScanPolicy {
    Allow,
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaterializedSortPolicy {
    Allow,
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OffsetPolicy {
    Allow,
    RejectNonZero,
}

/// Read-admission policy attached to one query surface.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct QueryAdmissionPolicy {
    lane: QueryAdmissionLane,
    limit_requirement: LimitRequirement,
    max_returned_rows: Option<NonZeroU32>,
    max_scanned_rows: Option<NonZeroU64>,
    max_response_bytes: Option<NonZeroU32>,
    max_primary_key_input_terms: Option<NonZeroU32>,
    max_primary_key_input_bytes: Option<NonZeroU32>,
    index_requirement: IndexRequirement,
    offset_policy: OffsetPolicy,
    full_scan_policy: FullScanPolicy,
    materialized_sort_policy: MaterializedSortPolicy,
    max_materialized_rows: Option<NonZeroU32>,
    max_projection_columns: Option<NonZeroU32>,
    grouped: GroupedAdmissionPolicy,
}

impl QueryAdmissionPolicy {
    /// Build the safe default policy for caller-facing bounded read endpoints.
    #[must_use]
    pub(in crate::db) const fn public_read(
        max_returned_rows: NonZeroU32,
        max_response_bytes: NonZeroU32,
    ) -> Self {
        Self {
            lane: QueryAdmissionLane::PublicRead,
            limit_requirement: LimitRequirement::Required,
            max_returned_rows: Some(max_returned_rows),
            max_scanned_rows: None,
            max_response_bytes: Some(max_response_bytes),
            max_primary_key_input_terms: Some(non_zero_default(
                DEFAULT_BOUNDED_READ_MAX_PRIMARY_KEY_INPUT_TERMS,
            )),
            max_primary_key_input_bytes: Some(non_zero_default(
                DEFAULT_BOUNDED_READ_MAX_PRIMARY_KEY_INPUT_BYTES,
            )),
            index_requirement: IndexRequirement::Required,
            offset_policy: OffsetPolicy::RejectNonZero,
            full_scan_policy: FullScanPolicy::Reject,
            materialized_sort_policy: MaterializedSortPolicy::Reject,
            max_materialized_rows: None,
            max_projection_columns: None,
            grouped: GroupedAdmissionPolicy::disabled(),
        }
    }

    /// Build the default bounded policy used by ordinary typed/fluent reads.
    ///
    /// The policy rejects unindexed full scans, non-zero offsets, materialized
    /// sorts, and queries without a proven row bound. Callers that intentionally
    /// need a broader read must use an explicitly trusted execution method or
    /// evaluate their own policy before executing.
    #[must_use]
    pub(in crate::db) const fn default_bounded_read() -> Self {
        Self::public_read(
            non_zero_default(DEFAULT_BOUNDED_READ_MAX_ROWS),
            non_zero_default(DEFAULT_BOUNDED_READ_RESPONSE_BYTES),
        )
        .with_grouped_policy(GroupedAdmissionPolicy::default_bounded_read())
    }

    /// Return this policy with explicit grouped execution budgets attached.
    ///
    /// Public read policies still reject grouped queries unless the selected
    /// plan is executed with matching group-count and per-group byte caps.
    #[must_use]
    pub(in crate::db) const fn with_grouped_policy(
        mut self,
        grouped: GroupedAdmissionPolicy,
    ) -> Self {
        self.grouped = grouped;
        self
    }

    /// Build a trusted ad-hoc policy with explicit execution budgets.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn admin_ad_hoc(
        max_returned_rows: NonZeroU32,
        max_scanned_rows: NonZeroU64,
        max_response_bytes: NonZeroU32,
    ) -> Self {
        Self {
            lane: QueryAdmissionLane::AdminAdHoc,
            limit_requirement: LimitRequirement::Optional,
            max_returned_rows: Some(max_returned_rows),
            max_scanned_rows: Some(max_scanned_rows),
            max_response_bytes: Some(max_response_bytes),
            max_primary_key_input_terms: None,
            max_primary_key_input_bytes: None,
            index_requirement: IndexRequirement::Optional,
            offset_policy: OffsetPolicy::Allow,
            full_scan_policy: FullScanPolicy::Allow,
            materialized_sort_policy: MaterializedSortPolicy::Allow,
            max_materialized_rows: Some(max_returned_rows),
            max_projection_columns: None,
            grouped: GroupedAdmissionPolicy::disabled(),
        }
    }

    /// Build an EXPLAIN-only policy that cannot execute rows.
    #[must_use]
    pub(in crate::db) const fn diagnostic_explain() -> Self {
        Self {
            lane: QueryAdmissionLane::DiagnosticExplain,
            limit_requirement: LimitRequirement::Optional,
            max_returned_rows: None,
            max_scanned_rows: None,
            max_response_bytes: None,
            max_primary_key_input_terms: None,
            max_primary_key_input_bytes: None,
            index_requirement: IndexRequirement::Optional,
            offset_policy: OffsetPolicy::Allow,
            full_scan_policy: FullScanPolicy::Allow,
            materialized_sort_policy: MaterializedSortPolicy::Allow,
            max_materialized_rows: None,
            max_projection_columns: None,
            grouped: GroupedAdmissionPolicy::disabled(),
        }
    }

    /// Return the lane this policy governs.
    #[must_use]
    pub(in crate::db) const fn lane(&self) -> QueryAdmissionLane {
        self.lane
    }

    /// Return whether the surface requires caller-visible LIMIT.
    #[must_use]
    pub(in crate::db) const fn require_limit(&self) -> bool {
        matches!(self.limit_requirement, LimitRequirement::Required)
    }

    /// Return the maximum rows that may be returned.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn max_returned_rows(&self) -> Option<NonZeroU32> {
        self.max_returned_rows
    }

    /// Return the maximum rows that may be scanned.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn max_scanned_rows(&self) -> Option<NonZeroU64> {
        self.max_scanned_rows
    }

    /// Return the maximum response bytes.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn max_response_bytes(&self) -> Option<NonZeroU32> {
        self.max_response_bytes
    }

    /// Return whether the selected plan must use an index-backed path.
    #[must_use]
    pub(in crate::db) const fn require_index(&self) -> bool {
        matches!(self.index_requirement, IndexRequirement::Required)
    }

    /// Return whether this surface rejects non-zero OFFSET execution.
    #[must_use]
    pub(in crate::db) const fn reject_non_zero_offset(&self) -> bool {
        matches!(self.offset_policy, OffsetPolicy::RejectNonZero)
    }

    /// Return whether a full entity scan may execute.
    #[must_use]
    pub(in crate::db) const fn allow_full_scan(&self) -> bool {
        matches!(self.full_scan_policy, FullScanPolicy::Allow)
    }

    /// Return whether this surface permits materialized ORDER BY execution.
    #[must_use]
    pub(in crate::db) const fn allow_materialized_sort(&self) -> bool {
        matches!(self.materialized_sort_policy, MaterializedSortPolicy::Allow)
    }

    /// Return the maximum rows that may be materialized for sort/projection work.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn max_materialized_rows(&self) -> Option<NonZeroU32> {
        self.max_materialized_rows
    }

    /// Return grouped/aggregate budgets.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn grouped(&self) -> GroupedAdmissionPolicy {
        self.grouped
    }

    /// Return whether public-read construction kept the mandatory finite caps.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn public_caps_are_finite(&self) -> bool {
        !matches!(self.lane, QueryAdmissionLane::PublicRead)
            || (self.max_returned_rows.is_some() && self.max_response_bytes.is_some())
    }

    /// Return this policy with explicit primary-key input work caps.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn with_primary_key_input_caps(
        mut self,
        max_terms: NonZeroU32,
        max_bytes: NonZeroU32,
    ) -> Self {
        self.max_primary_key_input_terms = Some(max_terms);
        self.max_primary_key_input_bytes = Some(max_bytes);
        self
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

        if matches!(summary.plan_shape(), QueryAdmissionPlanShape::Delete) {
            return Some(QueryAdmissionRejection::UnsupportedStatementForQueryLane);
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

        if self.reject_non_zero_offset() && summary.offset().unwrap_or_default() != 0 {
            return Some(QueryAdmissionRejection::PublicQueryOffsetRejected);
        }

        if let Some(rejection) = self.returned_row_bound_rejection(summary) {
            return Some(rejection);
        }

        if let Some(rejection) = self.scan_bound_rejection(summary) {
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

        if grouped.distinct_aggregate_count() > 0 && self.grouped.max_distinct_entries().is_none() {
            return Some(QueryAdmissionRejection::GroupedQueryRequiresLimits);
        }

        None
    }

    fn returned_row_bound_rejection(
        &self,
        summary: &QueryAdmissionSummary,
    ) -> Option<QueryAdmissionRejection> {
        let max_returned_rows = self.max_returned_rows?;

        if matches!(
            summary.returned_row_bound_kind(),
            QueryBoundKind::EstimateOnly
        ) {
            return Some(QueryAdmissionRejection::EstimatedOnlyBoundRejected);
        }

        if !summary.returned_row_bound_kind().admits_public_read() {
            return Some(QueryAdmissionRejection::ScanBoundUnavailable);
        }

        let Some(returned_row_bound) = summary.returned_row_bound() else {
            return Some(QueryAdmissionRejection::ScanBoundUnavailable);
        };

        if returned_row_bound > max_returned_rows.get() {
            return Some(QueryAdmissionRejection::ReturnedRowBoundExceedsPolicy);
        }

        None
    }

    fn scan_bound_rejection(
        &self,
        summary: &QueryAdmissionSummary,
    ) -> Option<QueryAdmissionRejection> {
        let max_scanned_rows = self.max_scanned_rows?;

        if matches!(summary.scan_bound_kind(), QueryBoundKind::EstimateOnly) {
            return Some(QueryAdmissionRejection::EstimatedOnlyBoundRejected);
        }

        if !summary.scan_bound_kind().admits_public_read() {
            return Some(QueryAdmissionRejection::ScanBoundUnavailable);
        }

        let Some(scan_bound) = summary.scan_bound() else {
            return Some(QueryAdmissionRejection::ScanBoundUnavailable);
        };

        if scan_bound > max_scanned_rows.get() {
            return Some(QueryAdmissionRejection::ScanBoundExceedsPolicy);
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

        let max_materialized_rows = self.max_materialized_rows?;
        let materialized_rows = summary.materialization().materialized_rows()?;

        if materialized_rows > max_materialized_rows.get() {
            Some(QueryAdmissionRejection::MaterializationExceedsBudget)
        } else {
            None
        }
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
