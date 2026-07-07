//! Module: db::query::admission
//! Responsibility: shared read-admission vocabulary for query surfaces.
//! Does not own: physical planning, executor runtime, or SQL/fluent lowering.
//! Boundary: describes policy, proven bounds, and stable rejection diagnostics.

use std::fmt::Write as _;
use std::num::{NonZeroU32, NonZeroU64};
use std::ops::Bound;

use crate::{
    db::{
        access::IndexBranchSetOrderedSuffix,
        query::plan::{
            AccessPlanProjection, AccessPlannedQuery, GroupPlan, PrimaryKeyInputResourceSummary,
            QueryMode, ResidualFilterShape, ScalarPlan, project_access_plan,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    Diagnostic, DiagnosticCode, DiagnosticDetail, ErrorCode, ErrorOrigin, QueryReadAdmissionCode,
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

/// Query execution lane selected by the public or internal caller surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryAdmissionLane {
    /// Caller-facing bounded read path for generated canister query endpoints.
    PublicRead,
    /// Trusted/admin ad-hoc read path with explicit budgets supplied by the embedder.
    AdminAdHoc,
    /// EXPLAIN-only path that describes planning and admission without row execution.
    DiagnosticExplain,
    /// Test-only lane for local harnesses that need to bypass production policy.
    DevTest,
}

impl QueryAdmissionLane {
    /// Return a stable lowercase diagnostic label for this lane.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublicRead => "public_read",
            Self::AdminAdHoc => "admin_ad_hoc",
            Self::DiagnosticExplain => "diagnostic_explain",
            Self::DevTest => "dev_test",
        }
    }

    /// Return whether this lane may execute and return data rows.
    #[must_use]
    pub const fn executes_rows(self) -> bool {
        !matches!(self, Self::DiagnosticExplain)
    }
}

/// Quality of the bound carried into read admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryBoundKind {
    /// Exact count known before execution.
    Exact,
    /// Conservative upper bound proven before execution.
    ConservativeUpperBound,
    /// Runtime cap enforced by the executor while producing rows.
    EnforcedRuntimeCap,
    /// Planner estimate only; not safe as public admission authority.
    EstimateOnly,
    /// No bound is available.
    Unavailable,
}

impl QueryBoundKind {
    /// Return a stable lowercase diagnostic label for this bound quality.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::ConservativeUpperBound => "conservative_upper_bound",
            Self::EnforcedRuntimeCap => "enforced_runtime_cap",
            Self::EstimateOnly => "estimate_only",
            Self::Unavailable => "unavailable",
        }
    }

    /// Return whether this bound kind is acceptable proof for public reads.
    #[must_use]
    pub const fn admits_public_read(self) -> bool {
        matches!(
            self,
            Self::Exact | Self::ConservativeUpperBound | Self::EnforcedRuntimeCap
        )
    }
}

/// Final read-admission decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryAdmissionDecision {
    /// The selected plan is allowed under the lane policy.
    Admitted,
    /// The selected plan is rejected before execution.
    Rejected,
}

impl QueryAdmissionDecision {
    /// Return a stable lowercase diagnostic label for this decision.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Rejected => "rejected",
        }
    }

    /// Return whether the selected plan may execute.
    #[must_use]
    pub const fn is_admitted(self) -> bool {
        matches!(self, Self::Admitted)
    }
}

/// Coarse selected access-path class used by admission and EXPLAIN.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryAdmissionAccessKind {
    /// Access class has not been summarized yet.
    Unknown,
    /// Direct primary-key lookup.
    ByKey,
    /// Multiple direct primary-key lookups.
    ByKeys,
    /// Primary-key range access.
    KeyRange,
    /// Secondary-index prefix access.
    IndexPrefix,
    /// Secondary-index multi-lookup access.
    IndexMultiLookup,
    /// Secondary-index branch-set access.
    IndexBranchSet,
    /// Secondary-index range access.
    IndexRange,
    /// Full entity scan.
    FullScan,
    /// Union of multiple access paths.
    Union,
    /// Intersection of multiple access paths.
    Intersection,
}

impl QueryAdmissionAccessKind {
    /// Return a stable lowercase diagnostic label for this access class.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::ByKey => "by_key",
            Self::ByKeys => "by_keys",
            Self::KeyRange => "key_range",
            Self::IndexPrefix => "index_prefix",
            Self::IndexMultiLookup => "index_multi_lookup",
            Self::IndexBranchSet => "index_branch_set",
            Self::IndexRange => "index_range",
            Self::FullScan => "full_scan",
            Self::Union => "union",
            Self::Intersection => "intersection",
        }
    }

    /// Return whether this access class is backed by a secondary index.
    #[must_use]
    pub const fn is_secondary_index(self) -> bool {
        matches!(
            self,
            Self::IndexPrefix | Self::IndexMultiLookup | Self::IndexBranchSet | Self::IndexRange
        )
    }

    /// Return whether this access class is a full entity scan.
    #[must_use]
    pub const fn is_full_scan(self) -> bool {
        matches!(self, Self::FullScan)
    }
}

/// Coarse scalar/grouped statement shape used by read admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryAdmissionPlanShape {
    /// Scalar read shape, including projection-only and global-aggregate scalar plans.
    ScalarRead,
    /// Grouped aggregate read shape.
    GroupedAggregate,
    /// Delete shape surfaced only for diagnostics; public read lanes must not execute it.
    Delete,
}

impl QueryAdmissionPlanShape {
    /// Return a stable lowercase diagnostic label for this plan shape.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScalarRead => "scalar_read",
            Self::GroupedAggregate => "grouped_aggregate",
            Self::Delete => "delete",
        }
    }
}

/// Post-access residual filter shape relevant to admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryAdmissionResidualFilter {
    /// No residual runtime filter remains after access planning.
    Absent,
    /// A predicate-native residual filter remains.
    Predicate,
    /// An expression-backed residual filter remains.
    Expression,
    /// Both expression and predicate residual forms remain available.
    ExpressionAndPredicate,
}

impl QueryAdmissionResidualFilter {
    /// Return a stable lowercase diagnostic label for this residual shape.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "none",
            Self::Predicate => "predicate",
            Self::Expression => "expression",
            Self::ExpressionAndPredicate => "expression_and_predicate",
        }
    }

    /// Return whether no residual runtime filter remains.
    #[must_use]
    pub const fn is_absent(self) -> bool {
        matches!(self, Self::Absent)
    }
}

/// ORDER BY facts relevant to read admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryAdmissionOrdering {
    /// No caller-visible ordering is requested.
    None,
    /// Ordering is requested but not yet resolved into executor slots.
    Requested,
    /// Ordering has a planner-resolved executor contract.
    Resolved,
}

impl QueryAdmissionOrdering {
    /// Return a stable lowercase diagnostic label for this ordering state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Requested => "requested",
            Self::Resolved => "resolved",
        }
    }
}

/// Grouped query facts relevant to read admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryAdmissionGroupedSummary {
    group_field_count: u32,
    aggregate_count: u32,
    distinct_aggregate_count: u32,
    max_groups: u64,
    max_group_bytes: u64,
    having_filter: bool,
}

impl QueryAdmissionGroupedSummary {
    /// Build one grouped admission summary from planner-owned grouped facts.
    #[must_use]
    pub const fn new(
        group_field_count: u32,
        aggregate_count: u32,
        distinct_aggregate_count: u32,
        max_groups: u64,
        max_group_bytes: u64,
        having_filter: bool,
    ) -> Self {
        Self {
            group_field_count,
            aggregate_count,
            distinct_aggregate_count,
            max_groups,
            max_group_bytes,
            having_filter,
        }
    }

    /// Return the number of GROUP BY fields.
    #[must_use]
    pub const fn group_field_count(self) -> u32 {
        self.group_field_count
    }

    /// Return the number of aggregate expressions.
    #[must_use]
    pub const fn aggregate_count(self) -> u32 {
        self.aggregate_count
    }

    /// Return the number of aggregate expressions with DISTINCT state.
    #[must_use]
    pub const fn distinct_aggregate_count(self) -> u32 {
        self.distinct_aggregate_count
    }

    /// Return the grouped execution maximum group count.
    #[must_use]
    pub const fn max_groups(self) -> u64 {
        self.max_groups
    }

    /// Return the grouped execution maximum bytes per group accumulator.
    #[must_use]
    pub const fn max_group_bytes(self) -> u64 {
        self.max_group_bytes
    }

    /// Return whether the grouped plan has a HAVING residual expression.
    #[must_use]
    pub const fn has_having_filter(self) -> bool {
        self.having_filter
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
            && !access_satisfies_index_requirement(summary.selected_access(), summary.scan_bound())
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

/// Materialization facts relevant to read admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryMaterializationSummary {
    materialized_sort: bool,
    materialized_rows: Option<u32>,
    row_bound_kind: QueryBoundKind,
}

impl QueryMaterializationSummary {
    /// Build a summary for a plan that does not materialize rows for sorting.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            materialized_sort: false,
            materialized_rows: None,
            row_bound_kind: QueryBoundKind::Unavailable,
        }
    }

    /// Build a summary for a plan that materializes rows for sorting.
    #[must_use]
    pub const fn sort(materialized_rows: Option<u32>, row_bound_kind: QueryBoundKind) -> Self {
        Self {
            materialized_sort: true,
            materialized_rows,
            row_bound_kind,
        }
    }

    /// Return whether the plan materializes rows for sorting.
    #[must_use]
    pub const fn materialized_sort(&self) -> bool {
        self.materialized_sort
    }

    /// Return the row materialization bound, if known.
    #[must_use]
    pub const fn materialized_rows(&self) -> Option<u32> {
        self.materialized_rows
    }

    /// Return the quality of the materialization row bound.
    #[must_use]
    pub const fn row_bound_kind(&self) -> QueryBoundKind {
        self.row_bound_kind
    }
}

/// Stable read-admission rejection reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryAdmissionRejection {
    /// Public reads require a bounded read intent.
    PublicQueryRequiresLimit,
    /// Public reads require a proven index-backed access path.
    PublicQueryRequiresIndex,
    /// The selected plan is an unbounded full scan.
    UnboundedFullScanRejected,
    /// No scan bound was available for a policy that requires one.
    ScanBoundUnavailable,
    /// The proven scan bound exceeds the policy.
    ScanBoundExceedsPolicy,
    /// Only an estimate was available for a policy that requires proof.
    EstimatedOnlyBoundRejected,
    /// ORDER BY requires materializing rows.
    SortRequiresMaterialization,
    /// Materialization exceeds the policy.
    MaterializationExceedsBudget,
    /// Projection bytes may exceed the response budget.
    ProjectionResponseMayExceedLimit,
    /// Grouped reads need explicit group and memory budgets.
    GroupedQueryRequiresLimits,
    /// Grouped read planning exceeds the policy.
    GroupedQueryExceedsBudget,
    /// Diagnostic lanes do not execute rows.
    DiagnosticLaneDoesNotExecute,
    /// Introspection is disabled for the selected lane.
    IntrospectionDisabledForLane,
    /// The statement shape is not supported by the selected lane.
    UnsupportedStatementForQueryLane,
    /// Public read endpoints do not permit non-zero OFFSET execution.
    PublicQueryOffsetRejected,
    /// The returned-row bound exceeds the selected policy.
    ReturnedRowBoundExceedsPolicy,
    /// Primary-key predicate input work exceeds the selected policy.
    PrimaryKeyInputExceedsPolicy,
}

impl QueryAdmissionRejection {
    /// Return a stable lowercase diagnostic label for this rejection.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublicQueryRequiresLimit => "public_query_requires_limit",
            Self::PublicQueryRequiresIndex => "public_query_requires_index",
            Self::UnboundedFullScanRejected => "unbounded_full_scan_rejected",
            Self::ScanBoundUnavailable => "scan_bound_unavailable",
            Self::ScanBoundExceedsPolicy => "scan_bound_exceeds_policy",
            Self::EstimatedOnlyBoundRejected => "estimated_only_bound_rejected",
            Self::SortRequiresMaterialization => "sort_requires_materialization",
            Self::MaterializationExceedsBudget => "materialization_exceeds_budget",
            Self::ProjectionResponseMayExceedLimit => "projection_response_may_exceed_limit",
            Self::GroupedQueryRequiresLimits => "grouped_query_requires_limits",
            Self::GroupedQueryExceedsBudget => "grouped_query_exceeds_budget",
            Self::DiagnosticLaneDoesNotExecute => "diagnostic_lane_does_not_execute",
            Self::IntrospectionDisabledForLane => "introspection_disabled_for_lane",
            Self::UnsupportedStatementForQueryLane => "unsupported_statement_for_query_lane",
            Self::PublicQueryOffsetRejected => "public_query_offset_rejected",
            Self::ReturnedRowBoundExceedsPolicy => "returned_row_bound_exceeds_policy",
            Self::PrimaryKeyInputExceedsPolicy => "primary_key_input_exceeds_policy",
        }
    }

    /// Return the compact diagnostic detail code for this rejection.
    #[must_use]
    pub const fn code(self) -> QueryReadAdmissionCode {
        match self {
            Self::PublicQueryRequiresLimit => QueryReadAdmissionCode::PublicQueryRequiresLimit,
            Self::PublicQueryRequiresIndex => QueryReadAdmissionCode::PublicQueryRequiresIndex,
            Self::UnboundedFullScanRejected => QueryReadAdmissionCode::UnboundedFullScanRejected,
            Self::ScanBoundUnavailable => QueryReadAdmissionCode::ScanBoundUnavailable,
            Self::ScanBoundExceedsPolicy => QueryReadAdmissionCode::ScanBoundExceedsPolicy,
            Self::EstimatedOnlyBoundRejected => QueryReadAdmissionCode::EstimatedOnlyBoundRejected,
            Self::SortRequiresMaterialization => {
                QueryReadAdmissionCode::SortRequiresMaterialization
            }
            Self::MaterializationExceedsBudget => {
                QueryReadAdmissionCode::MaterializationExceedsBudget
            }
            Self::ProjectionResponseMayExceedLimit => {
                QueryReadAdmissionCode::ProjectionResponseMayExceedLimit
            }
            Self::GroupedQueryRequiresLimits => QueryReadAdmissionCode::GroupedQueryRequiresLimits,
            Self::GroupedQueryExceedsBudget => QueryReadAdmissionCode::GroupedQueryExceedsBudget,
            Self::DiagnosticLaneDoesNotExecute => {
                QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute
            }
            Self::IntrospectionDisabledForLane => {
                QueryReadAdmissionCode::IntrospectionDisabledForLane
            }
            Self::UnsupportedStatementForQueryLane => {
                QueryReadAdmissionCode::UnsupportedStatementForQueryLane
            }
            Self::PublicQueryOffsetRejected => QueryReadAdmissionCode::PublicQueryOffsetRejected,
            Self::ReturnedRowBoundExceedsPolicy => {
                QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy
            }
            Self::PrimaryKeyInputExceedsPolicy => {
                QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy
            }
        }
    }

    /// Return a compact diagnostic payload for this rejection.
    #[must_use]
    pub const fn diagnostic(self) -> Diagnostic {
        Diagnostic::new(
            DiagnosticCode::QueryReadAdmission,
            ErrorOrigin::Query,
            Some(DiagnosticDetail::QueryReadAdmission {
                reason: self.code(),
            }),
        )
    }

    /// Return the public wire code for this rejection.
    #[must_use]
    pub const fn error_code(self) -> ErrorCode {
        self.diagnostic().error_code()
    }
}

/// Read-admission result and plan facts for diagnostics and EXPLAIN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAdmissionSummary {
    lane: QueryAdmissionLane,
    decision: QueryAdmissionDecision,
    plan_shape: QueryAdmissionPlanShape,
    selected_access: QueryAdmissionAccessKind,
    selected_index: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
    scan_bound: Option<u64>,
    scan_bound_kind: QueryBoundKind,
    returned_row_bound: Option<u32>,
    returned_row_bound_kind: QueryBoundKind,
    response_byte_bound: Option<u32>,
    response_byte_bound_kind: QueryBoundKind,
    primary_key_input_terms: Option<u32>,
    primary_key_input_payload_bytes: Option<u32>,
    residual_filter: QueryAdmissionResidualFilter,
    ordering: QueryAdmissionOrdering,
    grouped: Option<QueryAdmissionGroupedSummary>,
    materialization: QueryMaterializationSummary,
    rejection: Option<QueryAdmissionRejection>,
}

impl QueryAdmissionSummary {
    /// Build an admitted summary with unknown bound details.
    #[must_use]
    pub const fn admitted(
        lane: QueryAdmissionLane,
        selected_access: QueryAdmissionAccessKind,
    ) -> Self {
        Self {
            lane,
            decision: QueryAdmissionDecision::Admitted,
            plan_shape: QueryAdmissionPlanShape::ScalarRead,
            selected_access,
            selected_index: None,
            limit: None,
            offset: None,
            scan_bound: None,
            scan_bound_kind: QueryBoundKind::Unavailable,
            returned_row_bound: None,
            returned_row_bound_kind: QueryBoundKind::Unavailable,
            response_byte_bound: None,
            response_byte_bound_kind: QueryBoundKind::Unavailable,
            primary_key_input_terms: None,
            primary_key_input_payload_bytes: None,
            residual_filter: QueryAdmissionResidualFilter::Absent,
            ordering: QueryAdmissionOrdering::None,
            grouped: None,
            materialization: QueryMaterializationSummary::none(),
            rejection: None,
        }
    }

    /// Build a rejected summary with unknown bound details.
    #[must_use]
    pub const fn rejected(
        lane: QueryAdmissionLane,
        selected_access: QueryAdmissionAccessKind,
        rejection: QueryAdmissionRejection,
    ) -> Self {
        Self {
            lane,
            decision: QueryAdmissionDecision::Rejected,
            plan_shape: QueryAdmissionPlanShape::ScalarRead,
            selected_access,
            selected_index: None,
            limit: None,
            offset: None,
            scan_bound: None,
            scan_bound_kind: QueryBoundKind::Unavailable,
            returned_row_bound: None,
            returned_row_bound_kind: QueryBoundKind::Unavailable,
            response_byte_bound: None,
            response_byte_bound_kind: QueryBoundKind::Unavailable,
            primary_key_input_terms: None,
            primary_key_input_payload_bytes: None,
            residual_filter: QueryAdmissionResidualFilter::Absent,
            ordering: QueryAdmissionOrdering::None,
            grouped: None,
            materialization: QueryMaterializationSummary::none(),
            rejection: Some(rejection),
        }
    }

    /// Build one admitted summary from the already-selected access plan.
    #[must_use]
    pub(in crate::db) fn from_plan(lane: QueryAdmissionLane, plan: &AccessPlannedQuery) -> Self {
        let access = summarize_access_plan(plan);
        let grouped = plan.grouped_plan().map(summarize_grouped_plan);
        let (limit, offset) = scalar_limit_and_offset(plan.scalar_plan());
        let (mut returned_row_bound, mut returned_row_bound_kind) =
            returned_row_bound_from_plan(limit, grouped);
        if returned_row_bound.is_none() && grouped.is_none() {
            (returned_row_bound, returned_row_bound_kind) =
                returned_row_bound_from_exact_access(&access);
        }
        let primary_key_input_resource = plan.access_choice().primary_key_input_resource();
        let scan_bound_kind = access.scan_bound_kind();
        Self {
            lane,
            decision: QueryAdmissionDecision::Admitted,
            plan_shape: plan_shape(plan),
            selected_access: access.kind,
            selected_index: access.selected_index,
            limit,
            offset: Some(offset),
            scan_bound: access.exact_scan_bound,
            scan_bound_kind,
            returned_row_bound,
            returned_row_bound_kind,
            response_byte_bound: None,
            response_byte_bound_kind: QueryBoundKind::Unavailable,
            primary_key_input_terms: primary_key_input_resource
                .map(PrimaryKeyInputResourceSummary::raw_term_count),
            primary_key_input_payload_bytes: primary_key_input_resource
                .map(PrimaryKeyInputResourceSummary::estimated_payload_bytes),
            residual_filter: admission_residual_filter(plan.residual_filter_shape()),
            ordering: admission_ordering(plan),
            grouped,
            materialization: QueryMaterializationSummary::none(),
            rejection: None,
        }
    }

    const fn admit(mut self) -> Self {
        self.decision = QueryAdmissionDecision::Admitted;
        self.rejection = None;
        self
    }

    const fn reject(mut self, rejection: QueryAdmissionRejection) -> Self {
        self.decision = QueryAdmissionDecision::Rejected;
        self.rejection = Some(rejection);
        self
    }

    /// Return the admission lane.
    #[must_use]
    pub const fn lane(&self) -> QueryAdmissionLane {
        self.lane
    }

    /// Return the final decision.
    #[must_use]
    pub const fn decision(&self) -> QueryAdmissionDecision {
        self.decision
    }

    /// Return the scalar/grouped statement shape.
    #[must_use]
    pub const fn plan_shape(&self) -> QueryAdmissionPlanShape {
        self.plan_shape
    }

    /// Return the selected access class.
    #[must_use]
    pub const fn selected_access(&self) -> QueryAdmissionAccessKind {
        self.selected_access
    }

    /// Return the selected index name, if one exists.
    #[must_use]
    pub fn selected_index(&self) -> Option<&str> {
        self.selected_index.as_deref()
    }

    /// Return the caller-visible LIMIT, if present.
    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Return the caller-visible OFFSET, if present.
    #[must_use]
    pub const fn offset(&self) -> Option<u32> {
        self.offset
    }

    /// Return the scan bound, if known.
    #[must_use]
    pub const fn scan_bound(&self) -> Option<u64> {
        self.scan_bound
    }

    /// Return the quality of the scan bound.
    #[must_use]
    pub const fn scan_bound_kind(&self) -> QueryBoundKind {
        self.scan_bound_kind
    }

    /// Return the returned-row bound, if known.
    #[must_use]
    pub const fn returned_row_bound(&self) -> Option<u32> {
        self.returned_row_bound
    }

    /// Return the quality of the returned-row bound.
    #[must_use]
    pub const fn returned_row_bound_kind(&self) -> QueryBoundKind {
        self.returned_row_bound_kind
    }

    /// Return the response-byte bound, if known.
    #[must_use]
    pub const fn response_byte_bound(&self) -> Option<u32> {
        self.response_byte_bound
    }

    /// Return the quality of the response-byte bound.
    #[must_use]
    pub const fn response_byte_bound_kind(&self) -> QueryBoundKind {
        self.response_byte_bound_kind
    }

    /// Return primary-key predicate input terms when the selected exact-key
    /// route carries planner-owned resource facts.
    #[must_use]
    pub const fn primary_key_input_terms(&self) -> Option<u32> {
        self.primary_key_input_terms
    }

    /// Return estimated primary-key predicate input payload bytes when known.
    #[must_use]
    pub const fn primary_key_input_payload_bytes(&self) -> Option<u32> {
        self.primary_key_input_payload_bytes
    }

    /// Return post-access residual filter facts.
    #[must_use]
    pub const fn residual_filter(&self) -> QueryAdmissionResidualFilter {
        self.residual_filter
    }

    /// Return ORDER BY facts.
    #[must_use]
    pub const fn ordering(&self) -> QueryAdmissionOrdering {
        self.ordering
    }

    /// Return grouped query facts, if this is a grouped plan.
    #[must_use]
    pub const fn grouped(&self) -> Option<QueryAdmissionGroupedSummary> {
        self.grouped
    }

    /// Return materialization facts.
    #[must_use]
    pub const fn materialization(&self) -> QueryMaterializationSummary {
        self.materialization
    }

    /// Return a copy of this summary with route-derived materialization facts attached.
    #[must_use]
    #[cfg_attr(not(feature = "sql"), allow(dead_code))]
    pub(in crate::db) const fn with_materialization(
        mut self,
        materialization: QueryMaterializationSummary,
    ) -> Self {
        self.materialization = materialization;
        self
    }

    /// Return the rejection reason, when the decision is rejected.
    #[must_use]
    pub const fn rejection(&self) -> Option<QueryAdmissionRejection> {
        self.rejection
    }

    /// Render this summary as a stable top-level verbose EXPLAIN block.
    #[must_use]
    pub(in crate::db) fn render_text_block(&self) -> String {
        let mut out = String::from("admission:");
        push_text_field(&mut out, "lane", self.lane().as_str());
        push_text_field(&mut out, "decision", self.decision().as_str());
        push_text_field(
            &mut out,
            "reason",
            self.rejection()
                .map_or("none", QueryAdmissionRejection::as_str),
        );
        push_text_field(&mut out, "plan_shape", self.plan_shape().as_str());
        push_text_field(&mut out, "selected_access", self.selected_access().as_str());
        push_text_field(
            &mut out,
            "selected_index",
            self.selected_index().unwrap_or("none"),
        );
        push_text_option_u32(&mut out, "limit", self.limit());
        push_text_option_u32(&mut out, "offset", self.offset());
        push_text_option_u64(&mut out, "scan_bound", self.scan_bound());
        push_text_field(&mut out, "scan_bound_kind", self.scan_bound_kind().as_str());
        push_text_option_u32(&mut out, "returned_row_bound", self.returned_row_bound());
        push_text_field(
            &mut out,
            "returned_row_bound_kind",
            self.returned_row_bound_kind().as_str(),
        );
        push_text_option_u32(&mut out, "response_byte_bound", self.response_byte_bound());
        push_text_field(
            &mut out,
            "response_byte_bound_kind",
            self.response_byte_bound_kind().as_str(),
        );
        push_text_option_u32(
            &mut out,
            "primary_key_input_terms",
            self.primary_key_input_terms(),
        );
        push_text_option_u32(
            &mut out,
            "primary_key_input_payload_bytes",
            self.primary_key_input_payload_bytes(),
        );
        push_text_field(&mut out, "residual_filter", self.residual_filter().as_str());
        push_text_field(&mut out, "ordering", self.ordering().as_str());
        push_text_bool(
            &mut out,
            "materialized_sort",
            self.materialization().materialized_sort(),
        );
        push_text_option_u32(
            &mut out,
            "materialized_rows",
            self.materialization().materialized_rows(),
        );
        push_text_field(
            &mut out,
            "materialized_row_bound_kind",
            self.materialization().row_bound_kind().as_str(),
        );

        if let Some(grouped) = self.grouped() {
            push_text_bool(&mut out, "grouped", true);
            push_text_u64(
                &mut out,
                "group_field_count",
                u64::from(grouped.group_field_count()),
            );
            push_text_u64(
                &mut out,
                "aggregate_count",
                u64::from(grouped.aggregate_count()),
            );
            push_text_u64(
                &mut out,
                "distinct_aggregate_count",
                u64::from(grouped.distinct_aggregate_count()),
            );
            push_text_u64(&mut out, "max_groups", grouped.max_groups());
            push_text_u64(&mut out, "max_group_bytes", grouped.max_group_bytes());
            push_text_bool(&mut out, "having_filter", grouped.has_having_filter());
        } else {
            push_text_bool(&mut out, "grouped", false);
        }

        out
    }
}

fn push_text_field(out: &mut String, key: &str, value: &str) {
    out.push('\n');
    out.push_str("  ");
    out.push_str(key);
    out.push('=');
    out.push_str(value);
}

fn push_text_bool(out: &mut String, key: &str, value: bool) {
    push_text_field(out, key, if value { "true" } else { "false" });
}

fn push_text_u64(out: &mut String, key: &str, value: u64) {
    out.push('\n');
    out.push_str("  ");
    out.push_str(key);
    out.push('=');
    let _ = write!(out, "{value}");
}

fn push_text_option_u32(out: &mut String, key: &str, value: Option<u32>) {
    match value {
        Some(value) => push_text_u64(out, key, u64::from(value)),
        None => push_text_field(out, key, "none"),
    }
}

fn push_text_option_u64(out: &mut String, key: &str, value: Option<u64>) {
    match value {
        Some(value) => push_text_u64(out, key, value),
        None => push_text_field(out, key, "none"),
    }
}

// Keep the staged extractor live before admission enforcement calls it directly.
const _: fn(QueryAdmissionLane, &AccessPlannedQuery) -> QueryAdmissionSummary =
    QueryAdmissionSummary::from_plan;

const fn access_satisfies_index_requirement(
    kind: QueryAdmissionAccessKind,
    scan_bound: Option<u64>,
) -> bool {
    kind.is_secondary_index()
        || matches!(
            (kind, scan_bound),
            (
                QueryAdmissionAccessKind::ByKey | QueryAdmissionAccessKind::ByKeys,
                Some(_)
            )
        )
}

struct AdmissionAccessProjection;

#[derive(Clone, Debug, Eq, PartialEq)]
struct AdmissionAccessSummary {
    kind: QueryAdmissionAccessKind,
    selected_index: Option<String>,
    exact_scan_bound: Option<u64>,
}

impl AdmissionAccessSummary {
    const fn non_index(kind: QueryAdmissionAccessKind, exact_scan_bound: Option<u64>) -> Self {
        Self {
            kind,
            selected_index: None,
            exact_scan_bound,
        }
    }

    fn secondary_index(kind: QueryAdmissionAccessKind, index_name: &str) -> Self {
        Self {
            kind,
            selected_index: Some(index_name.to_string()),
            exact_scan_bound: None,
        }
    }

    const fn composite(kind: QueryAdmissionAccessKind) -> Self {
        Self {
            kind,
            selected_index: None,
            exact_scan_bound: None,
        }
    }

    const fn scan_bound_kind(&self) -> QueryBoundKind {
        if self.exact_scan_bound.is_some() {
            QueryBoundKind::Exact
        } else {
            QueryBoundKind::Unavailable
        }
    }
}

impl AccessPlanProjection<Value> for AdmissionAccessProjection {
    type Output = AdmissionAccessSummary;

    fn by_key(&mut self, _key: &Value) -> Self::Output {
        AdmissionAccessSummary::non_index(QueryAdmissionAccessKind::ByKey, Some(1))
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        AdmissionAccessSummary::non_index(
            QueryAdmissionAccessKind::ByKeys,
            Some(u64::try_from(keys.len()).unwrap_or(u64::MAX)),
        )
    }

    fn key_range(&mut self, _start: &Value, _end: &Value) -> Self::Output {
        AdmissionAccessSummary::non_index(QueryAdmissionAccessKind::KeyRange, None)
    }

    fn index_prefix(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        AdmissionAccessSummary::secondary_index(QueryAdmissionAccessKind::IndexPrefix, index_name)
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _values: &[Value],
    ) -> Self::Output {
        AdmissionAccessSummary::secondary_index(
            QueryAdmissionAccessKind::IndexMultiLookup,
            index_name,
        )
    }

    fn index_branch_set(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _fixed_values: &[Value],
        _branch_values: &[Value],
        _ordered_suffix: IndexBranchSetOrderedSuffix,
    ) -> Self::Output {
        AdmissionAccessSummary::secondary_index(
            QueryAdmissionAccessKind::IndexBranchSet,
            index_name,
        )
    }

    fn index_range(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _prefix: &[Value],
        _lower: &Bound<Value>,
        _upper: &Bound<Value>,
    ) -> Self::Output {
        AdmissionAccessSummary::secondary_index(QueryAdmissionAccessKind::IndexRange, index_name)
    }

    fn full_scan(&mut self) -> Self::Output {
        AdmissionAccessSummary::non_index(QueryAdmissionAccessKind::FullScan, None)
    }

    fn union(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        AdmissionAccessSummary::composite(QueryAdmissionAccessKind::Union)
    }

    fn intersection(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        AdmissionAccessSummary::composite(QueryAdmissionAccessKind::Intersection)
    }
}

fn summarize_access_plan(plan: &AccessPlannedQuery) -> AdmissionAccessSummary {
    project_access_plan(&plan.access, &mut AdmissionAccessProjection)
}

fn summarize_grouped_plan(plan: &GroupPlan) -> QueryAdmissionGroupedSummary {
    QueryAdmissionGroupedSummary::new(
        u32::try_from(plan.group.group_fields.len()).unwrap_or(u32::MAX),
        u32::try_from(plan.group.aggregates.len()).unwrap_or(u32::MAX),
        u32::try_from(
            plan.group
                .aggregates
                .iter()
                .filter(|aggregate| aggregate.distinct)
                .count(),
        )
        .unwrap_or(u32::MAX),
        plan.group.execution.max_groups(),
        plan.group.execution.max_group_bytes(),
        plan.having_expr.is_some(),
    )
}

const fn scalar_limit_and_offset(plan: &ScalarPlan) -> (Option<u32>, u32) {
    match plan.mode {
        QueryMode::Load(load) => match &plan.page {
            Some(page) => (page.limit, page.offset),
            None => (load.limit(), load.offset()),
        },
        QueryMode::Delete(delete) => match plan.delete_limit {
            Some(delete_limit) => (delete_limit.limit, delete_limit.offset),
            None => (delete.limit(), delete.offset()),
        },
    }
}

fn returned_row_bound_from_plan(
    limit: Option<u32>,
    grouped: Option<QueryAdmissionGroupedSummary>,
) -> (Option<u32>, QueryBoundKind) {
    if let Some(limit) = limit {
        return (Some(limit), QueryBoundKind::EnforcedRuntimeCap);
    }

    let Some(grouped) = grouped else {
        return (None, QueryBoundKind::Unavailable);
    };
    if grouped.max_groups() == u64::MAX {
        return (None, QueryBoundKind::Unavailable);
    }

    (
        Some(u32::try_from(grouped.max_groups()).unwrap_or(u32::MAX)),
        QueryBoundKind::ConservativeUpperBound,
    )
}

fn returned_row_bound_from_exact_access(
    access: &AdmissionAccessSummary,
) -> (Option<u32>, QueryBoundKind) {
    match (access.kind, access.exact_scan_bound) {
        (QueryAdmissionAccessKind::ByKey | QueryAdmissionAccessKind::ByKeys, Some(bound)) => (
            Some(clamp_u64_to_u32(bound)),
            QueryBoundKind::ConservativeUpperBound,
        ),
        _ => (None, QueryBoundKind::Unavailable),
    }
}

fn clamp_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

const fn admission_residual_filter(shape: ResidualFilterShape) -> QueryAdmissionResidualFilter {
    match shape {
        ResidualFilterShape::Absent => QueryAdmissionResidualFilter::Absent,
        ResidualFilterShape::Predicate => QueryAdmissionResidualFilter::Predicate,
        ResidualFilterShape::Expression => QueryAdmissionResidualFilter::Expression,
        ResidualFilterShape::ExpressionAndPredicate => {
            QueryAdmissionResidualFilter::ExpressionAndPredicate
        }
    }
}

fn admission_ordering(plan: &AccessPlannedQuery) -> QueryAdmissionOrdering {
    if plan.scalar_plan().order.is_none() {
        return QueryAdmissionOrdering::None;
    }

    if plan.resolved_order().is_some() {
        QueryAdmissionOrdering::Resolved
    } else {
        QueryAdmissionOrdering::Requested
    }
}

const fn plan_shape(plan: &AccessPlannedQuery) -> QueryAdmissionPlanShape {
    if plan.grouped_plan().is_some() {
        return QueryAdmissionPlanShape::GroupedAggregate;
    }

    match plan.scalar_plan().mode {
        QueryMode::Load(_) => QueryAdmissionPlanShape::ScalarRead,
        QueryMode::Delete(_) => QueryAdmissionPlanShape::Delete,
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use crate::{
        db::{
            access::{AccessPath, SemanticIndexAccessContract},
            predicate::{MissingRowPolicy, Predicate},
            query::plan::{
                AccessPlannedQuery, AggregateKind, DeleteLimitSpec, DeleteSpec, FieldSlot,
                GroupAggregateSpec, GroupSpec, GroupedExecutionConfig, OrderDirection, OrderSpec,
                OrderTerm, PageSpec, QueryMode,
                expr::{Expr, FieldId},
            },
        },
        model::index::IndexModel,
        value::Value,
    };

    use super::{
        GroupedAdmissionPolicy, QueryAdmissionAccessKind, QueryAdmissionDecision,
        QueryAdmissionLane, QueryAdmissionOrdering, QueryAdmissionPlanShape, QueryAdmissionPolicy,
        QueryAdmissionRejection, QueryAdmissionResidualFilter, QueryAdmissionSummary,
        QueryBoundKind, QueryMaterializationSummary,
    };

    const ADMISSION_INDEX_FIELDS: [&str; 1] = ["tag"];
    const ADMISSION_INDEX: IndexModel = IndexModel::generated(
        "admission::tag",
        "admission::tag_store",
        &ADMISSION_INDEX_FIELDS,
        false,
    );

    #[test]
    fn public_read_policy_has_safe_finite_defaults() {
        let max_rows = NonZeroU32::new(50).expect("test max rows is non-zero");
        let max_bytes = NonZeroU32::new(32_768).expect("test max bytes is non-zero");
        let policy = QueryAdmissionPolicy::public_read(max_rows, max_bytes);

        assert_eq!(policy.lane(), QueryAdmissionLane::PublicRead);
        assert!(policy.require_limit());
        assert!(policy.require_index());
        assert!(policy.reject_non_zero_offset());
        assert!(!policy.allow_full_scan());
        assert!(!policy.allow_materialized_sort());
        assert_eq!(policy.max_returned_rows(), Some(max_rows));
        assert_eq!(policy.max_response_bytes(), Some(max_bytes));
        assert!(policy.public_caps_are_finite());
        assert!(!policy.grouped().has_hard_limits());
    }

    #[test]
    fn admin_policy_is_broader_but_still_budgeted() {
        let max_rows = NonZeroU32::new(100).expect("test max rows is non-zero");
        let max_scanned = NonZeroU64::new(1_000).expect("test scan cap is non-zero");
        let max_bytes = NonZeroU32::new(65_536).expect("test max bytes is non-zero");
        let policy = QueryAdmissionPolicy::admin_ad_hoc(max_rows, max_scanned, max_bytes);

        assert_eq!(policy.lane(), QueryAdmissionLane::AdminAdHoc);
        assert!(!policy.require_limit());
        assert!(!policy.require_index());
        assert!(policy.allow_full_scan());
        assert!(policy.allow_materialized_sort());
        assert_eq!(policy.max_scanned_rows(), Some(max_scanned));
        assert_eq!(policy.max_materialized_rows(), Some(max_rows));
    }

    #[test]
    fn diagnostic_explain_lane_does_not_execute_rows() {
        let policy = QueryAdmissionPolicy::diagnostic_explain();

        assert_eq!(policy.lane().as_str(), "diagnostic_explain");
        assert!(!policy.lane().executes_rows());
    }

    #[test]
    fn grouped_policy_requires_group_and_memory_budgets() {
        let max_groups = NonZeroU32::new(8).expect("test group cap is non-zero");
        let max_bytes = NonZeroU32::new(4096).expect("test byte cap is non-zero");
        let policy = GroupedAdmissionPolicy::bounded(max_groups, max_bytes, None);

        assert!(policy.has_hard_limits());
        assert_eq!(policy.max_groups(), Some(max_groups));
        assert_eq!(policy.max_group_bytes(), Some(max_bytes));
    }

    #[test]
    fn only_proven_or_enforced_bounds_admit_public_reads() {
        assert!(QueryBoundKind::Exact.admits_public_read());
        assert!(QueryBoundKind::ConservativeUpperBound.admits_public_read());
        assert!(QueryBoundKind::EnforcedRuntimeCap.admits_public_read());
        assert!(!QueryBoundKind::EstimateOnly.admits_public_read());
        assert!(!QueryBoundKind::Unavailable.admits_public_read());
    }

    #[test]
    fn access_kind_classifies_secondary_indexes_and_full_scans() {
        assert!(QueryAdmissionAccessKind::IndexPrefix.is_secondary_index());
        assert!(QueryAdmissionAccessKind::FullScan.is_full_scan());
        assert!(!QueryAdmissionAccessKind::ByKey.is_secondary_index());
    }

    #[test]
    fn rejection_maps_to_stable_diagnostic() {
        let rejection = QueryAdmissionRejection::PublicQueryRequiresLimit;
        let diagnostic = rejection.diagnostic();

        assert_eq!(
            rejection.error_code(),
            icydb_diagnostic_code::ErrorCode::QUERY_READ_PUBLIC_REQUIRES_LIMIT
        );
        assert_eq!(
            diagnostic.code(),
            icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission
        );
    }

    #[test]
    fn summaries_keep_decision_and_rejection_aligned() {
        let admitted = QueryAdmissionSummary::admitted(
            QueryAdmissionLane::PublicRead,
            QueryAdmissionAccessKind::ByKey,
        );
        let rejected = QueryAdmissionSummary::rejected(
            QueryAdmissionLane::PublicRead,
            QueryAdmissionAccessKind::FullScan,
            QueryAdmissionRejection::UnboundedFullScanRejected,
        );

        assert_eq!(admitted.decision(), QueryAdmissionDecision::Admitted);
        assert_eq!(admitted.rejection(), None);
        assert_eq!(rejected.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            rejected.rejection(),
            Some(QueryAdmissionRejection::UnboundedFullScanRejected)
        );
    }

    #[test]
    fn admission_summary_renders_stable_verbose_explain_block() {
        let summary = QueryAdmissionSummary::rejected(
            QueryAdmissionLane::PublicRead,
            QueryAdmissionAccessKind::FullScan,
            QueryAdmissionRejection::UnboundedFullScanRejected,
        );

        let rendered = summary.render_text_block();

        assert!(
            rendered.starts_with("admission:\n  lane=public_read\n  decision=rejected"),
            "admission block should start with stable lane and decision fields: {rendered}",
        );
        assert!(
            rendered.contains("\n  reason=unbounded_full_scan_rejected"),
            "admission block should include a stable rejection reason: {rendered}",
        );
        assert!(
            rendered.contains("\n  selected_access=full_scan"),
            "admission block should include the selected access class: {rendered}",
        );
        assert!(
            rendered.contains("\n  grouped=false"),
            "admission block should include grouped classification: {rendered}",
        );
    }

    #[test]
    fn plan_summary_classifies_full_scan_without_overclaiming_bounds() {
        let plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

        let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

        assert_eq!(summary.plan_shape(), QueryAdmissionPlanShape::ScalarRead);
        assert_eq!(
            summary.selected_access(),
            QueryAdmissionAccessKind::FullScan
        );
        assert_eq!(summary.selected_index(), None);
        assert_eq!(summary.limit(), None);
        assert_eq!(summary.offset(), Some(0));
        assert_eq!(summary.scan_bound(), None);
        assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Unavailable);
        assert_eq!(summary.returned_row_bound(), None);
        assert_eq!(
            summary.returned_row_bound_kind(),
            QueryBoundKind::Unavailable
        );
        assert_eq!(
            summary.residual_filter(),
            QueryAdmissionResidualFilter::Absent
        );
        assert_eq!(summary.ordering(), QueryAdmissionOrdering::None);
    }

    #[test]
    fn plan_summary_uses_point_lookup_and_limit_as_proven_bounds() {
        let mut plan =
            AccessPlannedQuery::new(AccessPath::ByKey(Value::Nat64(7)), MissingRowPolicy::Ignore);
        plan.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(5),
            offset: 2,
        });

        let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

        assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
        assert_eq!(summary.limit(), Some(5));
        assert_eq!(summary.offset(), Some(2));
        assert_eq!(summary.scan_bound(), Some(1));
        assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
        assert_eq!(summary.returned_row_bound(), Some(5));
        assert_eq!(
            summary.returned_row_bound_kind(),
            QueryBoundKind::EnforcedRuntimeCap
        );
    }

    #[test]
    fn plan_summary_uses_exact_primary_key_access_as_returned_row_bound_without_limit() {
        let plan =
            AccessPlannedQuery::new(AccessPath::ByKey(Value::Nat64(7)), MissingRowPolicy::Ignore);

        let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

        assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
        assert_eq!(summary.limit(), None);
        assert_eq!(summary.scan_bound(), Some(1));
        assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
        assert_eq!(summary.returned_row_bound(), Some(1));
        assert_eq!(
            summary.returned_row_bound_kind(),
            QueryBoundKind::ConservativeUpperBound
        );
    }

    #[test]
    fn plan_summary_uses_exact_primary_key_set_as_returned_row_bound_without_limit() {
        let plan = AccessPlannedQuery::new(
            AccessPath::ByKeys(vec![Value::Nat64(7), Value::Nat64(8)]),
            MissingRowPolicy::Ignore,
        );

        let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

        assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKeys);
        assert_eq!(summary.limit(), None);
        assert_eq!(summary.scan_bound(), Some(2));
        assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
        assert_eq!(summary.returned_row_bound(), Some(2));
        assert_eq!(
            summary.returned_row_bound_kind(),
            QueryBoundKind::ConservativeUpperBound
        );
    }

    #[test]
    fn plan_summary_preserves_selected_index_identity() {
        let plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: SemanticIndexAccessContract::model_only_from_generated_index(
                    ADMISSION_INDEX,
                ),
                values: vec![Value::Text("alpha".to_string())],
            },
            MissingRowPolicy::Ignore,
        );

        let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

        assert_eq!(
            summary.selected_access(),
            QueryAdmissionAccessKind::IndexPrefix
        );
        assert_eq!(summary.selected_index(), Some("admission::tag"));
        assert_eq!(summary.scan_bound(), None);
        assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Unavailable);
    }

    #[test]
    fn plan_summary_classifies_residual_and_requested_ordering() {
        let mut plan =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        plan.scalar_plan_mut().predicate = Some(Predicate::eq(
            "tag".to_string(),
            Value::Text("alpha".to_string()),
        ));
        plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![OrderTerm::field("tag", OrderDirection::Asc)],
        });

        let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::AdminAdHoc, &plan);

        assert_eq!(
            summary.residual_filter(),
            QueryAdmissionResidualFilter::Predicate
        );
        assert_eq!(summary.ordering(), QueryAdmissionOrdering::Requested);
        assert!(!summary.materialization().materialized_sort());
        assert_eq!(summary.materialization().materialized_rows(), None);
        assert_eq!(
            summary.materialization().row_bound_kind(),
            QueryBoundKind::Unavailable
        );
    }

    #[test]
    fn plan_summary_carries_grouped_execution_budgets() {
        let grouped =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
                .into_grouped_with_having_expr(
                    GroupSpec {
                        group_fields: vec![FieldSlot::from_test_slot(0, "tag")],
                        aggregates: vec![GroupAggregateSpec {
                            kind: AggregateKind::Count,
                            input_expr: None,
                            filter_expr: None,
                            distinct: false,
                        }],
                        execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
                    },
                    Some(Expr::Field(FieldId::new("tag"))),
                );

        let summary =
            QueryAdmissionSummary::from_plan(QueryAdmissionLane::DiagnosticExplain, &grouped);
        let grouped = summary
            .grouped()
            .expect("summary should include grouped facts");

        assert_eq!(
            summary.plan_shape(),
            QueryAdmissionPlanShape::GroupedAggregate
        );
        assert_eq!(grouped.group_field_count(), 1);
        assert_eq!(grouped.aggregate_count(), 1);
        assert_eq!(grouped.distinct_aggregate_count(), 0);
        assert_eq!(grouped.max_groups(), 12);
        assert_eq!(grouped.max_group_bytes(), 4096);
        assert!(grouped.has_having_filter());
        assert_eq!(summary.returned_row_bound(), Some(12));
        assert_eq!(
            summary.returned_row_bound_kind(),
            QueryBoundKind::ConservativeUpperBound
        );
    }

    #[test]
    fn plan_summary_reads_delete_window_without_executing_it() {
        let mut plan =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        plan.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
        plan.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec {
            limit: Some(3),
            offset: 1,
        });

        let summary =
            QueryAdmissionSummary::from_plan(QueryAdmissionLane::DiagnosticExplain, &plan);

        assert_eq!(summary.plan_shape(), QueryAdmissionPlanShape::Delete);
        assert_eq!(summary.limit(), Some(3));
        assert_eq!(summary.offset(), Some(1));
        assert_eq!(summary.returned_row_bound(), Some(3));
    }

    #[test]
    fn public_read_evaluation_rejects_missing_limit_before_access_shape() {
        let policy = public_read_policy();
        let summary = summary_for_index_prefix(None, 0);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::PublicQueryRequiresLimit)
        );
    }

    #[test]
    fn public_read_evaluation_rejects_full_scan_even_with_limit() {
        let policy = public_read_policy();
        let summary = summary_for_path(AccessPath::<Value>::FullScan, Some(5), 0);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::UnboundedFullScanRejected)
        );
    }

    #[test]
    fn public_read_evaluation_admits_indexed_bounded_scalar_read() {
        let policy = public_read_policy();
        let summary = summary_for_index_prefix(Some(5), 0);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Admitted);
        assert_eq!(evaluated.rejection(), None);
    }

    #[test]
    fn public_read_evaluation_admits_exact_primary_key_read() {
        let policy = public_read_policy();
        let summary = summary_for_path(
            AccessPath::ByKey(Value::Text("primary".to_string())),
            None,
            0,
        );

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Admitted);
        assert_eq!(evaluated.limit(), None);
        assert_eq!(evaluated.scan_bound(), Some(1));
        assert_eq!(evaluated.returned_row_bound(), Some(1));
    }

    #[test]
    fn public_read_evaluation_rejects_primary_key_set_above_returned_row_policy() {
        let policy = public_read_policy();
        let keys = (0..=50).map(Value::Nat64).collect();
        let summary = summary_for_path(AccessPath::ByKeys(keys), None, 0);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::ReturnedRowBoundExceedsPolicy)
        );
    }

    #[test]
    fn public_read_evaluation_rejects_non_zero_offset() {
        let policy = public_read_policy();
        let summary = summary_for_index_prefix(Some(5), 1);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::PublicQueryOffsetRejected)
        );
    }

    #[test]
    fn public_read_evaluation_rejects_returned_row_cap_overflow() {
        let policy = public_read_policy();
        let summary = summary_for_index_prefix(Some(51), 0);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::ReturnedRowBoundExceedsPolicy)
        );
    }

    #[test]
    fn public_read_evaluation_rejects_unresolved_order_materialized_sort() {
        let policy = public_read_policy();
        let summary = summary_for_index_prefix(Some(5), 0);
        let returned_row_bound = summary.returned_row_bound();
        let returned_row_bound_kind = summary.returned_row_bound_kind();
        let summary = summary.with_materialization(QueryMaterializationSummary::sort(
            returned_row_bound,
            returned_row_bound_kind,
        ));

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::SortRequiresMaterialization)
        );
    }

    #[test]
    fn public_read_evaluation_admits_exact_key_set_materialized_sort() {
        let policy = public_read_policy();
        let summary = summary_for_path(
            AccessPath::ByKeys(vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)]),
            None,
            0,
        );
        let returned_row_bound = summary.returned_row_bound();
        let returned_row_bound_kind = summary.returned_row_bound_kind();
        let summary = summary.with_materialization(QueryMaterializationSummary::sort(
            returned_row_bound,
            returned_row_bound_kind,
        ));

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Admitted);
        assert_eq!(evaluated.rejection(), None);
        assert_eq!(
            evaluated.selected_access(),
            QueryAdmissionAccessKind::ByKeys
        );
        assert_eq!(evaluated.scan_bound(), Some(3));
        assert_eq!(evaluated.returned_row_bound(), Some(3));
        assert!(evaluated.materialization().materialized_sort());
    }

    #[test]
    fn public_read_evaluation_rejects_underbounded_key_set_materialized_sort() {
        let policy = public_read_policy();
        let summary = summary_for_path(
            AccessPath::ByKeys(vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)]),
            Some(1),
            0,
        );
        let returned_row_bound = summary.returned_row_bound();
        let returned_row_bound_kind = summary.returned_row_bound_kind();
        let summary = summary.with_materialization(QueryMaterializationSummary::sort(
            returned_row_bound,
            returned_row_bound_kind,
        ));

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::SortRequiresMaterialization)
        );
    }

    #[test]
    fn public_read_evaluation_fails_closed_when_bounded_route_falls_back_to_materialized_order() {
        let policy = public_read_policy();
        let bounded = summary_for_index_prefix(Some(1), 0);
        let admitted = policy.evaluate(bounded.clone());

        assert_eq!(admitted.decision(), QueryAdmissionDecision::Admitted);
        assert_eq!(admitted.returned_row_bound(), Some(1));

        let returned_row_bound = bounded.returned_row_bound();
        let returned_row_bound_kind = bounded.returned_row_bound_kind();
        let fallback = bounded.with_materialization(QueryMaterializationSummary::sort(
            returned_row_bound,
            returned_row_bound_kind,
        ));

        let evaluated = policy.evaluate(fallback);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::SortRequiresMaterialization)
        );
    }

    #[test]
    fn public_read_evaluation_rejects_grouped_query_without_group_budgets() {
        let policy = public_read_policy();
        let summary = grouped_summary_for_index_prefix(12, 4096, false);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::GroupedQueryRequiresLimits)
        );
    }

    #[test]
    fn public_read_evaluation_admits_grouped_query_with_group_budgets_without_limit() {
        let policy = public_grouped_read_policy(None);
        let summary = grouped_summary_for_index_prefix(12, 4096, false);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Admitted);
        assert_eq!(evaluated.limit(), None);
        assert_eq!(evaluated.returned_row_bound(), Some(12));
        assert_eq!(evaluated.rejection(), None);
    }

    #[test]
    fn public_read_evaluation_rejects_grouped_query_above_policy_budget() {
        let policy = public_grouped_read_policy(None);
        let summary = grouped_summary_for_index_prefix(51, 4096, false);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::GroupedQueryExceedsBudget)
        );
    }

    #[test]
    fn public_read_evaluation_rejects_distinct_grouped_query_without_distinct_budget() {
        let policy = public_grouped_read_policy(None);
        let summary = grouped_summary_for_index_prefix(12, 4096, true);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::GroupedQueryRequiresLimits)
        );
    }

    #[test]
    fn diagnostic_explain_policy_rejects_row_execution() {
        let policy = QueryAdmissionPolicy::diagnostic_explain();
        let summary = summary_for_index_prefix(Some(5), 0);

        let evaluated = policy.evaluate(summary);

        assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
        assert_eq!(
            evaluated.rejection(),
            Some(QueryAdmissionRejection::DiagnosticLaneDoesNotExecute)
        );
    }

    fn public_read_policy() -> QueryAdmissionPolicy {
        QueryAdmissionPolicy::public_read(
            NonZeroU32::new(50).expect("test public row cap is non-zero"),
            NonZeroU32::new(32_768).expect("test public byte cap is non-zero"),
        )
    }

    fn public_grouped_read_policy(distinct_entries: Option<NonZeroU32>) -> QueryAdmissionPolicy {
        public_read_policy().with_grouped_policy(GroupedAdmissionPolicy::bounded(
            NonZeroU32::new(50).expect("test public group cap is non-zero"),
            NonZeroU32::new(8192).expect("test public group byte cap is non-zero"),
            distinct_entries,
        ))
    }

    fn summary_for_index_prefix(limit: Option<u32>, offset: u32) -> QueryAdmissionSummary {
        summary_for_path(
            AccessPath::IndexPrefix {
                index: SemanticIndexAccessContract::model_only_from_generated_index(
                    ADMISSION_INDEX,
                ),
                values: vec![Value::Text("alpha".to_string())],
            },
            limit,
            offset,
        )
    }

    fn summary_for_path(
        path: AccessPath<Value>,
        limit: Option<u32>,
        offset: u32,
    ) -> QueryAdmissionSummary {
        let mut plan = AccessPlannedQuery::new(path, MissingRowPolicy::Ignore);
        plan.scalar_plan_mut().page = Some(PageSpec { limit, offset });

        QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan)
    }

    fn grouped_summary_for_index_prefix(
        max_groups: u64,
        max_group_bytes: u64,
        distinct: bool,
    ) -> QueryAdmissionSummary {
        let grouped = AccessPlannedQuery::new(index_prefix_path(), MissingRowPolicy::Ignore)
            .into_grouped(GroupSpec {
                group_fields: vec![FieldSlot::from_test_slot(0, "tag")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    input_expr: Some(Box::new(Expr::Field(FieldId::new("tag")))),
                    filter_expr: None,
                    distinct,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(max_groups, max_group_bytes),
            });

        QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &grouped)
    }

    fn index_prefix_path() -> AccessPath<Value> {
        AccessPath::IndexPrefix {
            index: SemanticIndexAccessContract::model_only_from_generated_index(ADMISSION_INDEX),
            values: vec![Value::Text("alpha".to_string())],
        }
    }
}
