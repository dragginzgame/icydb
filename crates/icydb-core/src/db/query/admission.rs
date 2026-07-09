//! Module: db::query::admission
//! Responsibility: shared read-admission vocabulary for query surfaces.
//! Does not own: physical planning, executor runtime, or SQL/fluent lowering.
//! Boundary: describes policy, proven bounds, and stable rejection diagnostics.

mod plan_summary;
mod policy;
mod render;

use crate::db::query::plan::AccessPlannedQuery;
use icydb_diagnostic_code::{
    Diagnostic, DiagnosticCode, DiagnosticDetail, ErrorCode, ErrorOrigin, QueryReadAdmissionCode,
};

#[cfg(test)]
pub(in crate::db) use policy::GroupedAdmissionPolicy;
pub(in crate::db) use policy::QueryAdmissionPolicy;
pub(in crate::db::query) use policy::{
    DEFAULT_BOUNDED_READ_MAX_ROWS, DEFAULT_BOUNDED_READ_RESPONSE_BYTES,
};

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
        plan_summary::summary_from_plan(lane, plan)
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
        render::render_text_block(self)
    }
}

#[cfg(test)]
mod tests;
