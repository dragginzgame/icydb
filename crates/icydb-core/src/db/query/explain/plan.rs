//! Module: query::explain::plan
//! Responsibility: deterministic planned-query projection for EXPLAIN,
//! including logical shape, access shape, and pushdown observability.
//! Does not own: execution descriptor rendering or access visitor adapters.
//! Boundary: explain DTOs and plan-side projection logic for query observability.

mod predicate;

#[cfg(test)]
mod tests;

use crate::{
    db::QueryError,
    db::{
        access::AccessPlan,
        executor::SharedPreparedExecutionPlan,
        predicate::{CoercionSpec, CompareOp, MissingRowPolicy},
        query::{
            builder::scalar_projection::write_scalar_projection_expr_plan_label,
            explain::{
                access_projection::write_access_json,
                explain_access_plan,
                writer::{JsonWriter, render_logical},
            },
            plan::{
                AccessChoiceCandidateExplainSummary, AccessChoiceExplainSnapshot,
                AccessChoiceRejectedIndex, AccessChoiceResidualBurden, AccessChoiceSelectedReason,
                AccessPlannedQuery, AggregateKind, DeleteLimitSpec, GroupedPlanFallbackReason,
                LogicalPlan, OrderDirection, OrderSpec, PageSpec, QueryMode, ScalarPlan,
                expr::{Expr, PathSpec},
                grouped_plan_strategy_for_explain, write_explain_access_strategy_label,
            },
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::{fmt, ops::Bound};

///
/// ExplainPlan
///
/// Stable, deterministic representation of a planned query for observability.
///

#[derive(Clone, Eq, PartialEq)]
pub struct ExplainPlan {
    mode: QueryMode,
    access: ExplainAccessPath,
    access_decision: ExplainAccessDecision,
    filter_expr: Option<String>,
    predicate: ExplainPredicate,
    order_by: ExplainOrderBy,
    distinct: bool,
    grouping: ExplainGrouping,
    order_pushdown: ExplainOrderPushdown,
    page: ExplainPagination,
    delete_limit: ExplainDeleteLimit,
    consistency: MissingRowPolicy,
}

#[expect(clippy::missing_fields_in_debug)]
impl fmt::Debug for ExplainPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExplainPlan")
            .field("mode", &self.mode)
            .field("access", &self.access)
            .field("filter_expr", &self.filter_expr)
            .field("predicate", &self.predicate)
            .field("order_by", &self.order_by)
            .field("distinct", &self.distinct)
            .field("grouping", &self.grouping)
            .field("order_pushdown", &self.order_pushdown)
            .field("page", &self.page)
            .field("delete_limit", &self.delete_limit)
            .field("consistency", &self.consistency)
            .finish()
    }
}

impl ExplainPlan {
    /// Return query mode projected by this explain plan.
    #[must_use]
    pub const fn mode(&self) -> QueryMode {
        self.mode
    }

    /// Borrow projected access-path shape.
    #[must_use]
    pub const fn access(&self) -> &ExplainAccessPath {
        &self.access
    }

    /// Borrow the structured planner access-decision projection.
    #[must_use]
    pub const fn access_decision(&self) -> &ExplainAccessDecision {
        &self.access_decision
    }

    /// Borrow projected semantic scalar filter expression when present.
    #[must_use]
    pub fn filter_expr(&self) -> Option<&str> {
        self.filter_expr.as_deref()
    }

    /// Borrow projected predicate shape.
    #[must_use]
    pub const fn predicate(&self) -> &ExplainPredicate {
        &self.predicate
    }

    /// Borrow projected ORDER BY shape.
    #[must_use]
    pub const fn order_by(&self) -> &ExplainOrderBy {
        &self.order_by
    }

    /// Return whether DISTINCT is enabled.
    #[must_use]
    pub const fn distinct(&self) -> bool {
        self.distinct
    }

    /// Borrow projected grouped-shape metadata.
    #[must_use]
    pub const fn grouping(&self) -> &ExplainGrouping {
        &self.grouping
    }

    /// Borrow projected ORDER pushdown status.
    #[must_use]
    pub const fn order_pushdown(&self) -> &ExplainOrderPushdown {
        &self.order_pushdown
    }

    /// Borrow projected pagination status.
    #[must_use]
    pub const fn page(&self) -> &ExplainPagination {
        &self.page
    }

    /// Borrow projected delete-limit status.
    #[must_use]
    pub const fn delete_limit(&self) -> &ExplainDeleteLimit {
        &self.delete_limit
    }

    /// Return missing-row consistency policy.
    #[must_use]
    pub const fn consistency(&self) -> MissingRowPolicy {
        self.consistency
    }
}

impl ExplainPlan {
    /// Render this logical explain plan as deterministic canonical text.
    ///
    /// Output is limited to 1 MiB of UTF-8 bytes per call. Limit or formatter
    /// failure returns an error, never partial text. This detached operation
    /// does not charge a session, bound prior planning, or redact values.
    /// Access operands and predicate/HAVING trees are summarized, not dumped.
    /// Existing clause labels can still contain literal values.
    pub fn render_text_canonical(&self) -> Result<String, QueryError> {
        render_logical(|out| {
            write!(out, "mode={:?}\naccess=", self.mode())?;
            write_access_json(self.access(), out)?;
            out.write_str("\naccess_decision=")?;
            write_access_decision_json(self.access_decision(), out)?;
            write!(
                out,
                "\nfilter_expr={:?}\nhas_predicate={}\norder_by={:?}\ndistinct={}\ngrouping=",
                self.filter_expr(),
                !matches!(self.predicate(), ExplainPredicate::None),
                self.order_by(),
                self.distinct(),
            )?;
            write_grouping_json(self.grouping(), out)?;
            write!(
                out,
                "\norder_pushdown={:?}\npage={:?}\ndelete_limit={:?}\nconsistency={:?}",
                self.order_pushdown(),
                self.page(),
                self.delete_limit(),
                self.consistency(),
            )
        })
    }

    /// Render this logical explain plan as canonical JSON.
    ///
    /// Output is limited to 1 MiB of UTF-8 bytes, including JSON escaping.
    /// Failure returns no partial JSON. This detached operation does not charge
    /// a session, bound prior planning, or redact values.
    /// Access operands and predicate/HAVING trees are summarized, not dumped.
    /// Existing clause labels can still contain literal values.
    pub fn render_json_canonical(&self) -> Result<String, QueryError> {
        render_logical(|out| write_logical_explain_json(self, out))
    }
}

///
/// ExplainGrouping
///
/// Grouped-shape annotation for deterministic explain reports.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainGrouping {
    None,
    Grouped {
        strategy: &'static str,
        fallback_reason: Option<&'static str>,
        group_fields: Vec<ExplainGroupField>,
        aggregates: Vec<ExplainGroupAggregate>,
        having: Option<ExplainGroupHaving>,
        max_groups: u64,
        max_group_bytes: u64,
    },
}

///
/// ExplainGroupField
///
/// Stable grouped-key field identity carried by explain reports.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupField {
    pub(in crate::db) slot_index: usize,
    pub(in crate::db) field: String,
    pub(in crate::db) path: Option<PathSpec>,
}

impl ExplainGroupField {
    /// Return grouped slot index.
    #[must_use]
    pub const fn slot_index(&self) -> usize {
        self.slot_index
    }

    /// Borrow grouped field name.
    #[must_use]
    pub const fn field(&self) -> &str {
        self.field.as_str()
    }
}

///
/// ExplainGroupAggregate
///
/// Stable explain-surface projection of one grouped aggregate terminal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupAggregate {
    pub(in crate::db) kind: AggregateKind,
    pub(in crate::db) target_field: Option<String>,
    pub(in crate::db) input_expr: Option<String>,
    pub(in crate::db) filter_expr: Option<String>,
    pub(in crate::db) distinct: bool,
}

impl ExplainGroupAggregate {
    /// Return grouped aggregate kind.
    #[must_use]
    pub const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow optional grouped aggregate target field.
    #[must_use]
    pub fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Borrow optional grouped aggregate input expression label.
    #[must_use]
    pub fn input_expr(&self) -> Option<&str> {
        self.input_expr.as_deref()
    }

    /// Borrow optional grouped aggregate filter expression label.
    #[must_use]
    pub fn filter_expr(&self) -> Option<&str> {
        self.filter_expr.as_deref()
    }

    /// Return whether grouped aggregate uses DISTINCT input semantics.
    #[must_use]
    pub const fn distinct(&self) -> bool {
        self.distinct
    }
}

///
/// ExplainGroupHaving
///
/// Deterministic explain projection of grouped HAVING clauses.
/// This surface now carries the shared planner-owned post-aggregate expression
/// directly so explain no longer keeps a second grouped HAVING AST.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupHaving {
    pub(in crate::db) expr: Expr,
}

///
/// ExplainOrderPushdown
///
/// Deterministic ORDER BY pushdown eligibility reported by explain.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainOrderPushdown {
    MissingModelContext,
    EligibleSecondaryIndex { index: String, prefix_len: usize },
    Rejected(SecondaryOrderPushdownRejection),
}

///
/// SecondaryOrderPushdownRejection
///
/// Stable explain-surface reason why secondary-index ORDER BY pushdown was
/// rejected. Executor route planning converts its runtime route reasons into
/// this neutral query DTO before rendering explain payloads.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecondaryOrderPushdownRejection {
    NoOrderBy,
    AccessPathNotSingleIndexPrefix,
    AccessPathIndexRangeUnsupported {
        index: String,
        prefix_len: usize,
    },
    InvalidIndexPrefixBounds {
        prefix_len: usize,
        index_field_len: usize,
    },
    MissingPrimaryKeyTieBreak {
        field: String,
    },
    PrimaryKeyDirectionNotAscending {
        field: String,
    },
    MixedDirectionNotEligible {
        field: String,
    },
    OrderFieldsDoNotMatchIndex {
        index: String,
        prefix_len: usize,
        expected_suffix: Vec<String>,
        expected_full: Vec<String>,
        actual: Vec<String>,
    },
    VariablePrefixSuffixOrderUnsupported {
        index: String,
        prefix_len: usize,
        expected_full: Vec<String>,
        actual: Vec<String>,
    },
}

///
/// ExplainAccessPath
///
/// Deterministic projection of logical access path shape for diagnostics.
/// Mirrors planner-selected structural paths without runtime cursor state.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainAccessPath {
    ByKey {
        key: Value,
    },
    ByKeys {
        keys: Vec<Value>,
    },
    KeyRange {
        start: Value,
        end: Value,
    },
    IndexPrefix {
        name: String,
        fields: Vec<String>,
        prefix_len: usize,
        values: Vec<Value>,
    },
    IndexMultiLookup {
        name: String,
        fields: Vec<String>,
        values: Vec<Value>,
    },
    IndexBranchSet {
        name: String,
        fields: Vec<String>,
        fixed_values: Vec<Value>,
        branch_values: Vec<Value>,
        branch_field: Option<String>,
    },
    IndexRange {
        name: String,
        fields: Vec<String>,
        prefix_len: usize,
        prefix: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    },
    FullScan,
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

/// Stable JSON-facing access-decision projection for logical EXPLAIN.
///
/// This DTO is derived from the planner-owned access-choice snapshot and the
/// selected explain access path. It is not an optimizer model and does not
/// participate in access selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainAccessDecision {
    /// Selected access path summary.
    pub selected: ExplainSelectedAccess,
    /// Planner candidate summaries recorded for the selected access family.
    pub candidates: Vec<ExplainAccessCandidate>,
    /// Eligible alternatives not selected by the planner.
    pub alternatives: Vec<ExplainEligibleAlternative>,
    /// Rejected index candidates and planner-owned reason strings.
    pub rejections: Vec<ExplainRejectedIndex>,
    /// Residual-work summary for the selected route when available.
    pub residual: ExplainResidualSummary,
    /// Availability class of exact cardinality evidence used at selection time.
    pub cardinality_evidence_state: &'static str,
}

impl ExplainAccessDecision {
    fn from_snapshot(
        selected_access: &ExplainAccessPath,
        snapshot: &AccessChoiceExplainSnapshot,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let selected_label =
            work.render_text(|out| write_explain_access_strategy_label(selected_access, out))?;
        let index_name = selected_index_name(selected_access)
            .map(|name| work.copy_text(name))
            .transpose()?;
        // Match the first selected identity while copying candidates, avoiding
        // a second list walk. Labels are diagnostic text, never matching keys.
        let mut selected_candidate = None;
        let mut candidates = work.vec_with_capacity(snapshot.candidates.len())?;
        for candidate in &snapshot.candidates {
            let copied = ExplainAccessCandidate::from_candidate(candidate, work)?;
            if selected_candidate.is_none()
                && let Some(name) = index_name.as_deref()
                && name.len() == candidate.index_name().len()
            {
                work.charge(Resource::PredicateExpressionSteps, name.len() as u64)?;
                if name == candidate.index_name() {
                    selected_candidate = Some(candidate);
                }
            }
            candidates.push(copied);
        }

        Ok(Self {
            selected: ExplainSelectedAccess {
                kind: ExplainAccessDecisionKind::from_access_path(selected_access),
                index_name,
                label: selected_label,
                reason: snapshot.chosen_reason().code(),
            },
            candidates,
            alternatives: work.copy_slice(&snapshot.alternatives, |name| {
                Ok(ExplainEligibleAlternative {
                    index_name: work.copy_text(name)?,
                })
            })?,
            rejections: work.copy_slice(&snapshot.rejected, |rejection| {
                ExplainRejectedIndex::from_rejection(rejection, work)
            })?,
            residual: ExplainResidualSummary::from_selected_access_and_candidate(
                access_bound_predicate_count(selected_access, work)?,
                selected_candidate,
                snapshot.chosen_reason(),
            ),
            cardinality_evidence_state: snapshot.cardinality_evidence_state,
        })
    }
}

impl fmt::Display for ExplainAccessDecision {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let index = self
            .selected
            .index_name
            .as_deref()
            .map_or("none", |index| index);

        write!(
            out,
            "kind={} index={} reason={} residual={} cardinality_evidence={} candidates={} alternatives={} rejections={}",
            self.selected.kind.code(),
            index,
            self.selected.reason,
            self.residual.burden_class,
            self.cardinality_evidence_state,
            self.candidates.len(),
            self.alternatives.len(),
            self.rejections.len(),
        )
    }
}

/// Selected access path summary inside an access-decision explain payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainSelectedAccess {
    /// Selected access kind.
    pub kind: ExplainAccessDecisionKind,
    /// Selected semantic index name, when the selected route is index-backed.
    pub index_name: Option<String>,
    /// Planner access label used for candidate matching and diagnostics.
    pub label: String,
    /// Planner-owned selected reason code.
    pub reason: &'static str,
}

/// Stable access-kind code used by the access-decision explain payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainAccessDecisionKind {
    /// Direct primary-key lookup.
    ByKey,
    /// Multiple primary-key lookup.
    ByKeys,
    /// Primary-key range lookup.
    KeyRange,
    /// Secondary-index equality prefix lookup.
    IndexPrefix,
    /// Secondary-index multi-value lookup.
    IndexMultiLookup,
    /// Branch-aware secondary-index composite prefix lookup.
    IndexBranchSet,
    /// Secondary-index range lookup.
    IndexRange,
    /// Full entity scan.
    FullScan,
    /// Union access route.
    Union,
    /// Intersection access route.
    Intersection,
}

impl ExplainAccessDecisionKind {
    const fn from_access_path(access: &ExplainAccessPath) -> Self {
        match access {
            ExplainAccessPath::ByKey { .. } => Self::ByKey,
            ExplainAccessPath::ByKeys { .. } => Self::ByKeys,
            ExplainAccessPath::KeyRange { .. } => Self::KeyRange,
            ExplainAccessPath::IndexPrefix { .. } => Self::IndexPrefix,
            ExplainAccessPath::IndexMultiLookup { .. } => Self::IndexMultiLookup,
            ExplainAccessPath::IndexBranchSet { .. } => Self::IndexBranchSet,
            ExplainAccessPath::IndexRange { .. } => Self::IndexRange,
            ExplainAccessPath::FullScan => Self::FullScan,
            ExplainAccessPath::Union(_) => Self::Union,
            ExplainAccessPath::Intersection(_) => Self::Intersection,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::ByKey => "ByKey",
            Self::ByKeys => "ByKeys",
            Self::KeyRange => "KeyRange",
            Self::IndexPrefix => "IndexPrefix",
            Self::IndexMultiLookup => "IndexMultiLookup",
            Self::IndexBranchSet => "IndexBranchSet",
            Self::IndexRange => "IndexRange",
            Self::FullScan => "FullScan",
            Self::Union => "Union",
            Self::Intersection => "Intersection",
        }
    }
}

/// Candidate summary recorded by the planner access-choice snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainAccessCandidate {
    /// Planner access label for the candidate route.
    pub label: String,
    /// Whether the candidate structurally satisfied all usable predicates.
    pub exact: bool,
    /// Whether the candidate uses a filtered index contract.
    pub filtered: bool,
    /// Number of range-bound fields recorded by the planner scorer.
    pub range_bound_count: usize,
    /// Whether candidate ordering is compatible with query ordering.
    pub order_compatible: bool,
    /// Residual burden class recorded by the planner.
    pub residual_burden: &'static str,
    /// Number of residual predicate terms recorded by the planner.
    pub residual_predicate_terms: usize,
    /// Exact matching prefix entries at selection time, when available.
    pub exact_prefix_entries: Option<u64>,
}

impl ExplainAccessCandidate {
    fn from_candidate(
        candidate: &AccessChoiceCandidateExplainSummary,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        Ok(Self {
            label: work.render_text(|out| write!(out, "{candidate}"))?,
            exact: candidate.exact,
            filtered: candidate.filtered,
            range_bound_count: candidate.range_bound_count,
            order_compatible: candidate.order_compatible,
            residual_burden: candidate.residual_burden.label(),
            residual_predicate_terms: candidate.residual_predicate_terms,
            exact_prefix_entries: candidate.exact_prefix_entries,
        })
    }
}

/// Eligible alternative index name recorded by the planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainEligibleAlternative {
    /// Semantic index name of the eligible alternative.
    pub index_name: String,
}

/// Rejected index candidate summary recorded by the planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainRejectedIndex {
    /// Semantic index name carried by the planner rejection.
    pub index_name: Option<String>,
    /// Planner-owned rejection reason code.
    pub reason: Option<String>,
    /// Stable rendered planner rejection label.
    pub label: String,
}

impl ExplainRejectedIndex {
    fn from_rejection(
        rejection: &AccessChoiceRejectedIndex,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        Ok(Self {
            index_name: Some(work.copy_text(rejection.index_name())?),
            reason: Some(work.copy_text(rejection.reason_code())?),
            label: work.render_text(|out| write!(out, "{rejection}"))?,
        })
    }
}

/// Residual-work summary for the selected access route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainResidualSummary {
    /// Residual burden class for the selected access route.
    pub burden_class: &'static str,
    /// Whether any residual scalar filter expression survives access planning.
    pub has_residual_filter: bool,
    /// Whether any residual predicate model survives access planning.
    pub has_residual_predicate: bool,
    /// Number of predicate-like constraints structurally consumed by access.
    pub access_bound_predicate_count: usize,
    /// Number of residual predicate terms for the selected access route.
    pub residual_predicate_count: usize,
}

impl ExplainResidualSummary {
    const fn from_selected_access_and_candidate(
        access_bound_predicate_count: usize,
        selected_candidate: Option<&AccessChoiceCandidateExplainSummary>,
        selected_reason: AccessChoiceSelectedReason,
    ) -> Self {
        if let Some(candidate) = selected_candidate {
            Self {
                burden_class: candidate.residual_burden.label(),
                has_residual_filter: matches!(
                    candidate.residual_burden,
                    AccessChoiceResidualBurden::ScalarExpression
                ),
                has_residual_predicate: candidate.residual_predicate_terms > 0,
                access_bound_predicate_count,
                residual_predicate_count: candidate.residual_predicate_terms,
            }
        } else if matches!(
            selected_reason,
            AccessChoiceSelectedReason::PlannerExactIndexIntersection
        ) {
            Self {
                burden_class: AccessChoiceResidualBurden::PredicateOnly.label(),
                has_residual_filter: false,
                has_residual_predicate: true,
                access_bound_predicate_count,
                residual_predicate_count: access_bound_predicate_count,
            }
        } else {
            Self {
                burden_class: AccessChoiceResidualBurden::None.label(),
                has_residual_filter: false,
                has_residual_predicate: false,
                access_bound_predicate_count,
                residual_predicate_count: 0,
            }
        }
    }
}

///
/// ExplainPredicate
///
/// Deterministic projection of canonical predicate structure for explain output.
/// This preserves the planner's normalized predicate shape for inspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainPredicate {
    None,
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare {
        field: String,
        op: CompareOp,
        value: Value,
        coercion: CoercionSpec,
    },
    CompareFields {
        left_field: String,
        op: CompareOp,
        right_field: String,
        coercion: CoercionSpec,
    },
    IsNull {
        field: String,
    },
    IsNotNull {
        field: String,
    },
    IsMissing {
        field: String,
    },
    IsEmpty {
        field: String,
    },
    IsNotEmpty {
        field: String,
    },
    TextContains {
        field: String,
        value: Value,
    },
    TextContainsCi {
        field: String,
        value: Value,
    },
}

///
/// ExplainOrderBy
///
/// Deterministic projection of canonical ORDER BY shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainOrderBy {
    None,
    Fields(Vec<ExplainOrder>),
}

///
/// ExplainOrder
///
/// One canonical ORDER BY field + direction pair.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainOrder {
    pub(in crate::db) field: String,
    pub(in crate::db) direction: OrderDirection,
}

impl ExplainOrder {
    /// Borrow ORDER BY field name.
    #[must_use]
    pub const fn field(&self) -> &str {
        self.field.as_str()
    }

    /// Return ORDER BY direction.
    #[must_use]
    pub const fn direction(&self) -> OrderDirection {
        self.direction
    }
}

///
/// ExplainPagination
///
/// Explain-surface projection of pagination window configuration.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainPagination {
    None,
    Page { limit: Option<u32>, offset: u32 },
}

///
/// ExplainDeleteLimit
///
/// Explain-surface projection of delete-limit configuration.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainDeleteLimit {
    None,
    Limit { max_rows: u32 },
    Window { limit: Option<u32>, offset: u32 },
}

impl SharedPreparedExecutionPlan {
    /// Project a finalized shared plan without rebuilding its execution facts.
    /// The session must establish current authority and cache validity before
    /// calling; this operation owns diagnostic construction only.
    pub(in crate::db) fn explain(
        &self,
        work: &PreparationWork<'_>,
    ) -> Result<ExplainPlan, QueryError> {
        self.logical_plan().project_explain(work)
    }
}

impl AccessPlannedQuery {
    /// Produce a stable, deterministic explanation of this logical plan.
    /// Diagnostic construction consumes the caller's existing request budget;
    /// it never mutates the borrowed plan or resets that budget on cache hits.
    pub(in crate::db::query) fn project_explain(
        &self,
        work: &PreparationWork<'_>,
    ) -> Result<ExplainPlan, QueryError> {
        work.charge(Resource::PredicateExpressionSteps, 1)?;

        // Phase 1: project logical plan variant into scalar core + grouped metadata.
        let (logical, grouping) = match &self.logical {
            LogicalPlan::Scalar(logical) => (logical, ExplainGrouping::None),
            LogicalPlan::Grouped(logical) => {
                let grouped_strategy = grouped_plan_strategy_for_explain(self, logical, work)?;

                (
                    &logical.scalar,
                    ExplainGrouping::Grouped {
                        strategy: grouped_strategy.code(),
                        fallback_reason: grouped_strategy
                            .fallback_reason()
                            .map(GroupedPlanFallbackReason::code),
                        group_fields: {
                            let mut fields =
                                work.vec_with_capacity(logical.group.group_fields.len())?;
                            for group_field in logical.group.group_fields.iter() {
                                fields.push(ExplainGroupField {
                                    slot_index: group_field.root_slot(),
                                    field: work.copy_text(group_field.field())?,
                                    path: group_field
                                        .as_scalar_path()
                                        .map(|path| {
                                            let path = path.path();
                                            Ok::<_, QueryError>(PathSpec::new(
                                                work.copy_text(path.root().as_str())?,
                                                work.copy_slice(path.segments(), |segment| {
                                                    work.copy_text(segment)
                                                })?,
                                            ))
                                        })
                                        .transpose()?,
                                });
                            }
                            fields
                        },
                        aggregates: work.copy_slice(&logical.group.aggregates, |aggregate| {
                            work.charge(Resource::PredicateExpressionSteps, 1)?;
                            Ok(ExplainGroupAggregate {
                                kind: aggregate.kind(),
                                target_field: aggregate
                                    .target_field()
                                    .map(|field| work.copy_text(field))
                                    .transpose()?,
                                input_expr: aggregate
                                    .input_expr()
                                    .map(|expr| explain_expr_label(expr, work))
                                    .transpose()?,
                                filter_expr: aggregate
                                    .filter_expr()
                                    .map(|expr| explain_expr_label(expr, work))
                                    .transpose()?,
                                distinct: aggregate.raw_distinct(),
                            })
                        })?,
                        having: explain_group_having(logical, work)?,
                        max_groups: logical.group.execution.max_groups(),
                        max_group_bytes: logical.group.execution.max_group_bytes(),
                    },
                )
            }
        };

        // Phase 2: project scalar plan + access path into deterministic explain surface.
        explain_scalar_inner(logical, grouping, &self.access, self.access_choice(), work)
    }
}

fn explain_group_having(
    logical: &crate::db::query::plan::GroupPlan,
    work: &PreparationWork<'_>,
) -> Result<Option<ExplainGroupHaving>, QueryError> {
    logical
        .having_expr()
        .map(|expr| {
            Ok(ExplainGroupHaving {
                expr: work.copy_expr(expr)?,
            })
        })
        .transpose()
}

// Render the canonical model directly into the request-owned construction
// sink. Do not re-normalize syntax or allocate an uncharged intermediate label.
fn explain_expr_label(expr: &Expr, work: &PreparationWork<'_>) -> Result<String, QueryError> {
    work.render_text(|out| write_scalar_projection_expr_plan_label(expr, out))
}

fn explain_scalar_inner(
    logical: &ScalarPlan,
    grouping: ExplainGrouping,
    access: &AccessPlan<Value>,
    access_choice: &AccessChoiceExplainSnapshot,
    work: &PreparationWork<'_>,
) -> Result<ExplainPlan, QueryError> {
    // Phase 1: consume canonical predicate model from planner-owned scalar semantics.
    let filter_expr = logical
        .filter_expr
        .as_ref()
        .map(|expr| explain_expr_label(expr, work))
        .transpose()?;
    let predicate = match &logical.predicate {
        Some(predicate) => ExplainPredicate::from_predicate(predicate, work)?,
        None => ExplainPredicate::None,
    };

    // Phase 2: project scalar-plan fields into explain-specific enums.
    let order_by = explain_order(logical.order.as_ref(), work)?;
    let order_pushdown = explain_order_pushdown();
    let page = explain_page(logical.page.as_ref());
    let delete_limit = explain_delete_limit(logical.delete_limit.as_ref());

    // Phase 3: assemble one stable explain payload.
    let access = explain_access_plan(access, work)?;
    let access_decision = ExplainAccessDecision::from_snapshot(&access, access_choice, work)?;

    Ok(ExplainPlan {
        mode: logical.mode,
        access,
        access_decision,
        filter_expr,
        predicate,
        order_by,
        distinct: logical.distinct,
        grouping,
        order_pushdown,
        page,
        delete_limit,
        consistency: logical.consistency,
    })
}

const fn selected_index_name(access: &ExplainAccessPath) -> Option<&str> {
    match access {
        ExplainAccessPath::IndexPrefix { name, .. }
        | ExplainAccessPath::IndexMultiLookup { name, .. }
        | ExplainAccessPath::IndexBranchSet { name, .. }
        | ExplainAccessPath::IndexRange { name, .. } => Some(name.as_str()),
        ExplainAccessPath::ByKey { .. }
        | ExplainAccessPath::ByKeys { .. }
        | ExplainAccessPath::KeyRange { .. }
        | ExplainAccessPath::FullScan
        | ExplainAccessPath::Union(_)
        | ExplainAccessPath::Intersection(_) => None,
    }
}

fn access_bound_predicate_count(
    access: &ExplainAccessPath,
    work: &PreparationWork<'_>,
) -> Result<usize, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    Ok(match access {
        ExplainAccessPath::ByKey { .. }
        | ExplainAccessPath::ByKeys { .. }
        | ExplainAccessPath::IndexMultiLookup { .. } => 1,
        ExplainAccessPath::IndexBranchSet {
            fixed_values,
            branch_values,
            ..
        } => fixed_values.len() + usize::from(!branch_values.is_empty()),
        ExplainAccessPath::KeyRange { .. } => 2,
        ExplainAccessPath::IndexPrefix { prefix_len, .. } => *prefix_len,
        ExplainAccessPath::IndexRange {
            prefix_len,
            lower,
            upper,
            ..
        } => *prefix_len + bound_constraint_count(lower) + bound_constraint_count(upper),
        ExplainAccessPath::FullScan => 0,
        ExplainAccessPath::Union(children) | ExplainAccessPath::Intersection(children) => {
            let mut count = 0;
            for child in children {
                count += access_bound_predicate_count(child, work)?;
            }
            count
        }
    })
}

const fn bound_constraint_count(bound: &Bound<Value>) -> usize {
    match bound {
        Bound::Included(_) | Bound::Excluded(_) => 1,
        Bound::Unbounded => 0,
    }
}

pub(in crate::db) const fn explain_order_pushdown() -> ExplainOrderPushdown {
    // Query explain does not own physical pushdown feasibility routing.
    ExplainOrderPushdown::MissingModelContext
}

fn explain_order(
    order: Option<&OrderSpec>,
    work: &PreparationWork<'_>,
) -> Result<ExplainOrderBy, QueryError> {
    let Some(order) = order else {
        return Ok(ExplainOrderBy::None);
    };

    if order.fields.is_empty() {
        return Ok(ExplainOrderBy::None);
    }

    Ok(ExplainOrderBy::Fields(work.copy_slice(
        &order.fields,
        |term| {
            Ok(ExplainOrder {
                field: explain_expr_label(term.expr(), work)?,
                direction: term.direction(),
            })
        },
    )?))
}

pub(in crate::db) const fn explain_page(page: Option<&PageSpec>) -> ExplainPagination {
    match page {
        Some(page) => ExplainPagination::Page {
            limit: page.limit,
            offset: page.offset,
        },
        None => ExplainPagination::None,
    }
}

const fn explain_delete_limit(limit: Option<&DeleteLimitSpec>) -> ExplainDeleteLimit {
    match limit {
        Some(limit) if limit.offset == 0 => match limit.limit {
            Some(max_rows) => ExplainDeleteLimit::Limit { max_rows },
            None => ExplainDeleteLimit::Window {
                limit: None,
                offset: 0,
            },
        },
        Some(limit) => ExplainDeleteLimit::Window {
            limit: limit.limit,
            offset: limit.offset,
        },
        None => ExplainDeleteLimit::None,
    }
}

fn write_logical_explain_json(explain: &ExplainPlan, out: &mut dyn fmt::Write) -> fmt::Result {
    let mut object = JsonWriter::begin_object(out)?;
    object.field_with("mode", |out| {
        let mut object = JsonWriter::begin_object(out)?;
        match explain.mode() {
            QueryMode::Load(spec) => {
                object.field_str("type", "Load")?;
                match spec.limit() {
                    Some(limit) => object.field_u64("limit", u64::from(limit))?,
                    None => object.field_null("limit")?,
                }
                object.field_u64("offset", u64::from(spec.offset()))?;
            }
            QueryMode::Delete(spec) => {
                object.field_str("type", "Delete")?;
                match spec.limit() {
                    Some(limit) => object.field_u64("limit", u64::from(limit))?,
                    None => object.field_null("limit")?,
                }
            }
        }
        object.finish()?;
        Ok(())
    })?;
    object.field_with("access", |out| {
        write_access_json(explain.access(), out)?;
        Ok(())
    })?;
    object.field_with("access_decision", |out| {
        write_access_decision_json(explain.access_decision(), out)?;
        Ok(())
    })?;
    match explain.filter_expr() {
        Some(filter_expr) => object.field_str("filter_expr", filter_expr)?,
        None => object.field_null("filter_expr")?,
    }
    object.field_bool(
        "has_predicate",
        !matches!(explain.predicate(), ExplainPredicate::None),
    )?;
    object.field_value_debug("order_by", explain.order_by())?;
    object.field_bool("distinct", explain.distinct())?;
    object.field_with("grouping", |out| {
        write_grouping_json(explain.grouping(), out)
    })?;
    object.field_value_debug("order_pushdown", explain.order_pushdown())?;
    object.field_with("page", |out| {
        let mut object = JsonWriter::begin_object(out)?;
        match explain.page() {
            ExplainPagination::None => {
                object.field_str("type", "None")?;
            }
            ExplainPagination::Page { limit, offset } => {
                object.field_str("type", "Page")?;
                match limit {
                    Some(limit) => object.field_u64("limit", u64::from(*limit))?,
                    None => object.field_null("limit")?,
                }
                object.field_u64("offset", u64::from(*offset))?;
            }
        }
        object.finish()?;
        Ok(())
    })?;
    object.field_with("delete_limit", |out| {
        let mut object = JsonWriter::begin_object(out)?;
        match explain.delete_limit() {
            ExplainDeleteLimit::None => {
                object.field_str("type", "None")?;
            }
            ExplainDeleteLimit::Limit { max_rows } => {
                object.field_str("type", "Limit")?;
                object.field_u64("max_rows", u64::from(*max_rows))?;
            }
            ExplainDeleteLimit::Window { limit, offset } => {
                object.field_str("type", "Window")?;
                object.field_with("limit", |out| match limit {
                    Some(limit) => write!(out, "{limit}"),
                    None => out.write_str("null"),
                })?;
                object.field_u64("offset", u64::from(*offset))?;
            }
        }
        object.finish()?;
        Ok(())
    })?;
    object.field_value_debug("consistency", &explain.consistency())?;
    object.finish()
}

// Canonical output describes grouping decisions and already-admitted labels.
// It never formats the retained HAVING expression or its arbitrary-size values.
fn write_grouping_json(grouping: &ExplainGrouping, out: &mut dyn fmt::Write) -> fmt::Result {
    let ExplainGrouping::Grouped {
        strategy,
        fallback_reason,
        group_fields,
        aggregates,
        having,
        max_groups,
        max_group_bytes,
    } = grouping
    else {
        return out.write_str("null");
    };
    let mut object = JsonWriter::begin_object(out)?;
    object.field_str("strategy", strategy)?;
    match fallback_reason {
        Some(reason) => object.field_str("fallback_reason", reason)?,
        None => object.field_null("fallback_reason")?,
    }
    object.field_with("group_fields", |out| {
        out.write_char('[')?;
        for (index, field) in group_fields.iter().enumerate() {
            if index != 0 {
                out.write_char(',')?;
            }
            let mut field_object = JsonWriter::begin_object(out)?;
            field_object.field_u64("slot_index", field.slot_index as u64)?;
            field_object.field_str("field", &field.field)?;
            field_object.finish()?;
        }
        out.write_char(']')
    })?;
    object.field_with("aggregates", |out| {
        out.write_char('[')?;
        for (index, aggregate) in aggregates.iter().enumerate() {
            if index != 0 {
                out.write_char(',')?;
            }
            let mut aggregate_object = JsonWriter::begin_object(out)?;
            aggregate_object.field_value_debug("kind", &aggregate.kind)?;
            for (name, label) in [
                ("target_field", aggregate.target_field()),
                ("input_expr", aggregate.input_expr()),
                ("filter_expr", aggregate.filter_expr()),
            ] {
                match label {
                    Some(label) => aggregate_object.field_str(name, label)?,
                    None => aggregate_object.field_null(name)?,
                }
            }
            aggregate_object.field_bool("distinct", aggregate.distinct)?;
            aggregate_object.finish()?;
        }
        out.write_char(']')
    })?;
    object.field_bool("has_having", having.is_some())?;
    object.field_u64("max_groups", *max_groups)?;
    object.field_u64("max_group_bytes", *max_group_bytes)?;
    object.finish()
}

fn write_access_decision_json(
    decision: &ExplainAccessDecision,
    out: &mut dyn fmt::Write,
) -> fmt::Result {
    let mut object = JsonWriter::begin_object(out)?;
    object.field_with("selected", |out| {
        let mut selected = JsonWriter::begin_object(out)?;
        selected.field_str("kind", decision.selected.kind.code())?;
        match decision.selected.index_name.as_deref() {
            Some(index_name) => selected.field_str("index_name", index_name)?,
            None => selected.field_null("index_name")?,
        }
        selected.field_str("label", decision.selected.label.as_str())?;
        selected.field_str("reason", decision.selected.reason)?;
        selected.finish()?;
        Ok(())
    })?;
    object.field_with("candidates", |out| {
        out.write_char('[')?;
        for (index, candidate) in decision.candidates.iter().enumerate() {
            if index > 0 {
                out.write_char(',')?;
            }
            write_access_candidate_json(candidate, out)?;
        }
        out.write_char(']')?;
        Ok(())
    })?;
    object.field_with("alternatives", |out| {
        out.write_char('[')?;
        for (index, alternative) in decision.alternatives.iter().enumerate() {
            if index > 0 {
                out.write_char(',')?;
            }
            let mut object = JsonWriter::begin_object(out)?;
            object.field_str("index_name", alternative.index_name.as_str())?;
            object.finish()?;
        }
        out.write_char(']')?;
        Ok(())
    })?;
    object.field_with("rejections", |out| {
        out.write_char('[')?;
        for (index, rejection) in decision.rejections.iter().enumerate() {
            if index > 0 {
                out.write_char(',')?;
            }
            let mut object = JsonWriter::begin_object(out)?;
            match rejection.index_name.as_deref() {
                Some(index_name) => object.field_str("index_name", index_name)?,
                None => object.field_null("index_name")?,
            }
            match rejection.reason.as_deref() {
                Some(reason) => object.field_str("reason", reason)?,
                None => object.field_null("reason")?,
            }
            object.field_str("label", rejection.label.as_str())?;
            object.finish()?;
        }
        out.write_char(']')?;
        Ok(())
    })?;
    object.field_with("residual", |out| {
        let mut residual = JsonWriter::begin_object(out)?;
        residual.field_str("burden_class", decision.residual.burden_class)?;
        residual.field_bool("has_residual_filter", decision.residual.has_residual_filter)?;
        residual.field_bool(
            "has_residual_predicate",
            decision.residual.has_residual_predicate,
        )?;
        residual.field_u64(
            "access_bound_predicate_count",
            decision.residual.access_bound_predicate_count as u64,
        )?;
        residual.field_u64(
            "residual_predicate_count",
            decision.residual.residual_predicate_count as u64,
        )?;
        residual.finish()?;
        Ok(())
    })?;
    object.field_str(
        "cardinality_evidence_state",
        decision.cardinality_evidence_state,
    )?;
    object.finish()
}

fn write_access_candidate_json(
    candidate: &ExplainAccessCandidate,
    out: &mut dyn fmt::Write,
) -> fmt::Result {
    let mut object = JsonWriter::begin_object(out)?;
    object.field_str("label", candidate.label.as_str())?;
    object.field_bool("exact", candidate.exact)?;
    object.field_bool("filtered", candidate.filtered)?;
    object.field_u64("range_bound_count", candidate.range_bound_count as u64)?;
    object.field_bool("order_compatible", candidate.order_compatible)?;
    object.field_str("residual_burden", candidate.residual_burden)?;
    object.field_u64(
        "residual_predicate_terms",
        candidate.residual_predicate_terms as u64,
    )?;
    if let Some(entries) = candidate.exact_prefix_entries {
        object.field_u64("exact_prefix_entries", entries)?;
    } else {
        object.field_null("exact_prefix_entries")?;
    }
    object.finish()
}
