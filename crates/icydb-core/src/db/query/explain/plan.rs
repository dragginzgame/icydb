//! Module: query::explain::plan
//! Responsibility: deterministic planned-query projection for EXPLAIN,
//! including logical shape, access shape, and pushdown observability.
//! Does not own: execution descriptor rendering or access visitor adapters.
//! Boundary: explain DTOs and plan-side projection logic for query observability.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{CoercionSpec, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            builder::scalar_projection::render_scalar_projection_expr_plan_label,
            explain::{
                access_projection::write_access_json_detailed, explain_access_plan,
                writer::JsonWriter,
            },
            plan::{
                AccessChoiceCandidateExplainSummary, AccessChoiceExplainSnapshot,
                AccessChoiceResidualBurden, AccessPlannedQuery, AggregateKind, DeleteLimitSpec,
                GroupedPlanFallbackReason, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
                QueryMode, ScalarPlan, explain_access_strategy_label, expr::Expr,
                grouped_plan_strategy, render_scalar_filter_expr_plan_label,
            },
        },
    },
    traits::KeyValueCodec,
    value::Value,
};
use std::{fmt, ops::Bound};

///
/// ExplainPlan
///
/// Stable, deterministic representation of a planned query for observability.
///

#[derive(Clone, Eq, PartialEq)]
pub struct ExplainPlan {
    pub(in crate::db) mode: QueryMode,
    pub(in crate::db) access: ExplainAccessPath,
    pub(in crate::db) access_decision: ExplainAccessDecisionV1,
    pub(in crate::db) filter_expr: Option<String>,
    filter_expr_model: Option<Expr>,
    pub(in crate::db) predicate: ExplainPredicate,
    predicate_model: Option<Predicate>,
    pub(in crate::db) order_by: ExplainOrderBy,
    pub(in crate::db) distinct: bool,
    pub(in crate::db) grouping: ExplainGrouping,
    pub(in crate::db) order_pushdown: ExplainOrderPushdown,
    pub(in crate::db) page: ExplainPagination,
    pub(in crate::db) delete_limit: ExplainDeleteLimit,
    pub(in crate::db) consistency: MissingRowPolicy,
}

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for ExplainPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExplainPlan")
            .field("mode", &self.mode)
            .field("access", &self.access)
            .field("filter_expr", &self.filter_expr)
            .field("filter_expr_model", &self.filter_expr_model)
            .field("predicate", &self.predicate)
            .field("predicate_model", &self.predicate_model)
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
    pub const fn access_decision(&self) -> &ExplainAccessDecisionV1 {
        &self.access_decision
    }

    /// Borrow projected semantic scalar filter expression when present.
    #[must_use]
    pub fn filter_expr(&self) -> Option<&str> {
        self.filter_expr.as_deref()
    }

    /// Borrow the canonical scalar filter model used for identity hashing.
    #[must_use]
    pub(in crate::db::query) fn filter_expr_model_for_hash(&self) -> Option<&Expr> {
        if let Some(filter_expr_model) = &self.filter_expr_model {
            debug_assert_eq!(
                self.filter_expr(),
                Some(render_scalar_filter_expr_plan_label(filter_expr_model).as_str()),
                "explain scalar filter label drifted from canonical filter model"
            );
            Some(filter_expr_model)
        } else {
            debug_assert!(
                self.filter_expr.is_none(),
                "missing canonical filter model requires filter_expr=None"
            );
            None
        }
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
    /// Return the canonical predicate model used as the fallback hash surface.
    ///
    /// When a semantic scalar `filter_expr` exists, hashing now prefers that
    /// canonical filter surface instead. The explain predicate projection must
    /// still remain a faithful rendering of this fallback model.
    #[must_use]
    pub(in crate::db::query) fn predicate_model_for_hash(&self) -> Option<&Predicate> {
        if let Some(predicate) = &self.predicate_model {
            debug_assert_eq!(
                self.predicate,
                ExplainPredicate::from_predicate(predicate),
                "explain predicate surface drifted from canonical predicate model"
            );
            Some(predicate)
        } else {
            debug_assert!(
                matches!(self.predicate, ExplainPredicate::None),
                "missing canonical predicate model requires ExplainPredicate::None"
            );
            None
        }
    }

    /// Render this logical explain plan as deterministic canonical text.
    ///
    /// This surface is frontend-facing and intentionally stable for SQL/CLI
    /// explain output and snapshot-style diagnostics.
    #[must_use]
    pub fn render_text_canonical(&self) -> String {
        format!(
            concat!(
                "mode={:?}\n",
                "access={:?}\n",
                "access_decision={}\n",
                "filter_expr={:?}\n",
                "predicate={:?}\n",
                "order_by={:?}\n",
                "distinct={}\n",
                "grouping={:?}\n",
                "order_pushdown={:?}\n",
                "page={:?}\n",
                "delete_limit={:?}\n",
                "consistency={:?}",
            ),
            self.mode(),
            self.access(),
            self.access_decision().render_compact_summary(),
            self.filter_expr(),
            self.predicate(),
            self.order_by(),
            self.distinct(),
            self.grouping(),
            self.order_pushdown(),
            self.page(),
            self.delete_limit(),
            self.consistency(),
        )
    }

    /// Render this logical explain plan as canonical JSON.
    #[must_use]
    pub fn render_json_canonical(&self) -> String {
        let mut out = String::new();
        write_logical_explain_json(self, &mut out);

        out
    }
}

///
/// ExplainGrouping
///
/// Grouped-shape annotation for deterministic explain/fingerprint surfaces.
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
/// Stable grouped-key field identity carried by explain/hash surfaces.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupField {
    pub(in crate::db) slot_index: usize,
    pub(in crate::db) field: String,
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

impl ExplainGroupHaving {
    /// Borrow grouped HAVING expression.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
        &self.expr
    }
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
pub struct ExplainAccessDecisionV1 {
    /// Schema version for this access-decision payload shape.
    pub schema_version: u32,
    /// Selected access path summary.
    pub selected: ExplainSelectedAccessV1,
    /// Planner candidate summaries recorded for the selected access family.
    pub candidates: Vec<ExplainAccessCandidateV1>,
    /// Eligible alternatives not selected by the planner.
    pub alternatives: Vec<ExplainEligibleAlternativeV1>,
    /// Rejected index candidates and planner-owned reason strings.
    pub rejections: Vec<ExplainRejectedIndexV1>,
    /// Residual-work summary for the selected route when available.
    pub residual: ExplainResidualSummaryV1,
}

impl ExplainAccessDecisionV1 {
    const SCHEMA_VERSION: u32 = 1;

    fn from_snapshot(
        selected_access: &ExplainAccessPath,
        snapshot: &AccessChoiceExplainSnapshot,
    ) -> Self {
        let selected_label = explain_access_strategy_label(selected_access);
        let selected_candidate = selected_candidate_summary(&selected_label, &snapshot.candidates);

        Self {
            schema_version: Self::SCHEMA_VERSION,
            selected: ExplainSelectedAccessV1 {
                kind: ExplainAccessDecisionKind::from_access_path(selected_access),
                index_name: selected_index_name(selected_access).map(ToOwned::to_owned),
                label: selected_label,
                reason: snapshot.chosen_reason().code(),
            },
            candidates: snapshot
                .candidates
                .iter()
                .map(ExplainAccessCandidateV1::from_candidate)
                .collect(),
            alternatives: snapshot
                .alternatives
                .iter()
                .map(|index_name| ExplainEligibleAlternativeV1 {
                    index_name: index_name.clone(),
                })
                .collect(),
            rejections: snapshot
                .rejected
                .iter()
                .map(|rejection| ExplainRejectedIndexV1::from_rejection(rejection))
                .collect(),
            residual: ExplainResidualSummaryV1::from_selected_access_and_candidate(
                selected_access,
                selected_candidate,
            ),
        }
    }

    fn render_compact_summary(&self) -> String {
        let index = self
            .selected
            .index_name
            .as_deref()
            .map_or("none", |index| index);

        format!(
            "kind={} index={} reason={} residual={} candidates={} alternatives={} rejections={}",
            self.selected.kind.code(),
            index,
            self.selected.reason,
            self.residual.burden_class,
            self.candidates.len(),
            self.alternatives.len(),
            self.rejections.len(),
        )
    }
}

/// Selected access path summary inside an access-decision explain payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainSelectedAccessV1 {
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
            Self::IndexRange => "IndexRange",
            Self::FullScan => "FullScan",
            Self::Union => "Union",
            Self::Intersection => "Intersection",
        }
    }
}

/// Candidate summary recorded by the planner access-choice snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainAccessCandidateV1 {
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
}

impl ExplainAccessCandidateV1 {
    fn from_candidate(candidate: &AccessChoiceCandidateExplainSummary) -> Self {
        Self {
            label: candidate.label.clone(),
            exact: candidate.exact,
            filtered: candidate.filtered,
            range_bound_count: candidate.range_bound_count,
            order_compatible: candidate.order_compatible,
            residual_burden: candidate.residual_burden.label(),
            residual_predicate_terms: candidate.residual_predicate_terms,
        }
    }
}

/// Eligible alternative index name recorded by the planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainEligibleAlternativeV1 {
    /// Semantic index name of the eligible alternative.
    pub index_name: String,
}

/// Rejected index candidate summary recorded by the planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainRejectedIndexV1 {
    /// Semantic index name when parsed from the planner rejection label.
    pub index_name: Option<String>,
    /// Planner-owned rejection reason code when parsed from the rejection label.
    pub reason: Option<String>,
    /// Original planner rejection label.
    pub label: String,
}

impl ExplainRejectedIndexV1 {
    fn from_rejection(rejection: &str) -> Self {
        let (index_name, reason) = parse_rejected_index_label(rejection);

        Self {
            index_name,
            reason,
            label: rejection.to_string(),
        }
    }
}

/// Residual-work summary for the selected access route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainResidualSummaryV1 {
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
    /// Deprecated JSON compatibility mirror of `residual_predicate_count`.
    pub predicate_terms: usize,
}

impl ExplainResidualSummaryV1 {
    fn from_selected_access_and_candidate(
        selected_access: &ExplainAccessPath,
        selected_candidate: Option<&AccessChoiceCandidateExplainSummary>,
    ) -> Self {
        match selected_candidate {
            Some(candidate) => Self {
                burden_class: candidate.residual_burden.label(),
                has_residual_filter: matches!(
                    candidate.residual_burden,
                    AccessChoiceResidualBurden::ScalarExpression
                ),
                has_residual_predicate: candidate.residual_predicate_terms > 0,
                access_bound_predicate_count: access_bound_predicate_count(selected_access),
                residual_predicate_count: candidate.residual_predicate_terms,
                predicate_terms: candidate.residual_predicate_terms,
            },
            None => Self {
                burden_class: AccessChoiceResidualBurden::None.label(),
                has_residual_filter: false,
                has_residual_predicate: false,
                access_bound_predicate_count: access_bound_predicate_count(selected_access),
                residual_predicate_count: 0,
                predicate_terms: 0,
            },
        }
    }
}

///
/// ExplainPredicate
///
/// Deterministic projection of canonical predicate structure for explain output.
/// This preserves normalized predicate shape used by hashing/fingerprints.
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

impl AccessPlannedQuery {
    /// Produce a stable, deterministic explanation of this logical plan.
    #[must_use]
    pub(in crate::db) fn explain(&self) -> ExplainPlan {
        self.explain_inner()
    }

    pub(in crate::db::query::explain) fn explain_inner(&self) -> ExplainPlan {
        // Phase 1: project logical plan variant into scalar core + grouped metadata.
        let (logical, grouping) = match &self.logical {
            LogicalPlan::Scalar(logical) => (logical, ExplainGrouping::None),
            LogicalPlan::Grouped(logical) => {
                let grouped_strategy = grouped_plan_strategy(self).expect(
                    "grouped logical explain projection requires planner-owned grouped strategy",
                );

                (
                    &logical.scalar,
                    ExplainGrouping::Grouped {
                        strategy: grouped_strategy.code(),
                        fallback_reason: grouped_strategy
                            .fallback_reason()
                            .map(GroupedPlanFallbackReason::code),
                        group_fields: logical
                            .group
                            .group_fields
                            .iter()
                            .map(|field_slot| ExplainGroupField {
                                slot_index: field_slot.index(),
                                field: field_slot.field().to_string(),
                            })
                            .collect(),
                        aggregates: logical
                            .group
                            .aggregates
                            .iter()
                            .map(|aggregate| ExplainGroupAggregate {
                                kind: aggregate.kind,
                                target_field: aggregate.target_field().map(str::to_string),
                                input_expr: aggregate
                                    .input_expr()
                                    .map(render_scalar_projection_expr_plan_label),
                                filter_expr: aggregate
                                    .filter_expr()
                                    .map(render_scalar_projection_expr_plan_label),
                                distinct: aggregate.distinct,
                            })
                            .collect(),
                        having: explain_group_having(logical),
                        max_groups: logical.group.execution.max_groups(),
                        max_group_bytes: logical.group.execution.max_group_bytes(),
                    },
                )
            }
        };

        // Phase 2: project scalar plan + access path into deterministic explain surface.
        explain_scalar_inner(logical, grouping, &self.access, self.access_choice())
    }
}

fn explain_group_having(logical: &crate::db::query::plan::GroupPlan) -> Option<ExplainGroupHaving> {
    let expr = logical.effective_having_expr()?;

    Some(ExplainGroupHaving {
        expr: expr.into_owned(),
    })
}

fn explain_scalar_inner<K>(
    logical: &ScalarPlan,
    grouping: ExplainGrouping,
    access: &AccessPlan<K>,
    access_choice: &AccessChoiceExplainSnapshot,
) -> ExplainPlan
where
    K: KeyValueCodec,
{
    // Phase 1: consume canonical predicate model from planner-owned scalar semantics.
    let filter_expr = logical
        .filter_expr
        .as_ref()
        .map(render_scalar_filter_expr_plan_label);
    let filter_expr_model = logical.filter_expr.clone();
    let predicate_model = logical.predicate.clone();
    let predicate = match &predicate_model {
        Some(predicate) => ExplainPredicate::from_predicate(predicate),
        None => ExplainPredicate::None,
    };

    // Phase 2: project scalar-plan fields into explain-specific enums.
    let order_by = explain_order(logical.order.as_ref());
    let order_pushdown = explain_order_pushdown();
    let page = explain_page(logical.page.as_ref());
    let delete_limit = explain_delete_limit(logical.delete_limit.as_ref());

    // Phase 3: assemble one stable explain payload.
    let access = explain_access_plan(access);
    let access_decision = ExplainAccessDecisionV1::from_snapshot(&access, access_choice);

    ExplainPlan {
        mode: logical.mode,
        access,
        access_decision,
        filter_expr,
        filter_expr_model,
        predicate,
        predicate_model,
        order_by,
        distinct: logical.distinct,
        grouping,
        order_pushdown,
        page,
        delete_limit,
        consistency: logical.consistency,
    }
}

fn selected_candidate_summary<'a>(
    selected_label: &str,
    candidates: &'a [AccessChoiceCandidateExplainSummary],
) -> Option<&'a AccessChoiceCandidateExplainSummary> {
    candidates
        .iter()
        .find(|candidate| candidate.label == selected_label)
        .or_else(|| (candidates.len() == 1).then(|| &candidates[0]))
}

const fn selected_index_name(access: &ExplainAccessPath) -> Option<&str> {
    match access {
        ExplainAccessPath::IndexPrefix { name, .. }
        | ExplainAccessPath::IndexMultiLookup { name, .. }
        | ExplainAccessPath::IndexRange { name, .. } => Some(name.as_str()),
        ExplainAccessPath::ByKey { .. }
        | ExplainAccessPath::ByKeys { .. }
        | ExplainAccessPath::KeyRange { .. }
        | ExplainAccessPath::FullScan
        | ExplainAccessPath::Union(_)
        | ExplainAccessPath::Intersection(_) => None,
    }
}

fn access_bound_predicate_count(access: &ExplainAccessPath) -> usize {
    match access {
        ExplainAccessPath::ByKey { .. }
        | ExplainAccessPath::ByKeys { .. }
        | ExplainAccessPath::IndexMultiLookup { .. } => 1,
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
            children.iter().map(access_bound_predicate_count).sum()
        }
    }
}

const fn bound_constraint_count(bound: &Bound<Value>) -> usize {
    match bound {
        Bound::Included(_) | Bound::Excluded(_) => 1,
        Bound::Unbounded => 0,
    }
}

fn parse_rejected_index_label(rejection: &str) -> (Option<String>, Option<String>) {
    let Some(rest) = rejection.strip_prefix("index:") else {
        return (None, None);
    };

    match rest.split_once('=') {
        Some((index_name, reason)) => (Some(index_name.to_string()), Some(reason.to_string())),
        None => (Some(rest.to_string()), None),
    }
}

const fn explain_order_pushdown() -> ExplainOrderPushdown {
    // Query explain does not own physical pushdown feasibility routing.
    ExplainOrderPushdown::MissingModelContext
}

impl ExplainPredicate {
    pub(in crate::db) fn from_predicate(predicate: &Predicate) -> Self {
        match predicate {
            Predicate::True => Self::True,
            Predicate::False => Self::False,
            Predicate::And(children) => {
                Self::And(children.iter().map(Self::from_predicate).collect())
            }
            Predicate::Or(children) => {
                Self::Or(children.iter().map(Self::from_predicate).collect())
            }
            Predicate::Not(inner) => Self::Not(Box::new(Self::from_predicate(inner))),
            Predicate::Compare(compare) => Self::from_compare(compare),
            Predicate::CompareFields(compare) => Self::CompareFields {
                left_field: compare.left_field().to_string(),
                op: compare.op(),
                right_field: compare.right_field().to_string(),
                coercion: compare.coercion().clone(),
            },
            Predicate::IsNull { field } => Self::IsNull {
                field: field.clone(),
            },
            Predicate::IsNotNull { field } => Self::IsNotNull {
                field: field.clone(),
            },
            Predicate::IsMissing { field } => Self::IsMissing {
                field: field.clone(),
            },
            Predicate::IsEmpty { field } => Self::IsEmpty {
                field: field.clone(),
            },
            Predicate::IsNotEmpty { field } => Self::IsNotEmpty {
                field: field.clone(),
            },
            Predicate::TextContains { field, value } => Self::TextContains {
                field: field.clone(),
                value: value.clone(),
            },
            Predicate::TextContainsCi { field, value } => Self::TextContainsCi {
                field: field.clone(),
                value: value.clone(),
            },
        }
    }

    fn from_compare(compare: &ComparePredicate) -> Self {
        Self::Compare {
            field: compare.field.clone(),
            op: compare.op,
            value: compare.value.clone(),
            coercion: compare.coercion.clone(),
        }
    }
}

fn explain_order(order: Option<&OrderSpec>) -> ExplainOrderBy {
    let Some(order) = order else {
        return ExplainOrderBy::None;
    };

    if order.fields.is_empty() {
        return ExplainOrderBy::None;
    }

    ExplainOrderBy::Fields(
        order
            .fields
            .iter()
            .map(|term| ExplainOrder {
                field: term.rendered_label(),
                direction: term.direction(),
            })
            .collect(),
    )
}

const fn explain_page(page: Option<&PageSpec>) -> ExplainPagination {
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

fn write_logical_explain_json(explain: &ExplainPlan, out: &mut String) {
    let mut object = JsonWriter::begin_object(out);
    object.field_with("mode", |out| {
        let mut object = JsonWriter::begin_object(out);
        match explain.mode() {
            QueryMode::Load(spec) => {
                object.field_str("type", "Load");
                match spec.limit() {
                    Some(limit) => object.field_u64("limit", u64::from(limit)),
                    None => object.field_null("limit"),
                }
                object.field_u64("offset", u64::from(spec.offset()));
            }
            QueryMode::Delete(spec) => {
                object.field_str("type", "Delete");
                match spec.limit() {
                    Some(limit) => object.field_u64("limit", u64::from(limit)),
                    None => object.field_null("limit"),
                }
            }
        }
        object.finish();
    });
    object.field_with("access", |out| {
        write_access_json_detailed(explain.access(), out);
    });
    object.field_with("access_decision", |out| {
        write_access_decision_json(explain.access_decision(), out);
    });
    match explain.filter_expr() {
        Some(filter_expr) => object.field_str("filter_expr", filter_expr),
        None => object.field_null("filter_expr"),
    }
    object.field_value_debug("predicate", explain.predicate());
    object.field_value_debug("order_by", explain.order_by());
    object.field_bool("distinct", explain.distinct());
    object.field_value_debug("grouping", explain.grouping());
    object.field_value_debug("order_pushdown", explain.order_pushdown());
    object.field_with("page", |out| {
        let mut object = JsonWriter::begin_object(out);
        match explain.page() {
            ExplainPagination::None => {
                object.field_str("type", "None");
            }
            ExplainPagination::Page { limit, offset } => {
                object.field_str("type", "Page");
                match limit {
                    Some(limit) => object.field_u64("limit", u64::from(*limit)),
                    None => object.field_null("limit"),
                }
                object.field_u64("offset", u64::from(*offset));
            }
        }
        object.finish();
    });
    object.field_with("delete_limit", |out| {
        let mut object = JsonWriter::begin_object(out);
        match explain.delete_limit() {
            ExplainDeleteLimit::None => {
                object.field_str("type", "None");
            }
            ExplainDeleteLimit::Limit { max_rows } => {
                object.field_str("type", "Limit");
                object.field_u64("max_rows", u64::from(*max_rows));
            }
            ExplainDeleteLimit::Window { limit, offset } => {
                object.field_str("type", "Window");
                object.field_with("limit", |out| match limit {
                    Some(limit) => out.push_str(&limit.to_string()),
                    None => out.push_str("null"),
                });
                object.field_u64("offset", u64::from(*offset));
            }
        }
        object.finish();
    });
    object.field_value_debug("consistency", &explain.consistency());
    object.finish();
}

fn write_access_decision_json(decision: &ExplainAccessDecisionV1, out: &mut String) {
    let mut object = JsonWriter::begin_object(out);
    object.field_u64("schema_version", u64::from(decision.schema_version));
    object.field_with("selected", |out| {
        let mut selected = JsonWriter::begin_object(out);
        selected.field_str("kind", decision.selected.kind.code());
        match decision.selected.index_name.as_deref() {
            Some(index_name) => selected.field_str("index_name", index_name),
            None => selected.field_null("index_name"),
        }
        selected.field_str("label", decision.selected.label.as_str());
        selected.field_str("reason", decision.selected.reason);
        selected.finish();
    });
    object.field_with("candidates", |out| {
        out.push('[');
        for (index, candidate) in decision.candidates.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            write_access_candidate_json(candidate, out);
        }
        out.push(']');
    });
    object.field_with("alternatives", |out| {
        out.push('[');
        for (index, alternative) in decision.alternatives.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let mut object = JsonWriter::begin_object(out);
            object.field_str("index_name", alternative.index_name.as_str());
            object.finish();
        }
        out.push(']');
    });
    object.field_with("rejections", |out| {
        out.push('[');
        for (index, rejection) in decision.rejections.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let mut object = JsonWriter::begin_object(out);
            match rejection.index_name.as_deref() {
                Some(index_name) => object.field_str("index_name", index_name),
                None => object.field_null("index_name"),
            }
            match rejection.reason.as_deref() {
                Some(reason) => object.field_str("reason", reason),
                None => object.field_null("reason"),
            }
            object.field_str("label", rejection.label.as_str());
            object.finish();
        }
        out.push(']');
    });
    object.field_with("residual", |out| {
        let mut residual = JsonWriter::begin_object(out);
        residual.field_str("burden_class", decision.residual.burden_class);
        residual.field_bool("has_residual_filter", decision.residual.has_residual_filter);
        residual.field_bool(
            "has_residual_predicate",
            decision.residual.has_residual_predicate,
        );
        residual.field_u64(
            "access_bound_predicate_count",
            decision.residual.access_bound_predicate_count as u64,
        );
        residual.field_u64(
            "residual_predicate_count",
            decision.residual.residual_predicate_count as u64,
        );
        residual.field_u64("predicate_terms", decision.residual.predicate_terms as u64);
        residual.finish();
    });
    object.finish();
}

fn write_access_candidate_json(candidate: &ExplainAccessCandidateV1, out: &mut String) {
    let mut object = JsonWriter::begin_object(out);
    object.field_str("label", candidate.label.as_str());
    object.field_bool("exact", candidate.exact);
    object.field_bool("filtered", candidate.filtered);
    object.field_u64("range_bound_count", candidate.range_bound_count as u64);
    object.field_bool("order_compatible", candidate.order_compatible);
    object.field_str("residual_burden", candidate.residual_burden);
    object.field_u64(
        "residual_predicate_terms",
        candidate.residual_predicate_terms as u64,
    );
    object.finish();
}
