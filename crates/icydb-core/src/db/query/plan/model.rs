//! Module: query::plan::model
//! Responsibility: pure logical query-plan data contracts.
//! Does not own: constructors, plan assembly, or semantic interpretation.
//! Boundary: data-only types shared by plan builder/semantics/validation layers.

use crate::{
    db::{
        cursor::ContinuationSignature,
        direction::Direction,
        predicate::{MissingRowPolicy, Predicate},
        query::{
            builder::scalar_projection::render_scalar_projection_expr_plan_label,
            plan::{
                expr::{Expr, FieldId, normalize_bool_expr},
                order_contract::DeterministicSecondaryOrderContract,
                semantics::LogicalPushdownEligibility,
            },
        },
    },
    model::field::FieldKind,
};

///
/// QueryMode
///
/// Discriminates load vs delete intent at planning time.
/// Encodes mode-specific fields so invalid states are unrepresentable.
/// Mode checks are explicit and stable at execution time.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryMode {
    Load(LoadSpec),
    Delete(DeleteSpec),
}

///
/// LoadSpec
///
/// Mode-specific fields for load intents.
/// Encodes pagination without leaking into delete intents.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoadSpec {
    pub(in crate::db) limit: Option<u32>,
    pub(in crate::db) offset: u32,
}

impl LoadSpec {
    /// Return optional row-limit bound for this load-mode spec.
    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Return zero-based pagination offset for this load-mode spec.
    #[must_use]
    pub const fn offset(&self) -> u32 {
        self.offset
    }
}

///
/// DeleteSpec
///
/// Mode-specific fields for delete intents.
/// Encodes delete limits without leaking into load intents.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeleteSpec {
    pub(in crate::db) limit: Option<u32>,
    pub(in crate::db) offset: u32,
}

impl DeleteSpec {
    /// Return optional row-limit bound for this delete-mode spec.
    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Return zero-based ordered delete offset for this delete-mode spec.
    #[must_use]
    pub const fn offset(&self) -> u32 {
        self.offset
    }
}

///
/// OrderDirection
/// Executor-facing ordering direction (applied after filtering).
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

///
/// OrderTerm
///
/// Planner-owned canonical ORDER BY term contract.
/// Carries one semantic expression plus direction so downstream validation and
/// execution stay expression-first, with rendered labels derived only at
/// diagnostic, explain, and hashing edges.
///

#[derive(Clone, Eq, PartialEq)]
pub(in crate::db) struct OrderTerm {
    pub(in crate::db) expr: Expr,
    pub(in crate::db) direction: OrderDirection,
}

impl OrderTerm {
    /// Construct one planner-owned ORDER BY term from one semantic expression.
    #[must_use]
    pub(in crate::db) const fn new(expr: Expr, direction: OrderDirection) -> Self {
        Self { expr, direction }
    }

    /// Construct one direct field ORDER BY term.
    #[must_use]
    pub(in crate::db) fn field(field: impl Into<String>, direction: OrderDirection) -> Self {
        Self::new(Expr::Field(FieldId::new(field.into())), direction)
    }

    /// Borrow the semantic ORDER BY expression.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
        &self.expr
    }

    /// Return the direct field name when this ORDER BY term is field-backed.
    #[must_use]
    pub(in crate::db) const fn direct_field(&self) -> Option<&str> {
        let Expr::Field(field) = &self.expr else {
            return None;
        };

        Some(field.as_str())
    }

    /// Render the stable ORDER BY display label for diagnostics and hashing.
    #[must_use]
    pub(in crate::db) fn rendered_label(&self) -> String {
        render_scalar_projection_expr_plan_label(&self.expr)
    }

    /// Return the executor-facing direction for this ORDER BY term.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> OrderDirection {
        self.direction
    }
}

impl std::fmt::Debug for OrderTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrderTerm")
            .field("label", &self.rendered_label())
            .field("expr", &self.expr)
            .field("direction", &self.direction)
            .finish()
    }
}

impl PartialEq<(String, OrderDirection)> for OrderTerm {
    fn eq(&self, other: &(String, OrderDirection)) -> bool {
        self.rendered_label() == other.0 && self.direction == other.1
    }
}

impl PartialEq<OrderTerm> for (String, OrderDirection) {
    fn eq(&self, other: &OrderTerm) -> bool {
        self.0 == other.rendered_label() && self.1 == other.direction
    }
}

/// Render one planner-owned scalar filter expression label for explain and
/// diagnostics surfaces.
#[must_use]
pub(in crate::db) fn render_scalar_filter_expr_plan_label(expr: &Expr) -> String {
    render_scalar_projection_expr_plan_label(&normalize_bool_expr(expr.clone()))
}

///
/// OrderSpec
///
/// Executor-facing ordering specification.
/// Carries the canonical ordered term list after planner expression lowering.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct OrderSpec {
    pub(in crate::db) fields: Vec<OrderTerm>,
}

///
/// DeleteLimitSpec
/// Executor-facing ordered delete window.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct DeleteLimitSpec {
    pub(in crate::db) limit: Option<u32>,
    pub(in crate::db) offset: u32,
}

///
/// DistinctExecutionStrategy
///
/// Planner-owned scalar DISTINCT execution strategy.
/// This is execution-mechanics only and must not be used for semantic
/// admissibility decisions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum DistinctExecutionStrategy {
    None,
    PreOrdered,
    HashMaterialize,
}

impl DistinctExecutionStrategy {
    /// Return true when scalar DISTINCT execution is enabled.
    #[must_use]
    pub(in crate::db) const fn is_enabled(self) -> bool {
        !matches!(self, Self::None)
    }
}

///
/// PlannerRouteProfile
///
/// Planner-projected route profile consumed by executor route planning.
/// Carries planner-owned continuation policy plus deterministic order/pushdown
/// contracts that route/load layers must honor without recomputing order shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PlannerRouteProfile {
    continuation_policy: ContinuationPolicy,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
    secondary_order_contract: Option<DeterministicSecondaryOrderContract>,
}

impl PlannerRouteProfile {
    /// Construct one planner-projected route profile.
    #[must_use]
    pub(in crate::db) const fn new(
        continuation_policy: ContinuationPolicy,
        logical_pushdown_eligibility: LogicalPushdownEligibility,
        secondary_order_contract: Option<DeterministicSecondaryOrderContract>,
    ) -> Self {
        Self {
            continuation_policy,
            logical_pushdown_eligibility,
            secondary_order_contract,
        }
    }

    /// Construct one fail-closed route profile for manually assembled plans
    /// that have not yet been finalized against model authority.
    #[must_use]
    pub(in crate::db) const fn seeded_unfinalized(is_grouped: bool) -> Self {
        Self {
            continuation_policy: ContinuationPolicy::new(true, true, !is_grouped),
            logical_pushdown_eligibility: LogicalPushdownEligibility::new(false, is_grouped, false),
            secondary_order_contract: None,
        }
    }

    /// Borrow planner-projected continuation policy contract.
    #[must_use]
    pub(in crate::db) const fn continuation_policy(&self) -> &ContinuationPolicy {
        &self.continuation_policy
    }

    /// Borrow planner-owned logical pushdown eligibility contract.
    #[must_use]
    pub(in crate::db) const fn logical_pushdown_eligibility(&self) -> LogicalPushdownEligibility {
        self.logical_pushdown_eligibility
    }

    /// Borrow the planner-owned deterministic secondary-order contract, if one exists.
    #[must_use]
    pub(in crate::db) const fn secondary_order_contract(
        &self,
    ) -> Option<&DeterministicSecondaryOrderContract> {
        self.secondary_order_contract.as_ref()
    }
}

///
/// ContinuationPolicy
///
/// Planner-projected continuation contract carried into route/executor layers.
/// This contract captures static continuation invariants and must not be
/// rederived by route/load orchestration code.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ContinuationPolicy {
    requires_anchor: bool,
    requires_strict_advance: bool,
    is_grouped_safe: bool,
}

impl ContinuationPolicy {
    /// Construct one planner-projected continuation policy contract.
    #[must_use]
    pub(in crate::db) const fn new(
        requires_anchor: bool,
        requires_strict_advance: bool,
        is_grouped_safe: bool,
    ) -> Self {
        Self {
            requires_anchor,
            requires_strict_advance,
            is_grouped_safe,
        }
    }

    /// Return true when continuation resume paths require an anchor boundary.
    #[must_use]
    pub(in crate::db) const fn requires_anchor(self) -> bool {
        self.requires_anchor
    }

    /// Return true when continuation resume paths require strict advancement.
    #[must_use]
    pub(in crate::db) const fn requires_strict_advance(self) -> bool {
        self.requires_strict_advance
    }

    /// Return true when grouped continuation usage is semantically safe.
    #[must_use]
    pub(in crate::db) const fn is_grouped_safe(self) -> bool {
        self.is_grouped_safe
    }
}

///
/// ExecutionShapeSignature
///
/// Immutable planner-projected semantic shape signature contract.
/// Continuation transport encodes this contract; route/load consume it as a
/// read-only execution identity boundary without re-deriving semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutionShapeSignature {
    continuation_signature: ContinuationSignature,
}

impl ExecutionShapeSignature {
    /// Construct one immutable execution-shape signature contract.
    #[must_use]
    pub(in crate::db) const fn new(continuation_signature: ContinuationSignature) -> Self {
        Self {
            continuation_signature,
        }
    }

    /// Borrow the canonical continuation signature for this execution shape.
    #[must_use]
    pub(in crate::db) const fn continuation_signature(self) -> ContinuationSignature {
        self.continuation_signature
    }
}

///
/// PageSpec
/// Executor-facing pagination specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PageSpec {
    pub(in crate::db) limit: Option<u32>,
    pub(in crate::db) offset: u32,
}

///
/// AggregateKind
///
/// Canonical aggregate terminal taxonomy owned by query planning.
/// All layers (query, explain, fingerprint, executor) must interpret aggregate
/// terminal semantics through this single enum authority.
/// Executor must derive traversal and fold direction exclusively from this enum.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateKind {
    Count,
    Sum,
    Avg,
    Exists,
    Min,
    Max,
    First,
    Last,
}

///
/// GlobalDistinctAggregateKind
///
/// Canonical support-family for grouped global-DISTINCT field aggregates.
/// This keeps the admitted `COUNT | SUM | AVG` family on one planner-owned
/// support surface instead of repeating that support set across grouped
/// semantics and grouped executor handoff.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GlobalDistinctAggregateKind {
    Count,
    Sum,
    Avg,
}

impl GlobalDistinctAggregateKind {}

///
/// GroupedPlanAggregateFamily
///
/// Planner-owned grouped aggregate-family profile.
/// This is intentionally coarse and execution-oriented: it captures which
/// grouped aggregate family the planner admitted so runtime can select grouped
/// execution paths without rebuilding family policy from raw aggregate
/// expressions again.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedPlanAggregateFamily {
    CountRowsOnly,
    FieldTargetRows,
    GenericRows,
}

impl GroupedPlanAggregateFamily {
    /// Return the stable planner-owned aggregate-family code.
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::CountRowsOnly => "count_rows_only",
            Self::FieldTargetRows => "field_target_rows",
            Self::GenericRows => "generic_rows",
        }
    }
}

impl AggregateKind {
    /// Return the canonical uppercase render label for this aggregate kind.
    #[must_use]
    pub(in crate::db) const fn canonical_label(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Exists => "EXISTS",
            Self::First => "FIRST",
            Self::Last => "LAST",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }

    /// Return whether this terminal kind is `COUNT`.
    #[must_use]
    pub(in crate::db) const fn is_count(self) -> bool {
        matches!(self, Self::Count)
    }

    /// Return whether this terminal kind belongs to the SUM/AVG numeric fold family.
    #[must_use]
    pub(in crate::db) const fn is_sum(self) -> bool {
        matches!(self, Self::Sum | Self::Avg)
    }

    /// Return whether this terminal kind belongs to the extrema family.
    #[must_use]
    pub(in crate::db) const fn is_extrema(self) -> bool {
        matches!(self, Self::Min | Self::Max)
    }

    /// Return whether this kind supports one grouped or global field target.
    #[must_use]
    pub(in crate::db) const fn supports_field_target_v1(self) -> bool {
        matches!(
            self,
            Self::Count | Self::Sum | Self::Avg | Self::Min | Self::Max
        )
    }

    /// Return whether reducer updates for this kind require a decoded id payload.
    #[must_use]
    pub(in crate::db) const fn requires_decoded_id(self) -> bool {
        !matches!(self, Self::Count | Self::Sum | Self::Avg | Self::Exists)
    }

    /// Return whether grouped aggregate DISTINCT is supported for this kind.
    #[must_use]
    pub(in crate::db) const fn supports_grouped_distinct_v1(self) -> bool {
        matches!(self, Self::Count | Self::Sum | Self::Avg)
    }

    /// Return the stable aggregate discriminant used by projection and
    /// aggregate fingerprint hashing.
    #[must_use]
    pub(in crate::db::query) const fn fingerprint_tag(self) -> u8 {
        match self {
            Self::Count => 0x01,
            Self::Sum => 0x02,
            Self::Exists => 0x03,
            Self::Min => 0x04,
            Self::Max => 0x05,
            Self::First => 0x06,
            Self::Last => 0x07,
            Self::Avg => 0x08,
        }
    }

    /// Return whether global DISTINCT aggregate shape is supported without GROUP BY keys.
    #[must_use]
    pub(in crate::db) const fn global_distinct_kind(self) -> Option<GlobalDistinctAggregateKind> {
        match self {
            Self::Count => Some(GlobalDistinctAggregateKind::Count),
            Self::Sum => Some(GlobalDistinctAggregateKind::Sum),
            Self::Avg => Some(GlobalDistinctAggregateKind::Avg),
            Self::Exists | Self::Min | Self::Max | Self::First | Self::Last => None,
        }
    }

    /// Return whether global DISTINCT aggregate shape is supported without GROUP BY keys.
    #[must_use]
    pub(in crate::db) const fn supports_global_distinct_without_group_keys(self) -> bool {
        self.global_distinct_kind().is_some()
    }

    /// Return the planner-owned grouped aggregate-family profile for one aggregate shape.
    #[must_use]
    pub(in crate::db) const fn grouped_plan_family(
        self,
        has_target_field: bool,
    ) -> GroupedPlanAggregateFamily {
        if has_target_field && self.supports_field_target_v1() {
            GroupedPlanAggregateFamily::FieldTargetRows
        } else {
            GroupedPlanAggregateFamily::GenericRows
        }
    }

    /// Return whether this grouped aggregate shape supports ordered grouped streaming.
    #[must_use]
    pub(in crate::db) const fn supports_grouped_streaming_v1(
        self,
        has_target_field: bool,
        distinct: bool,
    ) -> bool {
        if self.supports_field_target_v1() {
            return !distinct && (self.is_count() || has_target_field);
        }

        !has_target_field && (!distinct || self.supports_grouped_distinct_v1())
    }

    /// Return the canonical extrema traversal direction for this kind.
    #[must_use]
    pub(in crate::db) const fn extrema_direction(self) -> Option<Direction> {
        match self {
            Self::Min => Some(Direction::Asc),
            Self::Max => Some(Direction::Desc),
            Self::Count | Self::Sum | Self::Avg | Self::Exists | Self::First | Self::Last => None,
        }
    }

    /// Return the canonical materialized fold direction for this kind.
    #[must_use]
    pub(in crate::db) const fn materialized_fold_direction(self) -> Direction {
        match self {
            Self::Min => Direction::Desc,
            Self::Count
            | Self::Sum
            | Self::Avg
            | Self::Exists
            | Self::Max
            | Self::First
            | Self::Last => Direction::Asc,
        }
    }

    /// Return true when this kind can use bounded aggregate probe hints.
    #[must_use]
    pub(in crate::db) const fn supports_bounded_probe_hint(self) -> bool {
        !self.is_count() && !self.is_sum()
    }

    /// Derive a bounded aggregate probe fetch hint for this kind.
    #[must_use]
    pub(in crate::db) fn bounded_probe_fetch_hint(
        self,
        direction: Direction,
        offset: usize,
        page_limit: Option<usize>,
    ) -> Option<usize> {
        match self {
            Self::Exists | Self::First => Some(offset.saturating_add(1)),
            Self::Min if direction == Direction::Asc => Some(offset.saturating_add(1)),
            Self::Max if direction == Direction::Desc => Some(offset.saturating_add(1)),
            Self::Last => page_limit.map(|limit| offset.saturating_add(limit)),
            Self::Count | Self::Sum | Self::Avg | Self::Min | Self::Max => None,
        }
    }

    /// Return the explain projection mode label for this kind and projection surface.
    #[must_use]
    pub(in crate::db) const fn explain_projection_mode_label(
        self,
        has_projected_field: bool,
        covering_projection: bool,
    ) -> &'static str {
        if has_projected_field {
            if covering_projection {
                "field_idx"
            } else {
                "field_mat"
            }
        } else if matches!(self, Self::Min | Self::Max | Self::First | Self::Last) {
            "entity_term"
        } else {
            "scalar_agg"
        }
    }

    /// Return whether this terminal kind can remain covering on existing-row plans.
    #[must_use]
    pub(in crate::db) const fn supports_covering_existing_rows_terminal(self) -> bool {
        matches!(self, Self::Count | Self::Exists)
    }
}

///
/// GroupAggregateSpec
///
/// One grouped aggregate terminal specification declared at query-plan time.
/// `input_expr` is the single expression source for grouped aggregate identity.
/// Field-target behavior is derived from plain `Expr::Field` leaves so grouped
/// semantics, explain, fingerprinting, and runtime do not carry a second
/// compatibility shape beside the canonical aggregate input expression.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct GroupAggregateSpec {
    pub(in crate::db) kind: AggregateKind,
    pub(in crate::db) input_expr: Option<Box<Expr>>,
    pub(in crate::db) filter_expr: Option<Box<Expr>>,
    pub(in crate::db) distinct: bool,
}

impl PartialEq for GroupAggregateSpec {
    fn eq(&self, other: &Self) -> bool {
        self.semantic_key() == other.semantic_key()
    }
}

impl Eq for GroupAggregateSpec {}

impl GroupedPlanAggregateFamily {
    /// Derive the grouped aggregate-family profile from one planner aggregate list.
    #[must_use]
    pub(in crate::db) fn from_grouped_aggregates(aggregates: &[GroupAggregateSpec]) -> Self {
        if matches!(aggregates, [aggregate] if aggregate.identity().is_count_rows_only()) {
            return Self::CountRowsOnly;
        }

        if aggregates.iter().all(|aggregate| {
            aggregate
                .kind()
                .grouped_plan_family(aggregate.target_field().is_some())
                == Self::FieldTargetRows
        }) {
            return Self::FieldTargetRows;
        }

        Self::GenericRows
    }
}

///
/// FieldSlot
///
/// Canonical resolved field reference used by logical planning.
/// `index` is the stable slot in `EntityModel::fields`; `field` is retained
/// for diagnostics and explain surfaces.
/// `kind` freezes planner-resolved field metadata so executor boundaries do
/// not need to reopen `EntityModel` just to recover type/capability shape.
///

#[derive(Clone, Debug)]
pub(crate) struct FieldSlot {
    pub(in crate::db) index: usize,
    pub(in crate::db) field: String,
    pub(in crate::db) kind: Option<FieldKind>,
}

impl PartialEq for FieldSlot {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.field == other.field
    }
}

impl Eq for FieldSlot {}

///
/// GroupedExecutionConfig
///
/// Declarative grouped-execution budget policy selected by query planning.
/// This remains planner-owned input; executor policy bridges may still apply
/// defaults and enforcement strategy at runtime boundaries.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedExecutionConfig {
    pub(in crate::db) max_groups: u64,
    pub(in crate::db) max_group_bytes: u64,
}

///
/// GroupSpec
///
/// Declarative GROUP BY stage contract attached to a validated base plan.
/// This wrapper is intentionally semantic-only; field-slot resolution and
/// execution-mode derivation remain executor-owned boundaries.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupSpec {
    pub(in crate::db) group_fields: Vec<FieldSlot>,
    pub(in crate::db) aggregates: Vec<GroupAggregateSpec>,
    pub(in crate::db) execution: GroupedExecutionConfig,
}

///
/// ScalarPlan
///
/// Pure scalar logical query intent produced by the planner.
///
/// A `ScalarPlan` represents the access-independent query semantics:
/// predicate/filter, ordering, distinct behavior, pagination/delete windows,
/// and read-consistency mode.
///
/// Design notes:
/// - Predicates are applied *after* data access
/// - Ordering is applied after filtering
/// - Pagination is applied after ordering (load only)
/// - Delete limits are applied after ordering (delete only)
/// - Missing-row policy is explicit and must not depend on access strategy
///
/// This struct is the logical compiler stage output and intentionally excludes
/// access-path details.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarPlan {
    /// Load vs delete intent.
    pub(in crate::db) mode: QueryMode,

    /// Optional planner-owned scalar filter expression.
    pub(in crate::db) filter_expr: Option<Expr>,

    /// Whether the predicate fully covers the scalar filter expression.
    pub(in crate::db) predicate_covers_filter_expr: bool,

    /// Optional residual predicate applied after access.
    pub(in crate::db) predicate: Option<Predicate>,

    /// Optional ordering specification.
    pub(in crate::db) order: Option<OrderSpec>,

    /// Optional distinct semantics over ordered rows.
    pub(in crate::db) distinct: bool,

    /// Optional ordered delete window (delete intents only).
    pub(in crate::db) delete_limit: Option<DeleteLimitSpec>,

    /// Optional pagination specification.
    pub(in crate::db) page: Option<PageSpec>,

    /// Missing-row policy for execution.
    pub(in crate::db) consistency: MissingRowPolicy,
}

///
/// GroupPlan
///
/// Pure grouped logical intent emitted by grouped planning.
/// Group metadata is carried through one canonical `GroupSpec` contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupPlan {
    pub(in crate::db) scalar: ScalarPlan,
    pub(in crate::db) group: GroupSpec,
    pub(in crate::db) having_expr: Option<Expr>,
}

///
/// LogicalPlan
///
/// Exclusive logical query intent emitted by planning.
/// Scalar and grouped semantics are distinct variants by construction.
///

// Logical plans keep scalar and grouped shapes inline because planner/executor handoff
// passes these variants by ownership and boxing would widen that boundary for little benefit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum LogicalPlan {
    Scalar(ScalarPlan),
    Grouped(GroupPlan),
}
