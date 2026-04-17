//! Module: query::plan::model
//! Responsibility: pure logical query-plan data contracts.
//! Does not own: constructors, plan assembly, or semantic interpretation.
//! Boundary: data-only types shared by plan builder/semantics/validation layers.

use crate::{
    db::{
        cursor::ContinuationSignature,
        direction::Direction,
        predicate::{CompareOp, MissingRowPolicy, PredicateExecutionModel},
        query::plan::{
            expr::{BinaryOp, Expr, Function},
            order_contract::DeterministicSecondaryOrderContract,
            semantics::LogicalPushdownEligibility,
        },
    },
    model::field::FieldKind,
    value::Value,
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
    pub(crate) limit: Option<u32>,
    pub(crate) offset: u32,
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
    pub(crate) limit: Option<u32>,
    pub(crate) offset: u32,
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
/// OrderSpec
/// Executor-facing ordering specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderSpec {
    pub(crate) fields: Vec<(String, OrderDirection)>,
}

///
/// DeleteLimitSpec
/// Executor-facing ordered delete window.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteLimitSpec {
    pub(crate) limit: Option<u32>,
    pub(crate) offset: u32,
}

///
/// DistinctExecutionStrategy
///
/// Planner-owned scalar DISTINCT execution strategy.
/// This is execution-mechanics only and must not be used for semantic
/// admissibility decisions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DistinctExecutionStrategy {
    None,
    PreOrdered,
    HashMaterialize,
}

impl DistinctExecutionStrategy {
    /// Return true when scalar DISTINCT execution is enabled.
    #[must_use]
    pub(crate) const fn is_enabled(self) -> bool {
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
pub(crate) struct PageSpec {
    pub(crate) limit: Option<u32>,
    pub(crate) offset: u32,
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

impl AggregateKind {
    /// Return the canonical uppercase SQL/render label for this aggregate kind.
    #[must_use]
    pub(in crate::db) const fn sql_label(self) -> &'static str {
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
    pub(crate) const fn is_count(self) -> bool {
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

    /// Return whether reducer updates for this kind require a decoded id payload.
    #[must_use]
    pub(in crate::db) const fn requires_decoded_id(self) -> bool {
        !matches!(self, Self::Count | Self::Sum | Self::Avg | Self::Exists)
    }

    /// Return whether grouped aggregate DISTINCT is supported for this kind.
    #[must_use]
    pub(in crate::db) const fn supports_grouped_distinct_v1(self) -> bool {
        matches!(
            self,
            Self::Count | Self::Min | Self::Max | Self::Sum | Self::Avg
        )
    }

    /// Return whether global DISTINCT aggregate shape is supported without GROUP BY keys.
    #[must_use]
    pub(in crate::db) const fn supports_global_distinct_without_group_keys(self) -> bool {
        matches!(self, Self::Count | Self::Sum | Self::Avg)
    }

    /// Return the canonical extrema traversal direction for this kind.
    #[must_use]
    pub(crate) const fn extrema_direction(self) -> Option<Direction> {
        match self {
            Self::Min => Some(Direction::Asc),
            Self::Max => Some(Direction::Desc),
            Self::Count | Self::Sum | Self::Avg | Self::Exists | Self::First | Self::Last => None,
        }
    }

    /// Return the canonical materialized fold direction for this kind.
    #[must_use]
    pub(crate) const fn materialized_fold_direction(self) -> Direction {
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
    pub(crate) const fn supports_bounded_probe_hint(self) -> bool {
        !self.is_count() && !self.is_sum()
    }

    /// Derive a bounded aggregate probe fetch hint for this kind.
    #[must_use]
    pub(crate) fn bounded_probe_fetch_hint(
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
/// `target_field` keeps the direct field-target fast path explicit for grouped
/// streaming/distinct policy and route hints.
/// `input_expr` carries the canonical aggregate input shape so grouped
/// semantics, explain, fingerprinting, and runtime do not split again on
/// field-only versus expression-backed aggregate inputs.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupAggregateSpec {
    pub(crate) kind: AggregateKind,
    pub(crate) target_field: Option<String>,
    pub(crate) input_expr: Option<Box<Expr>>,
    pub(crate) distinct: bool,
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
    pub(crate) index: usize,
    pub(crate) field: String,
    pub(crate) kind: Option<FieldKind>,
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
pub(crate) struct GroupedExecutionConfig {
    pub(crate) max_groups: u64,
    pub(crate) max_group_bytes: u64,
}

///
/// GroupSpec
///
/// Declarative GROUP BY stage contract attached to a validated base plan.
/// This wrapper is intentionally semantic-only; field-slot resolution and
/// execution-mode derivation remain executor-owned boundaries.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupSpec {
    pub(crate) group_fields: Vec<FieldSlot>,
    pub(crate) aggregates: Vec<GroupAggregateSpec>,
    pub(crate) execution: GroupedExecutionConfig,
}

///
/// GroupHavingSymbol
///
/// Reference to one grouped HAVING input symbol.
/// Group-field symbols reference resolved grouped key slots.
/// Aggregate symbols reference grouped aggregate outputs by declaration index.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GroupHavingSymbol {
    GroupField(FieldSlot),
    AggregateIndex(usize),
}

///
/// GroupHavingClause
///
/// One conservative grouped HAVING clause.
/// This clause model intentionally supports one symbol-to-literal comparison
/// and excludes arbitrary expression trees in grouped v1.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupHavingClause {
    pub(crate) symbol: GroupHavingSymbol,
    pub(crate) op: CompareOp,
    pub(crate) value: Value,
}

///
/// GroupHavingValueExpr
///
/// Slot-resolved grouped HAVING value expression.
/// Leaves are restricted to grouped key slots, finalized aggregate outputs,
/// and literals so grouped HAVING stays on the post-aggregate surface.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GroupHavingValueExpr {
    GroupField(FieldSlot),
    AggregateIndex(usize),
    Literal(Value),
    FunctionCall {
        function: Function,
        args: Vec<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

///
/// GroupHavingExpr
///
/// Post-aggregate grouped HAVING boolean expression.
/// This is the `0.86` grouped HAVING backbone: grouped runtime evaluates this
/// tree over finalized grouped outputs without changing grouping mechanics.
///

// Grouped HAVING keeps compare nodes inline so the runtime evaluator can recurse over one
// owned tree shape without adding another layer of pointer chasing to every compare node.
#[expect(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GroupHavingExpr {
    Compare {
        left: GroupHavingValueExpr,
        op: CompareOp,
        right: GroupHavingValueExpr,
    },
    And(Vec<Self>),
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
pub(crate) struct ScalarPlan {
    /// Load vs delete intent.
    pub(crate) mode: QueryMode,

    /// Optional residual predicate applied after access.
    pub(crate) predicate: Option<PredicateExecutionModel>,

    /// Optional ordering specification.
    pub(crate) order: Option<OrderSpec>,

    /// Optional distinct semantics over ordered rows.
    pub(crate) distinct: bool,

    /// Optional ordered delete window (delete intents only).
    pub(crate) delete_limit: Option<DeleteLimitSpec>,

    /// Optional pagination specification.
    pub(crate) page: Option<PageSpec>,

    /// Missing-row policy for execution.
    pub(crate) consistency: MissingRowPolicy,
}

///
/// GroupPlan
///
/// Pure grouped logical intent emitted by grouped planning.
/// Group metadata is carried through one canonical `GroupSpec` contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupPlan {
    pub(crate) scalar: ScalarPlan,
    pub(crate) group: GroupSpec,
    pub(crate) having_expr: Option<GroupHavingExpr>,
}

///
/// LogicalPlan
///
/// Exclusive logical query intent emitted by planning.
/// Scalar and grouped semantics are distinct variants by construction.
///

// Logical plans keep scalar and grouped shapes inline because planner/executor handoff
// passes these variants by ownership and boxing would widen that boundary for little benefit.
#[expect(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LogicalPlan {
    Scalar(ScalarPlan),
    Grouped(GroupPlan),
}
