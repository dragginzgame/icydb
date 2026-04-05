//! Module: query::plan::model
//! Responsibility: pure logical query-plan data contracts.
//! Does not own: constructors, plan assembly, or semantic interpretation.
//! Boundary: data-only types shared by plan builder/semantics/validation layers.

use crate::{
    db::{
        cursor::ContinuationSignature,
        direction::Direction,
        predicate::{CompareOp, MissingRowPolicy, PredicateExecutionModel},
        query::plan::semantics::LogicalPushdownEligibility,
    },
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
}

impl DeleteSpec {
    /// Return optional row-limit bound for this delete-mode spec.
    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
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

impl OrderSpec {
    /// Return the single ordered field when `ORDER BY` has exactly one element.
    #[must_use]
    pub(in crate::db) fn single_field(&self) -> Option<(&str, OrderDirection)> {
        let [(field, direction)] = self.fields.as_slice() else {
            return None;
        };

        Some((field.as_str(), *direction))
    }

    /// Return ordering direction when `ORDER BY` is primary-key-only.
    #[must_use]
    pub(in crate::db) fn primary_key_only_direction(
        &self,
        primary_key_name: &str,
    ) -> Option<OrderDirection> {
        let (field, direction) = self.single_field()?;
        (field == primary_key_name).then_some(direction)
    }

    /// Return true when `ORDER BY` is exactly one primary-key field.
    #[must_use]
    pub(in crate::db) fn is_primary_key_only(&self, primary_key_name: &str) -> bool {
        self.primary_key_only_direction(primary_key_name).is_some()
    }

    /// Return true when ORDER BY includes exactly one primary-key tie-break
    /// and that tie-break is the terminal sort component.
    #[must_use]
    pub(in crate::db) fn has_exact_primary_key_tie_break(&self, primary_key_name: &str) -> bool {
        let pk_count = self
            .fields
            .iter()
            .filter(|(field, _)| field == primary_key_name)
            .count();
        let trailing_pk = self
            .fields
            .last()
            .is_some_and(|(field, _)| field == primary_key_name);

        pk_count == 1 && trailing_pk
    }

    /// Return direction when ORDER BY preserves one deterministic secondary
    /// ordering contract (`..., primary_key`) with uniform direction.
    #[must_use]
    pub(in crate::db) fn deterministic_secondary_order_direction(
        &self,
        primary_key_name: &str,
    ) -> Option<OrderDirection> {
        let (_, expected_direction) = self.fields.last()?;
        if !self.has_exact_primary_key_tie_break(primary_key_name) {
            return None;
        }
        if self
            .fields
            .iter()
            .any(|(_, direction)| *direction != *expected_direction)
        {
            return None;
        }

        Some(*expected_direction)
    }

    /// Return true when ORDER BY non-primary-key terms match one expected
    /// deterministic sequence, followed by the primary key tie-break.
    #[must_use]
    pub(in crate::db) fn matches_expected_term_sequence_plus_primary_key<'a, I>(
        &self,
        expected_non_pk_terms: I,
        primary_key_name: &str,
    ) -> bool
    where
        I: IntoIterator<Item = &'a str>,
    {
        let expected_non_pk_terms = expected_non_pk_terms.into_iter().collect::<Vec<_>>();

        if !self.has_exact_primary_key_tie_break(primary_key_name) {
            return false;
        }
        if self.fields.len() != expected_non_pk_terms.len().saturating_add(1) {
            return false;
        }

        self.fields
            .iter()
            .take(expected_non_pk_terms.len())
            .map(|(field, _)| field.as_str())
            .zip(expected_non_pk_terms.iter().copied())
            .all(|(actual, expected)| actual == expected)
    }

    /// Return true when ORDER BY non-PK fields match the index suffix
    /// beginning at `prefix_len`, followed by primary key.
    #[must_use]
    pub(in crate::db) fn matches_index_suffix_plus_primary_key(
        &self,
        index_fields: &[&str],
        prefix_len: usize,
        primary_key_name: &str,
    ) -> bool {
        if prefix_len > index_fields.len() {
            return false;
        }

        self.matches_expected_term_sequence_plus_primary_key(
            index_fields[prefix_len..].iter().copied(),
            primary_key_name,
        )
    }

    /// Return true when ORDER BY non-PK fields match full index order,
    /// followed by primary key.
    #[must_use]
    pub(in crate::db) fn matches_index_full_plus_primary_key(
        &self,
        index_fields: &[&str],
        primary_key_name: &str,
    ) -> bool {
        self.matches_expected_term_sequence_plus_primary_key(
            index_fields.iter().copied(),
            primary_key_name,
        )
    }
}

///
/// DeleteLimitSpec
/// Executor-facing delete bound with no offsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteLimitSpec {
    pub(crate) max_rows: u32,
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
/// Carries planner-owned continuation policy that route/load layers must honor.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PlannerRouteProfile {
    continuation_policy: ContinuationPolicy,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
}

impl PlannerRouteProfile {
    /// Construct one planner-projected route profile.
    #[must_use]
    pub(in crate::db) const fn new(
        continuation_policy: ContinuationPolicy,
        logical_pushdown_eligibility: LogicalPushdownEligibility,
    ) -> Self {
        Self {
            continuation_policy,
            logical_pushdown_eligibility,
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
/// `target_field` remains optional so future field-target grouped terminals can
/// reuse this contract without mutating the wrapper shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupAggregateSpec {
    pub(crate) kind: AggregateKind,
    pub(crate) target_field: Option<String>,
    pub(crate) distinct: bool,
}

///
/// FieldSlot
///
/// Canonical resolved field reference used by logical planning.
/// `index` is the stable slot in `EntityModel::fields`; `field` is retained
/// for diagnostics and explain surfaces.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FieldSlot {
    pub(crate) index: usize,
    pub(crate) field: String,
}

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
/// GroupHavingSpec
///
/// Declarative grouped HAVING specification evaluated after grouped
/// aggregate finalization and before grouped pagination emission.
/// Clauses are AND-composed in declaration order.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupHavingSpec {
    pub(crate) clauses: Vec<GroupHavingClause>,
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

    /// Optional delete bound (delete intents only).
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
    pub(crate) having: Option<GroupHavingSpec>,
}

///
/// LogicalPlan
///
/// Exclusive logical query intent emitted by planning.
/// Scalar and grouped semantics are distinct variants by construction.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LogicalPlan {
    Scalar(ScalarPlan),
    Grouped(GroupPlan),
}
