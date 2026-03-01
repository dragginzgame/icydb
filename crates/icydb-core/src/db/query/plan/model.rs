//! Module: query::plan::model
//! Responsibility: pure logical query-plan data contracts.
//! Does not own: constructors, plan assembly, or semantic interpretation.
//! Boundary: data-only types shared by plan builder/semantics/validation layers.

use crate::db::predicate::{MissingRowPolicy, PredicateExecutionModel};

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
    pub limit: Option<u32>,
    pub offset: u32,
}

///
/// DeleteSpec
///
/// Mode-specific fields for delete intents.
/// Encodes delete limits without leaking into load intents.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeleteSpec {
    pub limit: Option<u32>,
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
/// Executor-facing delete bound with no offsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteLimitSpec {
    pub max_rows: u32,
}

///
/// PageSpec
/// Executor-facing pagination specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}

///
/// AggregateKind
///
/// Canonical aggregate terminal taxonomy owned by query planning.
/// All layers (query, explain, fingerprint, executor) must interpret aggregate
/// terminal semantics through this single enum authority.
/// Executor must derive traversal and fold direction exclusively from this enum.
///

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateKind {
    Count,
    Exists,
    Min,
    Max,
    First,
    Last,
}

/// Compatibility alias for grouped planning callsites.
pub(crate) type GroupAggregateKind = AggregateKind;

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
