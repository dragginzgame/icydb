//! Query plan contracts, planning, and validation wiring.

mod group;
mod planner;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

use crate::{
    db::{
        access::{
            AccessPath, AccessPlan, PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        cursor::CursorBoundary,
        direction::Direction,
        predicate::{MissingRowPolicy, PredicateExecutionModel},
        query::explain::ExplainAccessPath,
    },
    error::InternalError,
    model::entity::{EntityModel, resolve_field_slot},
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::ops::Bound;
#[cfg(test)]
use std::ops::{Deref, DerefMut};

pub(in crate::db) use group::{GroupedExecutorHandoff, grouped_executor_handoff};
pub use validate::PlanError;
pub(crate) use validate::{GroupPlanError, validate_group_query_semantics};

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

impl QueryMode {
    /// True if this mode represents a load intent.
    #[must_use]
    pub const fn is_load(&self) -> bool {
        match self {
            Self::Load(_) => true,
            Self::Delete(_) => false,
        }
    }

    /// True if this mode represents a delete intent.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        match self {
            Self::Delete(_) => true,
            Self::Load(_) => false,
        }
    }
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

impl LoadSpec {
    /// Create an empty load spec.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            limit: None,
            offset: 0,
        }
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
    pub limit: Option<u32>,
}

impl DeleteSpec {
    /// Create an empty delete spec.
    #[must_use]
    pub const fn new() -> Self {
        Self { limit: None }
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

impl OrderDirection {
    /// Convert canonical order direction into execution scan direction.
    #[must_use]
    pub(in crate::db) const fn as_direction(self) -> Direction {
        match self {
            Self::Asc => Direction::Asc,
            Self::Desc => Direction::Desc,
        }
    }

    /// Convert execution scan direction into canonical order direction.
    #[must_use]
    pub(in crate::db) const fn from_direction(direction: Direction) -> Self {
        match direction {
            Direction::Asc => Self::Asc,
            Direction::Desc => Self::Desc,
        }
    }
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
/// GroupAggregateKind
///
/// Declarative grouped aggregate terminal taxonomy owned by query planning.
/// This query-layer enum intentionally avoids coupling to executor aggregate
/// reducer internals while preserving terminal intent shape.
///

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupAggregateKind {
    Count,
    Exists,
    Min,
    Max,
    First,
    Last,
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
    pub(crate) kind: GroupAggregateKind,
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
    index: usize,
    field: String,
}

impl FieldSlot {
    /// Resolve one field name into its canonical model slot.
    #[must_use]
    pub(crate) fn resolve(model: &EntityModel, field: &str) -> Option<Self> {
        let index = resolve_field_slot(model, field)?;
        let canonical = model
            .fields
            .get(index)
            .map_or(field, |model_field| model_field.name);

        Some(Self {
            index,
            field: canonical.to_string(),
        })
    }

    /// Build one field slot directly for tests that need invalid slot shapes.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn from_parts_for_test(index: usize, field: impl Into<String>) -> Self {
        Self {
            index,
            field: field.into(),
        }
    }

    /// Return the stable slot index in `EntityModel::fields`.
    #[must_use]
    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    /// Return the diagnostic field label associated with this slot.
    #[must_use]
    pub(crate) fn field(&self) -> &str {
        &self.field
    }

    /// Resolve the canonical model field name for this slot index.
    #[must_use]
    pub(crate) fn canonical_name<'a>(&self, model: &'a EntityModel) -> Option<&'a str> {
        model.fields.get(self.index).map(|field| field.name)
    }
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

impl GroupedExecutionConfig {
    /// Build one grouped execution config with explicit hard limits.
    #[must_use]
    pub(crate) const fn with_hard_limits(max_groups: u64, max_group_bytes: u64) -> Self {
        Self {
            max_groups,
            max_group_bytes,
        }
    }

    /// Build one unbounded grouped execution config.
    #[must_use]
    pub(crate) const fn unbounded() -> Self {
        Self::with_hard_limits(u64::MAX, u64::MAX)
    }

    /// Return grouped hard limit for maximum groups.
    #[must_use]
    pub(crate) const fn max_groups(&self) -> u64 {
        self.max_groups
    }

    /// Return grouped hard limit for estimated grouped bytes.
    #[must_use]
    pub(crate) const fn max_group_bytes(&self) -> u64 {
        self.max_group_bytes
    }
}

impl Default for GroupedExecutionConfig {
    fn default() -> Self {
        Self::unbounded()
    }
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

impl LogicalPlan {
    /// Borrow scalar semantic fields shared by scalar/grouped logical variants.
    #[must_use]
    pub(in crate::db) const fn scalar_semantics(&self) -> &ScalarPlan {
        match self {
            Self::Scalar(plan) => plan,
            Self::Grouped(plan) => &plan.scalar,
        }
    }

    /// Borrow scalar semantic fields mutably across logical variants.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_semantics_mut(&mut self) -> &mut ScalarPlan {
        match self {
            Self::Scalar(plan) => plan,
            Self::Grouped(plan) => &mut plan.scalar,
        }
    }
}

#[cfg(test)]
impl Deref for LogicalPlan {
    type Target = ScalarPlan;

    fn deref(&self) -> &Self::Target {
        self.scalar_semantics()
    }
}

#[cfg(test)]
impl DerefMut for LogicalPlan {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scalar_semantics_mut()
    }
}

///
/// AccessPlannedQuery
///
/// Access-planned query produced after access-path selection.
/// Binds one pure `LogicalPlan` to one chosen `AccessPlan`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccessPlannedQuery<K> {
    pub(crate) logical: LogicalPlan,
    pub(crate) access: AccessPlan<K>,
}

impl<K> AccessPlannedQuery<K> {
    /// Construct an access-planned query from logical + access stages.
    #[must_use]
    pub(crate) const fn from_parts(logical: LogicalPlan, access: AccessPlan<K>) -> Self {
        Self { logical, access }
    }

    /// Decompose into logical + access stages.
    #[must_use]
    pub(crate) fn into_parts(self) -> (LogicalPlan, AccessPlan<K>) {
        (self.logical, self.access)
    }

    /// Convert this plan into grouped logical form with one explicit group spec.
    #[must_use]
    pub(in crate::db) fn into_grouped(self, group: GroupSpec) -> Self {
        let Self { logical, access } = self;
        let scalar = match logical {
            LogicalPlan::Scalar(plan) => plan,
            LogicalPlan::Grouped(plan) => plan.scalar,
        };

        Self {
            logical: LogicalPlan::Grouped(GroupPlan { scalar, group }),
            access,
        }
    }

    /// Borrow scalar semantic fields shared by scalar/grouped logical variants.
    #[must_use]
    pub(in crate::db) const fn scalar_plan(&self) -> &ScalarPlan {
        self.logical.scalar_semantics()
    }

    /// Borrow grouped semantic fields when this plan is grouped.
    #[must_use]
    pub(in crate::db) const fn grouped_plan(&self) -> Option<&GroupPlan> {
        match &self.logical {
            LogicalPlan::Scalar(_) => None,
            LogicalPlan::Grouped(plan) => Some(plan),
        }
    }

    /// Borrow scalar semantic fields mutably across logical variants.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_plan_mut(&mut self) -> &mut ScalarPlan {
        self.logical.scalar_semantics_mut()
    }

    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(
        access: crate::db::access::AccessPath<K>,
        consistency: MissingRowPolicy,
    ) -> Self {
        Self {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency,
            }),
            access: AccessPlan::path(access),
        }
    }

    /// Build one continuation boundary from one entity using canonical order.
    pub(in crate::db) fn cursor_boundary_from_entity<E>(
        &self,
        entity: &E,
    ) -> Result<CursorBoundary, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
    {
        let order = match &self.logical {
            LogicalPlan::Scalar(logical) => logical.order.as_ref(),
            LogicalPlan::Grouped(_) => {
                return Err(InternalError::query_executor_invariant(
                    "cannot build cursor boundary for grouped logical plans",
                ));
            }
        };
        let Some(order) = order else {
            return Err(InternalError::query_executor_invariant(
                "cannot build cursor boundary without ordering",
            ));
        };

        Ok(CursorBoundary::from_ordered_entity(entity, order))
    }
}

#[cfg(test)]
impl<K> Deref for AccessPlannedQuery<K> {
    type Target = ScalarPlan;

    fn deref(&self) -> &Self::Target {
        self.scalar_plan()
    }
}

#[cfg(test)]
impl<K> DerefMut for AccessPlannedQuery<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scalar_plan_mut()
    }
}

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction.as_direction()))
        .collect()
}

fn applicability_from_eligibility(
    eligibility: SecondaryOrderPushdownEligibility,
) -> PushdownApplicability {
    match eligibility {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy
            | SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        ) => PushdownApplicability::NotApplicable,
        other => PushdownApplicability::Applicable(other),
    }
}

// Core matcher for secondary ORDER BY pushdown eligibility.
fn match_secondary_order_pushdown_core(
    model: &EntityModel,
    order_fields: &[(&str, Direction)],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    let Some((last_field, last_direction)) = order_fields.last() else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if *last_field != model.primary_key.name {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                field: model.primary_key.name.to_string(),
            },
        );
    }

    let expected_direction = *last_direction;
    for (field, direction) in order_fields {
        if *direction != expected_direction {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: (*field).to_string(),
                },
            );
        }
    }

    let actual_non_pk_len = order_fields.len().saturating_sub(1);
    let matches_expected_suffix = actual_non_pk_len
        == index_fields.len().saturating_sub(prefix_len)
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().skip(prefix_len).copied())
            .all(|(actual, expected)| actual == expected);
    let matches_expected_full = actual_non_pk_len == index_fields.len()
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().copied())
            .all(|(actual, expected)| actual == expected);
    if matches_expected_suffix || matches_expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix: index_fields
                .iter()
                .skip(prefix_len)
                .map(|field| (*field).to_string())
                .collect(),
            expected_full: index_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            actual: order_fields
                .iter()
                .take(actual_non_pk_len)
                .map(|(field, _)| (*field).to_string())
                .collect(),
        },
    )
}

// Evaluate pushdown eligibility for ORDER BY + single index-prefix shapes.
fn assess_secondary_order_pushdown_for_applicable_shape(
    model: &EntityModel,
    order_fields: &[(&str, Direction)],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    match_secondary_order_pushdown_core(model, order_fields, index_name, index_fields, prefix_len)
}

// Evaluate secondary ORDER BY pushdown over one access-planned query.
fn assess_secondary_order_pushdown_for_plan<K>(
    model: &EntityModel,
    order_fields: Option<&[(&str, Direction)]>,
    access_plan: &AccessPlan<K>,
) -> SecondaryOrderPushdownEligibility {
    let Some(order_fields) = order_fields else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if order_fields.is_empty() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    }

    let Some(access) = access_plan.as_path() else {
        if let Some((index, prefix_len)) = access_plan.first_index_range_details() {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: index.name,
                    prefix_len,
                },
            );
        }

        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        );
    };
    if let Some((index, values)) = access.as_index_prefix() {
        if values.len() > index.fields.len() {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: values.len(),
                    index_field_len: index.fields.len(),
                },
            );
        }

        return assess_secondary_order_pushdown_for_applicable_shape(
            model,
            order_fields,
            index.name,
            index.fields,
            values.len(),
        );
    }
    if let Some((index, prefix_len)) = access.index_range_details() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                index: index.name,
                prefix_len,
            },
        );
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
    )
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub(crate) fn assess_secondary_order_pushdown_from_parts<K>(
    model: &EntityModel,
    logical: &ScalarPlan,
    access: &AccessPlan<K>,
) -> SecondaryOrderPushdownEligibility {
    let order_fields = logical
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_for_plan(model, order_fields.as_deref(), access)
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub(crate) fn assess_secondary_order_pushdown<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> SecondaryOrderPushdownEligibility {
    assess_secondary_order_pushdown_from_parts(model, plan.scalar_plan(), &plan.access)
}

/// Derive pushdown applicability from one plan already validated by planner semantics.
pub(in crate::db) fn derive_secondary_pushdown_applicability_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    let logical = plan.scalar_plan();
    debug_assert!(
        !matches!(logical.order.as_ref(), Some(order) if order.fields.is_empty()),
        "validated plan must not contain an empty ORDER BY specification",
    );

    applicability_from_eligibility(assess_secondary_order_pushdown(model, plan))
}

#[cfg(test)]
/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
pub(crate) fn assess_secondary_order_pushdown_if_applicable<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    derive_secondary_pushdown_applicability_validated(model, plan)
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
#[cfg(test)]
pub(crate) fn assess_secondary_order_pushdown_if_applicable_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    derive_secondary_pushdown_applicability_validated(model, plan)
}

pub(crate) use planner::{PlannerError, plan_access};

///
/// AccessPlanProjection
///
/// Shared visitor for projecting `AccessPlan` / `AccessPath` into
/// diagnostics-specific representations.
///

pub(crate) trait AccessPlanProjection<K> {
    type Output;

    fn by_key(&mut self, key: &K) -> Self::Output;
    fn by_keys(&mut self, keys: &[K]) -> Self::Output;
    fn key_range(&mut self, start: &K, end: &K) -> Self::Output;
    fn index_prefix(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output;
    fn index_range(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output;
    fn full_scan(&mut self) -> Self::Output;
    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output;
    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output;
}

/// Project an access plan by exhaustively walking canonical access variants.
pub(crate) fn project_access_plan<K, P>(plan: &AccessPlan<K>, projection: &mut P) -> P::Output
where
    P: AccessPlanProjection<K>,
{
    plan.project(projection)
}

impl<K> AccessPlan<K> {
    // Project this plan by recursively visiting all access nodes.
    fn project<P>(&self, projection: &mut P) -> P::Output
    where
        P: AccessPlanProjection<K>,
    {
        match self {
            Self::Path(path) => path.project(projection),
            Self::Union(children) => {
                let children = children
                    .iter()
                    .map(|child| child.project(projection))
                    .collect();
                projection.union(children)
            }
            Self::Intersection(children) => {
                let children = children
                    .iter()
                    .map(|child| child.project(projection))
                    .collect();
                projection.intersection(children)
            }
        }
    }
}

impl<K> AccessPath<K> {
    // Project one concrete path variant via the shared projection surface.
    fn project<P>(&self, projection: &mut P) -> P::Output
    where
        P: AccessPlanProjection<K>,
    {
        match self {
            Self::ByKey(key) => projection.by_key(key),
            Self::ByKeys(keys) => projection.by_keys(keys),
            Self::KeyRange { start, end } => projection.key_range(start, end),
            Self::IndexPrefix { index, values } => {
                projection.index_prefix(index.name, index.fields, values.len(), values)
            }
            Self::IndexRange { spec } => projection.index_range(
                spec.index().name,
                spec.index().fields,
                spec.prefix_values().len(),
                spec.prefix_values(),
                spec.lower(),
                spec.upper(),
            ),
            Self::FullScan => projection.full_scan(),
        }
    }
}

pub(crate) fn project_explain_access_path<P>(
    access: &ExplainAccessPath,
    projection: &mut P,
) -> P::Output
where
    P: AccessPlanProjection<Value>,
{
    match access {
        ExplainAccessPath::ByKey { key } => projection.by_key(key),
        ExplainAccessPath::ByKeys { keys } => projection.by_keys(keys),
        ExplainAccessPath::KeyRange { start, end } => projection.key_range(start, end),
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            values,
        } => projection.index_prefix(name, fields, *prefix_len, values),
        ExplainAccessPath::IndexRange {
            name,
            fields,
            prefix_len,
            prefix,
            lower,
            upper,
        } => projection.index_range(name, fields, *prefix_len, prefix, lower, upper),
        ExplainAccessPath::FullScan => projection.full_scan(),
        ExplainAccessPath::Union(children) => {
            let children = children
                .iter()
                .map(|child| project_explain_access_path(child, projection))
                .collect();
            projection.union(children)
        }
        ExplainAccessPath::Intersection(children) => {
            let children = children
                .iter()
                .map(|child| project_explain_access_path(child, projection))
                .collect();
            projection.intersection(children)
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod access_projection_tests {
    use super::*;
    use crate::{model::index::IndexModel, value::Value};

    const TEST_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const TEST_INDEX: IndexModel = IndexModel::new(
        "tests::group_rank",
        "tests::store",
        &TEST_INDEX_FIELDS,
        false,
    );

    #[derive(Default)]
    struct AccessPlanEventProjection {
        events: Vec<&'static str>,
        union_child_counts: Vec<usize>,
        intersection_child_counts: Vec<usize>,
        seen_index: Option<(&'static str, usize, usize, usize)>,
    }

    impl AccessPlanProjection<u64> for AccessPlanEventProjection {
        type Output = ();

        fn by_key(&mut self, _key: &u64) -> Self::Output {
            self.events.push("by_key");
        }

        fn by_keys(&mut self, keys: &[u64]) -> Self::Output {
            self.events.push("by_keys");
            assert_eq!(keys, [2, 3].as_slice());
        }

        fn key_range(&mut self, start: &u64, end: &u64) -> Self::Output {
            self.events.push("key_range");
            assert_eq!((*start, *end), (4, 9));
        }

        fn index_prefix(
            &mut self,
            index_name: &'static str,
            index_fields: &[&'static str],
            prefix_len: usize,
            values: &[Value],
        ) -> Self::Output {
            self.events.push("index_prefix");
            self.seen_index = Some((index_name, index_fields.len(), prefix_len, values.len()));
        }

        fn index_range(
            &mut self,
            index_name: &'static str,
            index_fields: &[&'static str],
            prefix_len: usize,
            prefix: &[Value],
            lower: &Bound<Value>,
            upper: &Bound<Value>,
        ) -> Self::Output {
            self.events.push("index_range");
            self.seen_index = Some((index_name, index_fields.len(), prefix_len, prefix.len()));
            assert_eq!(lower, &Bound::Included(Value::Uint(8)));
            assert_eq!(upper, &Bound::Excluded(Value::Uint(12)));
        }

        fn full_scan(&mut self) -> Self::Output {
            self.events.push("full_scan");
        }

        fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
            self.events.push("union");
            self.union_child_counts.push(children.len());
        }

        fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
            self.events.push("intersection");
            self.intersection_child_counts.push(children.len());
        }
    }

    #[test]
    fn project_access_plan_walks_canonical_access_variants() {
        let plan: AccessPlan<u64> = AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKey(1)),
            AccessPlan::path(AccessPath::ByKeys(vec![2, 3])),
            AccessPlan::path(AccessPath::KeyRange { start: 4, end: 9 }),
            AccessPlan::path(AccessPath::IndexPrefix {
                index: TEST_INDEX,
                values: vec![Value::Uint(7)],
            }),
            AccessPlan::path(AccessPath::index_range(
                TEST_INDEX,
                vec![Value::Uint(7)],
                Bound::Included(Value::Uint(8)),
                Bound::Excluded(Value::Uint(12)),
            )),
            AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::FullScan),
                AccessPlan::path(AccessPath::ByKey(11)),
            ]),
        ]);

        let mut projection = AccessPlanEventProjection::default();
        project_access_plan(&plan, &mut projection);

        assert_eq!(projection.union_child_counts, vec![6]);
        assert_eq!(projection.intersection_child_counts, vec![2]);
        assert_eq!(projection.seen_index, Some((TEST_INDEX.name, 2, 1, 1)));
        assert!(
            projection.events.contains(&"by_key"),
            "projection must visit by-key variants"
        );
        assert!(
            projection.events.contains(&"by_keys"),
            "projection must visit by-keys variants"
        );
        assert!(
            projection.events.contains(&"key_range"),
            "projection must visit key-range variants"
        );
        assert!(
            projection.events.contains(&"index_prefix"),
            "projection must visit index-prefix variants"
        );
        assert!(
            projection.events.contains(&"index_range"),
            "projection must visit index-range variants"
        );
        assert!(
            projection.events.contains(&"full_scan"),
            "projection must visit full-scan variants"
        );
    }

    #[derive(Default)]
    struct ExplainAccessEventProjection {
        events: Vec<&'static str>,
        union_child_counts: Vec<usize>,
        intersection_child_counts: Vec<usize>,
        seen_index: Option<(&'static str, usize, usize, usize)>,
    }

    impl AccessPlanProjection<Value> for ExplainAccessEventProjection {
        type Output = ();

        fn by_key(&mut self, key: &Value) -> Self::Output {
            self.events.push("by_key");
            assert_eq!(key, &Value::Uint(10));
        }

        fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
            self.events.push("by_keys");
            assert_eq!(keys, [Value::Uint(20), Value::Uint(30)].as_slice());
        }

        fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
            self.events.push("key_range");
            assert_eq!((start, end), (&Value::Uint(40), &Value::Uint(90)));
        }

        fn index_prefix(
            &mut self,
            index_name: &'static str,
            index_fields: &[&'static str],
            prefix_len: usize,
            values: &[Value],
        ) -> Self::Output {
            self.events.push("index_prefix");
            self.seen_index = Some((index_name, index_fields.len(), prefix_len, values.len()));
        }

        fn index_range(
            &mut self,
            index_name: &'static str,
            index_fields: &[&'static str],
            prefix_len: usize,
            prefix: &[Value],
            lower: &Bound<Value>,
            upper: &Bound<Value>,
        ) -> Self::Output {
            self.events.push("index_range");
            self.seen_index = Some((index_name, index_fields.len(), prefix_len, prefix.len()));
            assert_eq!(lower, &Bound::Included(Value::Uint(8)));
            assert_eq!(upper, &Bound::Excluded(Value::Uint(12)));
        }

        fn full_scan(&mut self) -> Self::Output {
            self.events.push("full_scan");
        }

        fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
            self.events.push("union");
            self.union_child_counts.push(children.len());
        }

        fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
            self.events.push("intersection");
            self.intersection_child_counts.push(children.len());
        }
    }

    #[test]
    fn project_explain_access_path_walks_canonical_access_variants() {
        let access = ExplainAccessPath::Union(vec![
            ExplainAccessPath::ByKey {
                key: Value::Uint(10),
            },
            ExplainAccessPath::ByKeys {
                keys: vec![Value::Uint(20), Value::Uint(30)],
            },
            ExplainAccessPath::KeyRange {
                start: Value::Uint(40),
                end: Value::Uint(90),
            },
            ExplainAccessPath::IndexPrefix {
                name: TEST_INDEX.name,
                fields: vec!["group", "rank"],
                prefix_len: 1,
                values: vec![Value::Uint(7)],
            },
            ExplainAccessPath::IndexRange {
                name: TEST_INDEX.name,
                fields: vec!["group", "rank"],
                prefix_len: 1,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Included(Value::Uint(8)),
                upper: Bound::Excluded(Value::Uint(12)),
            },
            ExplainAccessPath::Intersection(vec![
                ExplainAccessPath::FullScan,
                ExplainAccessPath::ByKey {
                    key: Value::Uint(10),
                },
            ]),
        ]);

        let mut projection = ExplainAccessEventProjection::default();
        project_explain_access_path(&access, &mut projection);

        assert_eq!(projection.union_child_counts, vec![6]);
        assert_eq!(projection.intersection_child_counts, vec![2]);
        assert_eq!(projection.seen_index, Some((TEST_INDEX.name, 2, 1, 1)));
        assert!(
            projection.events.contains(&"by_key"),
            "projection must visit by-key variants"
        );
        assert!(
            projection.events.contains(&"by_keys"),
            "projection must visit by-keys variants"
        );
        assert!(
            projection.events.contains(&"key_range"),
            "projection must visit key-range variants"
        );
        assert!(
            projection.events.contains(&"index_prefix"),
            "projection must visit index-prefix variants"
        );
        assert!(
            projection.events.contains(&"index_range"),
            "projection must visit index-range variants"
        );
        assert!(
            projection.events.contains(&"full_scan"),
            "projection must visit full-scan variants"
        );
    }
}
