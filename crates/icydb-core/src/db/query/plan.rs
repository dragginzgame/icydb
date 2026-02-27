//! Query plan contracts, planning, and validation wiring.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        access::{
            AccessPath, AccessPlan, PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        contracts::{PredicateExecutionModel, ReadConsistency},
        direction::Direction,
        query::explain::ExplainAccessPath,
    },
    model::entity::EntityModel,
    value::Value,
};
use std::ops::{Bound, Deref, DerefMut};

pub(in crate::db) use crate::db::query::fingerprint::canonical;

pub(crate) use crate::db::query::plan_validate::OrderPlanError;
///
/// Re-Exports
///
pub use crate::db::query::plan_validate::PlanError;

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
/// LogicalPlan
///
/// Pure logical query intent produced by the planner.
///
/// A `LogicalPlan` represents the access-independent query semantics:
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
pub(crate) struct LogicalPlan {
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
    pub(crate) consistency: ReadConsistency,
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

    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(
        access: crate::db::access::AccessPath<K>,
        consistency: ReadConsistency,
    ) -> Self {
        Self {
            logical: LogicalPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency,
            },
            access: AccessPlan::path(access),
        }
    }
}

impl<K> Deref for AccessPlannedQuery<K> {
    type Target = LogicalPlan;

    fn deref(&self) -> &Self::Target {
        &self.logical
    }
}

impl<K> DerefMut for AccessPlannedQuery<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.logical
    }
}

fn direction_from_order(direction: OrderDirection) -> Direction {
    if direction == OrderDirection::Desc {
        Direction::Desc
    } else {
        Direction::Asc
    }
}

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction_from_order(*direction)))
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
                    index,
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
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported { index, prefix_len },
        );
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
    )
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub(crate) fn assess_secondary_order_pushdown<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> SecondaryOrderPushdownEligibility {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_for_plan(model, order_fields.as_deref(), &plan.access)
}

#[cfg(test)]
/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
pub(crate) fn assess_secondary_order_pushdown_if_applicable<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    applicability_from_eligibility(assess_secondary_order_pushdown(model, plan))
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
pub(crate) fn assess_secondary_order_pushdown_if_applicable_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    debug_assert!(
        !matches!(plan.order.as_ref(), Some(order) if order.fields.is_empty()),
        "validated plan must not contain an empty ORDER BY specification",
    );

    applicability_from_eligibility(assess_secondary_order_pushdown(model, plan))
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

mod planner {
    //! Semantic planning from predicates to access strategies; must not assert invariants.
    //!
    //! Determinism: the planner canonicalizes output so the same model and
    //! predicate shape always produce identical access plans.

    use super::PlanError;
    use crate::{
        db::{
            access::{AccessPath, AccessPlan, SemanticIndexRangeSpec},
            contracts::{
                CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo,
                literal_matches_type,
            },
            query::predicate::normalize as normalize_predicate,
        },
        error::InternalError,
        model::entity::EntityModel,
        model::index::IndexModel,
        value::Value,
    };
    use std::ops::Bound;
    use thiserror::Error as ThisError;

    ///
    /// PlannerError
    ///

    #[derive(Debug, ThisError)]
    pub enum PlannerError {
        #[error("{0}")]
        Plan(Box<PlanError>),

        #[error("{0}")]
        Internal(Box<InternalError>),
    }

    impl From<PlanError> for PlannerError {
        fn from(err: PlanError) -> Self {
            Self::Plan(Box::new(err))
        }
    }

    impl From<InternalError> for PlannerError {
        fn from(err: InternalError) -> Self {
            Self::Internal(Box::new(err))
        }
    }

    /// Planner entrypoint that operates on a prebuilt schema surface.
    ///
    /// CONTRACT: the caller is responsible for predicate validation.
    pub(crate) fn plan_access(
        model: &EntityModel,
        schema: &SchemaInfo,
        predicate: Option<&Predicate>,
    ) -> Result<AccessPlan<Value>, PlannerError> {
        let Some(predicate) = predicate else {
            return Ok(AccessPlan::full_scan());
        };

        // Planner determinism guarantee:
        // Given a validated EntityModel and normalized predicate, planning is pure and deterministic.
        //
        // Planner determinism rules:
        // - Predicate normalization sorts AND/OR children by (field, operator, value, coercion).
        // - Index candidates are considered in lexicographic IndexModel.name order.
        // - Access paths are ranked: primary key lookups, exact index matches, prefix matches, full scans.
        // - Order specs preserve user order after validation (planner does not reorder).
        // - Field resolution uses SchemaInfo's name map (sorted by field name).
        let normalized = normalize_predicate(predicate);
        let plan = normalize::normalize_access_plan(plan_predicate(model, schema, &normalized)?);

        Ok(plan)
    }

    fn plan_predicate(
        model: &EntityModel,
        schema: &SchemaInfo,
        predicate: &Predicate,
    ) -> Result<AccessPlan<Value>, InternalError> {
        let plan = match predicate {
            Predicate::True
            | Predicate::False
            | Predicate::Not(_)
            | Predicate::IsNull { .. }
            | Predicate::IsMissing { .. }
            | Predicate::IsEmpty { .. }
            | Predicate::IsNotEmpty { .. }
            | Predicate::TextContains { .. }
            | Predicate::TextContainsCi { .. } => AccessPlan::full_scan(),
            Predicate::And(children) => {
                if let Some(range_spec) = range::index_range_from_and(model, schema, children) {
                    return Ok(AccessPlan::path(AccessPath::IndexRange {
                        spec: range_spec,
                    }));
                }

                let mut plans = children
                    .iter()
                    .map(|child| plan_predicate(model, schema, child))
                    .collect::<Result<Vec<_>, _>>()?;

                // Composite index planning phase:
                // - Range candidate extraction is resolved before child recursion.
                // - If no range candidate exists, retain equality-prefix planning.
                if let Some(prefix) = index_prefix_from_and(model, schema, children) {
                    plans.push(AccessPlan::path(prefix));
                }

                AccessPlan::Intersection(plans)
            }
            Predicate::Or(children) => AccessPlan::Union(
                children
                    .iter()
                    .map(|child| plan_predicate(model, schema, child))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Predicate::Compare(cmp) => plan_compare(model, schema, cmp),
        };

        Ok(plan)
    }

    fn plan_compare(
        model: &EntityModel,
        schema: &SchemaInfo,
        cmp: &ComparePredicate,
    ) -> AccessPlan<Value> {
        if cmp.coercion.id != CoercionId::Strict {
            return AccessPlan::full_scan();
        }

        if is_primary_key_model(schema, model, &cmp.field)
            && let Some(path) = plan_pk_compare(schema, model, cmp)
        {
            return AccessPlan::path(path);
        }

        match cmp.op {
            CompareOp::Eq => {
                if let Some(paths) = index_prefix_for_eq(model, schema, &cmp.field, &cmp.value) {
                    return AccessPlan::Union(paths);
                }
            }
            CompareOp::In => {
                if let Value::List(items) = &cmp.value {
                    let mut plans = Vec::new();
                    for item in items {
                        if let Some(paths) = index_prefix_for_eq(model, schema, &cmp.field, item) {
                            plans.extend(paths);
                        }
                    }
                    if !plans.is_empty() {
                        return AccessPlan::Union(plans);
                    }
                }
            }
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                // Single compare predicates only map directly to one-field indexes.
                // Composite prefix+range extraction remains AND-group driven.
                if index_literal_matches_schema(schema, &cmp.field, &cmp.value) {
                    let (lower, upper) = match cmp.op {
                        CompareOp::Gt => (Bound::Excluded(cmp.value.clone()), Bound::Unbounded),
                        CompareOp::Gte => (Bound::Included(cmp.value.clone()), Bound::Unbounded),
                        CompareOp::Lt => (Bound::Unbounded, Bound::Excluded(cmp.value.clone())),
                        CompareOp::Lte => (Bound::Unbounded, Bound::Included(cmp.value.clone())),
                        _ => unreachable!("range arm must be one of Gt/Gte/Lt/Lte"),
                    };

                    for index in sorted_indexes(model) {
                        if index.fields.len() == 1
                            && index.fields[0] == cmp.field.as_str()
                            && index.is_field_indexable(&cmp.field, cmp.op)
                        {
                            let semantic_range = SemanticIndexRangeSpec::new(
                                *index,
                                vec![0usize],
                                Vec::new(),
                                lower,
                                upper,
                            );

                            return AccessPlan::path(AccessPath::IndexRange {
                                spec: semantic_range,
                            });
                        }
                    }
                }
            }
            _ => {
                // NOTE: Other non-equality comparisons do not currently map to key access paths.
            }
        }

        AccessPlan::full_scan()
    }

    fn plan_pk_compare(
        schema: &SchemaInfo,
        model: &EntityModel,
        cmp: &ComparePredicate,
    ) -> Option<AccessPath<Value>> {
        match cmp.op {
            CompareOp::Eq => {
                if !value_matches_pk_model(schema, model, &cmp.value) {
                    return None;
                }

                Some(AccessPath::ByKey(cmp.value.clone()))
            }
            CompareOp::In => {
                let Value::List(items) = &cmp.value else {
                    return None;
                };

                for item in items {
                    if !value_matches_pk_model(schema, model, item) {
                        return None;
                    }
                }
                // NOTE: key order is canonicalized during access-plan normalization.
                Some(AccessPath::ByKeys(items.clone()))
            }
            _ => {
                // NOTE: Only Eq/In comparisons can be expressed as key access paths.
                None
            }
        }
    }

    pub(in crate::db::query::plan::planner) fn sorted_indexes(
        model: &EntityModel,
    ) -> Vec<&'static IndexModel> {
        let mut indexes = model.indexes.to_vec();
        indexes.sort_by(|left, right| left.name.cmp(right.name));

        indexes
    }

    fn index_prefix_for_eq(
        model: &EntityModel,
        schema: &SchemaInfo,
        field: &str,
        value: &Value,
    ) -> Option<Vec<AccessPlan<Value>>> {
        if !index_literal_matches_schema(schema, field, value) {
            return None;
        }

        let mut out = Vec::new();
        for index in sorted_indexes(model) {
            if index.fields.first() != Some(&field)
                || !index.is_field_indexable(field, CompareOp::Eq)
            {
                continue;
            }
            out.push(AccessPlan::path(AccessPath::IndexPrefix {
                index: *index,
                values: vec![value.clone()],
            }));
        }

        if out.is_empty() { None } else { Some(out) }
    }

    fn index_prefix_from_and(
        model: &EntityModel,
        schema: &SchemaInfo,
        children: &[Predicate],
    ) -> Option<AccessPath<Value>> {
        // Cache literal/schema compatibility once per equality literal so index
        // candidate selection does not repeat schema checks on every index iteration.
        let mut field_values = Vec::new();

        for child in children {
            let Predicate::Compare(cmp) = child else {
                continue;
            };
            if cmp.op != CompareOp::Eq {
                continue;
            }
            if cmp.coercion.id != CoercionId::Strict {
                continue;
            }
            field_values.push(CachedEqLiteral {
                field: cmp.field.as_str(),
                value: &cmp.value,
                compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
            });
        }

        let mut best: Option<(usize, bool, &IndexModel, Vec<Value>)> = None;
        for index in sorted_indexes(model) {
            let mut prefix = Vec::new();
            for field in index.fields {
                // NOTE: duplicate equality predicates on the same field are assumed
                // to have been validated upstream (no conflict). Planner picks the first.
                let Some(cached) = field_values.iter().find(|cached| cached.field == *field) else {
                    break;
                };
                if !index.is_field_indexable(field, CompareOp::Eq) || !cached.compatible {
                    prefix.clear();
                    break;
                }
                prefix.push(cached.value.clone());
            }

            if prefix.is_empty() {
                continue;
            }

            let exact = prefix.len() == index.fields.len();
            match &best {
                None => best = Some((prefix.len(), exact, index, prefix)),
                Some((best_len, best_exact, best_index, _)) => {
                    if better_index(
                        (prefix.len(), exact, index),
                        (*best_len, *best_exact, best_index),
                    ) {
                        best = Some((prefix.len(), exact, index, prefix));
                    }
                }
            }
        }

        best.map(|(_, _, index, values)| AccessPath::IndexPrefix {
            index: *index,
            values,
        })
    }

    ///
    /// CachedEqLiteral
    ///
    /// Equality literal plus its precomputed planner-side schema compatibility.
    ///

    struct CachedEqLiteral<'a> {
        field: &'a str,
        value: &'a Value,
        compatible: bool,
    }

    fn better_index(
        candidate: (usize, bool, &IndexModel),
        current: (usize, bool, &IndexModel),
    ) -> bool {
        let (cand_len, cand_exact, cand_index) = candidate;
        let (best_len, best_exact, best_index) = current;

        cand_len > best_len
            || (cand_len == best_len && cand_exact && !best_exact)
            || (cand_len == best_len
                && cand_exact == best_exact
                && cand_index.name < best_index.name)
    }

    fn is_primary_key_model(schema: &SchemaInfo, model: &EntityModel, field: &str) -> bool {
        field == model.primary_key.name && schema.field(field).is_some()
    }

    fn value_matches_pk_model(schema: &SchemaInfo, model: &EntityModel, value: &Value) -> bool {
        let field = model.primary_key.name;
        let Some(field_type) = schema.field(field) else {
            return false;
        };

        field_type.is_keyable() && literal_matches_type(value, field_type)
    }

    pub(in crate::db::query::plan::planner) fn index_literal_matches_schema(
        schema: &SchemaInfo,
        field: &str,
        value: &Value,
    ) -> bool {
        let Some(field_type) = schema.field(field) else {
            return false;
        };
        if !literal_matches_type(value, field_type) {
            return false;
        }

        true
    }

    impl IndexModel {
        /// Return true when this index can structurally support the field/operator pair.
        #[must_use]
        pub(in crate::db::query::plan::planner) fn is_field_indexable(
            &self,
            field: &str,
            op: CompareOp,
        ) -> bool {
            if !self.fields.contains(&field) {
                return false;
            }

            matches!(
                op,
                CompareOp::Eq
                    | CompareOp::In
                    | CompareOp::Gt
                    | CompareOp::Gte
                    | CompareOp::Lt
                    | CompareOp::Lte
            )
        }
    }

    ///
    /// TESTS
    ///

    #[cfg(test)]
    mod planner_tests {
        use super::*;
        use crate::types::Ulid;

        #[test]
        fn normalize_union_dedups_identical_paths() {
            let key = Value::Ulid(Ulid::from_u128(1));
            let plan = AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKey(key.clone())),
                AccessPlan::path(AccessPath::ByKey(key)),
            ]);

            let normalized = normalize::normalize_access_plan(plan);

            assert_eq!(
                normalized,
                AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(1))))
            );
        }

        #[test]
        fn normalize_union_sorts_by_key() {
            let a = Value::Ulid(Ulid::from_u128(1));
            let b = Value::Ulid(Ulid::from_u128(2));
            let plan = AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKey(b.clone())),
                AccessPlan::path(AccessPath::ByKey(a.clone())),
            ]);

            let normalized = normalize::normalize_access_plan(plan);
            let AccessPlan::Union(children) = normalized else {
                panic!("expected union");
            };

            assert_eq!(children.len(), 2);
            assert_eq!(children[0], AccessPlan::path(AccessPath::ByKey(a)));
            assert_eq!(children[1], AccessPlan::path(AccessPath::ByKey(b)));
        }

        #[test]
        fn normalize_intersection_removes_full_scan() {
            let key = Value::Ulid(Ulid::from_u128(7));
            let plan = AccessPlan::Intersection(vec![
                AccessPlan::full_scan(),
                AccessPlan::path(AccessPath::ByKey(key)),
            ]);

            let normalized = normalize::normalize_access_plan(plan);

            assert_eq!(
                normalized,
                AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(7))))
            );
        }
    }

    mod normalize {
        use crate::{
            db::{
                access::{AccessPath, AccessPlan},
                query::plan::canonical,
            },
            value::Value,
        };

        // Normalize composite access plans into canonical, flattened forms.
        pub(in crate::db::query::plan::planner) fn normalize_access_plan(
            plan: AccessPlan<Value>,
        ) -> AccessPlan<Value> {
            plan.normalize_for_planner()
        }

        impl AccessPlan<Value> {
            // Normalize this access plan into a canonical deterministic form.
            fn normalize_for_planner(self) -> Self {
                match self {
                    Self::Path(path) => Self::path(path.normalize_for_planner()),
                    Self::Union(children) => Self::normalize_union(children),
                    Self::Intersection(children) => Self::normalize_intersection(children),
                }
            }

            fn normalize_union(children: Vec<Self>) -> Self {
                let mut out = Vec::new();

                for child in children {
                    let child = child.normalize_for_planner();
                    if child.is_single_full_scan() {
                        return Self::full_scan();
                    }

                    Self::append_union_child(&mut out, child);
                }

                Self::collapse_composite(out, true)
            }

            fn normalize_intersection(children: Vec<Self>) -> Self {
                let mut out = Vec::new();

                for child in children {
                    let child = child.normalize_for_planner();
                    if child.is_single_full_scan() {
                        continue;
                    }

                    Self::append_intersection_child(&mut out, child);
                }

                Self::collapse_composite(out, false)
            }

            fn collapse_composite(mut out: Vec<Self>, is_union: bool) -> Self {
                if out.is_empty() {
                    return Self::full_scan();
                }
                if out.len() == 1 {
                    return out.pop().expect("single composite child");
                }

                canonical::canonicalize_access_plans_value(&mut out);
                out.dedup();
                if out.len() == 1 {
                    return out.pop().expect("single composite child");
                }

                if is_union {
                    Self::Union(out)
                } else {
                    Self::Intersection(out)
                }
            }

            fn append_union_child(out: &mut Vec<Self>, child: Self) {
                match child {
                    Self::Union(children) => out.extend(children),
                    other => out.push(other),
                }
            }

            fn append_intersection_child(out: &mut Vec<Self>, child: Self) {
                match child {
                    Self::Intersection(children) => out.extend(children),
                    other => out.push(other),
                }
            }
        }

        impl AccessPath<Value> {
            // Normalize one concrete access path for deterministic planning.
            fn normalize_for_planner(self) -> Self {
                match self {
                    Self::ByKeys(mut keys) => {
                        canonical::canonicalize_key_values(&mut keys);
                        Self::ByKeys(keys)
                    }
                    other => other,
                }
            }
        }
    }

    mod range {
        use crate::{
            db::{
                access::SemanticIndexRangeSpec,
                contracts::{
                    CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo, canonical_cmp,
                },
                query::plan::planner::{index_literal_matches_schema, sorted_indexes},
            },
            model::{entity::EntityModel, index::IndexModel},
            value::{CoercionFamily, CoercionFamilyExt, Value},
        };
        use std::{mem::discriminant, ops::Bound};

        ///
        /// RangeConstraint
        /// One-field bounded interval used for index-range candidate extraction.
        ///

        #[derive(Clone, Debug, Eq, PartialEq)]
        struct RangeConstraint {
            lower: Bound<Value>,
            upper: Bound<Value>,
        }

        impl Default for RangeConstraint {
            fn default() -> Self {
                Self {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                }
            }
        }

        ///
        /// IndexFieldConstraint
        /// Per-index-field constraint classification while extracting range candidates.
        ///

        #[derive(Clone, Debug, Eq, PartialEq)]
        enum IndexFieldConstraint {
            None,
            Eq(Value),
            Range(RangeConstraint),
        }

        ///
        /// CachedCompare
        ///
        /// Compare predicate plus precomputed planner-side schema compatibility.
        ///

        #[derive(Clone)]
        struct CachedCompare<'a> {
            cmp: &'a ComparePredicate,
            literal_compatible: bool,
        }

        // Build one deterministic secondary-range candidate from a normalized AND-group.
        //
        // Extraction contract:
        // - Every child must be a Compare predicate.
        // - Supported operators are Eq/Gt/Gte/Lt/Lte only.
        // - For a chosen index: fields 0..k must be Eq, field k must be Range,
        //   fields after k must be unconstrained.
        pub(in crate::db::query::plan::planner) fn index_range_from_and(
            model: &EntityModel,
            schema: &SchemaInfo,
            children: &[Predicate],
        ) -> Option<SemanticIndexRangeSpec> {
            let mut compares = Vec::with_capacity(children.len());
            for child in children {
                let Predicate::Compare(cmp) = child else {
                    return None;
                };
                if !matches!(
                    cmp.op,
                    CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
                ) {
                    return None;
                }
                if !matches!(
                    cmp.coercion.id,
                    CoercionId::Strict | CoercionId::NumericWiden
                ) {
                    return None;
                }
                compares.push(CachedCompare {
                    cmp,
                    literal_compatible: index_literal_matches_schema(
                        schema, &cmp.field, &cmp.value,
                    ),
                });
            }

            let mut best: Option<(
                usize,
                &'static IndexModel,
                usize,
                Vec<Value>,
                RangeConstraint,
            )> = None;
            for index in sorted_indexes(model) {
                let Some((range_slot, prefix, range)) =
                    index_range_candidate_for_index(index, &compares)
                else {
                    continue;
                };

                let prefix_len = prefix.len();
                match best {
                    None => best = Some((prefix_len, index, range_slot, prefix, range)),
                    Some((best_len, best_index, _, _, _))
                        if prefix_len > best_len
                            || (prefix_len == best_len && index.name < best_index.name) =>
                    {
                        best = Some((prefix_len, index, range_slot, prefix, range));
                    }
                    _ => {}
                }
            }

            best.map(|(_, index, range_slot, prefix, range)| {
                let field_slots = (0..=range_slot).collect();

                SemanticIndexRangeSpec::new(*index, field_slots, prefix, range.lower, range.upper)
            })
        }

        // Extract an index-range candidate for one concrete index.
        fn index_range_candidate_for_index(
            index: &'static IndexModel,
            compares: &[CachedCompare<'_>],
        ) -> Option<(usize, Vec<Value>, RangeConstraint)> {
            // Phase 1: classify each index field as Eq/Range/None for this compare set.
            let constraints = classify_index_field_constraints(index, compares)?;

            // Phase 2: materialize deterministic prefix+range shape from constraints.
            select_prefix_and_range(index.fields.len(), &constraints)
        }

        // Build per-field constraint classes for one index from compare predicates.
        fn classify_index_field_constraints(
            index: &'static IndexModel,
            compares: &[CachedCompare<'_>],
        ) -> Option<Vec<IndexFieldConstraint>> {
            let mut constraints = vec![IndexFieldConstraint::None; index.fields.len()];

            for cached in compares {
                let cmp = cached.cmp;
                let Some(position) = index
                    .fields
                    .iter()
                    .position(|field| *field == cmp.field.as_str())
                else {
                    continue;
                };

                if !cached.literal_compatible
                    || !index.is_field_indexable(cmp.field.as_str(), cmp.op)
                {
                    return None;
                }

                match cmp.op {
                    CompareOp::Eq => match &mut constraints[position] {
                        IndexFieldConstraint::None => {
                            constraints[position] = IndexFieldConstraint::Eq(cmp.value.clone());
                        }
                        IndexFieldConstraint::Eq(existing) => {
                            if existing != &cmp.value {
                                return None;
                            }
                        }
                        IndexFieldConstraint::Range(_) => return None,
                    },
                    CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                        let mut range = match &constraints[position] {
                            IndexFieldConstraint::None => RangeConstraint::default(),
                            IndexFieldConstraint::Eq(_) => return None,
                            IndexFieldConstraint::Range(existing) => existing.clone(),
                        };
                        if !merge_range_constraint(&mut range, cmp.op, &cmp.value) {
                            return None;
                        }
                        constraints[position] = IndexFieldConstraint::Range(range);
                    }
                    _ => return None,
                }
            }

            Some(constraints)
        }

        // Convert classified constraints into one valid prefix+range candidate shape.
        fn select_prefix_and_range(
            field_count: usize,
            constraints: &[IndexFieldConstraint],
        ) -> Option<(usize, Vec<Value>, RangeConstraint)> {
            let mut prefix = Vec::new();
            let mut range: Option<RangeConstraint> = None;
            let mut range_position = None;

            for (position, constraint) in constraints.iter().enumerate() {
                match constraint {
                    IndexFieldConstraint::Eq(value) if range.is_none() => {
                        prefix.push(value.clone());
                    }
                    IndexFieldConstraint::Range(candidate) if range.is_none() => {
                        range = Some(candidate.clone());
                        range_position = Some(position);
                    }
                    IndexFieldConstraint::None if range.is_none() => return None,
                    IndexFieldConstraint::None => {}
                    _ => return None,
                }
            }

            let (Some(range_position), Some(range)) = (range_position, range) else {
                return None;
            };
            if range_position >= field_count {
                return None;
            }
            if prefix.len() >= field_count {
                return None;
            }

            Some((range_position, prefix, range))
        }

        // Merge one comparison operator into a bounded range without widening semantics.
        fn merge_range_constraint(
            existing: &mut RangeConstraint,
            op: CompareOp,
            value: &Value,
        ) -> bool {
            let merged = match op {
                CompareOp::Gt => {
                    merge_lower_bound(&mut existing.lower, Bound::Excluded(value.clone()))
                }
                CompareOp::Gte => {
                    merge_lower_bound(&mut existing.lower, Bound::Included(value.clone()))
                }
                CompareOp::Lt => {
                    merge_upper_bound(&mut existing.upper, Bound::Excluded(value.clone()))
                }
                CompareOp::Lte => {
                    merge_upper_bound(&mut existing.upper, Bound::Included(value.clone()))
                }
                _ => false,
            };
            if !merged {
                return false;
            }

            range_bounds_are_compatible(existing)
        }

        fn merge_lower_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
            if !bounds_numeric_variants_compatible(existing, &candidate) {
                return false;
            }

            let replace = match (&candidate, &*existing) {
                (Bound::Unbounded, _) => false,
                (_, Bound::Unbounded) => true,
                (
                    Bound::Included(left) | Bound::Excluded(left),
                    Bound::Included(right) | Bound::Excluded(right),
                ) => match canonical_cmp(left, right) {
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Less => false,
                    std::cmp::Ordering::Equal => {
                        matches!(candidate, Bound::Excluded(_))
                            && matches!(existing, Bound::Included(_))
                    }
                },
            };

            if replace {
                *existing = candidate;
            }

            true
        }

        fn merge_upper_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
            if !bounds_numeric_variants_compatible(existing, &candidate) {
                return false;
            }

            let replace = match (&candidate, &*existing) {
                (Bound::Unbounded, _) => false,
                (_, Bound::Unbounded) => true,
                (
                    Bound::Included(left) | Bound::Excluded(left),
                    Bound::Included(right) | Bound::Excluded(right),
                ) => match canonical_cmp(left, right) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => {
                        matches!(candidate, Bound::Excluded(_))
                            && matches!(existing, Bound::Included(_))
                    }
                },
            };

            if replace {
                *existing = candidate;
            }

            true
        }

        // Validate interval shape and reject empty/mixed-numeric intervals.
        fn range_bounds_are_compatible(range: &RangeConstraint) -> bool {
            let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper))
            else {
                return true;
            };

            if !numeric_variants_compatible(lower, upper) {
                return false;
            }

            !range_is_empty(range)
        }

        // Return true when a bounded range is empty under canonical value ordering.
        fn range_is_empty(range: &RangeConstraint) -> bool {
            let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper))
            else {
                return false;
            };

            match canonical_cmp(lower, upper) {
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => {
                    !matches!(range.lower, Bound::Included(_))
                        || !matches!(range.upper, Bound::Included(_))
                }
            }
        }

        const fn bound_value(bound: &Bound<Value>) -> Option<&Value> {
            match bound {
                Bound::Included(value) | Bound::Excluded(value) => Some(value),
                Bound::Unbounded => None,
            }
        }

        fn bounds_numeric_variants_compatible(left: &Bound<Value>, right: &Bound<Value>) -> bool {
            match (bound_value(left), bound_value(right)) {
                (Some(left), Some(right)) => numeric_variants_compatible(left, right),
                _ => true,
            }
        }

        fn numeric_variants_compatible(left: &Value, right: &Value) -> bool {
            if left.coercion_family() != CoercionFamily::Numeric
                || right.coercion_family() != CoercionFamily::Numeric
            {
                return true;
            }

            discriminant(left) == discriminant(right)
        }
    }
}
