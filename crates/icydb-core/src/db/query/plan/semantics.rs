//! Module: query::plan::semantics
//! Responsibility: semantic interpretation for query-plan model contracts.
//! Does not own: constructors or planner algorithm selection.
//! Boundary: meaning-level helpers over data-only plan model types.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        query::{
            explain::ExplainAccessPath,
            plan::{
                AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec,
                GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupPlan, LogicalPlan,
                OrderSpec, QueryMode, ScalarPlan,
            },
        },
    },
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};
use std::ops::Bound;

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

impl AggregateKind {
    /// Return whether this terminal kind is `COUNT`.
    #[must_use]
    pub(in crate::db) const fn is_count(self) -> bool {
        matches!(self, Self::Count)
    }

    /// Return whether this terminal kind supports explicit field targets.
    #[must_use]
    pub(in crate::db) const fn supports_field_targets(self) -> bool {
        matches!(self, Self::Min | Self::Max)
    }

    /// Return whether this terminal kind belongs to the extrema family.
    #[must_use]
    pub(in crate::db) const fn is_extrema(self) -> bool {
        self.supports_field_targets()
    }

    /// Return whether this terminal kind supports first/last value projection.
    #[must_use]
    pub(in crate::db) const fn supports_terminal_value_projection(self) -> bool {
        matches!(self, Self::First | Self::Last)
    }

    /// Return whether reducer updates for this kind require a decoded id payload.
    #[must_use]
    pub(in crate::db) const fn requires_decoded_id(self) -> bool {
        !matches!(self, Self::Count | Self::Exists)
    }

    /// Return whether grouped aggregate DISTINCT is supported for this kind.
    #[must_use]
    pub(in crate::db) const fn supports_grouped_distinct_v1(self) -> bool {
        matches!(self, Self::Count | Self::Min | Self::Max)
    }

    /// Return the canonical grouped aggregate fingerprint tag (v1).
    #[must_use]
    pub(in crate::db) const fn fingerprint_tag_v1(self) -> u8 {
        match self {
            Self::Count => 0x01,
            Self::Exists => 0x02,
            Self::Min => 0x03,
            Self::Max => 0x04,
            Self::First => 0x05,
            Self::Last => 0x06,
        }
    }
}

impl GroupAggregateSpec {
    /// Return the canonical grouped aggregate terminal kind.
    #[must_use]
    pub(crate) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Return the optional grouped aggregate target field.
    #[must_use]
    pub(crate) fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Return whether this grouped aggregate terminal uses DISTINCT semantics.
    #[must_use]
    pub(crate) const fn distinct(&self) -> bool {
        self.distinct
    }
}

impl GroupHavingSpec {
    /// Borrow grouped HAVING clauses in declaration order.
    #[must_use]
    pub(crate) const fn clauses(&self) -> &[GroupHavingClause] {
        self.clauses.as_slice()
    }
}

impl GroupHavingClause {
    /// Borrow grouped HAVING symbol reference.
    #[must_use]
    pub(crate) const fn symbol(&self) -> &GroupHavingSymbol {
        &self.symbol
    }

    /// Borrow grouped HAVING compare operator.
    #[must_use]
    pub(crate) const fn op(&self) -> crate::db::predicate::CompareOp {
        self.op
    }

    /// Borrow grouped HAVING comparison value.
    #[must_use]
    pub(crate) const fn value(&self) -> &Value {
        &self.value
    }
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

    /// Test-only shorthand for explicit scalar semantic borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar(&self) -> &ScalarPlan {
        self.scalar_semantics()
    }

    /// Test-only shorthand for explicit mutable scalar semantic borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_mut(&mut self) -> &mut ScalarPlan {
        self.scalar_semantics_mut()
    }
}

impl<K> AccessPlannedQuery<K> {
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

    /// Test-only shorthand for explicit scalar plan borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar(&self) -> &ScalarPlan {
        self.scalar_plan()
    }

    /// Test-only shorthand for explicit mutable scalar plan borrowing.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn scalar_mut(&mut self) -> &mut ScalarPlan {
        self.scalar_plan_mut()
    }
}

///
/// GroupedPlanStrategyHint
///
/// Planner-side grouped execution strategy hint projected from logical + access shape.
/// Executor routing may revalidate this hint against runtime capability constraints.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedPlanStrategyHint {
    HashGroup,
    OrderedGroup,
}

/// Project one grouped execution strategy hint from one access-planned query.
#[must_use]
pub(crate) fn grouped_plan_strategy_hint<K>(
    plan: &AccessPlannedQuery<K>,
) -> Option<GroupedPlanStrategyHint> {
    let grouped = plan.grouped_plan()?;
    if grouped.scalar.distinct {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if grouped.scalar.predicate.is_some() {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if !grouped_aggregates_streaming_compatible(grouped.group.aggregates.as_slice()) {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if !grouped_having_streaming_compatible(grouped.having.as_ref()) {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if !grouped_order_prefix_matches_group_fields(
        grouped.scalar.order.as_ref(),
        grouped.group.group_fields.as_slice(),
    ) {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if grouped_access_path_proves_group_order(grouped.group.group_fields.as_slice(), &plan.access) {
        return Some(GroupedPlanStrategyHint::OrderedGroup);
    }

    Some(GroupedPlanStrategyHint::HashGroup)
}

fn grouped_aggregates_streaming_compatible(aggregates: &[GroupAggregateSpec]) -> bool {
    aggregates.iter().all(|aggregate| {
        aggregate.target_field().is_none()
            && (!aggregate.distinct() || aggregate.kind().supports_grouped_distinct_v1())
    })
}

fn grouped_having_streaming_compatible(having: Option<&GroupHavingSpec>) -> bool {
    having.is_none_or(|having| {
        having.clauses().iter().all(|clause| {
            matches!(
                clause.op(),
                crate::db::predicate::CompareOp::Eq
                    | crate::db::predicate::CompareOp::Ne
                    | crate::db::predicate::CompareOp::Lt
                    | crate::db::predicate::CompareOp::Lte
                    | crate::db::predicate::CompareOp::Gt
                    | crate::db::predicate::CompareOp::Gte
            )
        })
    })
}

fn grouped_order_prefix_matches_group_fields(
    order: Option<&OrderSpec>,
    group_fields: &[FieldSlot],
) -> bool {
    let Some(order) = order else {
        return true;
    };
    if order.fields.len() < group_fields.len() {
        return false;
    }

    group_fields
        .iter()
        .zip(order.fields.iter())
        .all(|(group_field, (order_field, _))| order_field == group_field.field())
}

fn grouped_access_path_proves_group_order<K>(
    group_fields: &[FieldSlot],
    access: &AccessPlan<K>,
) -> bool {
    match access {
        AccessPlan::Path(path) => match path.as_ref() {
            AccessPath::IndexPrefix { index, values } => {
                let prefix_len = values.len();
                let required_end = prefix_len.saturating_add(group_fields.len());
                if required_end > index.fields.len() {
                    return false;
                }

                group_fields
                    .iter()
                    .zip(index.fields[prefix_len..required_end].iter())
                    .all(|(group_field, index_field)| group_field.field() == *index_field)
            }
            AccessPath::ByKey(_)
            | AccessPath::ByKeys(_)
            | AccessPath::KeyRange { .. }
            | AccessPath::IndexRange { .. }
            | AccessPath::FullScan => false,
        },
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => false,
    }
}

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
