//! Module: query::intent::cache_key
//! Responsibility: canonical shared-cache identity normalization for structural queries.
//! Does not own: planner validation, executor runtime behavior, or SQL surface routing.
//! Boundary: turns semantic query intent into one explicit derived-hash cache key.

use crate::{
    db::{
        access::{
            AccessPlan,
            dispatch::{AccessPathDispatch, AccessPlanDispatch, dispatch_access_plan},
        },
        predicate::{CompareOp, MissingRowPolicy, Predicate, predicate_fingerprint},
        query::{
            builder::aggregate::AggregateExpr,
            intent::{build_access_plan_from_keys, model::QueryModel, state::GroupedIntent},
            plan::{
                GroupHavingSpec, GroupHavingSymbol, OrderDirection, OrderSpec, QueryMode,
                expr::{Expr, Function, ProjectionField, ProjectionSelection},
            },
        },
    },
    traits::FieldValue,
    value::{Value, hash_value},
};

///
/// StructuralQueryCacheKey
///
/// Canonical semantic identity for the shared fluent/lower query-plan cache.
/// This key is intentionally explicit: normalization owns semantic equivalence,
/// while `Hash` ownership stays mechanical at the map boundary.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct StructuralQueryCacheKey {
    mode: QueryModeCacheKey,
    predicate: Option<PredicateCacheKey>,
    key_access: Option<AccessPathCacheKey>,
    order: Option<OrderCacheKey>,
    distinct: bool,
    projection: ProjectionCacheKey,
    grouping: Option<GroupingCacheKey>,
    consistency: ConsistencyCacheKey,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum QueryModeCacheKey {
    Load { limit: Option<u32>, offset: u32 },
    Delete { limit: Option<u32>, offset: u32 },
}

// Predicate identity stays anchored to the predicate subsystem's canonical
// structural hash so the shared cache does not grow its own predicate walker.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum PredicateCacheKey {
    Canonical([u8; 32]),
}

// Value identity uses the existing canonical value hash while preserving one
// stable fallback when some nested structured value cannot hash cleanly.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ValueCacheKey {
    Canonical([u8; 16]),
    HashError(String),
}

// Shared lower cache identity still needs one explicit access-path key
// because fluent key-only routing can materially change planner reuse.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum AccessPathCacheKey {
    ByKey(ValueCacheKey),
    ByKeys(Vec<ValueCacheKey>),
    KeyRange {
        start: ValueCacheKey,
        end: ValueCacheKey,
    },
    IndexPrefix {
        index: String,
        values: Vec<ValueCacheKey>,
    },
    IndexMultiLookup {
        index: String,
        values: Vec<ValueCacheKey>,
    },
    IndexRange(IndexRangeCacheKey),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
    FullScan,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IndexRangeCacheKey {
    index: String,
    field_slots: Vec<usize>,
    prefix_values: Vec<ValueCacheKey>,
    lower: RangeBoundCacheKey,
    upper: RangeBoundCacheKey,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum RangeBoundCacheKey {
    Unbounded,
    Included(ValueCacheKey),
    Excluded(ValueCacheKey),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OrderCacheKey {
    fields: Vec<OrderFieldCacheKey>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OrderFieldCacheKey {
    field: String,
    direction: OrderDirectionCacheKey,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum OrderDirectionCacheKey {
    Asc,
    Desc,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ProjectionCacheKey {
    All,
    Fields(Vec<String>),
    Exprs(Vec<ProjectionExprCacheKey>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ProjectionExprCacheKey {
    Field(String),
    Literal(ValueCacheKey),
    FunctionCall {
        function: Function,
        args: Vec<Self>,
    },
    Binary {
        op: BinaryOpCacheKey,
        left: Box<Self>,
        right: Box<Self>,
    },
    Aggregate(AggregateExprCacheKey),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BinaryOpCacheKey {
    Add,
    Sub,
    Mul,
    Div,
    #[cfg(test)]
    And,
    #[cfg(test)]
    Eq,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AggregateExprCacheKey {
    kind_tag: u8,
    target_field: Option<String>,
    distinct: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupingCacheKey {
    group_fields: Vec<GroupFieldCacheKey>,
    aggregates: Vec<GroupAggregateCacheKey>,
    having: Option<GroupHavingCacheKey>,
    max_groups: u64,
    max_group_bytes: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupFieldCacheKey {
    index: usize,
    field: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupAggregateCacheKey {
    kind_tag: u8,
    target_field: Option<String>,
    distinct: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupHavingCacheKey {
    clauses: Vec<GroupHavingClauseCacheKey>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupHavingClauseCacheKey {
    symbol: GroupHavingSymbolCacheKey,
    op: CompareOpCacheKey,
    value: ValueCacheKey,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum GroupHavingSymbolCacheKey {
    GroupField(GroupFieldCacheKey),
    AggregateIndex(usize),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CompareOpCacheKey(u8);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ConsistencyCacheKey {
    Ignore,
    Error,
}

impl StructuralQueryCacheKey {
    pub(in crate::db) fn from_query_model<K: FieldValue>(model: &QueryModel<'_, K>) -> Self {
        let scalar = model.scalar_intent_for_cache_key();
        let key_access = scalar
            .key_access
            .as_ref()
            .map(|state| build_access_plan_from_keys(&state.access));

        Self {
            mode: QueryModeCacheKey::from_query_mode(model.mode()),
            predicate: scalar
                .predicate
                .as_ref()
                .map(PredicateCacheKey::from_predicate),
            key_access: key_access
                .as_ref()
                .map(AccessPathCacheKey::from_access_plan),
            order: scalar.order.as_ref().map(OrderCacheKey::from_order_spec),
            distinct: scalar.distinct,
            projection: ProjectionCacheKey::from_projection_selection(&scalar.projection_selection),
            grouping: model
                .grouped_intent_for_cache_key()
                .map(GroupingCacheKey::from_grouped_intent),
            consistency: ConsistencyCacheKey::from_missing_row_policy(
                model.consistency_for_cache_key(),
            ),
        }
    }
}

impl QueryModeCacheKey {
    const fn from_query_mode(mode: QueryMode) -> Self {
        match mode {
            QueryMode::Load(spec) => Self::Load {
                limit: spec.limit(),
                offset: spec.offset(),
            },
            QueryMode::Delete(spec) => Self::Delete {
                limit: spec.limit(),
                offset: spec.offset(),
            },
        }
    }
}

impl PredicateCacheKey {
    fn from_predicate(predicate: &Predicate) -> Self {
        Self::Canonical(predicate_fingerprint(predicate))
    }
}

impl ValueCacheKey {
    fn from_value(value: &Value) -> Self {
        match hash_value(value) {
            Ok(digest) => Self::Canonical(digest),
            Err(err) => Self::HashError(err.display_with_class()),
        }
    }
}

impl AccessPathCacheKey {
    fn from_access_plan(path: &AccessPlan<Value>) -> Self {
        match dispatch_access_plan(path) {
            AccessPlanDispatch::Path(path) => Self::from_access_path(path),
            AccessPlanDispatch::Union(children) => Self::Union(
                children
                    .iter()
                    .map(Self::from_access_plan)
                    .collect::<Vec<_>>(),
            ),
            AccessPlanDispatch::Intersection(children) => Self::Intersection(
                children
                    .iter()
                    .map(Self::from_access_plan)
                    .collect::<Vec<_>>(),
            ),
        }
    }

    fn from_access_path(path: AccessPathDispatch<'_, Value>) -> Self {
        match path {
            AccessPathDispatch::ByKey(key) => Self::ByKey(ValueCacheKey::from_value(key)),
            AccessPathDispatch::ByKeys(keys) => Self::ByKeys(
                keys.iter()
                    .map(ValueCacheKey::from_value)
                    .collect::<Vec<_>>(),
            ),
            AccessPathDispatch::KeyRange { start, end } => Self::KeyRange {
                start: ValueCacheKey::from_value(start),
                end: ValueCacheKey::from_value(end),
            },
            AccessPathDispatch::IndexPrefix { index, values } => Self::IndexPrefix {
                index: index.name().to_string(),
                values: values.iter().map(ValueCacheKey::from_value).collect(),
            },
            AccessPathDispatch::IndexMultiLookup { index, values } => Self::IndexMultiLookup {
                index: index.name().to_string(),
                values: values.iter().map(ValueCacheKey::from_value).collect(),
            },
            AccessPathDispatch::IndexRange { spec } => Self::IndexRange(IndexRangeCacheKey {
                index: spec.index().name().to_string(),
                field_slots: spec.field_slots().to_vec(),
                prefix_values: spec
                    .prefix_values()
                    .iter()
                    .map(ValueCacheKey::from_value)
                    .collect(),
                lower: RangeBoundCacheKey::from_range_bound(spec.lower()),
                upper: RangeBoundCacheKey::from_range_bound(spec.upper()),
            }),
            AccessPathDispatch::FullScan => Self::FullScan,
        }
    }
}

impl RangeBoundCacheKey {
    fn from_range_bound(bound: &std::ops::Bound<Value>) -> Self {
        match bound {
            std::ops::Bound::Unbounded => Self::Unbounded,
            std::ops::Bound::Included(value) => Self::Included(ValueCacheKey::from_value(value)),
            std::ops::Bound::Excluded(value) => Self::Excluded(ValueCacheKey::from_value(value)),
        }
    }
}

impl OrderCacheKey {
    fn from_order_spec(order: &OrderSpec) -> Self {
        Self {
            fields: order
                .fields
                .iter()
                .map(|(field, direction)| OrderFieldCacheKey {
                    field: field.clone(),
                    direction: OrderDirectionCacheKey::from_order_direction(*direction),
                })
                .collect(),
        }
    }
}

impl OrderDirectionCacheKey {
    const fn from_order_direction(direction: OrderDirection) -> Self {
        match direction {
            OrderDirection::Asc => Self::Asc,
            OrderDirection::Desc => Self::Desc,
        }
    }
}

impl ProjectionCacheKey {
    fn from_projection_selection(projection: &ProjectionSelection) -> Self {
        match projection {
            ProjectionSelection::All => Self::All,
            ProjectionSelection::Fields(fields) => Self::Fields(
                fields
                    .iter()
                    .map(|field| field.as_str().to_string())
                    .collect(),
            ),
            ProjectionSelection::Exprs(fields) => Self::Exprs(
                fields
                    .iter()
                    .map(ProjectionExprCacheKey::from_projection_field)
                    .collect(),
            ),
        }
    }
}

impl ProjectionExprCacheKey {
    fn from_projection_field(field: &ProjectionField) -> Self {
        match field {
            ProjectionField::Scalar { expr, alias: _ } => Self::from_expr(expr),
        }
    }

    fn from_expr(expr: &Expr) -> Self {
        match expr {
            Expr::Field(field) => Self::Field(field.as_str().to_string()),
            Expr::Literal(value) => Self::Literal(ValueCacheKey::from_value(value)),
            Expr::FunctionCall { function, args } => Self::FunctionCall {
                function: *function,
                args: args.iter().map(Self::from_expr).collect(),
            },
            Expr::Binary { op, left, right } => Self::Binary {
                op: BinaryOpCacheKey::from_binary_op(*op),
                left: Box::new(Self::from_expr(left.as_ref())),
                right: Box::new(Self::from_expr(right.as_ref())),
            },
            Expr::Aggregate(aggregate) => {
                Self::Aggregate(AggregateExprCacheKey::from_aggregate_expr(aggregate))
            }
            #[cfg(test)]
            Expr::Alias { expr, name: _ } => Self::from_expr(expr.as_ref()),
            #[cfg(test)]
            Expr::Unary { op: _, expr } => Self::from_expr(expr.as_ref()),
        }
    }
}

impl BinaryOpCacheKey {
    const fn from_binary_op(op: crate::db::query::plan::expr::BinaryOp) -> Self {
        match op {
            crate::db::query::plan::expr::BinaryOp::Add => Self::Add,
            crate::db::query::plan::expr::BinaryOp::Sub => Self::Sub,
            crate::db::query::plan::expr::BinaryOp::Mul => Self::Mul,
            crate::db::query::plan::expr::BinaryOp::Div => Self::Div,
            #[cfg(test)]
            crate::db::query::plan::expr::BinaryOp::And => Self::And,
            #[cfg(test)]
            crate::db::query::plan::expr::BinaryOp::Eq => Self::Eq,
        }
    }
}

impl AggregateExprCacheKey {
    fn from_aggregate_expr(aggregate: &AggregateExpr) -> Self {
        Self {
            kind_tag: aggregate_kind_tag(aggregate.kind()),
            target_field: aggregate.target_field().map(str::to_owned),
            distinct: aggregate.is_distinct(),
        }
    }
}

impl GroupingCacheKey {
    fn from_grouped_intent<K>(grouped: &GroupedIntent<K>) -> Self {
        Self {
            group_fields: grouped
                .group
                .group_fields
                .iter()
                .map(GroupFieldCacheKey::from_field_slot)
                .collect(),
            aggregates: grouped
                .group
                .aggregates
                .iter()
                .map(GroupAggregateCacheKey::from_group_aggregate_spec)
                .collect(),
            having: grouped
                .having
                .as_ref()
                .map(GroupHavingCacheKey::from_having_spec),
            max_groups: grouped.group.execution.max_groups,
            max_group_bytes: grouped.group.execution.max_group_bytes,
        }
    }
}

impl GroupFieldCacheKey {
    fn from_field_slot(field: &crate::db::query::plan::FieldSlot) -> Self {
        Self {
            index: field.index,
            field: field.field.clone(),
        }
    }
}

impl GroupAggregateCacheKey {
    fn from_group_aggregate_spec(aggregate: &crate::db::query::plan::GroupAggregateSpec) -> Self {
        Self {
            kind_tag: aggregate_kind_tag(aggregate.kind),
            target_field: aggregate.target_field.clone(),
            distinct: aggregate.distinct,
        }
    }
}

impl GroupHavingCacheKey {
    fn from_having_spec(having: &GroupHavingSpec) -> Self {
        Self {
            clauses: having
                .clauses
                .iter()
                .map(GroupHavingClauseCacheKey::from_having_clause)
                .collect(),
        }
    }
}

impl GroupHavingClauseCacheKey {
    fn from_having_clause(clause: &crate::db::query::plan::GroupHavingClause) -> Self {
        Self {
            symbol: GroupHavingSymbolCacheKey::from_having_symbol(&clause.symbol),
            op: CompareOpCacheKey::from_compare_op(clause.op),
            value: ValueCacheKey::from_value(&clause.value),
        }
    }
}

impl GroupHavingSymbolCacheKey {
    fn from_having_symbol(symbol: &GroupHavingSymbol) -> Self {
        match symbol {
            GroupHavingSymbol::GroupField(field) => {
                Self::GroupField(GroupFieldCacheKey::from_field_slot(field))
            }
            GroupHavingSymbol::AggregateIndex(index) => Self::AggregateIndex(*index),
        }
    }
}

impl CompareOpCacheKey {
    const fn from_compare_op(op: CompareOp) -> Self {
        Self(op.tag())
    }
}

impl ConsistencyCacheKey {
    const fn from_missing_row_policy(policy: MissingRowPolicy) -> Self {
        match policy {
            MissingRowPolicy::Ignore => Self::Ignore,
            MissingRowPolicy::Error => Self::Error,
        }
    }
}

const fn aggregate_kind_tag(kind: crate::db::query::plan::AggregateKind) -> u8 {
    match kind {
        crate::db::query::plan::AggregateKind::Count => 0x01,
        crate::db::query::plan::AggregateKind::Sum => 0x02,
        crate::db::query::plan::AggregateKind::Avg => 0x03,
        crate::db::query::plan::AggregateKind::Exists => 0x04,
        crate::db::query::plan::AggregateKind::Min => 0x05,
        crate::db::query::plan::AggregateKind::Max => 0x06,
        crate::db::query::plan::AggregateKind::First => 0x07,
        crate::db::query::plan::AggregateKind::Last => 0x08,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            MissingRowPolicy,
            query::intent::{Query, StructuralQuery},
        },
        model::{entity::EntityModel, field::FieldKind},
        testing::PLAN_ENTITY_TAG,
        traits::{EntitySchema, Path},
        types::Ulid,
        value::Value,
    };
    use icydb_derive::FieldProjection;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
    struct CacheKeyEntity {
        id: Ulid,
        name: String,
    }

    struct CacheKeyCanister;

    impl Path for CacheKeyCanister {
        const PATH: &'static str = concat!(module_path!(), "::CacheKeyCanister");
    }

    impl crate::traits::CanisterKind for CacheKeyCanister {
        const COMMIT_MEMORY_ID: u8 = crate::testing::test_commit_memory_id();
    }

    struct CacheKeyStore;

    impl Path for CacheKeyStore {
        const PATH: &'static str = concat!(module_path!(), "::CacheKeyStore");
    }

    impl crate::traits::StoreKind for CacheKeyStore {
        type Canister = CacheKeyCanister;
    }

    crate::test_entity_schema! {
        ident = CacheKeyEntity,
        id = Ulid,
        id_field = id,
        entity_name = "CacheKeyEntity",
        entity_tag = PLAN_ENTITY_TAG,
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("name", FieldKind::Text),
        ],
        indexes = [],
        store = CacheKeyStore,
        canister = CacheKeyCanister,
    }

    fn basic_model() -> &'static EntityModel {
        <CacheKeyEntity as EntitySchema>::MODEL
    }

    #[test]
    fn structural_query_cache_key_matches_for_identical_scalar_queries() {
        let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore)
            .filter(crate::db::Predicate::eq(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            ))
            .order_by("name")
            .limit(2);
        let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore)
            .order_by("name")
            .filter(crate::db::Predicate::eq(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            ))
            .limit(2);

        assert_eq!(
            left.structural().structural_cache_key(),
            right.structural().structural_cache_key(),
            "equivalent scalar fluent queries must normalize onto one shared cache key",
        );
    }

    #[test]
    fn structural_query_cache_key_distinguishes_order_direction() {
        let asc = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore).order_by("name");
        let desc =
            StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore).order_by_desc("name");

        assert_ne!(
            asc.structural_cache_key(),
            desc.structural_cache_key(),
            "order direction must remain part of shared query cache identity",
        );
    }
}
