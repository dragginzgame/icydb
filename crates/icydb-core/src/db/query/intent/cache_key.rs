//! Module: query::intent::cache_key
//! Responsibility: canonical shared-cache identity normalization for structural queries.
//! Does not own: planner validation, executor runtime behavior, or SQL surface routing.
//! Boundary: turns semantic query intent into one explicit derived-hash cache key.

#[cfg(test)]
use crate::db::predicate::Predicate;
#[cfg(test)]
use crate::db::predicate::predicate_fingerprint;
use crate::{
    db::{
        access::{
            AccessPlan,
            dispatch::{AccessPathDispatch, AccessPlanDispatch, dispatch_access_plan},
        },
        predicate::MissingRowPolicy,
        query::{
            builder::{
                aggregate::AggregateExpr,
                scalar_projection::render_scalar_projection_expr_sql_label,
            },
            intent::{build_access_plan_from_keys, model::QueryModel, state::GroupedIntent},
            plan::{
                OrderDirection, OrderSpec, QueryMode,
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
    filter_expr: Option<ProjectionExprCacheKey>,
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

///
/// PredicateCacheKey
///
/// Predicate identity stays anchored to the predicate subsystem's canonical
/// structural hash so the shared cache does not grow its own predicate walker.
///

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

///
/// IndexRangeCacheKey
///
/// Canonical identity for one normalized index-range access path.
/// This exists so the shared query cache can distinguish different index slots,
/// prefix bindings, and resume bounds after fluent key access is lowered.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IndexRangeCacheKey {
    index: String,
    field_slots: Vec<usize>,
    prefix_values: Vec<ValueCacheKey>,
    lower: RangeBoundCacheKey,
    upper: RangeBoundCacheKey,
}

///
/// RangeBoundCacheKey
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum RangeBoundCacheKey {
    Unbounded,
    Included(ValueCacheKey),
    Excluded(ValueCacheKey),
}

///
/// OrderCacheKey
///
/// Canonical ordering segment for the structural query cache key.
/// The cache stores ordering separately so planner reuse only happens when the
/// normalized sort contract matches field-for-field.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OrderCacheKey {
    fields: Vec<OrderFieldCacheKey>,
}

///
/// OrderFieldCacheKey
///
/// Canonical representation of one `ORDER BY` field inside `OrderCacheKey`.
/// This wrapper keeps the field name and normalized direction explicit so cache
/// hits do not accidentally cross different sort layouts.
///

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
    Unary {
        op: UnaryOpCacheKey,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Vec<CaseWhenArmCacheKey>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOpCacheKey,
        left: Box<Self>,
        right: Box<Self>,
    },
    Aggregate(AggregateExprCacheKey),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CaseWhenArmCacheKey {
    condition: ProjectionExprCacheKey,
    result: ProjectionExprCacheKey,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BinaryOpCacheKey {
    Or,
    And,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum UnaryOpCacheKey {
    Not,
}

///
/// AggregateExprCacheKey
///
/// Canonical aggregate-expression identity for projected aggregate results.
/// It records only the semantic pieces that affect planner reuse: aggregate
/// kind, optional target field, and whether the aggregate is distinct.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AggregateExprCacheKey {
    kind_tag: u8,
    target_field: Option<String>,
    input_expr: Option<String>,
    filter_expr: Option<String>,
    distinct: bool,
}

///
/// GroupingCacheKey
///
/// Canonical identity for the grouped-query portion of a structural cache key.
/// This captures grouping fields, aggregate slots, grouped `HAVING`
/// expressions, and the configured grouping limits so grouped plans only reuse
/// compatible shapes.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupingCacheKey {
    group_fields: Vec<GroupFieldCacheKey>,
    aggregates: Vec<GroupAggregateCacheKey>,
    having_expr: Option<ProjectionExprCacheKey>,
    max_groups: u64,
    max_group_bytes: u64,
}

///
/// GroupFieldCacheKey
///
/// Canonical reference to one grouped field inside `GroupingCacheKey`.
/// The index is preserved alongside the field name because later grouped
/// projections and `HAVING` symbols refer back to aggregate/group slot order.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupFieldCacheKey {
    index: usize,
    field: String,
}

///
/// GroupAggregateCacheKey
///
/// Canonical identity for one aggregate slot inside grouped intent.
/// Grouped planning uses this wrapper to preserve aggregate order and semantics
/// without re-embedding full aggregate expressions into the parent key.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupAggregateCacheKey {
    kind_tag: u8,
    target_field: Option<String>,
    input_expr: Option<String>,
    filter_expr: Option<String>,
    distinct: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ConsistencyCacheKey {
    Ignore,
    Error,
}

impl StructuralQueryCacheKey {
    #[cfg(test)]
    pub(in crate::db) fn from_query_model<K: FieldValue>(model: &QueryModel<'_, K>) -> Self {
        Self::from_query_model_with_predicate(
            model,
            model.scalar_intent_for_cache_key().predicate.as_ref(),
        )
    }

    #[cfg(test)]
    pub(in crate::db) fn from_query_model_with_predicate<K: FieldValue>(
        model: &QueryModel<'_, K>,
        predicate: Option<&Predicate>,
    ) -> Self {
        Self::from_query_model_with_optional_predicate_key(
            model,
            predicate.map(PredicateCacheKey::from_predicate),
        )
    }

    pub(in crate::db) fn from_query_model_with_normalized_predicate_fingerprint<K: FieldValue>(
        model: &QueryModel<'_, K>,
        predicate_fingerprint: Option<[u8; 32]>,
    ) -> Self {
        Self::from_query_model_with_optional_predicate_key(
            model,
            predicate_fingerprint.map(PredicateCacheKey::from_fingerprint),
        )
    }

    // Build the shared structural cache key from one optional predicate-key
    // fragment so callers that already computed canonical predicate identity
    // do not walk the same normalized tree twice.
    fn from_query_model_with_optional_predicate_key<K: FieldValue>(
        model: &QueryModel<'_, K>,
        predicate: Option<PredicateCacheKey>,
    ) -> Self {
        let scalar = model.scalar_intent_for_cache_key();
        let filter_expr = scalar
            .filter_expr
            .as_ref()
            .map(ProjectionExprCacheKey::from_expr);
        let key_access = scalar
            .key_access
            .as_ref()
            .map(|state| build_access_plan_from_keys(&state.access));

        Self {
            mode: QueryModeCacheKey::from_query_mode(model.mode()),
            // Canonical scalar `filter_expr` owns semantic filter identity when
            // present. The derived predicate key remains only for plans that
            // still have no planner-owned semantic filter expression.
            predicate: if filter_expr.is_some() {
                None
            } else {
                predicate
            },
            filter_expr,
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
    #[cfg(test)]
    fn from_predicate(predicate: &Predicate) -> Self {
        Self::Canonical(predicate_fingerprint(predicate))
    }

    const fn from_fingerprint(fingerprint: [u8; 32]) -> Self {
        Self::Canonical(fingerprint)
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
                .map(|term| OrderFieldCacheKey {
                    field: term.rendered_label(),
                    direction: OrderDirectionCacheKey::from_order_direction(term.direction()),
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
            Expr::Unary { op, expr } => Self::Unary {
                op: UnaryOpCacheKey::from_unary_op(*op),
                expr: Box::new(Self::from_expr(expr.as_ref())),
            },
            Expr::Case {
                when_then_arms,
                else_expr,
            } => Self::Case {
                when_then_arms: when_then_arms
                    .iter()
                    .map(CaseWhenArmCacheKey::from_arm)
                    .collect(),
                else_expr: Box::new(Self::from_expr(else_expr.as_ref())),
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
        }
    }
}

impl BinaryOpCacheKey {
    const fn from_binary_op(op: crate::db::query::plan::expr::BinaryOp) -> Self {
        match op {
            crate::db::query::plan::expr::BinaryOp::Or => Self::Or,
            crate::db::query::plan::expr::BinaryOp::And => Self::And,
            crate::db::query::plan::expr::BinaryOp::Eq => Self::Eq,
            crate::db::query::plan::expr::BinaryOp::Ne => Self::Ne,
            crate::db::query::plan::expr::BinaryOp::Lt => Self::Lt,
            crate::db::query::plan::expr::BinaryOp::Lte => Self::Lte,
            crate::db::query::plan::expr::BinaryOp::Gt => Self::Gt,
            crate::db::query::plan::expr::BinaryOp::Gte => Self::Gte,
            crate::db::query::plan::expr::BinaryOp::Add => Self::Add,
            crate::db::query::plan::expr::BinaryOp::Sub => Self::Sub,
            crate::db::query::plan::expr::BinaryOp::Mul => Self::Mul,
            crate::db::query::plan::expr::BinaryOp::Div => Self::Div,
        }
    }
}

impl UnaryOpCacheKey {
    const fn from_unary_op(op: crate::db::query::plan::expr::UnaryOp) -> Self {
        match op {
            crate::db::query::plan::expr::UnaryOp::Not => Self::Not,
        }
    }
}

impl CaseWhenArmCacheKey {
    fn from_arm(arm: &crate::db::query::plan::expr::CaseWhenArm) -> Self {
        Self {
            condition: ProjectionExprCacheKey::from_expr(arm.condition()),
            result: ProjectionExprCacheKey::from_expr(arm.result()),
        }
    }
}

impl AggregateExprCacheKey {
    fn from_aggregate_expr(aggregate: &AggregateExpr) -> Self {
        Self {
            kind_tag: aggregate_kind_tag(aggregate.kind()),
            target_field: aggregate.target_field().map(str::to_owned),
            input_expr: aggregate
                .input_expr()
                .map(render_scalar_projection_expr_sql_label),
            filter_expr: aggregate
                .filter_expr()
                .map(render_scalar_projection_expr_sql_label),
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
            having_expr: grouped
                .having_expr
                .as_ref()
                .map(ProjectionExprCacheKey::from_expr),
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
            target_field: aggregate.target_field().map(str::to_owned),
            input_expr: aggregate
                .input_expr()
                .map(render_scalar_projection_expr_sql_label),
            filter_expr: aggregate
                .filter_expr()
                .map(render_scalar_projection_expr_sql_label),
            distinct: aggregate.distinct,
        }
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
