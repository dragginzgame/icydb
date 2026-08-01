//! Module: query::intent::cache_key
//! Responsibility: canonical shared-cache identity normalization for structural queries.
//! Does not own: planner validation, executor runtime behavior, or SQL surface routing.
//! Boundary: turns semantic query intent into one explicit derived-hash cache key.

use crate::{
    db::{
        predicate::MissingRowPolicy,
        query::{
            builder::{
                aggregate::AggregateExpr,
                scalar_projection::render_scalar_projection_expr_plan_label,
            },
            intent::{model::QueryModel, state::GroupedIntent},
            plan::{
                AggregateIdentity, OrderDirection, OrderSpec, QueryMode,
                expr::{Expr, Function, ProjectionField, ProjectionSelection},
            },
        },
    },
    error::InternalError,
    value::{Value, hash_value},
};

///
/// StructuralQueryCacheKey
///
/// Canonical semantic identity for the shared structural query-plan cache.
/// This key is intentionally explicit: normalization owns semantic equivalence,
/// while `Hash` ownership stays mechanical at the map boundary.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct StructuralQueryCacheKey {
    mode: QueryModeCacheKey,
    predicate: Option<[u8; 32]>,
    filter_expr: Option<ProjectionExprCacheKey>,
    order: Option<Vec<OrderFieldCacheKey>>,
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

// Value identity uses the existing canonical value hash while preserving one
// stable fallback when some nested structured value cannot hash cleanly.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ValueCacheKey {
    Canonical([u8; 16]),
    HashError(DiagnosticCacheKey),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DiagnosticCacheKey {
    code: u16,
    origin: u16,
}

impl DiagnosticCacheKey {
    fn from_internal_error(err: &InternalError) -> Self {
        let diagnostic = err.diagnostic();

        Self {
            code: diagnostic.error_code().raw(),
            origin: diagnostic.origin() as u16,
        }
    }
}

///
/// OrderFieldCacheKey
///
/// Canonical representation of one `ORDER BY` field in the structural query
/// cache key.
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
    FieldPath {
        root: String,
        segments: Vec<String>,
    },
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
    Aggregate(AggregateCacheKey),
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
/// AggregateCacheKey
///
/// Canonical aggregate identity shared by projected and grouped aggregate
/// cache entries. It records only the semantic pieces that affect planner
/// reuse.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AggregateCacheKey {
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
/// This is a canonicalized grouped structural/cache identity surface;
/// prepared/template identity remains outside this key and stays syntax-bound.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupingCacheKey {
    group_fields: Vec<GroupFieldCacheKey>,
    aggregates: Vec<AggregateCacheKey>,
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ConsistencyCacheKey {
    Ignore,
    Error,
}

impl StructuralQueryCacheKey {
    pub(in crate::db::query) fn from_query_model_with_normalized_predicate_fingerprint(
        model: &QueryModel,
        predicate_fingerprint: Option<[u8; 32]>,
    ) -> Self {
        Self::from_query_model_with_optional_predicate_key(model, predicate_fingerprint)
    }

    // Build the shared structural cache key from one optional predicate-key
    // fragment so callers that already computed canonical predicate identity
    // do not walk the same normalized tree twice.
    fn from_query_model_with_optional_predicate_key(
        model: &QueryModel,
        predicate: Option<[u8; 32]>,
    ) -> Self {
        let scalar = model.scalar_intent_for_cache_key();
        let filter_expr = scalar.filter.as_ref().and_then(|filter| {
            filter
                .logical_filter_expr()
                .map(ProjectionExprCacheKey::from_expr)
        });
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
            order: scalar
                .order
                .as_ref()
                .map(OrderFieldCacheKey::from_order_spec),
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

impl ValueCacheKey {
    fn from_value(value: &Value) -> Self {
        match hash_value(value) {
            Ok(digest) => Self::Canonical(digest),
            Err(err) => Self::HashError(DiagnosticCacheKey::from_internal_error(&err)),
        }
    }
}

impl OrderFieldCacheKey {
    fn from_order_spec(order: &OrderSpec) -> Vec<Self> {
        order
            .fields
            .iter()
            .map(|term| Self {
                field: term.rendered_label(),
                direction: OrderDirectionCacheKey::from_order_direction(term.direction()),
            })
            .collect()
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
            Expr::FieldPath(path) => Self::FieldPath {
                root: path.root().as_str().to_string(),
                segments: path.segments().to_vec(),
            },
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
                Self::Aggregate(AggregateCacheKey::from_aggregate_expr(aggregate))
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

impl AggregateCacheKey {
    fn from_aggregate_expr(aggregate: &AggregateExpr) -> Self {
        Self::from_identity(
            AggregateIdentity::from_aggregate_expr(aggregate),
            aggregate.target_field(),
            aggregate.filter_expr(),
        )
    }

    fn from_group_aggregate_spec(aggregate: &crate::db::query::plan::GroupAggregateSpec) -> Self {
        Self::from_identity(
            aggregate.identity(),
            aggregate.target_field(),
            aggregate.filter_expr(),
        )
    }

    fn from_identity(
        identity: AggregateIdentity,
        target_field: Option<&str>,
        filter_expr: Option<&crate::db::query::plan::expr::Expr>,
    ) -> Self {
        Self {
            kind_tag: identity.kind().fingerprint_tag(),
            target_field: target_field.map(str::to_owned),
            input_expr: identity
                .input_expr()
                .map(render_scalar_projection_expr_plan_label),
            filter_expr: filter_expr.map(render_scalar_projection_expr_plan_label),
            distinct: identity.distinct(),
        }
    }
}

impl GroupingCacheKey {
    fn from_grouped_intent(grouped: &GroupedIntent) -> Self {
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
                .map(AggregateCacheKey::from_group_aggregate_spec)
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

impl ConsistencyCacheKey {
    const fn from_missing_row_policy(policy: MissingRowPolicy) -> Self {
        match policy {
            MissingRowPolicy::Ignore => Self::Ignore,
            MissingRowPolicy::Error => Self::Error,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::query::{
            builder::aggregate,
            plan::{GroupAggregateSpec, expr::Expr},
        },
        value::Value,
    };

    use super::AggregateCacheKey;

    #[test]
    fn scalar_and_grouped_aggregate_cache_identity_stays_shared() {
        let aggregate = aggregate::sum("amount")
            .with_filter_expr(Expr::Literal(Value::Bool(true)))
            .distinct();
        let grouped = GroupAggregateSpec::from_aggregate_expr(&aggregate);

        assert_eq!(
            AggregateCacheKey::from_aggregate_expr(&aggregate),
            AggregateCacheKey::from_group_aggregate_spec(&grouped),
        );

        let different_filter = aggregate::sum("amount")
            .with_filter_expr(Expr::Literal(Value::Bool(false)))
            .distinct();
        assert_ne!(
            AggregateCacheKey::from_aggregate_expr(&aggregate),
            AggregateCacheKey::from_aggregate_expr(&different_filter),
        );
        assert_ne!(
            AggregateCacheKey::from_aggregate_expr(&aggregate),
            AggregateCacheKey::from_aggregate_expr(&aggregate::sum("other_amount").distinct()),
        );
        assert_ne!(
            AggregateCacheKey::from_aggregate_expr(&aggregate),
            AggregateCacheKey::from_aggregate_expr(&aggregate::sum("amount")),
        );
    }
}
