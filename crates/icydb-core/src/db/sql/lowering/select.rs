use crate::db::sql::lowering::{
    SqlLoweringError,
    aggregate::{
        grouped_projection_aggregate_calls, lower_aggregate_call, resolve_having_aggregate_index,
    },
};
use crate::{
    db::{
        predicate::{CoercionId, CompareOp, MissingRowPolicy, Predicate},
        query::intent::{Query, StructuralQuery},
        sql::parser::{
            SqlAggregateCall, SqlDeleteStatement, SqlHavingClause, SqlHavingSymbol,
            SqlOrderDirection, SqlOrderTerm, SqlProjection, SqlSelectItem, SqlSelectStatement,
        },
    },
    model::{entity::EntityModel, field::FieldKind},
    traits::EntityKind,
    value::Value,
};

///
/// ResolvedHavingClause
///
/// Pre-resolved HAVING clause shape after SQL projection aggregate index
/// resolution. This keeps SQL shape analysis entity-agnostic before typed
/// query binding.
///
#[derive(Clone, Debug)]
enum ResolvedHavingClause {
    GroupField {
        field: String,
        op: CompareOp,
        value: Value,
    },
    Aggregate {
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    },
}

///
/// LoweredSelectShape
///
/// Entity-agnostic lowered SQL SELECT shape prepared for typed `Query<E>`
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredSelectShape {
    scalar_projection_fields: Option<Vec<String>>,
    grouped_projection_aggregates: Vec<SqlAggregateCall>,
    group_by_fields: Vec<String>,
    distinct: bool,
    having: Vec<ResolvedHavingClause>,
    predicate: Option<Predicate>,
    order_by: Vec<SqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl LoweredSelectShape {
    // Report whether this lowered select shape carries grouped execution state.
    pub(in crate::db::sql::lowering) const fn has_grouping(&self) -> bool {
        !self.group_by_fields.is_empty()
    }
}

///
/// LoweredBaseQueryShape
///
/// Generic-free filter/order/window query modifiers shared by delete and
/// global-aggregate SQL lowering.
/// This keeps common SQL query-shape lowering shared before typed query
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredBaseQueryShape {
    pub(in crate::db::sql::lowering) predicate: Option<Predicate>,
    pub(in crate::db::sql::lowering) order_by: Vec<SqlOrderTerm>,
    pub(in crate::db::sql::lowering) limit: Option<u32>,
    pub(in crate::db::sql::lowering) offset: Option<u32>,
}

#[inline(never)]
pub(in crate::db::sql::lowering) fn lower_select_shape(
    statement: SqlSelectStatement,
) -> Result<LoweredSelectShape, SqlLoweringError> {
    let SqlSelectStatement {
        projection,
        predicate,
        distinct,
        group_by,
        having,
        order_by,
        limit,
        offset,
        entity: _,
    } = statement;
    let projection_for_having = projection.clone();

    // Phase 1: resolve scalar/grouped projection shape.
    let (scalar_projection_fields, grouped_projection_aggregates) = if group_by.is_empty() {
        let scalar_projection_fields = lower_scalar_projection_fields(projection, distinct)?;
        (scalar_projection_fields, Vec::new())
    } else {
        if distinct {
            return Err(SqlLoweringError::unsupported_select_distinct());
        }
        let grouped_projection_aggregates =
            grouped_projection_aggregate_calls(&projection, group_by.as_slice())?;
        (None, grouped_projection_aggregates)
    };

    // Phase 2: resolve HAVING symbols against grouped projection authority.
    let having = lower_having_clauses(
        having,
        &projection_for_having,
        group_by.as_slice(),
        grouped_projection_aggregates.as_slice(),
    )?;

    Ok(LoweredSelectShape {
        scalar_projection_fields,
        grouped_projection_aggregates,
        group_by_fields: group_by,
        distinct,
        having,
        predicate,
        order_by,
        limit,
        offset,
    })
}

fn lower_scalar_projection_fields(
    projection: SqlProjection,
    distinct: bool,
) -> Result<Option<Vec<String>>, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        if distinct {
            return Ok(None);
        }

        return Ok(None);
    };

    let has_aggregate = items
        .iter()
        .any(|item| matches!(item, SqlSelectItem::Aggregate(_)));
    if has_aggregate {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    let fields = items
        .into_iter()
        .map(|item| match item {
            SqlSelectItem::Field(field) => Ok(field),
            SqlSelectItem::Aggregate(_) | SqlSelectItem::TextFunction(_) => {
                Err(SqlLoweringError::unsupported_select_projection())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    if distinct && fields.is_empty() {
        return Ok(Some(fields));
    }

    Ok(Some(fields))
}

fn lower_having_clauses(
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    group_by_fields: &[String],
    grouped_projection_aggregates: &[SqlAggregateCall],
) -> Result<Vec<ResolvedHavingClause>, SqlLoweringError> {
    if having_clauses.is_empty() {
        return Ok(Vec::new());
    }
    if group_by_fields.is_empty() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    let projection_aggregates = grouped_projection_aggregate_calls(projection, group_by_fields)
        .map_err(|_| SqlLoweringError::unsupported_select_having())?;
    if projection_aggregates.as_slice() != grouped_projection_aggregates {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    let mut lowered = Vec::with_capacity(having_clauses.len());
    for clause in having_clauses {
        match clause.symbol {
            SqlHavingSymbol::Field(field) => lowered.push(ResolvedHavingClause::GroupField {
                field,
                op: clause.op,
                value: clause.value,
            }),
            SqlHavingSymbol::Aggregate(aggregate) => {
                let aggregate_index =
                    resolve_having_aggregate_index(&aggregate, grouped_projection_aggregates)?;
                lowered.push(ResolvedHavingClause::Aggregate {
                    aggregate_index,
                    op: clause.op,
                    value: clause.value,
                });
            }
        }
    }

    Ok(lowered)
}

// Canonicalize strict numeric SQL predicate literals onto the resolved model
// field kind so unsigned-width fields keep strict/indexable semantics even
// though reduced SQL integer tokens parse through one generic numeric value
// variant first.
fn canonicalize_sql_predicate_for_model(
    model: &'static EntityModel,
    predicate: Predicate,
) -> Predicate {
    match predicate {
        Predicate::And(children) => Predicate::And(
            children
                .into_iter()
                .map(|child| canonicalize_sql_predicate_for_model(model, child))
                .collect(),
        ),
        Predicate::Or(children) => Predicate::Or(
            children
                .into_iter()
                .map(|child| canonicalize_sql_predicate_for_model(model, child))
                .collect(),
        ),
        Predicate::Not(inner) => Predicate::Not(Box::new(canonicalize_sql_predicate_for_model(
            model, *inner,
        ))),
        Predicate::Compare(mut cmp) => {
            canonicalize_sql_compare_for_model(model, &mut cmp);
            Predicate::Compare(cmp)
        }
        Predicate::True
        | Predicate::False
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => predicate,
    }
}

// Resolve one lowered predicate field onto the runtime model kind that owns
// its strict literal compatibility rules.
fn model_field_kind(model: &'static EntityModel, field: &str) -> Option<FieldKind> {
    model
        .fields()
        .iter()
        .find(|candidate| candidate.name() == field)
        .map(crate::model::field::FieldModel::kind)
}

// Keep SQL-only literal widening narrow:
// - only strict equality-style numeric predicates are eligible
// - ordering already uses `NumericWiden`
// - text and expression-wrapped predicates stay untouched
fn canonicalize_sql_compare_for_model(
    model: &'static EntityModel,
    cmp: &mut crate::db::predicate::ComparePredicate,
) {
    if cmp.coercion.id != CoercionId::Strict {
        return;
    }

    let Some(field_kind) = model_field_kind(model, &cmp.field) else {
        return;
    };

    match cmp.op {
        CompareOp::Eq | CompareOp::Ne => {
            if let Some(value) =
                canonicalize_strict_sql_numeric_value_for_kind(&field_kind, &cmp.value)
            {
                cmp.value = value;
            }
        }
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(items) = &cmp.value else {
                return;
            };

            let items = items
                .iter()
                .map(|item| {
                    canonicalize_strict_sql_numeric_value_for_kind(&field_kind, item)
                        .unwrap_or_else(|| item.clone())
                })
                .collect();
            cmp.value = Value::List(items);
        }
        CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => {}
    }
}

// Convert one parsed SQL numeric literal into the exact runtime `Value` variant
// required by the field kind when that conversion is lossless and unambiguous.
// This preserves strict equality semantics while still letting SQL express
// unsigned-width comparisons such as `Nat16`/`u64` fields.
fn canonicalize_strict_sql_numeric_value_for_kind(
    kind: &FieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_strict_sql_numeric_value_for_kind(key_kind, value)
        }
        FieldKind::Int => match value {
            Value::Int(inner) => Some(Value::Int(*inner)),
            Value::Uint(inner) => i64::try_from(*inner).ok().map(Value::Int),
            _ => None,
        },
        FieldKind::Uint => match value {
            Value::Int(inner) => u64::try_from(*inner).ok().map(Value::Uint),
            Value::Uint(inner) => Some(Value::Uint(*inner)),
            _ => None,
        },
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Enum { .. }
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => None,
    }
}

#[inline(never)]
pub(in crate::db) fn apply_lowered_select_shape(
    mut query: StructuralQuery,
    lowered: LoweredSelectShape,
) -> Result<StructuralQuery, SqlLoweringError> {
    let LoweredSelectShape {
        scalar_projection_fields,
        grouped_projection_aggregates,
        group_by_fields,
        distinct,
        having,
        predicate,
        order_by,
        limit,
        offset,
    } = lowered;
    let model = query.model();

    // Phase 1: apply grouped declaration semantics.
    for field in group_by_fields {
        query = query.group_by(field)?;
    }

    // Phase 2: apply scalar DISTINCT and projection contracts.
    if distinct {
        query = query.distinct();
    }
    if let Some(fields) = scalar_projection_fields {
        query = query.select_fields(fields);
    }
    for aggregate in grouped_projection_aggregates {
        query = query.aggregate(lower_aggregate_call(aggregate)?);
    }

    // Phase 3: bind resolved HAVING clauses against grouped terminals.
    for clause in having {
        match clause {
            ResolvedHavingClause::GroupField { field, op, value } => {
                let value = model_field_kind(model, &field)
                    .and_then(|field_kind| {
                        canonicalize_strict_sql_numeric_value_for_kind(&field_kind, &value)
                    })
                    .unwrap_or(value);
                query = query.having_group(field, op, value)?;
            }
            ResolvedHavingClause::Aggregate {
                aggregate_index,
                op,
                value,
            } => {
                query = query.having_aggregate(aggregate_index, op, value)?;
            }
        }
    }

    // Phase 4: attach the shared filter/order/page tail through the base-query lane.
    Ok(apply_lowered_base_query_shape(
        query,
        LoweredBaseQueryShape {
            predicate: predicate
                .map(|predicate| canonicalize_sql_predicate_for_model(model, predicate)),
            order_by,
            limit,
            offset,
        },
    ))
}

pub(in crate::db::sql::lowering) fn apply_lowered_base_query_shape(
    mut query: StructuralQuery,
    lowered: LoweredBaseQueryShape,
) -> StructuralQuery {
    if let Some(predicate) = lowered.predicate {
        query = query.filter(predicate);
    }
    query = apply_order_terms_structural(query, lowered.order_by);
    if let Some(limit) = lowered.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = lowered.offset {
        query = query.offset(offset);
    }

    query
}

pub(in crate::db) fn bind_lowered_sql_query_structural(
    model: &'static EntityModel,
    lowered: crate::db::sql::lowering::LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<StructuralQuery, SqlLoweringError> {
    match lowered {
        crate::db::sql::lowering::LoweredSqlQuery::Select(select) => {
            bind_lowered_sql_select_query_structural(model, select, consistency)
        }
        crate::db::sql::lowering::LoweredSqlQuery::Delete(delete) => Ok(
            bind_lowered_sql_delete_query_structural(model, delete, consistency),
        ),
    }
}

/// Bind one lowered SQL SELECT shape onto the structural query surface.
///
/// This keeps the field-only SQL read lane narrow and owner-local: any caller
/// that already resolved entity authority can reuse the same lowered-SELECT to
/// structural-query boundary without reopening SQL shape application itself.
pub(in crate::db) fn bind_lowered_sql_select_query_structural(
    model: &'static EntityModel,
    select: LoweredSelectShape,
    consistency: MissingRowPolicy,
) -> Result<StructuralQuery, SqlLoweringError> {
    apply_lowered_select_shape(StructuralQuery::new(model, consistency), select)
}

pub(in crate::db) fn bind_lowered_sql_delete_query_structural(
    model: &'static EntityModel,
    delete: LoweredBaseQueryShape,
    consistency: MissingRowPolicy,
) -> StructuralQuery {
    apply_lowered_base_query_shape(StructuralQuery::new(model, consistency).delete(), delete)
}

pub(in crate::db) fn bind_lowered_sql_query<E: EntityKind>(
    lowered: crate::db::sql::lowering::LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    let structural = bind_lowered_sql_query_structural(E::MODEL, lowered, consistency)?;

    Ok(Query::from_inner(structural))
}

pub(in crate::db::sql::lowering) fn lower_delete_shape(
    statement: SqlDeleteStatement,
) -> LoweredBaseQueryShape {
    let SqlDeleteStatement {
        predicate,
        order_by,
        limit,
        offset,
        entity: _,
    } = statement;

    LoweredBaseQueryShape {
        predicate,
        order_by,
        limit,
        offset,
    }
}

fn apply_order_terms_structural(
    mut query: StructuralQuery,
    order_by: Vec<SqlOrderTerm>,
) -> StructuralQuery {
    for term in order_by {
        query = match term.direction {
            SqlOrderDirection::Asc => query.order_by(term.field),
            SqlOrderDirection::Desc => query.order_by_desc(term.field),
        };
    }

    query
}
