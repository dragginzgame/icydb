use crate::db::sql::lowering::{
    SqlLoweringError,
    aggregate::{grouped_projection_aggregate_calls, resolve_having_aggregate_index},
};
use crate::{
    db::{
        predicate::CompareOp,
        sql::parser::{SqlAggregateCall, SqlHavingClause, SqlHavingSymbol, SqlProjection},
    },
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
pub(super) enum ResolvedHavingClause {
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

pub(super) fn lower_having_clauses(
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
