//! Module: executor::aggregate::runtime::grouped_having
//! Responsibility: grouped HAVING clause evaluation over finalized grouped rows.
//! Does not own: grouped planning semantics or grouped fold execution.
//! Boundary: grouped HAVING runtime checks used by grouped read execution.

use crate::{
    db::{
        predicate::{CompareOp, evaluate_grouped_having_compare_v1},
        query::plan::{FieldSlot, GroupHavingClause, GroupHavingSpec, GroupHavingSymbol},
    },
    error::InternalError,
    value::Value,
};

// Evaluate grouped HAVING clauses on one finalized grouped output row.
pub(in crate::db::executor) fn group_matches_having(
    having: &GroupHavingSpec,
    group_fields: &[FieldSlot],
    group_key_value: &Value,
    aggregate_values: &[Value],
) -> Result<bool, InternalError> {
    for (index, clause) in having.clauses().iter().enumerate() {
        let actual = match clause.symbol() {
            GroupHavingSymbol::GroupField(field_slot) => {
                let group_key_list = match group_key_value {
                    Value::List(values) => values,
                    value => {
                        return Err(GroupHavingSymbol::grouped_key_must_be_list(value));
                    }
                };
                let Some(group_field_offset) = group_fields
                    .iter()
                    .position(|group_field| group_field.index() == field_slot.index())
                else {
                    return Err(GroupHavingSymbol::field_not_in_group_key_projection(
                        field_slot.field(),
                    ));
                };
                group_key_list.get(group_field_offset).ok_or_else(|| {
                    GroupHavingSymbol::group_key_offset_out_of_bounds(
                        index,
                        group_field_offset,
                        group_key_list.len(),
                    )
                })?
            }
            GroupHavingSymbol::AggregateIndex(aggregate_index) => {
                aggregate_values.get(*aggregate_index).ok_or_else(|| {
                    GroupHavingSymbol::aggregate_index_out_of_bounds(
                        index,
                        *aggregate_index,
                        aggregate_values.len(),
                    )
                })?
            }
        };

        if !having_compare_values(actual, clause.op(), clause.value())? {
            return Ok(false);
        }
    }

    Ok(true)
}

// Evaluate one grouped HAVING compare operator using strict value semantics.
fn having_compare_values(
    actual: &Value,
    op: CompareOp,
    expected: &Value,
) -> Result<bool, InternalError> {
    let Some(matches) = evaluate_grouped_having_compare_v1(actual, op, expected) else {
        return Err(GroupHavingClause::unsupported_operator(op));
    };

    Ok(matches)
}
