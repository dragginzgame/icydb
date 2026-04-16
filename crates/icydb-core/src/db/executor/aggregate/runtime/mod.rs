//! Module: executor::aggregate::runtime
//! Responsibility: aggregate-owned grouped runtime mechanics for read execution.
//! Does not own: grouped route derivation or shared executor contracts.
//! Boundary: grouped fold/distinct/having/output execution for grouped read paths.

mod grouped_distinct;
mod grouped_fold;
mod grouped_output;

use crate::{
    db::{
        predicate::{CompareOp, evaluate_grouped_having_compare},
        query::plan::{FieldSlot, GroupHavingClause, GroupHavingSpec, GroupHavingSymbol},
    },
    error::InternalError,
    value::Value,
};

#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use grouped_fold::{
    GroupedCountFoldMetrics, with_grouped_count_fold_metrics,
};
pub(in crate::db::executor) use grouped_fold::{
    build_grouped_stream_with_runtime, execute_group_fold_stage,
};
pub(in crate::db::executor) use grouped_output::{
    GroupedOutputRuntimeObserverBindings, finalize_grouped_output_with_observer,
    finalize_path_outcome_for_path,
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
    let Some(matches) = evaluate_grouped_having_compare(actual, op, expected) else {
        return Err(GroupHavingClause::unsupported_operator(op));
    };

    Ok(matches)
}
