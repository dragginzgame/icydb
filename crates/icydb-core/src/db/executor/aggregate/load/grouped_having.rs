//! Module: executor::aggregate::load::grouped_having
//! Responsibility: grouped HAVING clause evaluation over finalized grouped rows.
//! Does not own: grouped planning semantics or grouped fold execution.
//! Boundary: grouped HAVING runtime checks used by grouped read execution.

use crate::{
    db::{
        executor::shared::load_contracts::LoadExecutor,
        predicate::{CompareOp, evaluate_grouped_having_compare_v1},
        query::plan::{FieldSlot, GroupHavingSpec, GroupHavingSymbol},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Evaluate grouped HAVING clauses on one finalized grouped output row.
    pub(super) fn group_matches_having(
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
                            return Err(crate::db::error::query_executor_invariant(format!(
                                "grouped HAVING requires list-shaped grouped keys, found {value:?}"
                            )));
                        }
                    };
                    let Some(group_field_offset) = group_fields
                        .iter()
                        .position(|group_field| group_field.index() == field_slot.index())
                    else {
                        return Err(crate::db::error::query_executor_invariant(format!(
                            "grouped HAVING field is not in grouped key projection: field='{}'",
                            field_slot.field()
                        )));
                    };
                    group_key_list.get(group_field_offset).ok_or_else(|| {
                        crate::db::error::query_executor_invariant(format!(
                            "grouped HAVING group key offset out of bounds: clause_index={index}, offset={group_field_offset}, key_len={}",
                            group_key_list.len()
                        ))
                    })?
                }
                GroupHavingSymbol::AggregateIndex(aggregate_index) => {
                    aggregate_values.get(*aggregate_index).ok_or_else(|| {
                        crate::db::error::query_executor_invariant(format!(
                            "grouped HAVING aggregate index out of bounds: clause_index={index}, aggregate_index={aggregate_index}, aggregate_count={}",
                            aggregate_values.len()
                        ))
                    })?
                }
            };

            if !Self::having_compare_values(actual, clause.op(), clause.value())? {
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
            return Err(crate::db::error::query_executor_invariant(format!(
                "unsupported grouped HAVING operator reached executor: {op:?}",
            )));
        };

        Ok(matches)
    }
}
