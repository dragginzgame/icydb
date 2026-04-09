//! Module: db::executor::aggregate::runtime::grouped_fold::ingest
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::ingest.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::{cmp::Ordering, collections::HashMap};

use crate::{
    db::{
        contracts::canonical_value_compare,
        data::DataKey,
        executor::{
            KeyStreamLoopControl,
            aggregate::{ExecutionContext, FoldControl, GroupError, GroupedAggregateEngine},
            group::{GroupKey, StableHash},
            pipeline::contracts::{GroupedRouteStage, GroupedStreamStage, RowView},
        },
        query::plan::FieldSlot,
    },
    error::InternalError,
    value::Value,
};

///
/// ShortCircuitGroupSet
///
/// Bucketed short-circuit group tracker for one grouped aggregate engine.
/// Completed groups are indexed by stable grouped hash so the ingest hot loop
/// only scans same-hash canonical candidates instead of linearly walking every
/// completed grouped key for every incoming row.
///

pub(super) struct ShortCircuitGroupSet {
    groups_by_hash: HashMap<StableHash, Vec<Value>>,
    group_count: usize,
}

impl ShortCircuitGroupSet {
    pub(super) fn new() -> Self {
        Self {
            groups_by_hash: HashMap::new(),
            group_count: 0,
        }
    }

    fn contains_row(
        &self,
        row_view: &RowView,
        group_fields: &[FieldSlot],
        borrowed_group_hash: Option<StableHash>,
        owned_group_key: &mut Option<GroupKey>,
    ) -> Result<bool, InternalError> {
        if let Some(borrowed_group_hash) = borrowed_group_hash {
            let Some(bucket) = self.groups_by_hash.get(&borrowed_group_hash) else {
                return Ok(false);
            };
            for done_group_key in bucket {
                if super::canonical_group_value_matches_row_view(
                    done_group_key,
                    row_view,
                    group_fields,
                )? {
                    return Ok(true);
                }
            }

            return Ok(false);
        }

        let group_key = if let Some(group_key) = owned_group_key {
            group_key
        } else {
            owned_group_key.insert(super::materialize_group_key_from_row_view(
                row_view,
                group_fields,
            )?)
        };

        Ok(self.contains_group_key(group_key))
    }

    fn insert(&mut self, group_key: &GroupKey) {
        self.groups_by_hash
            .entry(group_key.hash())
            .or_default()
            .push(group_key.canonical_value().clone());
        self.group_count = self.group_count.saturating_add(1);
    }

    const fn len(&self) -> usize {
        self.group_count
    }

    fn contains_group_key(&self, group_key: &GroupKey) -> bool {
        let Some(bucket) = self.groups_by_hash.get(&group_key.hash()) else {
            return false;
        };
        for done_group_key in bucket {
            if canonical_value_compare(done_group_key, group_key.canonical_value())
                == Ordering::Equal
            {
                return true;
            }
        }

        false
    }
}

// Ingest grouped source rows into aggregate reducers while preserving budget contracts.
pub(super) fn fold_group_rows_into_engines(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage<'_>,
    grouped_execution_context: &mut ExecutionContext,
    grouped_engines: &mut [Box<dyn GroupedAggregateEngine>],
    short_circuit_keys: &mut [ShortCircuitGroupSet],
    max_groups_bound: usize,
) -> Result<(usize, usize), InternalError> {
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let compiled_predicate = execution_preparation.compiled_predicate();
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    let consistency = route.consistency();
    let mut on_key = |data_key: DataKey| -> Result<KeyStreamLoopControl, InternalError> {
        let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
            return Ok(KeyStreamLoopControl::Emit);
        };
        scanned_rows = scanned_rows.saturating_add(1);
        if let Some(compiled_predicate) = compiled_predicate
            && !row_view.eval_predicate(compiled_predicate)
        {
            return Ok(KeyStreamLoopControl::Emit);
        }
        filtered_rows = filtered_rows.saturating_add(1);
        let borrowed_group_hash =
            if super::supports_borrowed_group_probe(&row_view, route.group_fields())? {
                Some(super::stable_hash_group_values_from_row_view(
                    &row_view,
                    route.group_fields(),
                )?)
            } else {
                None
            };
        let mut owned_group_key = None;
        let row_input = GroupedFoldRowInput {
            data_key: &data_key,
            group_fields: route.group_fields(),
            borrowed_group_hash,
            owned_group_key: &mut owned_group_key,
            row_view: &row_view,
        };
        fold_group_input_with_engines(
            short_circuit_keys,
            max_groups_bound,
            grouped_execution_context,
            grouped_engines,
            row_input,
        )?;

        Ok(KeyStreamLoopControl::Emit)
    };
    crate::db::executor::drive_key_stream_with_control_flow(
        resolved.key_stream_mut(),
        &mut || KeyStreamLoopControl::Emit,
        &mut on_key,
    )?;

    Ok((scanned_rows, filtered_rows))
}

///
/// GroupedFoldRowInput
///
/// GroupedFoldRowInput carries the one decoded grouped row payload that every
/// grouped reducer needs during one hot ingest step.
/// The row-scoped borrowed probe plus lazy owned key keep generic grouped
/// ingest aligned with the dedicated grouped-count fast path.
///

struct GroupedFoldRowInput<'a> {
    data_key: &'a DataKey,
    group_fields: &'a [FieldSlot],
    borrowed_group_hash: Option<StableHash>,
    owned_group_key: &'a mut Option<GroupKey>,
    row_view: &'a RowView,
}

// Shared per-row grouped-engine ingest control flow.
// Typed wrappers inject aggregate-engine ingestion while this helper owns
// short-circuit key rejection and bounded tracking invariants.
fn fold_group_input_with_engines(
    short_circuit_keys: &mut [ShortCircuitGroupSet],
    max_groups_bound: usize,
    grouped_execution_context: &mut ExecutionContext,
    grouped_engines: &mut [Box<dyn GroupedAggregateEngine>],
    row_input: GroupedFoldRowInput<'_>,
) -> Result<(), InternalError> {
    let GroupedFoldRowInput {
        data_key,
        group_fields,
        borrowed_group_hash,
        owned_group_key,
        row_view,
    } = row_input;

    // Phase 1: specialize the common single-aggregate grouped shape so the
    // hot row-ingest loop avoids sibling-engine iteration and repeated bounds
    // checks when only one reducer exists.
    if grouped_engines.len() == 1 && short_circuit_keys.len() == 1 {
        return fold_group_input_single_engine(
            &mut short_circuit_keys[0],
            max_groups_bound,
            grouped_execution_context,
            &mut grouped_engines[0],
            GroupedFoldRowInput {
                data_key,
                group_fields,
                borrowed_group_hash,
                owned_group_key,
                row_view,
            },
        );
    }

    // Phase 2: retain the generic multi-engine ingest path for grouped shapes
    // that need sibling reducer coordination.
    for (index, done_group_keys) in short_circuit_keys.iter_mut().enumerate() {
        if done_group_keys.contains_row(
            row_view,
            group_fields,
            borrowed_group_hash,
            owned_group_key,
        )? {
            continue;
        }

        let Some(engine) = grouped_engines.get_mut(index) else {
            return Err(
                GroupedRouteStage::engine_index_out_of_bounds_during_fold_ingest(
                    index,
                    grouped_engines.len(),
                ),
            );
        };
        let fold_control = engine
            .ingest(
                grouped_execution_context,
                data_key,
                row_view,
                group_fields,
                borrowed_group_hash,
                owned_group_key,
            )
            .map_err(GroupError::into_internal_error)?;
        if matches!(fold_control, FoldControl::Break) {
            let group_key = owned_group_key.get_or_insert(
                super::materialize_group_key_from_row_view(row_view, group_fields)?,
            );
            done_group_keys.insert(group_key);
            debug_assert!(
                done_group_keys.len() <= max_groups_bound,
                "grouped short-circuit key tracking must stay bounded by max_groups",
            );
        }
    }

    Ok(())
}

// Ingest one grouped row into the common single-reducer grouped shape without
// paying the generic sibling-engine coordination loop.
fn fold_group_input_single_engine(
    done_group_keys: &mut ShortCircuitGroupSet,
    max_groups_bound: usize,
    grouped_execution_context: &mut ExecutionContext,
    grouped_engine: &mut Box<dyn GroupedAggregateEngine>,
    row_input: GroupedFoldRowInput<'_>,
) -> Result<(), InternalError> {
    let GroupedFoldRowInput {
        data_key,
        group_fields,
        borrowed_group_hash,
        owned_group_key,
        row_view,
    } = row_input;

    if done_group_keys.contains_row(row_view, group_fields, borrowed_group_hash, owned_group_key)? {
        return Ok(());
    }

    let fold_control = grouped_engine
        .ingest(
            grouped_execution_context,
            data_key,
            row_view,
            group_fields,
            borrowed_group_hash,
            owned_group_key,
        )
        .map_err(GroupError::into_internal_error)?;
    if matches!(fold_control, FoldControl::Break) {
        let group_key = owned_group_key.get_or_insert(super::materialize_group_key_from_row_view(
            row_view,
            group_fields,
        )?);
        done_group_keys.insert(group_key);
        debug_assert!(
            done_group_keys.len() <= max_groups_bound,
            "grouped short-circuit key tracking must stay bounded by max_groups",
        );
    }

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::executor::{
            aggregate::runtime::grouped_fold::ingest::ShortCircuitGroupSet, group::GroupKey,
        },
        value::Value,
    };

    #[test]
    fn short_circuit_group_set_uses_hashed_owned_key_membership() {
        let first = GroupKey::from_group_values(vec![Value::Uint(7), Value::Text("a".to_string())])
            .expect("first grouped key should materialize");
        let second =
            GroupKey::from_group_values(vec![Value::Uint(9), Value::Text("b".to_string())])
                .expect("second grouped key should materialize");
        let missing =
            GroupKey::from_group_values(vec![Value::Uint(11), Value::Text("c".to_string())])
                .expect("missing grouped key should materialize");
        let mut set = ShortCircuitGroupSet::new();

        set.insert(&first);
        set.insert(&second);

        assert_eq!(
            set.len(),
            2,
            "short-circuit group tracking should count inserted completed groups",
        );
        assert!(
            set.contains_group_key(&first),
            "inserted grouped key should be found through hashed membership",
        );
        assert!(
            set.contains_group_key(&second),
            "second inserted grouped key should be found through hashed membership",
        );
        assert!(
            !set.contains_group_key(&missing),
            "non-inserted grouped key should not be reported as already short-circuited",
        );
    }
}
