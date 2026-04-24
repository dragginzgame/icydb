//! Module: db::executor::aggregate::contracts::grouped::tests
//! Responsibility: grouped aggregate contract tests that validate state ownership, budgeting,
//! finalization order, and terminal structural outputs.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping fixture details internal.

mod budget;
mod determinism;
mod state;
mod target_field;

use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::aggregate::{
            FoldControl,
            contracts::{AggregateKind, ExecutionConfig, ExecutionContext, GroupError},
        },
        executor::group::CanonicalKey,
        executor::pipeline::runtime::RowView,
        query::{
            builder::aggregate::{count, min_by},
            plan::FieldSlot,
        },
    },
    model::field::FieldKind,
    testing,
    types::Decimal,
    value::{Value, with_test_hash_override},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;

type FinalizedGroupedRow = super::engine::GroupedAggregateOutput;

crate::test_canister! {
    ident = GroupedStateTestCanister,
    commit_memory_id = testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = GroupedStateTestStore,
    canister = GroupedStateTestCanister,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct GroupedStateTestEntity {
    id: u64,
}

crate::test_entity_schema! {
    ident = GroupedStateTestEntity,
    id = u64,
    id_field = id,
    entity_name = "GroupedStateTestEntity",
    entity_tag = crate::testing::GROUPED_STATE_TEST_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Uint)],
    indexes = [],
    store = GroupedStateTestStore,
    canister = GroupedStateTestCanister,
}

fn text_group_key(value: &str) -> crate::db::executor::group::GroupKey {
    Value::Text(value.to_string())
        .canonical_key()
        .expect("group key canonicalization should succeed")
}

fn data_key(id: u64) -> DataKey {
    DataKey::try_new::<GroupedStateTestEntity>(id).expect("test data key should build")
}

fn into_value_pairs(rows: Vec<FinalizedGroupedRow>) -> Vec<(Value, Value)> {
    rows.into_iter()
        .map(FinalizedGroupedRow::into_value_pair)
        .collect()
}

fn count_rows(rows: &[(Value, Value)]) -> Vec<(Value, u32)> {
    rows.iter()
        .map(|(group_key, output)| {
            let Value::Uint(count) = output else {
                panic!("grouped count-state test expects count outputs");
            };
            (
                group_key.clone(),
                u32::try_from(*count).expect("grouped count output must fit u32"),
            )
        })
        .collect()
}

fn value_rows(rows: &[(Value, Value)]) -> Vec<(Value, Value)> {
    rows.to_vec()
}

fn finalize_grouped_id_rows(
    kind: AggregateKind,
    direction: Direction,
    ids: &[u64],
    stop_on_break: bool,
) -> Vec<(Value, Value)> {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped = execution_context.create_grouped_state(kind, direction, false);

    for &id in ids {
        let fold_control = grouped
            .apply_borrowed(
                &text_group_key("alpha"),
                &data_key(id),
                &mut execution_context,
            )
            .expect("grouped id-terminal row should apply");
        if stop_on_break && matches!(fold_control, FoldControl::Break) {
            break;
        }
    }

    into_value_pairs(grouped.finalize())
}

// Apply one fixed grouped-count fixture through one insertion-order projection
// and return finalized `(group_key, count)` rows in emitted order.
fn grouped_count_rows_for_order(order: &[usize]) -> Vec<(Value, u32)> {
    let fixtures = [
        ("alpha", 1_u64),
        ("beta", 2_u64),
        ("alpha", 3_u64),
        ("gamma", 4_u64),
        ("beta", 5_u64),
    ];
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

    for fixture_index in order {
        let (group, id) = fixtures[*fixture_index];
        grouped
            .apply_borrowed(
                &text_group_key(group),
                &data_key(id),
                &mut execution_context,
            )
            .expect("determinism fixture rows should apply");
    }

    let finalized = into_value_pairs(grouped.finalize());
    count_rows(finalized.as_slice())
}
