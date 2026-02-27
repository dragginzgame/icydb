use crate::{
    db::{
        Db,
        access::{AccessPath, AccessPlan},
        contracts::ReadConsistency,
        direction::Direction,
        executor::{
            Context, IndexStreamConstraints, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            StreamExecutionHints,
        },
        registry::StoreRegistry,
    },
    model::{field::FieldKind, index::IndexModel},
    traits::Storable,
    types::Ulid,
    value::Value,
};
use icydb_derive::FieldProjection;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, ops::Bound};

const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
const INDEX_MODEL: IndexModel = IndexModel::new(
    "context::idx_group_rank",
    "context::InvariantStore",
    &INDEX_FIELDS,
    false,
);
const INDEX_MODEL_ALT: IndexModel = IndexModel::new(
    "context::idx_group_rank_alt",
    "context::InvariantStore",
    &INDEX_FIELDS,
    false,
);

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct ContextInvariantEntity {
    id: Ulid,
    group: u32,
    rank: u32,
}

crate::test_canister! {
    ident = ContextInvariantCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = ContextInvariantStore,
    canister = ContextInvariantCanister,
}

crate::test_entity_schema! {
    ident = ContextInvariantEntity,
    id = Ulid,
    id_field = id,
    entity_name = "ContextInvariantEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("group", FieldKind::Uint),
        ("rank", FieldKind::Uint),
    ],
    indexes = [&INDEX_MODEL],
    store = ContextInvariantStore,
    canister = ContextInvariantCanister,
}

thread_local! {
    static INVARIANT_STORE_REGISTRY: StoreRegistry = StoreRegistry::new();
}

static INVARIANT_DB: Db<ContextInvariantCanister> = Db::new(&INVARIANT_STORE_REGISTRY);

fn raw_index_key(byte: u8) -> crate::db::executor::LoweredKey {
    <crate::db::executor::LoweredKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
}

fn dummy_index_range_spec() -> LoweredIndexRangeSpec {
    LoweredIndexRangeSpec::new(
        INDEX_MODEL,
        Bound::Included(raw_index_key(0x01)),
        Bound::Included(raw_index_key(0x02)),
    )
}

fn dummy_index_prefix_spec() -> LoweredIndexPrefixSpec {
    LoweredIndexPrefixSpec::new(
        INDEX_MODEL,
        Bound::Included(raw_index_key(0x01)),
        Bound::Included(raw_index_key(0x02)),
    )
}

#[test]
fn index_range_path_requires_pre_lowered_spec() {
    let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
    let access = AccessPath::index_range(
        INDEX_MODEL,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(10)),
        Bound::Excluded(Value::Uint(20)),
    );

    let Err(err) = ctx.ordered_key_stream_from_access(
        &access,
        IndexStreamConstraints {
            prefix: None,
            range: None,
            anchor: None,
        },
        Direction::Asc,
        StreamExecutionHints {
            physical_fetch_hint: None,
            predicate_execution: None,
        },
    ) else {
        panic!("index-range access without lowered spec must fail")
    };

    assert!(
        err.to_string()
            .contains("index-range execution requires pre-lowered index-range spec"),
        "missing-spec error must be classified as an executor invariant"
    );
}

#[test]
fn index_prefix_path_direct_resolution_skips_alignment_invariant_check() {
    let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
    let access = AccessPath::IndexPrefix {
        index: INDEX_MODEL_ALT,
        values: vec![Value::Uint(7)],
    };
    let spec = dummy_index_prefix_spec();

    let result = ctx.ordered_key_stream_from_access(
        &access,
        IndexStreamConstraints {
            prefix: Some(&spec),
            range: None,
            anchor: None,
        },
        Direction::Asc,
        StreamExecutionHints {
            physical_fetch_hint: None,
            predicate_execution: None,
        },
    );

    if let Err(err) = result {
        assert!(
            !err.to_string()
                .contains("index-prefix spec does not match access path index"),
            "direct physical resolution must not enforce resolver-owned prefix-spec alignment",
        );
    }
}

#[test]
fn index_range_path_direct_resolution_skips_alignment_invariant_check() {
    let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
    let access = AccessPath::index_range(
        INDEX_MODEL_ALT,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(10)),
        Bound::Excluded(Value::Uint(20)),
    );
    let spec = dummy_index_range_spec();

    let result = ctx.ordered_key_stream_from_access(
        &access,
        IndexStreamConstraints {
            prefix: None,
            range: Some(&spec),
            anchor: None,
        },
        Direction::Asc,
        StreamExecutionHints {
            physical_fetch_hint: None,
            predicate_execution: None,
        },
    );

    if let Err(err) = result {
        assert!(
            !err.to_string()
                .contains("index-range spec does not match access path index"),
            "direct physical resolution must not enforce resolver-owned range-spec alignment",
        );
    }
}

#[test]
fn access_plan_rejects_unused_index_range_specs() {
    let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
    let access = AccessPlan::path(AccessPath::ByKey(Ulid::from_u128(1)));
    let extra_prefix_spec = dummy_index_prefix_spec();
    let extra_spec = dummy_index_range_spec();

    let err = ctx
        .rows_from_access_plan(
            &access,
            &[extra_prefix_spec],
            &[extra_spec],
            ReadConsistency::MissingOk,
        )
        .expect_err("unused index-range specs must fail invariant checks");

    assert!(
        err.to_string()
            .contains("unused index-prefix executable specs after access-plan traversal"),
        "unused-spec error must be classified as an executor invariant"
    );
}

#[test]
fn access_plan_rejects_misaligned_index_prefix_spec() {
    let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
    let access = AccessPlan::path(AccessPath::IndexPrefix {
        index: INDEX_MODEL_ALT,
        values: vec![Value::Uint(7)],
    });
    let prefix_spec = dummy_index_prefix_spec();

    let err = ctx
        .rows_from_access_plan(&access, &[prefix_spec], &[], ReadConsistency::MissingOk)
        .expect_err("misaligned index-prefix spec must fail invariant checks");

    assert!(
        err.to_string()
            .contains("index-prefix spec does not match access path index"),
        "misaligned prefix spec must fail fast before execution"
    );
}

#[test]
fn access_plan_rejects_misaligned_index_range_spec() {
    let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
    let access = AccessPlan::path(AccessPath::index_range(
        INDEX_MODEL_ALT,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(10)),
        Bound::Excluded(Value::Uint(20)),
    ));
    let range_spec = dummy_index_range_spec();

    let err = ctx
        .rows_from_access_plan(&access, &[], &[range_spec], ReadConsistency::MissingOk)
        .expect_err("misaligned index-range spec must fail invariant checks");

    assert!(
        err.to_string()
            .contains("index-range spec does not match access path index"),
        "misaligned range spec must fail fast before execution"
    );
}

#[test]
fn composite_union_rejects_misaligned_index_prefix_spec() {
    let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
    let access = AccessPlan::Union(vec![AccessPlan::path(AccessPath::IndexPrefix {
        index: INDEX_MODEL_ALT,
        values: vec![Value::Uint(7)],
    })]);
    let prefix_spec = dummy_index_prefix_spec();

    let err = ctx
        .rows_from_access_plan(&access, &[prefix_spec], &[], ReadConsistency::MissingOk)
        .expect_err("misaligned composite prefix spec must fail invariant checks");

    assert!(
        err.to_string()
            .contains("index-prefix spec does not match access path index"),
        "misaligned composite prefix spec must fail fast before execution"
    );
}

#[test]
fn composite_intersection_rejects_misaligned_index_range_spec() {
    let ctx = Context::<ContextInvariantEntity>::new(&INVARIANT_DB);
    let access = AccessPlan::Intersection(vec![AccessPlan::path(AccessPath::index_range(
        INDEX_MODEL_ALT,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(10)),
        Bound::Excluded(Value::Uint(20)),
    ))]);
    let range_spec = dummy_index_range_spec();

    let err = ctx
        .rows_from_access_plan(&access, &[], &[range_spec], ReadConsistency::MissingOk)
        .expect_err("misaligned composite range spec must fail invariant checks");

    assert!(
        err.to_string()
            .contains("index-range spec does not match access path index"),
        "misaligned composite range spec must fail fast before execution"
    );
}

#[test]
fn dedup_keys_returns_canonical_order_for_directional_consumers() {
    let low = Ulid::from_u128(10);
    let mid = Ulid::from_u128(11);
    let high = Ulid::from_u128(12);
    let deduped = Context::<ContextInvariantEntity>::dedup_keys(vec![high, low, high, mid, low]);

    assert_eq!(
        deduped,
        vec![low, mid, high],
        "dedup_keys must emit canonical ascending key order for ByKeys consumers",
    );

    let mut desc = deduped;
    desc.reverse();
    assert_eq!(
        desc,
        vec![high, mid, low],
        "reversing deduped keys must produce canonical descending order",
    );
}
