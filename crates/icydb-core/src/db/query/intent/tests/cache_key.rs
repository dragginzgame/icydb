//! Module: db::query::intent::tests::cache_key
//! Covers structural cache-key normalization across equivalent fluent query
//! shapes.
//! Does not own: shared query-intent test fixtures outside this focused cache
//! identity surface.
//! Boundary: exercises query-intent cache identity from the owner `tests/`
//! boundary rather than from the leaf implementation file.

use crate::{
    db::{
        MissingRowPolicy,
        query::intent::{Query, StructuralQuery},
    },
    model::{entity::EntityModel, field::FieldKind},
    testing::PLAN_ENTITY_TAG,
    traits::{EntitySchema, Path},
    types::Ulid,
    value::Value,
};
use icydb_derive::FieldProjection;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct CacheKeyEntity {
    id: Ulid,
    name: String,
}

struct CacheKeyCanister;

impl Path for CacheKeyCanister {
    const PATH: &'static str = concat!(module_path!(), "::CacheKeyCanister");
}

impl crate::traits::CanisterKind for CacheKeyCanister {
    const COMMIT_MEMORY_ID: u8 = crate::testing::test_commit_memory_id();
}

struct CacheKeyStore;

impl Path for CacheKeyStore {
    const PATH: &'static str = concat!(module_path!(), "::CacheKeyStore");
}

impl crate::traits::StoreKind for CacheKeyStore {
    type Canister = CacheKeyCanister;
}

crate::test_entity_schema! {
    ident = CacheKeyEntity,
    id = Ulid,
    id_field = id,
    entity_name = "CacheKeyEntity",
    entity_tag = PLAN_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
    ],
    indexes = [],
    store = CacheKeyStore,
    canister = CacheKeyCanister,
}

fn basic_model() -> &'static EntityModel {
    <CacheKeyEntity as EntitySchema>::MODEL
}

#[test]
fn structural_query_cache_key_matches_for_identical_scalar_queries() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore)
        .filter(crate::db::Predicate::eq(
            "name".to_string(),
            Value::Text("Ada".to_string()),
        ))
        .order_by("name")
        .limit(2);
    let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .filter(crate::db::Predicate::eq(
            "name".to_string(),
            Value::Text("Ada".to_string()),
        ))
        .limit(2);

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "equivalent scalar fluent queries must normalize onto one shared cache key",
    );
}

#[test]
fn structural_query_cache_key_distinguishes_order_direction() {
    let asc = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore).order_by("name");
    let desc = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore).order_by_desc("name");

    assert_ne!(
        asc.structural_cache_key(),
        desc.structural_cache_key(),
        "order direction must remain part of shared query cache identity",
    );
}
