use crate::{
    node::{Entity, Schema, Store},
    prelude::*,
};
use std::collections::BTreeMap;

pub fn validate_entity_naming(schema: &Schema, errs: &mut ErrorTree) {
    let mut by_canister: BTreeMap<String, BTreeMap<String, (String, String)>> = BTreeMap::new();

    for (entity_path, entity) in schema.get_nodes::<Entity>() {
        let store = match schema.cast_node::<Store>(entity.store()) {
            Ok(store) => store,
            Err(e) => {
                errs.add(e);
                continue;
            }
        };

        let canister = store.canister().to_string();
        let name = entity.name().to_string();
        let canonical_name = name.to_ascii_lowercase();
        let entity_path = entity_path.to_string();

        let entry = by_canister.entry(canister.clone()).or_default();

        if let Some((prev_name, prev_path)) =
            entry.insert(canonical_name, (name.clone(), entity_path.clone()))
        {
            err!(
                errs,
                "duplicate entity name '{name}' in canister '{canister}' for '{prev_path}' and '{entity_path}' (case-insensitive conflict with '{prev_name}')"
            );
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::node::{
        Canister, Def, Entity, FieldList, PrimaryKey, PrimaryKeySource, Schema, SchemaNode, Store,
        Type,
    };

    use super::*;

    #[expect(clippy::too_many_arguments)]
    fn insert_canister_store(
        schema: &mut Schema,
        module_path: &'static str,
        canister_ident: &'static str,
        canister_path: &'static str,
        store_ident: &'static str,
        store_path: &'static str,
        data_memory_id: u8,
        index_memory_id: u8,
        schema_memory_id: u8,
    ) {
        schema.insert_node(SchemaNode::Canister(Canister::new(
            Def::new(module_path, canister_ident),
            "test_db",
            100,
            254,
            254,
            253,
            None,
        )));
        schema.insert_node(SchemaNode::Store(Store::new_journaled(
            Def::new(module_path, store_ident),
            canister_path,
            StoreJournaledMemoryConfig::new(
                data_memory_id,
                index_memory_id,
                schema_memory_id,
                schema_memory_id.saturating_add(1),
            ),
        )));

        let inserted_store_path = format!("{module_path}::{store_ident}");
        assert_eq!(
            inserted_store_path, store_path,
            "test helper requires literal store-path to match Def path",
        );
    }

    fn insert_entity(
        schema: &mut Schema,
        module_path: &'static str,
        entity_ident: &'static str,
        store_path: &'static str,
    ) {
        schema.insert_node(SchemaNode::Entity(Entity::new(
            Def::new(module_path, entity_ident),
            store_path,
            1,
            PrimaryKey::new(&["id"], PrimaryKeySource::Internal),
            &[],
            &[],
            &[],
            FieldList::new(&[]),
            Type::new(&[], &[], &[]),
        )));
    }

    #[test]
    fn validate_entity_naming_rejects_case_insensitive_duplicates_per_canister() {
        let mut schema = Schema::new();

        insert_canister_store(
            &mut schema,
            "schema_case_conflict",
            "Canister",
            "schema_case_conflict::Canister",
            "Store",
            "schema_case_conflict::Store",
            110,
            111,
            112,
        );
        insert_entity(
            &mut schema,
            "schema_case_conflict",
            "Struct",
            "schema_case_conflict::Store",
        );
        insert_entity(
            &mut schema,
            "schema_case_conflict",
            "struct",
            "schema_case_conflict::Store",
        );

        let mut errs = ErrorTree::new();
        validate_entity_naming(&schema, &mut errs);
        let err = errs
            .result()
            .expect_err("case-insensitive duplicate entity names must fail");
        let rendered = err.to_string();

        assert!(
            rendered.contains("duplicate entity name"),
            "expected duplicate entity-name error, got: {rendered}",
        );
        assert!(
            rendered.contains("case-insensitive conflict"),
            "expected case-insensitive conflict detail, got: {rendered}",
        );
        assert!(
            rendered.contains("schema_case_conflict::Struct")
                && rendered.contains("schema_case_conflict::struct"),
            "expected both conflicting entity paths in error detail, got: {rendered}",
        );
    }

    #[test]
    fn validate_entity_naming_allows_same_name_when_canister_differs() {
        let mut schema = Schema::new();

        insert_canister_store(
            &mut schema,
            "schema_case_allowed_a",
            "CanisterA",
            "schema_case_allowed_a::CanisterA",
            "StoreA",
            "schema_case_allowed_a::StoreA",
            120,
            121,
            122,
        );
        insert_canister_store(
            &mut schema,
            "schema_case_allowed_b",
            "CanisterB",
            "schema_case_allowed_b::CanisterB",
            "StoreB",
            "schema_case_allowed_b::StoreB",
            130,
            131,
            132,
        );
        insert_entity(
            &mut schema,
            "schema_case_allowed_a",
            "Struct",
            "schema_case_allowed_a::StoreA",
        );
        insert_entity(
            &mut schema,
            "schema_case_allowed_b",
            "struct",
            "schema_case_allowed_b::StoreB",
        );

        let mut errs = ErrorTree::new();
        validate_entity_naming(&schema, &mut errs);
        errs.result()
            .expect("case variants across different canisters should pass");
    }
}
