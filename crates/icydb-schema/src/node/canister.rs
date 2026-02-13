use crate::prelude::*;
use std::collections::BTreeMap;

///
/// Canister
///

#[derive(CandidType, Clone, Debug, Serialize)]
pub struct Canister {
    pub def: Def,
    pub memory_min: u8,
    pub memory_max: u8,
}

impl MacroNode for Canister {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Canister {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // Check for duplicate memory IDs across all stores for this canister.
        // The collision domain includes:
        // - DataStore.memory_id
        // - IndexStore.entry_memory_id
        // - IndexStore.fingerprint_memory_id
        let canister_path = self.def.path();
        let mut seen_ids = BTreeMap::<u8, String>::new();

        for (path, store) in schema.filter_nodes::<DataStore>(|node| node.canister == canister_path)
        {
            assert_unique_memory_id(
                store.memory_id,
                format!("DataStore `{path}`.memory_id"),
                &canister_path,
                &mut seen_ids,
                &mut errs,
            );
        }

        for (path, store) in
            schema.filter_nodes::<IndexStore>(|node| node.canister == canister_path)
        {
            assert_unique_memory_id(
                store.entry_memory_id,
                format!("IndexStore `{path}`.entry_memory_id"),
                &canister_path,
                &mut seen_ids,
                &mut errs,
            );
            assert_unique_memory_id(
                store.fingerprint_memory_id,
                format!("IndexStore `{path}`.fingerprint_memory_id"),
                &canister_path,
                &mut seen_ids,
                &mut errs,
            );
        }

        errs.result()
    }
}

fn assert_unique_memory_id(
    memory_id: u8,
    slot: String,
    canister_path: &str,
    seen_ids: &mut BTreeMap<u8, String>,
    errs: &mut ErrorTree,
) {
    if let Some(existing) = seen_ids.get(&memory_id) {
        err!(
            errs,
            "duplicate memory_id `{}` used in canister `{}`: {} conflicts with {}",
            memory_id,
            canister_path,
            existing,
            slot
        );
    } else {
        seen_ids.insert(memory_id, slot);
    }
}

impl VisitableNode for Canister {
    fn route_key(&self) -> String {
        self.def.path()
    }
}

#[cfg(test)]
mod tests {
    use crate::build::schema_write;

    use super::*;

    fn insert_canister(path_module: &'static str, ident: &'static str) -> Canister {
        let canister = Canister {
            def: Def {
                module_path: path_module,
                ident,
                comments: None,
            },
            memory_min: 0,
            memory_max: 255,
        };
        schema_write().insert_node(SchemaNode::Canister(canister.clone()));

        canister
    }

    fn insert_data_store(
        path_module: &'static str,
        ident: &'static str,
        canister_path: &'static str,
        memory_id: u8,
    ) {
        schema_write().insert_node(SchemaNode::DataStore(DataStore {
            def: Def {
                module_path: path_module,
                ident,
                comments: None,
            },
            ident,
            canister: canister_path,
            memory_id,
        }));
    }

    fn insert_index_store(
        path_module: &'static str,
        ident: &'static str,
        canister_path: &'static str,
        entry_memory_id: u8,
        fingerprint_memory_id: u8,
    ) {
        schema_write().insert_node(SchemaNode::IndexStore(IndexStore {
            def: Def {
                module_path: path_module,
                ident,
                comments: None,
            },
            ident,
            canister: canister_path,
            entry_memory_id,
            fingerprint_memory_id,
        }));
    }

    #[test]
    fn validate_rejects_data_store_and_index_store_memory_id_collision() {
        let canister = insert_canister("schema_canister_tests_data_vs_index", "Canister");
        let canister_path = "schema_canister_tests_data_vs_index::Canister";
        insert_data_store(
            "schema_canister_tests_data_vs_index",
            "DataA",
            canister_path,
            10,
        );
        insert_index_store(
            "schema_canister_tests_data_vs_index",
            "IndexA",
            canister_path,
            10,
            11,
        );

        let err = canister
            .validate()
            .expect_err("data/index memory-id collision must fail");
        let rendered = err.to_string();
        assert!(
            rendered.contains("duplicate memory_id `10`"),
            "expected duplicate memory-id error, got: {rendered}"
        );
        assert!(
            rendered.contains("DataStore") && rendered.contains("IndexStore"),
            "error should identify conflicting slots, got: {rendered}"
        );
    }

    #[test]
    fn validate_rejects_entry_and_fingerprint_collision() {
        let canister = insert_canister("schema_canister_tests_index_self_collision", "Canister");
        let canister_path = "schema_canister_tests_index_self_collision::Canister";
        insert_index_store(
            "schema_canister_tests_index_self_collision",
            "IndexA",
            canister_path,
            22,
            22,
        );

        let err = canister
            .validate()
            .expect_err("index entry/fingerprint memory-id collision must fail");
        let rendered = err.to_string();
        assert!(
            rendered.contains("duplicate memory_id `22`"),
            "expected duplicate memory-id error, got: {rendered}"
        );
        assert!(
            rendered.contains("entry_memory_id") && rendered.contains("fingerprint_memory_id"),
            "error should identify conflicting index slots, got: {rendered}"
        );
    }

    #[test]
    fn validate_accepts_unique_memory_ids_across_data_and_index_stores() {
        let canister = insert_canister("schema_canister_tests_unique_ids", "Canister");
        let canister_path = "schema_canister_tests_unique_ids::Canister";
        insert_data_store(
            "schema_canister_tests_unique_ids",
            "DataA",
            canister_path,
            30,
        );
        insert_data_store(
            "schema_canister_tests_unique_ids",
            "DataB",
            canister_path,
            31,
        );
        insert_index_store(
            "schema_canister_tests_unique_ids",
            "IndexA",
            canister_path,
            32,
            33,
        );
        insert_index_store(
            "schema_canister_tests_unique_ids",
            "IndexB",
            canister_path,
            34,
            35,
        );

        canister
            .validate()
            .expect("unique memory IDs across data/index stores should pass");
    }
}
