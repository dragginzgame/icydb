use crate::node::{validate_memory_id_in_range, validate_memory_id_not_reserved};
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
    pub commit_memory_id: u8,
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

        let canister_path = self.def.path();
        let mut seen_ids = BTreeMap::<u8, String>::new();

        validate_memory_id_in_range(
            &mut errs,
            "commit_memory_id",
            self.commit_memory_id,
            self.memory_min,
            self.memory_max,
        );
        validate_memory_id_not_reserved(&mut errs, "commit_memory_id", self.commit_memory_id);

        assert_unique_memory_id(
            self.commit_memory_id,
            format!("Canister `{}`.commit_memory_id", self.def.path()),
            &canister_path,
            &mut seen_ids,
            &mut errs,
        );

        // Check all Store nodes for this canister
        for (path, store) in schema.filter_nodes::<Store>(|node| node.canister == canister_path) {
            assert_unique_memory_id(
                store.data_memory_id,
                format!("Store `{path}`.data_memory_id"),
                &canister_path,
                &mut seen_ids,
                &mut errs,
            );

            assert_unique_memory_id(
                store.index_memory_id,
                format!("Store `{path}`.index_memory_id"),
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

///
/// TESTS
///

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
            commit_memory_id: 254,
        };
        schema_write().insert_node(SchemaNode::Canister(canister.clone()));

        canister
    }

    fn insert_store(
        path_module: &'static str,
        ident: &'static str,
        canister_path: &'static str,
        data_memory_id: u8,
        index_memory_id: u8,
    ) {
        schema_write().insert_node(SchemaNode::Store(Store {
            def: Def {
                module_path: path_module,
                ident,
                comments: None,
            },
            ident,
            canister: canister_path,
            data_memory_id,
            index_memory_id,
        }));
    }

    #[test]
    fn validate_rejects_memory_id_collision_between_stores() {
        let canister = insert_canister("schema_store_collision", "Canister");
        let canister_path = "schema_store_collision::Canister";

        insert_store("schema_store_collision", "StoreA", canister_path, 10, 11);
        insert_store("schema_store_collision", "StoreB", canister_path, 12, 10); // collision

        let err = canister
            .validate()
            .expect_err("memory-id collision must fail");

        let rendered = err.to_string();
        assert!(
            rendered.contains("duplicate memory_id `10`"),
            "expected duplicate memory-id error, got: {rendered}"
        );
    }

    #[test]
    fn validate_accepts_unique_memory_ids() {
        let canister = insert_canister("schema_store_unique", "Canister");
        let canister_path = "schema_store_unique::Canister";

        insert_store("schema_store_unique", "StoreA", canister_path, 30, 31);
        insert_store("schema_store_unique", "StoreB", canister_path, 32, 33);

        canister.validate().expect("unique memory IDs should pass");
    }

    #[test]
    fn validate_rejects_reserved_commit_memory_id() {
        let canister = Canister {
            def: Def {
                module_path: "schema_reserved_commit",
                ident: "Canister",
                comments: None,
            },
            memory_min: 0,
            memory_max: 255,
            commit_memory_id: 255,
        };
        schema_write().insert_node(SchemaNode::Canister(canister.clone()));

        let err = canister
            .validate()
            .expect_err("reserved commit memory id must fail");

        let rendered = err.to_string();
        assert!(
            rendered.contains("reserved for stable-structures internals"),
            "expected reserved-id error, got: {rendered}"
        );
    }
}
