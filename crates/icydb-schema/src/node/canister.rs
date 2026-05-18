use crate::node::{
    stable_memory_key, validate_app_memory_id, validate_memory_id_in_range,
    validate_memory_id_not_reserved, validate_stable_key, validate_stable_key_segment,
};
use crate::prelude::*;
use std::collections::BTreeMap;

///
/// Canister
///

#[derive(CandidType, Clone, Debug, Serialize)]
pub struct Canister {
    def: Def,
    memory_namespace: &'static str,
    memory_min: u8,
    memory_max: u8,
    commit_memory_id: u8,
}

impl Canister {
    #[must_use]
    pub const fn new(
        def: Def,
        memory_namespace: &'static str,
        memory_min: u8,
        memory_max: u8,
        commit_memory_id: u8,
    ) -> Self {
        Self {
            def,
            memory_namespace,
            memory_min,
            memory_max,
            commit_memory_id,
        }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    #[must_use]
    pub const fn memory_namespace(&self) -> &'static str {
        self.memory_namespace
    }

    #[must_use]
    pub const fn memory_min(&self) -> u8 {
        self.memory_min
    }

    #[must_use]
    pub const fn memory_max(&self) -> u8 {
        self.memory_max
    }

    #[must_use]
    pub const fn commit_memory_id(&self) -> u8 {
        self.commit_memory_id
    }

    #[must_use]
    pub fn commit_stable_key(&self) -> String {
        stable_memory_key(self.memory_namespace(), "__commit", "control")
    }
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

        let canister_path = self.def().path();
        let mut seen_ids = BTreeMap::<u8, (String, String)>::new();
        let mut seen_keys = BTreeMap::<String, (u8, String)>::new();

        validate_stable_key_segment(
            &mut errs,
            "canister memory_namespace",
            self.memory_namespace(),
        );

        validate_memory_id_in_range(
            &mut errs,
            "commit_memory_id",
            self.commit_memory_id(),
            self.memory_min(),
            self.memory_max(),
        );
        validate_app_memory_id(&mut errs, "commit_memory_id", self.commit_memory_id());
        validate_memory_id_not_reserved(&mut errs, "commit_memory_id", self.commit_memory_id());
        validate_stable_key(&mut errs, "commit stable key", &self.commit_stable_key());

        assert_unique_memory_allocation(
            self.commit_memory_id(),
            self.commit_stable_key(),
            format!("Canister `{}`.commit_memory", self.def().path()),
            &canister_path,
            &mut seen_ids,
            &mut seen_keys,
            &mut errs,
        );

        // Check all Store nodes for this canister
        for (path, store) in schema.filter_nodes::<Store>(|node| node.canister() == canister_path) {
            assert_unique_memory_allocation(
                store.data_allocation(self.memory_namespace()).memory_id(),
                store
                    .data_allocation(self.memory_namespace())
                    .stable_key()
                    .to_string(),
                format!("Store `{path}`.data_memory"),
                &canister_path,
                &mut seen_ids,
                &mut seen_keys,
                &mut errs,
            );

            assert_unique_memory_allocation(
                store.index_allocation(self.memory_namespace()).memory_id(),
                store
                    .index_allocation(self.memory_namespace())
                    .stable_key()
                    .to_string(),
                format!("Store `{path}`.index_memory"),
                &canister_path,
                &mut seen_ids,
                &mut seen_keys,
                &mut errs,
            );

            assert_unique_memory_allocation(
                store.schema_allocation(self.memory_namespace()).memory_id(),
                store
                    .schema_allocation(self.memory_namespace())
                    .stable_key()
                    .to_string(),
                format!("Store `{path}`.schema_memory"),
                &canister_path,
                &mut seen_ids,
                &mut seen_keys,
                &mut errs,
            );
        }

        errs.result()
    }
}

fn assert_unique_memory_allocation(
    memory_id: u8,
    stable_key: String,
    slot: String,
    canister_path: &str,
    seen_ids: &mut BTreeMap<u8, (String, String)>,
    seen_keys: &mut BTreeMap<String, (u8, String)>,
    errs: &mut ErrorTree,
) {
    if let Some((existing_key, existing_slot)) = seen_ids.get(&memory_id) {
        err!(
            errs,
            "duplicate memory_id `{}` used in canister `{}`: {} ({}) conflicts with {} ({})",
            memory_id,
            canister_path,
            existing_slot,
            existing_key,
            slot,
            stable_key,
        );
    } else {
        seen_ids.insert(memory_id, (stable_key.clone(), slot.clone()));
    }

    if let Some((existing_id, existing_slot)) = seen_keys.get(&stable_key) {
        err!(
            errs,
            "duplicate stable_key `{}` used in canister `{}`: {} ({}) conflicts with {} ({})",
            stable_key,
            canister_path,
            existing_slot,
            existing_id,
            slot,
            memory_id,
        );
    } else {
        seen_keys.insert(stable_key, (memory_id, slot));
    }
}

impl VisitableNode for Canister {
    fn route_key(&self) -> String {
        self.def().path()
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use crate::build::schema_write;

    use super::*;

    fn insert_canister(path_module: &'static str, ident: &'static str) -> Canister {
        let canister = Canister::new(Def::new(path_module, ident), "test_db", 100, 254, 254);
        schema_write().insert_node(SchemaNode::Canister(canister.clone()));

        canister
    }

    fn insert_store(
        path_module: &'static str,
        ident: &'static str,
        store_name: &'static str,
        canister_path: &'static str,
        data_memory_id: u8,
        index_memory_id: u8,
        schema_memory_id: u8,
    ) {
        schema_write().insert_node(SchemaNode::Store(Store::new(
            Def::new(path_module, ident),
            ident,
            store_name,
            canister_path,
            data_memory_id,
            index_memory_id,
            schema_memory_id,
        )));
    }

    #[test]
    fn validate_rejects_memory_id_collision_between_stores() {
        let canister = insert_canister("schema_store_collision", "Canister");
        let canister_path = "schema_store_collision::Canister";

        insert_store(
            "schema_store_collision",
            "StoreA",
            "store_a",
            canister_path,
            110,
            111,
            112,
        );
        insert_store(
            "schema_store_collision",
            "StoreB",
            "store_b",
            canister_path,
            113,
            110,
            114,
        ); // collision

        let err = canister
            .validate()
            .expect_err("memory-id collision must fail");

        let rendered = err.to_string();
        assert!(
            rendered.contains("duplicate memory_id `110`"),
            "expected duplicate memory-id error, got: {rendered}"
        );
    }

    #[test]
    fn validate_accepts_unique_memory_ids() {
        let canister = insert_canister("schema_store_unique", "Canister");
        let canister_path = "schema_store_unique::Canister";

        insert_store(
            "schema_store_unique",
            "StoreA",
            "store_a",
            canister_path,
            130,
            131,
            132,
        );
        insert_store(
            "schema_store_unique",
            "StoreB",
            "store_b",
            canister_path,
            133,
            134,
            135,
        );

        canister.validate().expect("unique memory IDs should pass");
    }

    #[test]
    fn validate_rejects_reserved_commit_memory_id() {
        let canister = Canister::new(
            Def::new("schema_reserved_commit", "Canister"),
            "test_db",
            100,
            254,
            255,
        );
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

    #[test]
    fn store_allocation_identity_is_independent_of_schema_order() {
        let first = Store::new(
            Def::new("schema_allocation_order", "Users"),
            "USERS",
            "users",
            "schema_allocation_order::Canister",
            110,
            111,
            112,
        );
        let reordered = Store::new(
            Def::new("schema_allocation_order", "Users"),
            "USERS",
            "users",
            "schema_allocation_order::Canister",
            110,
            111,
            112,
        );

        assert!(
            first
                .data_allocation("test_db")
                .same_identity_as(&reordered.data_allocation("test_db"))
        );
        assert!(
            first
                .index_allocation("test_db")
                .same_identity_as(&reordered.index_allocation("test_db"))
        );
        assert!(
            first
                .schema_allocation("test_db")
                .same_identity_as(&reordered.schema_allocation("test_db"))
        );
    }

    #[test]
    fn adding_store_does_not_change_existing_store_allocation() {
        let existing = Store::new(
            Def::new("schema_allocation_add", "Users"),
            "USERS",
            "users",
            "schema_allocation_add::Canister",
            110,
            111,
            112,
        );
        let _new_store = Store::new(
            Def::new("schema_allocation_add", "AuditEvents"),
            "AUDIT_EVENTS",
            "audit_events",
            "schema_allocation_add::Canister",
            120,
            121,
            122,
        );

        assert_eq!(existing.data_allocation("test_db").memory_id(), 110);
        assert_eq!(
            existing.data_allocation("test_db").stable_key(),
            "icydb.test_db.users.data.v1"
        );
    }

    #[test]
    fn validate_rejects_same_stable_key_with_different_memory_id() {
        let canister = insert_canister("schema_store_key_collision", "Canister");
        let canister_path = "schema_store_key_collision::Canister";

        insert_store(
            "schema_store_key_collision",
            "StoreA",
            "users",
            canister_path,
            110,
            111,
            112,
        );
        insert_store(
            "schema_store_key_collision",
            "StoreB",
            "users",
            canister_path,
            120,
            121,
            122,
        );

        let err = canister
            .validate()
            .expect_err("stable-key collision must fail");

        let rendered = err.to_string();
        assert!(
            rendered.contains("duplicate stable_key `icydb.test_db.users.data.v1`"),
            "expected duplicate stable-key error, got: {rendered}"
        );
    }

    #[test]
    fn stable_memory_identity_ignores_schema_metadata() {
        let left = StableMemoryAllocation::new(
            110,
            "icydb.test_db.users.data.v1".to_string(),
            Some(1),
            Some("aaa".to_string()),
        );
        let right = StableMemoryAllocation::new(
            110,
            "icydb.test_db.users.data.v1".to_string(),
            Some(2),
            Some("bbb".to_string()),
        );

        assert!(left.same_identity_as(&right));
    }

    #[test]
    fn validate_rejects_app_memory_id_below_canic_reserved_range() {
        let canister = Canister::new(
            Def::new("schema_reserved_app_range", "Canister"),
            "test_db",
            99,
            110,
            99,
        );

        let err = canister
            .validate()
            .expect_err("app memory id below 100 must fail");

        let rendered = err.to_string();
        assert!(
            rendered.contains("outside of application memory range 100-254"),
            "expected app memory range error, got: {rendered}"
        );
    }

    #[test]
    fn stable_keys_reject_canic_prefix() {
        assert!(!stable_key_is_canonical("canic.test.users.data.v1"));
    }
}
