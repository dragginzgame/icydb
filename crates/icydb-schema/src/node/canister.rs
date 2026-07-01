//! Module: node::canister
//!
//! Responsibility: canister-level schema node metadata and memory allocation validation.
//! Does not own: ICP lifecycle management or runtime stable-memory implementation.
//! Boundary: validates declared memory ranges and stable keys before runtime use.

#[cfg(test)]
mod tests;

use crate::node::{
    stable_memory_key, validate_app_memory_id, validate_memory_id_in_range,
    validate_memory_id_not_reserved, validate_stable_key, validate_stable_key_segment,
};
use crate::prelude::*;
use std::collections::BTreeMap;

///
/// Canister
///

#[derive(Clone, Debug, Serialize)]
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
        stable_memory_key(self.memory_namespace(), "commit", "control")
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

        {
            let schema = schema_read();

            // Check all Store nodes for this canister
            for (path, store) in
                schema.filter_nodes::<Store>(|node| node.canister() == canister_path)
            {
                match store.storage() {
                    StoreStorage::Journaled(_) => {
                        assert_unique_memory_allocation(
                            store
                                .stable_data_allocation(self.memory_namespace())
                                .memory_id(),
                            store
                                .stable_data_allocation(self.memory_namespace())
                                .stable_key()
                                .to_string(),
                            format!("Store `{path}`.data_memory"),
                            &canister_path,
                            &mut seen_ids,
                            &mut seen_keys,
                            &mut errs,
                        );

                        assert_unique_memory_allocation(
                            store
                                .stable_index_allocation(self.memory_namespace())
                                .memory_id(),
                            store
                                .stable_index_allocation(self.memory_namespace())
                                .stable_key()
                                .to_string(),
                            format!("Store `{path}`.index_memory"),
                            &canister_path,
                            &mut seen_ids,
                            &mut seen_keys,
                            &mut errs,
                        );

                        assert_unique_memory_allocation(
                            store
                                .stable_schema_allocation(self.memory_namespace())
                                .memory_id(),
                            store
                                .stable_schema_allocation(self.memory_namespace())
                                .stable_key()
                                .to_string(),
                            format!("Store `{path}`.schema_memory"),
                            &canister_path,
                            &mut seen_ids,
                            &mut seen_keys,
                            &mut errs,
                        );

                        if store.is_journaled_storage() {
                            assert_unique_memory_allocation(
                                store
                                    .journal_allocation(self.memory_namespace())
                                    .memory_id(),
                                store
                                    .journal_allocation(self.memory_namespace())
                                    .stable_key()
                                    .to_string(),
                                format!("Store `{path}`.journal_memory"),
                                &canister_path,
                                &mut seen_ids,
                                &mut seen_keys,
                                &mut errs,
                            );
                        }
                    }
                    StoreStorage::Heap(_) => {}
                }
            }
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
