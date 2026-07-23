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
    integrity_progress_memory_id: u8,
}

impl Canister {
    #[must_use]
    pub const fn new(
        def: Def,
        memory_namespace: &'static str,
        memory_min: u8,
        memory_max: u8,
        commit_memory_id: u8,
        integrity_progress_memory_id: u8,
    ) -> Self {
        Self {
            def,
            memory_namespace,
            memory_min,
            memory_max,
            commit_memory_id,
            integrity_progress_memory_id,
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
    pub const fn integrity_progress_memory_id(&self) -> u8 {
        self.integrity_progress_memory_id
    }

    #[must_use]
    pub fn commit_stable_key(&self) -> String {
        stable_memory_key(self.memory_namespace(), "commit", "control")
    }

    #[must_use]
    pub fn integrity_progress_stable_key(&self) -> String {
        stable_memory_key(self.memory_namespace(), "integrity", "progress")
    }

    fn validate_declared_memory_contract(&self, errs: &mut ErrorTree) {
        validate_stable_key_segment(errs, "canister memory_namespace", self.memory_namespace());
        validate_memory_id_in_range(
            errs,
            "commit_memory_id",
            self.commit_memory_id(),
            self.memory_min(),
            self.memory_max(),
        );
        validate_app_memory_id(errs, "commit_memory_id", self.commit_memory_id());
        validate_memory_id_not_reserved(errs, "commit_memory_id", self.commit_memory_id());
        validate_stable_key(errs, "commit stable key", &self.commit_stable_key());
        validate_memory_id_in_range(
            errs,
            "integrity_progress_memory_id",
            self.integrity_progress_memory_id(),
            self.memory_min(),
            self.memory_max(),
        );
        validate_app_memory_id(
            errs,
            "integrity_progress_memory_id",
            self.integrity_progress_memory_id(),
        );
        validate_memory_id_not_reserved(
            errs,
            "integrity_progress_memory_id",
            self.integrity_progress_memory_id(),
        );
        validate_stable_key(
            errs,
            "integrity progress stable key",
            &self.integrity_progress_stable_key(),
        );
    }

    fn register_store_allocations(
        &self,
        canister_path: &str,
        seen_ids: &mut BTreeMap<u8, (String, String)>,
        seen_keys: &mut BTreeMap<String, (u8, String)>,
        errs: &mut ErrorTree,
    ) {
        let schema = schema_read();
        for (path, store) in schema.filter_nodes::<Store>(|node| node.canister() == canister_path) {
            if !matches!(store.storage(), StoreStorage::Journaled(_)) {
                continue;
            }
            for (allocation, role) in [
                (
                    store.stable_data_allocation(self.memory_namespace()),
                    "data",
                ),
                (
                    store.stable_index_allocation(self.memory_namespace()),
                    "index",
                ),
                (
                    store.stable_schema_allocation(self.memory_namespace()),
                    "schema",
                ),
            ] {
                assert_unique_memory_allocation(
                    allocation.memory_id(),
                    allocation.stable_key().to_string(),
                    format!("Store `{path}`.{role}_memory"),
                    canister_path,
                    seen_ids,
                    seen_keys,
                    errs,
                );
            }
            if store.is_journaled_storage() {
                let allocation = store.journal_allocation(self.memory_namespace());
                assert_unique_memory_allocation(
                    allocation.memory_id(),
                    allocation.stable_key().to_string(),
                    format!("Store `{path}`.journal_memory"),
                    canister_path,
                    seen_ids,
                    seen_keys,
                    errs,
                );
            }
        }
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

        self.validate_declared_memory_contract(&mut errs);

        assert_unique_memory_allocation(
            self.commit_memory_id(),
            self.commit_stable_key(),
            format!("Canister `{}`.commit_memory", self.def().path()),
            &canister_path,
            &mut seen_ids,
            &mut seen_keys,
            &mut errs,
        );
        assert_unique_memory_allocation(
            self.integrity_progress_memory_id(),
            self.integrity_progress_stable_key(),
            format!("Canister `{}`.integrity_progress_memory", self.def().path()),
            &canister_path,
            &mut seen_ids,
            &mut seen_keys,
            &mut errs,
        );
        self.register_store_allocations(&canister_path, &mut seen_ids, &mut seen_keys, &mut errs);

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
