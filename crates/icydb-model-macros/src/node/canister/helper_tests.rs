//! Test-only constructors and adapters for the parent module.

use super::*;

impl Canister {
    pub(super) fn commit_stable_key(&self) -> String {
        stable_memory_key(&self.memory_namespace, "commit", "control")
    }

    pub(super) fn integrity_progress_stable_key(&self) -> String {
        stable_memory_key(&self.memory_namespace, "integrity", "progress")
    }

    pub(super) fn startup_stable_key(&self) -> String {
        stable_memory_key(&self.memory_namespace, "startup", "control")
    }
}

pub(super) fn stable_memory_key(memory_namespace: &str, allocation: &str, role: &str) -> String {
    format!("icydb.{memory_namespace}.{allocation}.{role}.v1")
}
