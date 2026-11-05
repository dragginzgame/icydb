//! Test-only constructors and adapters for the parent module.

use super::*;

impl KernelRow {
    pub(in crate::db) fn slot(&self, slot: usize) -> Option<Value> {
        self.slot_ref(slot).cloned()
    }
}
