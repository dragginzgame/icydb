//! Test-only constructors and adapters for the parent module.

use super::*;

impl GroupedContinuationToken {
    pub(in crate::db) const fn signature(&self) -> ContinuationSignature {
        self.signature
    }

    pub(in crate::db) const fn last_group_key(&self) -> &[Value] {
        self.last_group_key.as_slice()
    }

    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    pub(in crate::db) const fn initial_offset(&self) -> u32 {
        self.initial_offset
    }
}
