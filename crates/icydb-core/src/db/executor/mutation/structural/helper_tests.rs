//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedStructuralMutationCommitOptions {
    pub(in crate::db) const fn with_mutation_progress() -> Self {
        Self {
            capture_output_values: false,
            packing: AcceptedStructuralMutationPacking::Complete,
        }
    }
}
