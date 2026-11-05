//! Test-only constructors and adapters for the parent module.

use super::*;

impl<C: CanisterKind> DbSession<C> {
    /// Commit one complete accepted update page and its exact durable progress successor.
    pub(in crate::db::session) fn execute_accepted_structural_update_with_mutation_progress(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
        mutation_progress: MutationProgressRecordOp,
    ) -> Result<usize, InternalError> {
        let mutation_capacity = mutations.len();
        let mut mutations = mutations.into_iter();
        execute_accepted_structural_mutation_batch(
            &self.db,
            catalog,
            mutation_capacity,
            0,
            || {
                Ok(mutations
                    .next()
                    .map(|mutation| AcceptedStructuralMutationBatchItem {
                        catalog: catalog.clone(),
                        mutation,
                    }))
            },
            operation_timestamp,
            AcceptedStructuralMutationCommitOptions::with_mutation_progress(),
            |rows, _report| {
                Ok((
                    rows.len(),
                    AcceptedStructuralMutationCommitDirective::WithMutationProgress(
                        mutation_progress,
                    ),
                ))
            },
        )
    }
}
