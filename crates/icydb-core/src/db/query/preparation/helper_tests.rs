//! Test-only helpers for the parent module.

use super::*;

/// Give unit-level preparation fixtures an explicit finite request, just as
/// session entrypoints do. Callers testing exhaustion supply their own root.
pub(in crate::db) fn with_preparation_work<T>(run: impl FnOnce(&PreparationWork<'_>) -> T) -> T {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        Ok(run(work))
    })
    .expect("fixture preparation fits a production request")
}
