//! Test-only constructors and adapters for the parent module.

use super::*;

impl CommitMarker {
    /// Borrow the exact database-wide schema-application record effect.
    #[must_use]
    pub(in crate::db) fn schema_application(&self) -> Option<&SchemaApplicationRecordOp> {
        let mut index = 0;
        while index < self.database_control.len() {
            if let DatabaseControlOp::SchemaApplication(operation) = &self.database_control[index] {
                return Some(operation);
            }
            index += 1;
        }
        None
    }

    pub(in crate::db) fn entity_source_lineage(
        &self,
    ) -> Option<&crate::db::schema::EntitySourceLineageCatalogOp> {
        self.database_control
            .iter()
            .find_map(|operation| match operation {
                DatabaseControlOp::EntitySourceLineage(operation) => Some(operation),
                DatabaseControlOp::SchemaApplication(_) | DatabaseControlOp::SchemaMigration(_) => {
                    None
                }
                DatabaseControlOp::MutationProgress(_) => None,
            })
    }
}

/// Encode one commit-marker payload in the canonical binary format.
pub(in crate::db) fn encode_commit_marker_payload(
    marker: &CommitMarker,
) -> Result<Vec<u8>, InternalError> {
    // Phase 1: size the output once so commit persistence writes one compact frame.
    let capacity = commit_marker_payload_capacity(marker);
    if capacity > u32::MAX as usize {
        return Err(InternalError::commit_marker_payload_exceeds_u32_length_limit());
    }

    // Phase 2: emit one length-delimited frame for deterministic recovery replay.
    let mut encoded = Vec::with_capacity(capacity);
    let _journal_batch_ranges = write_commit_marker_payload(&mut encoded, marker)?;

    Ok(encoded)
}

thread_local! {
    pub(super) static TEST_JOURNAL_SEQUENCE: Cell<u64> = const { Cell::new(1) };
}
