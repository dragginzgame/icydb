//! Test-only constructors and adapters for the parent module.

use super::*;

impl JournalBatch {
    pub(in crate::db) fn new(
        batch_id: JournalBatchId,
        commit_marker_id: JournalCommitMarkerId,
        journal_sequence: JournalSequence,
        records: Vec<JournalRecord>,
    ) -> Result<Self, InternalError> {
        Self::new_with_database_commit_sequence(
            batch_id,
            commit_marker_id,
            journal_sequence,
            DatabaseCommitSequence::new(journal_sequence.get().max(1)),
            records,
        )
    }
}

#[test]
fn schema_publication_record_keeps_current_little_endian_wire_bytes() {
    // Freeze a record layout independently of the decoder and sizing sink.
    let record = JournalRecord::AcceptedSchemaPublish {
        store_path: "s".into(),
        expected_revision: AcceptedSchemaRevision::new(7),
        schema_bundle_bytes: vec![8, 9],
        schema_root_bytes: vec![10],
    };
    let expected = [
        4, 1, 0, 0, 0, b's', 7, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 9, 1, 0, 0, 0, 10,
    ];
    let mut bytes = Vec::new();
    write_journal_record(&mut JournalRecordOutput::Bytes(&mut bytes), &record).unwrap();
    assert_eq!(bytes, expected);
    assert_eq!(journal_record_payload_len(&record), expected.len());
}
