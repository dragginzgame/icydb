//! Frozen mutation time reaches durable marker and journal identities.

use super::*;

#[test]
fn frozen_write_timestamp_reaches_standard_and_progress_commit_ids() {
    for (with_progress, millis) in [(false, 17), (false, -17), (true, 0), (true, i64::MAX)] {
        std::thread::spawn(move || {
            let session = initialize_journaled();
            let catalog = session
                .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                .unwrap();
            let timestamp = Timestamp::from_millis(millis);
            if with_progress {
                let (before, after, operation) = atomic_progress_fixture(91);
                with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
                    store.insert_mutation(&before)
                })
                .unwrap();
                session
                    .execute_accepted_structural_update_with_mutation_progress(
                        &catalog,
                        batch(&[7]),
                        timestamp,
                        operation,
                    )
                    .unwrap();
                assert_eq!(
                    with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
                        store.load_mutation(before.state().job_id)
                    })
                    .unwrap(),
                    after,
                );
            } else {
                session
                    .execute_accepted_structural_save_batch(
                        &catalog,
                        true,
                        batch(&[7]),
                        timestamp,
                        Ok,
                    )
                    .unwrap();
            }
            let journal = JOURNALED_TAIL_STORE.with(|tail| {
                tail.borrow()
                    .next_batch_after(JournalSequence::new(0))
                    .unwrap()
                    .unwrap()
            });
            assert_eq!(&journal.commit_marker_id()[..8], &millis.to_be_bytes());
            assert_eq!(journal.batch_id(), journal.commit_marker_id());
            assert!(
                journal
                    .records()
                    .iter()
                    .any(|record| matches!(record, JournalRecord::RowPut { .. }))
            );
            assert_dynamic_payload(&session, 1, 7);

            // Recovery consumes the same identities and row bytes after an upgrade.
            forget_recovered_domain_for_tests(&session.db).unwrap();
            drive_journaled_recovery_to_completion(&session);
            assert_dynamic_payload(&session, 1, 7);
        })
        .join()
        .unwrap();
    }
}
