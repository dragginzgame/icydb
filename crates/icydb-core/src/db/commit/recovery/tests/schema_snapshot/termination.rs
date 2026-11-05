//! Native replay/fold contradictions terminate after successful semantic preflight.

use super::*;
use crate::db::commit::recovery::apply_preflighted_fold;
use std::{os::unix::process::ExitStatusExt, process::Command};

#[test]
fn prepared_catalog_handoff_contradictions_abort_in_replay_and_fold() {
    const CASE_ENV: &str = "ICYDB_TEST_CATALOG_APPLY_TERMINATION";
    if let Some(case) = std::env::var_os(CASE_ENV) {
        let (mode, fault) = case.to_str().unwrap().split_once('/').unwrap();
        let mode = match mode {
            "replay" => JournalRecordApplyMode::Replay,
            "fold" => JournalRecordApplyMode::Fold,
            _ => panic!("unknown child mode"),
        };
        let (db, candidate, batch) = catalog_publication_fixture();
        let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
        let mut prepared = match mode {
            JournalRecordApplyMode::Replay => {
                prepare_replayed_journal_batch(&db, STORE, handle, &batch).unwrap()
            }
            JournalRecordApplyMode::Fold => {
                let mut prepared = vec![None];
                validate_journal_batch_records(&db, STORE, handle, &batch, mode, &mut prepared)
                    .unwrap();
                prepared
            }
        };
        assert!(matches!(
            (&prepared[0], mode),
            (
                Some(PreparedJournalOp::AcceptedSchemaReplay { .. }),
                JournalRecordApplyMode::Replay
            ) | (
                Some(PreparedJournalOp::AcceptedSchemaFold(_)),
                JournalRecordApplyMode::Fold
            )
        ));
        // Damage only the already-prepared handoff. A preflight rejection or
        // ordinary test panic cannot satisfy the parent's SIGABRT assertion.
        let apply_mode = match fault {
            "control" => mode,
            "missing" => {
                prepared[0] = None;
                mode
            }
            "truncated" => {
                prepared.clear();
                mode
            }
            "wrong-mode" => match mode {
                JournalRecordApplyMode::Replay => JournalRecordApplyMode::Fold,
                JournalRecordApplyMode::Fold => JournalRecordApplyMode::Replay,
            },
            _ => panic!("unknown child fault"),
        };
        match apply_mode {
            JournalRecordApplyMode::Replay => {
                apply_prepared_journal_batch(STORE, handle, &batch, &mut prepared, apply_mode);
            }
            JournalRecordApplyMode::Fold => {
                apply_preflighted_fold(STORE, handle, &batch, &mut prepared, None);
            }
        }
        if mode == JournalRecordApplyMode::Fold {
            SCHEMA
                .with_borrow_mut(SchemaStore::reset_journaled_live_projection)
                .unwrap();
        }
        verify_recovered_accepted_schema(
            &db,
            STORE,
            candidate.encoded_bundle(),
            candidate.encoded_root(),
            &mut BTreeSet::new(),
        )
        .unwrap();
        return;
    }

    let name = concat!(
        module_path!(),
        "::prepared_catalog_handoff_contradictions_abort_in_replay_and_fold"
    );
    let name = name.strip_prefix("icydb_core::").unwrap();
    for mode in ["replay", "fold"] {
        for fault in ["control", "missing", "truncated", "wrong-mode"] {
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env(CASE_ENV, format!("{mode}/{fault}"))
                .output()
                .unwrap();
            if fault == "control" {
                assert!(output.status.success(), "{mode}/{fault}: {output:?}");
            } else {
                // SIGABRT is 6 on supported Linux and macOS hosts; a returned
                // error, test panic or missing test must never qualify this path.
                assert_eq!(
                    output.status.signal(),
                    Some(6),
                    "{mode}/{fault}: {output:?}"
                );
            }
        }
    }
}
