//! Operation-local accepted setup reuse and failed-resolution boundaries.

use super::*;
use crate::db::commit::{CommitPrepareContextCache, CommitPrepareMode};

#[test]
fn preparation_context_reuses_success_without_crossing_entity_or_schema_identity() {
    let session = initialize_journaled_multi_entity();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("accepted catalog should resolve");
    let fingerprint = catalog.fingerprint();
    let mut contexts = CommitPrepareContextCache::new(CommitPrepareMode::DerivedRebuild);
    let first = contexts
        .get_or_prepare(ENTITY_SOURCE, fingerprint, |mode| {
            assert!(matches!(mode, CommitPrepareMode::DerivedRebuild));
            session
                .db
                .accepted_runtime_entity_for_path(ENTITY_SOURCE)?
                .prepare_commit_context(&session.db, fingerprint, mode)
        })
        .expect("first entity setup should resolve");
    assert_eq!(first.entity_tag(), ENTITY_TAG);
    assert_eq!(
        contexts
            .get_or_prepare(ENTITY_SOURCE, fingerprint, |_| {
                panic!("successful setup should be reused within this operation")
            })
            .expect("matching identity should reuse setup")
            .entity_tag(),
        ENTITY_TAG,
    );

    let mut changed_fingerprint = fingerprint;
    changed_fingerprint[0] ^= 1;
    let changed = contexts.get_or_prepare(ENTITY_SOURCE, changed_fingerprint, |_| {
        Err(InternalError::store_corruption())
    });
    assert_eq!(
        changed
            .err()
            .expect("a changed schema must resolve again")
            .class(),
        ErrorClass::Corruption,
    );

    let second_catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(SECOND_ENTITY_NAME))
        .expect("second accepted catalog should resolve");
    let second_fingerprint = second_catalog.fingerprint();
    let second = contexts
        .get_or_prepare(SECOND_ENTITY_SOURCE, second_fingerprint, |mode| {
            session
                .db
                .accepted_runtime_entity_for_path(SECOND_ENTITY_SOURCE)?
                .prepare_commit_context(&session.db, second_fingerprint, mode)
        })
        .expect("another entity needs its own accepted setup");
    assert_eq!(second.entity_tag(), SECOND_ENTITY_TAG);
    assert_eq!(
        contexts
            .get_or_prepare(ENTITY_SOURCE, fingerprint, |_| {
                panic!("interleaving entities must retain the first setup")
            })
            .expect("the first entity should remain reusable")
            .entity_tag(),
        ENTITY_TAG,
    );
}

#[test]
fn preparation_context_does_not_cache_failure_or_survive_an_operation() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("accepted catalog should resolve");
    let fingerprint = catalog.fingerprint();
    let resolutions = Cell::new(0);
    for _ in 0..2 {
        let mut contexts = CommitPrepareContextCache::new(CommitPrepareMode::DerivedRebuild);
        for _ in 0..2 {
            let error = contexts
                .get_or_prepare(ENTITY_SOURCE, fingerprint, |_| {
                    resolutions.set(resolutions.get() + 1);
                    Err(InternalError::store_corruption())
                })
                .err()
                .expect("failed setup should reject");
            assert_eq!(error.class(), ErrorClass::Corruption);
        }
        contexts
            .get_or_prepare(ENTITY_SOURCE, fingerprint, |mode| {
                resolutions.set(resolutions.get() + 1);
                session
                    .db
                    .accepted_runtime_entity_for_path(ENTITY_SOURCE)?
                    .prepare_commit_context(&session.db, fingerprint, mode)
            })
            .expect("failed attempts must not poison a subsequent resolution");
    }
    assert_eq!(resolutions.get(), 6);
}
