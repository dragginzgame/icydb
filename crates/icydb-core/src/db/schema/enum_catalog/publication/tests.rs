//! Candidate admission owns immutable integrity; publication checks current roots.

use super::{
    AcceptedSchemaBundleKey, AcceptedSchemaPublicationError, AcceptedSchemaRevision,
    CandidateSchemaRevision, empty_accepted_schema_candidate_for_tests,
    encode_accepted_schema_root, hash_bytes, prepare_accepted_schema_root_publication,
};
use crate::error::ErrorClass;

#[test]
fn candidate_constructors_reject_invalid_contents_before_publication() {
    let candidate = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::INITIAL,
    );
    let mut invalid_bundle = candidate.bundle().clone();
    invalid_bundle.store_path.clear();
    assert!(CandidateSchemaRevision::new(invalid_bundle).is_err());

    let mut malformed_bundle = candidate.encoded_bundle().to_vec();
    malformed_bundle[0] ^= 1;
    let mut matching_hash_root = candidate.root();
    matching_hash_root.bundle_hash = hash_bytes(&malformed_bundle);
    let mut wrong_fingerprint = candidate.root();
    wrong_fingerprint.fingerprint.0[0] ^= 1;
    let mut wrong_revision = candidate.root();
    wrong_revision.revision = AcceptedSchemaRevision::new(2);
    wrong_revision.bundle_key = AcceptedSchemaBundleKey::new(wrong_revision.revision).unwrap();
    let mut corrupt_root = candidate.encoded_root().to_vec();
    corrupt_root[0] ^= 1;
    let other =
        empty_accepted_schema_candidate_for_tests("test::Other", AcceptedSchemaRevision::INITIAL);
    for (bundle, root) in [
        (
            malformed_bundle,
            encode_accepted_schema_root(matching_hash_root).unwrap(),
        ),
        (
            candidate.encoded_bundle().to_vec(),
            encode_accepted_schema_root(wrong_fingerprint).unwrap(),
        ),
        (
            candidate.encoded_bundle().to_vec(),
            encode_accepted_schema_root(wrong_revision).unwrap(),
        ),
        (candidate.encoded_bundle().to_vec(), corrupt_root),
        (candidate.encoded_bundle().to_vec(), vec![1, 2, 3]),
        (
            other.encoded_bundle().to_vec(),
            candidate.encoded_root().to_vec(),
        ),
    ] {
        let error = CandidateSchemaRevision::from_encoded(bundle, root).unwrap_err();
        assert_eq!(error.class(), ErrorClass::Corruption);
    }
}

#[test]
fn constructed_and_decoded_candidates_publish_identical_root_bytes() {
    let initial = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::INITIAL,
    );
    let bootstrap = prepare_accepted_schema_root_publication(
        [None, None],
        AcceptedSchemaRevision::NONE,
        &initial,
    )
    .unwrap();
    assert_eq!(bootstrap.target_slot(), 0);
    assert_eq!(bootstrap.encoded_root(), initial.encoded_root());
    let second = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::new(2),
    );
    let decoded = CandidateSchemaRevision::from_encoded(
        second.encoded_bundle().to_vec(),
        second.encoded_root().to_vec(),
    )
    .unwrap();
    assert_eq!(decoded.bundle(), second.bundle());
    for candidate in [&second, &decoded] {
        let publication = prepare_accepted_schema_root_publication(
            [Some(initial.encoded_root()), None],
            AcceptedSchemaRevision::INITIAL,
            candidate,
        )
        .unwrap();
        assert_eq!(publication.target_slot(), 1);
        assert_eq!(publication.encoded_root(), second.encoded_root());
    }
}

#[test]
fn publication_rejects_stale_wrong_next_and_exhausted_revisions() {
    let initial = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::INITIAL,
    );
    let third = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::new(3),
    );
    let slots = [Some(initial.encoded_root()), None];
    assert_eq!(
        prepare_accepted_schema_root_publication(slots, AcceptedSchemaRevision::NONE, &third),
        Err(AcceptedSchemaPublicationError::StaleSchemaRevision {
            expected: AcceptedSchemaRevision::NONE,
            found: AcceptedSchemaRevision::INITIAL,
        })
    );
    assert_eq!(
        prepare_accepted_schema_root_publication(slots, AcceptedSchemaRevision::INITIAL, &third),
        Err(AcceptedSchemaPublicationError::InvalidCandidate)
    );
    let terminal = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::new(u64::MAX),
    );
    assert_eq!(
        prepare_accepted_schema_root_publication(
            [Some(terminal.encoded_root()), None],
            terminal.revision(),
            &third,
        ),
        Err(AcceptedSchemaPublicationError::RevisionExhausted)
    );
}

#[test]
fn publication_keeps_torn_slot_recovery_and_rejects_conflicting_roots() {
    let initial = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::INITIAL,
    );
    let second = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::new(2),
    );
    let torn: &[u8] = &[1, 2, 3];
    for (slots, target_slot) in [
        ([Some(initial.encoded_root()), Some(torn)], 1),
        ([Some(torn), Some(initial.encoded_root())], 0),
    ] {
        let publication = prepare_accepted_schema_root_publication(
            slots,
            AcceptedSchemaRevision::INITIAL,
            &second,
        )
        .unwrap();
        assert_eq!(publication.target_slot(), target_slot);
    }
    let conflicting = empty_accepted_schema_candidate_for_tests(
        "test::Conflicting",
        AcceptedSchemaRevision::INITIAL,
    );
    for slots in [
        [Some(torn), None],
        [Some(torn), Some(torn)],
        [
            Some(initial.encoded_root()),
            Some(conflicting.encoded_root()),
        ],
    ] {
        assert_eq!(
            prepare_accepted_schema_root_publication(
                slots,
                AcceptedSchemaRevision::INITIAL,
                &second,
            ),
            Err(AcceptedSchemaPublicationError::CorruptRootSlots)
        );
    }
}
