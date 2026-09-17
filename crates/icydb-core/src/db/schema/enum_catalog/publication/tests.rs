//! Candidate admission owns immutable integrity; publication checks current roots.

mod projection;

use super::{
    ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET, AcceptedSchemaBundleKey, AcceptedSchemaPublicationError,
    AcceptedSchemaRevision, CandidateSchemaRevision, decode_accepted_schema_root,
    empty_accepted_schema_candidate_for_tests, encode_accepted_schema_root, hash_bytes,
    prepare_accepted_schema_root_publication, select_current_accepted_schema_root,
};
use crate::{
    db::{database_format::crc32c, executor::budget::MaintenanceConstructionBudget},
    error::{ErrorClass, InternalError},
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};

#[test]
fn root_selection_preserves_corrupt_slot_and_field_rejection() {
    let candidate = empty_accepted_schema_candidate_for_tests(
        "test::RootSelection",
        AcceptedSchemaRevision::INITIAL,
    );
    let valid = candidate.encoded_root();
    assert_eq!(
        decode_accepted_schema_root(valid).unwrap(),
        candidate.root()
    );
    assert!(
        select_current_accepted_schema_root([None, None])
            .unwrap()
            .is_none()
    );
    let mut malformed = vec![Vec::new(), valid[..valid.len() - 1].to_vec()];
    // Corrupt each byte, including magic, version, fields and checksum.
    for offset in 0..valid.len() {
        let mut bytes = valid.to_vec();
        bytes[offset] ^= 1;
        malformed.push(bytes);
    }
    // A valid checksum must not bypass revision/bundle-key consistency.
    for offset in [10, 50] {
        let mut bytes = valid.to_vec();
        bytes[offset..offset + size_of::<u64>()].fill(0);
        let checksum = crc32c(&bytes[..ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET]);
        bytes[ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
        malformed.push(bytes);
    }
    for bytes in malformed {
        assert!(decode_accepted_schema_root(&bytes).is_err());
        for slots in [
            [Some(valid), Some(bytes.as_slice())],
            [Some(bytes.as_slice()), Some(valid)],
        ] {
            assert_eq!(
                select_current_accepted_schema_root(slots)
                    .unwrap()
                    .unwrap()
                    .root(),
                candidate.root(),
            );
        }
        for slots in [
            [Some(bytes.as_slice()), None],
            [Some(bytes.as_slice()), Some(bytes.as_slice())],
        ] {
            assert_eq!(
                select_current_accepted_schema_root(slots)
                    .unwrap_err()
                    .diagnostic_code(),
                InternalError::store_corruption().diagnostic_code(),
            );
        }
    }
}

#[test]
fn root_selection_checks_checksum_before_reporting_unsupported_format() {
    let candidate = empty_accepted_schema_candidate_for_tests(
        "test::RootFormat",
        AcceptedSchemaRevision::INITIAL,
    );
    let valid = candidate.encoded_root();
    let mut unsupported = valid.to_vec();
    unsupported[8..10].copy_from_slice(&0_u16.to_be_bytes());
    let incompatible = InternalError::serialize_incompatible_persisted_format().diagnostic_code();
    // Strict decoding rejects the version first; slot selection treats an
    // unverified version as torn bytes and can still use the other valid slot.
    assert_eq!(
        decode_accepted_schema_root(&unsupported)
            .unwrap_err()
            .diagnostic_code(),
        incompatible
    );
    for slots in [
        [Some(valid), Some(unsupported.as_slice())],
        [Some(unsupported.as_slice()), Some(valid)],
    ] {
        assert_eq!(
            select_current_accepted_schema_root(slots)
                .unwrap()
                .unwrap()
                .root(),
            candidate.root()
        );
    }
    let checksum = crc32c(&unsupported[..ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET]);
    unsupported[ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
    assert_eq!(
        decode_accepted_schema_root(&unsupported)
            .unwrap_err()
            .diagnostic_code(),
        incompatible
    );
    for slots in [
        [Some(valid), Some(unsupported.as_slice())],
        [Some(unsupported.as_slice()), Some(valid)],
    ] {
        assert_eq!(
            select_current_accepted_schema_root(slots)
                .unwrap_err()
                .diagnostic_code(),
            incompatible
        );
    }
}

#[test]
fn candidate_preparation_shares_admission_across_verification_and_identity() {
    let candidate = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::INITIAL,
    );
    let path_visits = 1 + candidate.store_path().len() as u64;
    let bundle_visits = candidate.encoded_bundle().len() as u64;
    let all_visits = path_visits + 2 * bundle_visits + candidate.encoded_root().len() as u64;

    // Admit the path and first wire pass, but not the next pass. A separate
    // allowance for hashing/root creation would incorrectly admit this input.
    for limit in [path_visits + bundle_visits, all_visits - 1] {
        let work = MaintenanceConstructionBudget::with_limit_for_tests(
            Resource::PredicateExpressionSteps,
            limit,
        );
        let error =
            CandidateSchemaRevision::prepare(candidate.bundle().clone(), &work).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw(),
        )));
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::Limit, limit))
        );
        // The failed operation stays failed; a second segment cannot obtain a
        // publishable candidate by restarting its local counter.
        let repeated =
            CandidateSchemaRevision::prepare(candidate.bundle().clone(), &work).unwrap_err();
        assert_eq!(repeated.diagnostic(), error.diagnostic());
    }

    let work = MaintenanceConstructionBudget::with_limit_for_tests(
        Resource::PredicateExpressionSteps,
        all_visits,
    );
    let admitted = CandidateSchemaRevision::prepare(candidate.bundle().clone(), &work).unwrap();
    assert_eq!(admitted.bundle(), candidate.bundle());
    assert_eq!(admitted.encoded_bundle(), candidate.encoded_bundle());
    assert_eq!(admitted.root(), candidate.root());
    assert_eq!(admitted.encoded_root(), candidate.encoded_root());
    // Admission does not alter the independent persisted reconstruction path.
    let restored = CandidateSchemaRevision::from_encoded(
        admitted.encoded_bundle().to_vec(),
        admitted.encoded_root().to_vec(),
    )
    .unwrap();
    assert_eq!(restored.root(), candidate.root());
}

#[test]
fn candidate_preparation_preserves_resource_and_semantic_rejections() {
    let candidate = empty_accepted_schema_candidate_for_tests(
        "test::Candidate",
        AcceptedSchemaRevision::INITIAL,
    );
    let error = CandidateSchemaRevision::prepare(
        candidate.bundle().clone(),
        &MaintenanceConstructionBudget::with_limit_for_tests(Resource::TemporaryBytes, 0),
    )
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
    let mut invalid = candidate.bundle().clone();
    invalid.store_path.clear();
    let error = CandidateSchemaRevision::prepare(invalid, &MaintenanceConstructionBudget::new())
        .unwrap_err();
    assert_eq!(error.class(), ErrorClass::InvariantViolation);
    assert!(
        !error
            .diagnostic_facts()
            .iter()
            .any(|(tag, _)| *tag == DiagnosticFactTag::BudgetResource)
    );
}

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
