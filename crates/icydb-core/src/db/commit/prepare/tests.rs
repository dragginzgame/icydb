mod images;

use super::{CommitPrepareMode, decode_canonical_commit_key};
use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey},
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
    },
    error::InternalError,
    types::EntityTag,
};

#[test]
fn canonical_commit_keys_preserve_identity_and_refusal_classification() {
    let tag = EntityTag::new(7);
    let keys = [
        PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(0)),
        PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(u64::MAX)),
        PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(&[
                PrimaryKeyComponent::Nat64(7),
                PrimaryKeyComponent::Nat64(9),
            ])
            .unwrap(),
        ),
    ];
    let expected = InternalError::query_executor_invariant();
    for key in keys {
        let raw = DecodedDataStoreKey::new(tag, &key).to_raw().unwrap();
        let decoded = decode_canonical_commit_key(&raw, tag).unwrap();
        assert_eq!(decoded.entity_tag(), tag);
        assert_eq!(decoded.primary_key_value(), key);
        assert_eq!(decoded.to_raw().unwrap(), raw);

        let mut invalid = (0..raw.as_bytes().len())
            .map(|length| RawDataStoreKey::from_store_range_bound(&raw.as_bytes()[..length]))
            .collect::<Vec<_>>();
        let mut trailing = raw.as_bytes().to_vec();
        trailing.push(0);
        invalid.push(RawDataStoreKey::from_store_range_bound(&trailing));
        invalid.push(
            DecodedDataStoreKey::new(EntityTag::new(8), &key)
                .to_raw()
                .unwrap(),
        );
        for raw in invalid {
            let error = decode_canonical_commit_key(&raw, tag).unwrap_err();
            assert_eq!(error.class(), expected.class());
            assert_eq!(error.origin(), expected.origin());
            assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
        }
    }
}

#[test]
fn commit_prepare_modes_separate_normal_admission_from_replay_and_rebuild() {
    assert!(CommitPrepareMode::NormalWrite.validate_relation_targets());
    assert!(CommitPrepareMode::NormalWrite.include_candidate_relation_effects());

    assert!(!CommitPrepareMode::RecoveryReplay.validate_relation_targets());
    assert!(CommitPrepareMode::RecoveryReplay.include_candidate_relation_effects());

    assert!(!CommitPrepareMode::DerivedRebuild.validate_relation_targets());
    assert!(!CommitPrepareMode::DerivedRebuild.include_candidate_relation_effects());
}
