//! Reverse-key framing agrees with the general index codec across accepted identities.

use super::*;
use crate::{
    db::{
        index::{IndexKey, IndexKeyKind, RawIndexStoreKey},
        relation::reverse_index::{relation_id_component, reverse_index_id_for_relation},
    },
    error::InternalError,
    types::{Account, Principal, Subaccount, Timestamp, U256, Ulid},
};

fn primary_keys() -> Vec<PrimaryKeyValue> {
    let account = Account::from_owner_and_subaccount(Principal::MAX, Some(Subaccount::MAX));
    let mut keys = [
        PrimaryKeyComponent::Unit,
        PrimaryKeyComponent::Nat64(0),
        PrimaryKeyComponent::Nat64(u64::MAX),
        PrimaryKeyComponent::Int64(i64::MIN),
        PrimaryKeyComponent::Int64(i64::MAX),
        PrimaryKeyComponent::Nat128(u128::MAX),
        PrimaryKeyComponent::Int128(i128::MIN),
        PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-42)),
        PrimaryKeyComponent::Ulid(Ulid::from_bytes([0xff; 16])),
        PrimaryKeyComponent::Principal(Principal::anonymous()),
        PrimaryKeyComponent::Principal(Principal::MAX),
        PrimaryKeyComponent::Subaccount(Subaccount::MAX),
        PrimaryKeyComponent::Account(Account::from_owner_and_subaccount(Principal::MAX, None)),
        PrimaryKeyComponent::Account(account),
        PrimaryKeyComponent::U256(U256::MAX),
    ]
    .into_iter()
    .map(PrimaryKeyValue::Scalar)
    .collect::<Vec<_>>();
    for components in [
        vec![
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Ulid(Ulid::from_bytes([9; 16])),
        ],
        vec![PrimaryKeyComponent::Account(account); 4],
        vec![
            PrimaryKeyComponent::Account(account),
            PrimaryKeyComponent::Account(account),
            PrimaryKeyComponent::Account(account),
            PrimaryKeyComponent::Subaccount(Subaccount::MAX),
        ],
    ] {
        keys.push(PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(&components).unwrap(),
        ));
    }
    keys
}

fn general_index_key(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target: &PrimaryKeyValue,
    source_key: &PrimaryKeyValue,
) -> Result<RawIndexStoreKey, InternalError> {
    let target = EncodedPrimaryKey::encode(*target)
        .map_err(|_| InternalError::persisted_row_decode_corruption())?;
    let relation_id = relation_id_component(relation);
    Ok(IndexKey::new_from_components_with_primary_key_value(
        &reverse_index_id_for_relation(source, relation),
        IndexKeyKind::System,
        &[relation_id.as_slice(), target.as_bytes()],
        source_key,
    )?
    .to_raw()?)
}

#[test]
fn reverse_relation_raw_framing_matches_general_keys_and_prefix_bounds() {
    let source = ReverseRelationSourceInfo {
        path: "Source".into(),
        entity_tag: EntityTag::new(u64::MAX),
    };
    let keys = primary_keys();
    let mut refusals = 0;
    for relation_id in [1, u32::MAX] {
        for generation in [0, u64::MAX] {
            let mut relation = relation(5, AcceptedFieldKind::Nat64);
            relation.reverse_identity = AcceptedRelationReverseIdentity::new(
                RelationId::new(relation_id).unwrap(),
                generation,
            );
            for target in &keys {
                let (lower, upper) = reverse_index_key_bounds_for_target_primary_key_value(
                    &source, &relation, target,
                )
                .unwrap()
                .unwrap();
                for source_key in &keys {
                    let actual = reverse_index_key_for_target_and_source_primary_key_value(
                        &source, &relation, target, source_key,
                    );
                    let expected = general_index_key(&source, &relation, target, source_key);
                    match (actual, expected) {
                        (Ok(Some(actual)), Ok(expected)) => {
                            assert_eq!(actual.as_bytes(), expected.as_bytes());
                            assert!(actual >= lower && actual < upper);
                            let decoded = actual.decode().unwrap();
                            assert_eq!(decoded.key_kind(), IndexStoreKeyKind::System);
                            assert_eq!(decoded.index_id().generation(), generation);
                            assert_eq!(decoded.primary_key().decode().unwrap(), *source_key);
                        }
                        (Err(actual), Err(expected)) => {
                            assert_eq!(actual.diagnostic_code(), expected.diagnostic_code());
                            assert_eq!(actual.origin(), expected.origin());
                            refusals += 1;
                        }
                        other => panic!("reverse/general framing disagrees: {other:?}"),
                    }
                }
            }
        }
    }
    assert!(
        refusals > 0,
        "the maximum identity pair must exercise the key bound"
    );
}

#[test]
fn reverse_relation_raw_framing_retains_complete_key_size_refusal() {
    let source = ReverseRelationSourceInfo {
        path: "Source".into(),
        entity_tag: EntityTag::new(9),
    };
    let relation = relation(5, AcceptedFieldKind::Nat64);
    let account = PrimaryKeyComponent::Account(Account::from_owner_and_subaccount(
        Principal::MAX,
        Some(Subaccount::MAX),
    ));
    let target = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[account; 4]).unwrap(),
    );
    let source_key = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            account,
            account,
            account,
            PrimaryKeyComponent::Subaccount(Subaccount::MAX),
        ])
        .unwrap(),
    );
    let admitted = reverse_index_key_for_target_and_source_primary_key_value(
        &source,
        &relation,
        &target,
        &source_key,
    )
    .unwrap()
    .unwrap();
    assert_eq!(admitted.as_bytes().len(), 508);

    let error = reverse_index_key_for_target_and_source_primary_key_value(
        &source, &relation, &target, &target,
    )
    .unwrap_err();
    let expected = general_index_key(&source, &relation, &target, &target).unwrap_err();
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.origin(), expected.origin());
    assert_eq!(
        error.diagnostic_code(),
        InternalError::index_unsupported().diagnostic_code()
    );
}
