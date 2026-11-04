use crate::{Account, Principal, Subaccount};

use proptest::prelude::*;
use std::collections::{BTreeSet, HashSet};

const OWNER: &str = "k2t6j-2nvnp-4zjm3-25dtz-6xhaa-c7boj-5gayf-oj3xs-i43lp-teztq-6ae";

#[test]
fn account_text_matches_icrc_vectors() {
    // Published ICRC-1 TextualEncoding.md vectors, independent of a ledger crate.
    let owner = OWNER.parse::<Principal>().unwrap();
    let mut one = [0; 32];
    one[31] = 1;
    let ascending = std::array::from_fn(|index| u8::try_from(index + 1).unwrap());
    for (suffix, subaccount) in [
        ("", None),
        ("-6cc627i.1", Some(Subaccount::from_array(one))),
        (
            "-dfxgiyy.102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
            Some(Subaccount::from_array(ascending)),
        ),
    ] {
        let text = format!("{OWNER}{suffix}");
        let account = Account::from_owner_and_subaccount(owner, subaccount);
        assert_eq!(account.to_string(), text);
        assert_eq!(text.parse::<Account>().unwrap(), account);
        if let Some((prefix, hex)) = text.split_once('.') {
            // The existing parser accepts uppercase hex and prints lowercase.
            assert_eq!(
                format!("{prefix}.{}", hex.to_ascii_uppercase())
                    .parse::<Account>()
                    .unwrap(),
                account
            );
        }
    }
    let default = Account::from_owner_and_subaccount(owner, Some(Subaccount::MIN));
    assert_eq!(default.to_string(), OWNER);
    // Text and the database atom share the canonical default representation.
    assert_eq!(default, Account::from(owner));
    assert_eq!(
        Account::from(owner).cmp(&default),
        std::cmp::Ordering::Equal
    );
    let widest = Account::from_owner_and_subaccount(
        Principal::from_slice(&[255; 29]),
        Some(Subaccount::MAX),
    );
    assert_eq!(widest.to_string().len(), 136);
    assert_eq!(widest.to_string().parse::<Account>().unwrap(), widest);
}

#[test]
fn account_text_rejects_malformed_or_oversized_input() {
    for suffix in [
        "-q6bn32y.",
        "-6cc627i.01",
        ".1",
        "-aaaaaaa.1",
        "-6cc627.1",
        "-6cc627i.0",
        "-6cc627i.g",
        "-6cc627i.1.1",
        "-6cc627i.é",
    ] {
        assert!(format!("{OWNER}{suffix}").parse::<Account>().is_err());
    }
    for text in [
        String::new(),
        "k2t6j2nvnp4zjm3-25dtz6xhaac7boj5gayfoj3xs-i43lp-teztq-6ae".into(),
        format!("{OWNER}-aaaaaaa.{}", "f".repeat(65)),
        "a".repeat(1_024),
    ] {
        assert!(text.parse::<Account>().is_err());
    }
}

#[test]
fn account_text_preserves_every_subaccount_width_across_owner_lengths() {
    for owner_len in [0, 1, 10, 29] {
        let owner = Principal::from_slice(&[0xa5; 29][..owner_len]);
        for first_nonzero in 0..32 {
            for byte in [1_u8, 15, 16, 255] {
                let mut bytes = [0; 32];
                bytes[first_nonzero] = byte;
                let account =
                    Account::from_owner_and_subaccount(owner, Some(Subaccount::from_array(bytes)));
                let text = account.to_string();
                let (prefix, hex) = text.split_once('.').unwrap();
                assert_eq!(hex.len(), 64 - 2 * first_nonzero - usize::from(byte < 16));
                assert!(
                    hex.bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                );
                for text in [
                    text.clone(),
                    format!("{prefix}.{}", hex.to_ascii_uppercase()),
                ] {
                    let parsed = text.parse::<Account>().unwrap();
                    assert_eq!(parsed.owner(), owner);
                    assert_eq!(parsed.subaccount().unwrap().to_bytes(), bytes);
                    assert_eq!(parsed.to_string(), account.to_string());
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn account_text_roundtrips_bounded_components(
        owner_bytes in prop::collection::vec(any::<u8>(), 0..=29),
        subaccount_bytes in any::<[u8; 32]>(),
    ) {
        let owner = Principal::from_slice(&owner_bytes);
        let subaccount = Subaccount::from_array(subaccount_bytes);
        let account = Account::from_owner_and_subaccount(owner, Some(subaccount));
        let text = account.to_string();
        prop_assert!(text.len() <= 136);
        let parsed = text.parse::<Account>().unwrap();
        prop_assert_eq!(parsed.owner(), owner);
        prop_assert_eq!(parsed.subaccount().unwrap_or(Subaccount::MIN), subaccount);
        prop_assert_eq!(Account::from(owner).to_string().parse::<Account>().unwrap(), Account::from(owner));
    }
}

#[test]
fn platform_conversions_canonicalize_default_and_preserve_nonzero_components() {
    let owner_bytes = [0xa5; 29];
    let mut last_byte = [0; 32];
    last_byte[31] = 1;
    for owner_len in 0..=owner_bytes.len() {
        for supplied in [
            None,
            Some([0; 32]),
            Some(last_byte),
            Some([1; 32]),
            Some([255; 32]),
        ] {
            let canonical = supplied.filter(|bytes| *bytes != [0; 32]);
            let platform = ic_platform_types::Account {
                owner: ic_platform_types::Principal::from_slice(&owner_bytes[..owner_len]),
                subaccount: supplied,
            };
            let database = Account::from(platform);
            assert_eq!(database.owner().as_slice(), platform.owner.as_slice());
            assert_eq!(database.subaccount().map(Subaccount::to_bytes), canonical);
            let restored = ic_platform_types::Account::from(database);
            assert_eq!(restored.owner, platform.owner);
            assert_eq!(restored.subaccount, canonical);
            assert_eq!(restored, platform);
            // Independent canonical Postcard frame: owner bytes, option, then
            // bounded fixed bytes only for a nonzero subaccount.
            let mut expected = vec![u8::try_from(owner_len).unwrap()];
            expected.extend_from_slice(&owner_bytes[..owner_len]);
            if let Some(bytes) = canonical {
                expected.extend_from_slice(&[1, 32]);
                expected.extend_from_slice(&bytes);
            } else {
                expected.push(0);
            }
            let wire = crate::wire::encode_one(database).unwrap();
            assert_eq!(wire, expected);
            assert_eq!(wire, crate::wire::encode_one(restored).unwrap());
            assert_eq!(crate::wire::decode_one::<Account>(&wire).unwrap(), database);
            let supplied_wire = crate::wire::encode_one(platform).unwrap();
            assert_eq!(
                crate::wire::decode_one::<Account>(&supplied_wire).unwrap(),
                database
            );
            let json = serde_json::to_value(platform).unwrap();
            assert_eq!(serde_json::from_value::<Account>(json).unwrap(), database);
            assert_eq!(database.to_string().parse::<Account>().unwrap(), database);
            for length in 0..wire.len() {
                assert!(crate::wire::decode_one::<Account>(&wire[..length]).is_err());
            }
        }
    }
}

#[test]
fn database_and_platform_share_default_account_identity() {
    let owner = OWNER.parse::<Principal>().unwrap();
    let absent = Account::from(owner);
    let zero = Account::from_owner_and_subaccount(owner, Some(Subaccount::MIN));
    let accounts = [absent, zero];
    assert_eq!(accounts.into_iter().collect::<HashSet<_>>().len(), 1);
    assert_eq!(accounts.into_iter().collect::<BTreeSet<_>>().len(), 1);
    let platform = accounts.map(ic_platform_types::Account::from);
    assert_eq!(platform.into_iter().collect::<HashSet<_>>().len(), 1);
    assert_eq!(platform.into_iter().collect::<BTreeSet<_>>().len(), 1);
    assert_eq!(platform.map(Account::from), accounts);
}
