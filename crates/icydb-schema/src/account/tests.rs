use crate::{Account, Principal, Subaccount};

use proptest::prelude::*;

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
    // Text omits the default subaccount; the native component remains explicit.
    assert_ne!(default, Account::from(owner));
    assert!(Account::from(owner) < default);
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
