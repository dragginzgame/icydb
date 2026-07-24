use super::*;

const PRINCIPAL_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
const SUBACCOUNT_LEN: usize = 32;
const TAG_SUBACCOUNT: u8 = 0x80;
const LEN_MASK: u8 = 0x7F;

fn principal() -> Principal {
    Principal::from_text("rrkah-fqaaa-aaaaa-aaaaq-cai").unwrap()
}

#[test]
fn storable_bytes_are_exact_size() {
    let account = Account::from_owner_and_subaccount(Principal::MAX, Some(Subaccount::MAX));
    let bytes = account.to_bytes().expect("account encode");
    let size = bytes.len();

    assert!(
        size == Account::STORED_SIZE as usize,
        "serialized Account size mismatch (got {size}, expected {})",
        Account::STORED_SIZE
    );
}

#[test]
fn to_bytes_is_deterministic() {
    let acc1 = Account::new(principal(), None::<Subaccount>);
    let acc2 = Account::new(principal(), None::<Subaccount>);
    assert_eq!(
        acc1.to_bytes().expect("account encode"),
        acc2.to_bytes().expect("account encode"),
        "encoding not deterministic"
    );
}

#[test]
fn to_bytes_length_is_consistent() {
    let acc = Account::new(principal(), Some(Subaccount::from_array([1u8; 32])));
    let bytes = acc.to_bytes().expect("account encode");
    assert_eq!(
        bytes.len(),
        Account::STORED_SIZE as usize,
        "layout length mismatch"
    );
}

#[test]
fn from_principal_creates_account_with_empty_subaccount() {
    let p = principal();
    let acc = Account::from(p);
    assert_eq!(acc.owner(), p);
    assert!(acc.subaccount().is_none());
}

#[test]
fn new_with_subaccount_sets_fields_correctly() {
    let sub: Subaccount = Subaccount::from_array([42u8; 32]);
    let acc = Account::new(principal(), Some(sub));
    assert_eq!(acc.owner(), principal());
    assert_eq!(acc.subaccount(), Some(sub));
}

#[test]
fn to_bytes_produces_expected_layout() {
    let p = principal();
    let acc = Account::new(p, Some(Subaccount::from_array([0xAAu8; 32])));
    let bytes = acc.to_bytes().expect("account encode");

    let tag = bytes[0];
    let len = (tag & LEN_MASK) as usize;
    let principal_part = if len > 0 { &bytes[1..=len] } else { &[] };
    let padding = if len < PRINCIPAL_MAX_LEN {
        &bytes[1 + len..=PRINCIPAL_MAX_LEN]
    } else {
        &[]
    };
    let sub_offset = 1 + PRINCIPAL_MAX_LEN;
    let subaccount_part = &bytes[sub_offset..sub_offset + SUBACCOUNT_LEN];

    assert_eq!(principal_part, p.as_slice(), "principal segment mismatch");
    assert!(tag & TAG_SUBACCOUNT != 0, "subaccount flag missing");
    assert!(
        padding.iter().all(|&b| b == 0),
        "principal padding not zero-filled"
    );
    assert_eq!(
        subaccount_part, &[0xAAu8; 32],
        "subaccount segment mismatch"
    );
}

#[test]
fn to_bytes_with_none_subaccount_encodes_zero_bytes() {
    let p = principal();
    let acc = Account::new(p, None::<Subaccount>);
    let bytes = acc.to_bytes().expect("account encode");
    let tag = bytes[0];
    let sub_offset = 1 + PRINCIPAL_MAX_LEN;
    let subaccount_part = &bytes[sub_offset..sub_offset + SUBACCOUNT_LEN];
    assert_eq!(tag & TAG_SUBACCOUNT, 0, "flag must be unset");
    assert!(
        subaccount_part.iter().all(|&b| b == 0),
        "None subaccount not zero-filled"
    );
}

#[test]
fn to_bytes_distinguishes_none_and_zero_subaccount() {
    let p = principal();
    let none = Account::new(p, None::<Subaccount>);
    let zero = Account::new(p, Some(Subaccount::from_array([0u8; 32])));

    assert_ne!(
        none.to_bytes().expect("account encode"),
        zero.to_bytes().expect("account encode"),
        "None and zero subaccount must serialize differently"
    );
}

#[test]
fn account_ordering_matches_bytes() {
    let accounts = vec![
        Account::new(Principal::from_slice(&[1]), None::<Subaccount>),
        Account::new(
            Principal::from_slice(&[1]),
            Some(Subaccount::from_array([0u8; 32])),
        ),
        Account::new(
            Principal::from_slice(&[1]),
            Some(Subaccount::from_array([1u8; 32])),
        ),
        Account::new(Principal::from_slice(&[1, 2]), None::<Subaccount>),
        Account::new(Principal::from_slice(&[2]), None::<Subaccount>),
    ];

    let mut sorted_by_ord = accounts.clone();
    sorted_by_ord.sort();

    let mut sorted_by_bytes = accounts;
    sorted_by_bytes.sort_by_key(|account| account.to_bytes().expect("account encode"));

    assert_eq!(
        sorted_by_ord, sorted_by_bytes,
        "Account Ord and byte ordering diverged"
    );
}

#[test]
fn round_trip_via_storable_preserves_data() {
    let original = Account::new(principal(), Some(Subaccount::from_array([0xABu8; 32])));

    let bytes = original.to_bytes().expect("account encode");
    let decoded = Account::try_from_bytes(&bytes).expect("decode should succeed");

    assert_eq!(original, decoded, "Account did not round-trip correctly");
}

#[test]
fn round_trip_custom_bytes_preserves_data() {
    let original = Account::new(principal(), Some(Subaccount::from_array([0xCDu8; 32])));
    let bytes = original.to_bytes().expect("account encode");

    let tag = bytes[0];
    let len = (tag & LEN_MASK) as usize;
    let principal_bytes = if len > 0 { &bytes[1..=len] } else { &[] };
    let sub_offset = 1 + PRINCIPAL_MAX_LEN;
    let sub_bytes = &bytes[sub_offset..sub_offset + SUBACCOUNT_LEN];

    let owner = Principal::from_slice(principal_bytes);
    let mut sub = [0u8; 32];
    sub.copy_from_slice(sub_bytes);
    let sub_opt = if tag & TAG_SUBACCOUNT != 0 {
        Some(Subaccount::from_array(sub))
    } else {
        None
    };

    let decoded = Account::from_owner_and_subaccount(owner, sub_opt);

    assert_eq!(original, decoded, "manual round-trip mismatch");
}

#[test]
fn from_bytes_rejects_empty_input() {
    assert!(Account::try_from_bytes(&[]).is_err());
}

#[test]
fn from_bytes_rejects_oversized_input() {
    let buf = vec![0u8; Account::STORED_SIZE as usize + 1];
    assert!(Account::try_from_bytes(&buf).is_err());
}

#[test]
#[expect(clippy::cast_possible_truncation)]
fn from_bytes_rejects_invalid_principal_len() {
    let mut buf = vec![0u8; Account::STORED_SIZE as usize];
    buf[0] = (Principal::MAX_LENGTH_IN_BYTES as u8) + 1;
    assert!(Account::try_from_bytes(&buf).is_err());
}

#[test]
fn from_bytes_rejects_principal_padding() {
    let acc = Account::new(Principal::from_slice(&[1]), None::<Subaccount>);
    let mut bytes = acc.to_bytes().expect("account encode");
    bytes[1 + acc.owner().as_slice().len()] = 1;
    assert!(Account::try_from_bytes(&bytes).is_err());
}

#[test]
fn from_bytes_rejects_subaccount_without_flag() {
    let acc = Account::new(Principal::from_slice(&[1]), None::<Subaccount>);
    let mut bytes = acc.to_bytes().expect("account encode");
    let sub_offset = 1 + PRINCIPAL_MAX_LEN;
    bytes[sub_offset] = 1;
    assert!(Account::try_from_bytes(&bytes).is_err());
}

#[test]
fn from_bytes_handles_anonymous_principal_with_subaccount() {
    let owner = Principal::anonymous();
    let owner_bytes = owner.as_slice();
    let mut bytes = vec![0u8; Account::STORED_SIZE as usize];
    let mut tag = u8::try_from(owner_bytes.len()).expect("principal length fits in u8");
    tag |= TAG_SUBACCOUNT;
    bytes[0] = tag;
    if !owner_bytes.is_empty() {
        bytes[1..=owner_bytes.len()].copy_from_slice(owner_bytes);
    }

    let sub_offset = 1 + PRINCIPAL_MAX_LEN;
    bytes[sub_offset] = 1;

    let decoded = Account::try_from_bytes(&bytes).expect("decode should succeed");
    assert_eq!(decoded.owner(), owner);
    assert!(decoded.subaccount().is_some());
}
