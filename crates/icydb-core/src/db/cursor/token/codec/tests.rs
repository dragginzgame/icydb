use super::*;
use crate::db::query::plan::CardinalityTiebreakFamily;

#[test]
fn authenticated_grouped_payload_still_enforces_value_depth() {
    let key = [0x55; 32];
    let mut payload = vec![0x42; 32];
    payload.push(0); // ascending
    write_u32_be(&mut payload, 0); // offset
    write_u32_be(&mut payload, 1); // one group key
    for _ in 0..129 {
        payload.push(12); // list
        write_u32_be(&mut payload, 1);
    }
    payload.push(14); // null leaf exceeds the maximum depth
    let wire = encode_authenticated_payload(&payload, TOKEN_VARIANT_GROUPED, &key).unwrap();
    assert!(matches!(
        decode_grouped_token(&wire, &key),
        Err(TokenWireError::Decode)
    ));
}

const ROUTE_LAYOUT_OFFSET: usize = TOKEN_HEADER_BYTES
    + 1
    + 32
    + 16
    + std::mem::size_of::<u64>()
    + 1
    + 32
    + 16
    + std::mem::size_of::<u64>();
const ROUTE_INDEX_OFFSET: usize = ROUTE_LAYOUT_OFFSET + 2;
const ROUTE_FAMILY_OFFSET: usize = ROUTE_INDEX_OFFSET + IndexId::STORED_SIZE_USIZE;
const ROUTE_ARITY_OFFSET: usize = ROUTE_FAMILY_OFFSET + 1;

fn pinned_token() -> ScalarPageToken {
    ScalarPageToken::new(
        ScalarPageMode::Live,
        ContinuationSignature::from_bytes([0x11; 32]),
        ScalarPageTokenAuthority::new([0x22; 16], 7, 1, [0x33; 32], [0x44; 16], EntityTag::new(9)),
        CardinalityTiebreakRoutePin::new(
            IndexId::new_with_generation(EntityTag::new(9), 3, 4),
            CardinalityTiebreakFamily::BranchSet,
            2,
        ),
        ScalarPageTokenWindow::new(0, Some(2_048), 0x55),
        vec![ScalarOrderTermContract::new("id", OrderDirection::Asc)],
        ScalarPageTokenProgress::new(None, None, None, 0, 2),
    )
}

fn resign(mut authenticated: Vec<u8>, key: &[u8; 32]) -> Vec<u8> {
    let payload_len = u32::try_from(authenticated.len() - TOKEN_HEADER_BYTES)
        .expect("test payload length should fit");
    authenticated[6..TOKEN_HEADER_BYTES].copy_from_slice(&payload_len.to_be_bytes());
    let mac = hmac_sha256(key, authenticated.as_slice());
    authenticated.extend_from_slice(&mac);
    authenticated
}

fn authenticated_without_mac(key: &[u8; 32]) -> Vec<u8> {
    let encoded = pinned_token()
        .encode(key)
        .expect("current pinned token should encode");
    encoded[..encoded.len() - TOKEN_MAC_BYTES].to_vec()
}

#[test]
fn authenticated_route_pin_corruption_fails_closed() {
    let key = [0x66; 32];
    let mut foreign_entity = authenticated_without_mac(&key);
    foreign_entity[ROUTE_INDEX_OFFSET + std::mem::size_of::<u64>() - 1] ^= 1;
    assert!(ScalarPageToken::decode(resign(foreign_entity, &key).as_slice(), &key).is_err(),);

    let mut unknown_family = authenticated_without_mac(&key);
    unknown_family[ROUTE_FAMILY_OFFSET] = u8::MAX;
    assert!(ScalarPageToken::decode(resign(unknown_family, &key).as_slice(), &key).is_err(),);

    let mut zero_arity = authenticated_without_mac(&key);
    zero_arity[ROUTE_ARITY_OFFSET] = 0;
    assert!(ScalarPageToken::decode(resign(zero_arity, &key).as_slice(), &key).is_err());
}
