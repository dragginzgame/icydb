use super::*;
use crate::{
    db::{
        cursor::CursorBoundarySlot,
        index::IndexId,
        query::plan::{CardinalityTiebreakFamily, OrderDirection},
    },
    value::Value,
};

fn token() -> ScalarPageToken {
    ScalarPageToken::new(
        ScalarPageMode::Live,
        ContinuationSignature::from_bytes([0x11; 32]),
        ScalarPageTokenAuthority::new([0x22; 16], 7, 1, [0x33; 32], [0x44; 16], EntityTag::new(9)),
        None,
        ScalarPageTokenWindow::new(0, Some(2_048), 0x55),
        vec![
            ScalarOrderTermContract::new("score", OrderDirection::Desc),
            ScalarOrderTermContract::new("nullable", OrderDirection::Asc),
            ScalarOrderTermContract::new("id", OrderDirection::Desc),
        ],
        ScalarPageTokenProgress::new(
            Some(CursorBoundary {
                slots: vec![
                    CursorBoundarySlot::Present(Value::Nat64(10)),
                    CursorBoundarySlot::Missing,
                    CursorBoundarySlot::Present(Value::Text("id-7".to_string())),
                ],
            }),
            Some(vec![1, 2, 3]),
            Some(vec![4, 5, 6]),
            3,
            1_024,
        ),
    )
}

#[test]
fn authenticated_scalar_token_round_trips_complete_mixed_order_contract() {
    let key = [0x66; 32];
    let token = token();
    let encoded = token.encode(&key).expect("bounded token should encode");
    assert_eq!(encoded[4], 1);
    let decoded = ScalarPageToken::decode(encoded.as_slice(), &key)
        .expect("authenticated token should decode");

    assert_eq!(decoded, token);
}

#[test]
fn current_version_one_scalar_token_round_trips_exact_route_pin() {
    let key = [0x66; 32];
    let mut token = token();
    token.route_pin = CardinalityTiebreakRoutePin::new(
        IndexId::new_with_generation(EntityTag::new(9), 3, 4),
        CardinalityTiebreakFamily::BranchSet,
        2,
    );
    let encoded = token.encode(&key).expect("bounded route pin should encode");
    let decoded = ScalarPageToken::decode(encoded.as_slice(), &key)
        .expect("current authenticated route pin should decode");

    assert_eq!(encoded[4], 1);
    assert_eq!(decoded, token);
}

#[test]
fn current_scalar_token_rejects_route_pin_for_another_entity() {
    let mut token = token();
    token.route_pin = CardinalityTiebreakRoutePin::new(
        IndexId::new_with_generation(EntityTag::new(10), 3, 4),
        CardinalityTiebreakFamily::Prefix,
        1,
    );

    assert!(token.encode(&[0x66; 32]).is_err());
}

#[test]
fn authenticated_scalar_token_rejects_tampering_and_wrong_database_key() {
    let key = [0x66; 32];
    let mut encoded = token().encode(&key).expect("bounded token should encode");
    let payload_index = 12;
    encoded[payload_index] ^= 0x80;

    assert!(ScalarPageToken::decode(encoded.as_slice(), &key).is_err());
    assert!(
        ScalarPageToken::decode(
            token()
                .encode(&key)
                .expect("bounded token should encode")
                .as_slice(),
            &[0x77; 32],
        )
        .is_err()
    );
}

#[test]
fn authenticated_scalar_token_rejects_oversized_order_contracts() {
    let mut order_terms = Vec::new();
    for index in 0..=MAX_SCALAR_CURSOR_ORDER_TERMS {
        order_terms.push(ScalarOrderTermContract::new(
            format!("field_{index}"),
            OrderDirection::Asc,
        ));
    }
    let mut token = token();
    token.order_terms = order_terms;

    assert!(token.encode(&[0x66; 32]).is_err());
}

#[test]
fn authenticated_scalar_token_rejects_oversized_logical_boundaries() {
    let mut token = token();
    token.progress.last_emitted_logical = Some(CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Text(
            "x".repeat(MAX_SCALAR_CURSOR_LOGICAL_BOUNDARY_BYTES + 1),
        ))],
    });

    assert!(token.encode(&[0x66; 32]).is_err());
}

#[test]
fn authenticated_compact_scalar_cursor_round_trips_and_measures_external_bytes() {
    use crate::db::cursor::{decode_cursor, encode_cursor};

    let key = [0x66; 32];
    let mut current = token();
    current.progress.last_emitted_logical = Some(CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Int128(i128::MIN)),
            CursorBoundarySlot::Present(Value::U256(crate::types::U256::MAX)),
            CursorBoundarySlot::Missing,
        ],
    });
    for mode in [ScalarPageMode::Live, ScalarPageMode::Exhaustive] {
        current.mode = mode;
        let bytes = current.encode(&key).unwrap();
        let text = encode_cursor(&bytes);
        assert_eq!(bytes.len(), 302);
        assert_eq!(text.len(), 403);
        assert_eq!(
            ScalarPageToken::decode(&decode_cursor(&text).unwrap(), &key).unwrap(),
            current
        );
        let mut changed = bytes.clone();
        changed[bytes.len() - 33] ^= 1;
        assert!(
            ScalarPageToken::decode(&decode_cursor(&encode_cursor(&changed)).unwrap(), &key)
                .is_err()
        );
        assert!(ScalarPageToken::decode(&bytes, &[0x67; 32]).is_err());
        for len in [0, 9, bytes.len() - 1] {
            assert!(ScalarPageToken::decode(&bytes[..len], &key).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(ScalarPageToken::decode(&trailing, &key).is_err());
    }
}
