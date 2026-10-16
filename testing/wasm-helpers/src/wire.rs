//! Shared IC Wire contract for the dynamic row-count endpoint trial.
//! Transport, query execution and database formats remain consumer-owned.

use ic_wire::{Limits, Method};

/// Current contract for the existing dynamic row-count query.
///
/// Unit requests still carry the method/direction envelope. An incompatible
/// layout must receive a new identity and replace its callers together.
pub struct DynamicRowsQuery;

impl Method for DynamicRowsQuery {
    type Request = ();
    type Response = u32;

    const ID: u64 = u64::from_le_bytes(*b"ICYDBDR1");
    const NAME: &'static str = "query_one_entity_dynamic_rows";
    const REQUEST_LIMITS: Limits = Limits {
        bytes: 11,
        allocation: 11,
        nodes: 1,
        depth: 1,
    };
    const RESPONSE_LIMITS: Limits = Limits {
        bytes: 16,
        allocation: 16,
        nodes: 1,
        depth: 1,
    };
}

#[cfg(test)]
mod tests {
    use super::DynamicRowsQuery;
    use ic_wire::{Error, decode_request, decode_response, encode_request, encode_response};

    #[test]
    fn dynamic_rows_vectors_bind_method_direction_and_integer_width() {
        let request = encode_request::<DynamicRowsQuery>(&()).unwrap();
        assert_eq!(request.as_bytes(), b"IWICYDBDR1\x00");
        assert_eq!(
            decode_request::<DynamicRowsQuery>(request.as_bytes()),
            Ok(())
        );

        let empty = encode_response::<DynamicRowsQuery>(&0).unwrap();
        assert_eq!(empty.as_bytes(), b"IWICYDBDR1\x01\x00");
        assert_eq!(decode_response::<DynamicRowsQuery>(empty.as_bytes()), Ok(0));

        let maximum = encode_response::<DynamicRowsQuery>(&u32::MAX).unwrap();
        assert_eq!(maximum.as_bytes(), b"IWICYDBDR1\x01\xff\xff\xff\xff\x0f");
        assert_eq!(
            decode_response::<DynamicRowsQuery>(maximum.as_bytes()),
            Ok(u32::MAX)
        );
        assert_eq!(
            decode_request::<DynamicRowsQuery>(empty.as_bytes()),
            Err(Error::Limit)
        );
        assert_eq!(
            decode_response::<DynamicRowsQuery>(request.as_bytes()),
            Err(Error::Identity)
        );
    }

    #[test]
    fn dynamic_rows_replies_reject_truncation_trailing_and_noncanonical_bytes() {
        let reply = encode_response::<DynamicRowsQuery>(&1).unwrap();
        for length in 0..reply.as_bytes().len() {
            assert!(decode_response::<DynamicRowsQuery>(&reply.as_bytes()[..length]).is_err());
        }
        let mut trailing = reply.as_bytes().to_vec();
        trailing.push(0);
        assert_eq!(
            decode_response::<DynamicRowsQuery>(&trailing),
            Err(Error::Trailing)
        );
        assert_eq!(
            decode_response::<DynamicRowsQuery>(b"IWICYDBDR1\x01\x81\x00"),
            Err(Error::Noncanonical)
        );
    }
}
