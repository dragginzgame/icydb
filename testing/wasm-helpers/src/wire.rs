//! Shared IC Wire contract for the dynamic row-count endpoint trial.
//! Transport, query execution and database formats remain consumer-owned.

use ic_wire::Limits;

/// Current contract for the existing dynamic row-count query.
///
/// Unit requests still carry the method/direction envelope. An incompatible
/// layout must receive a new identity and replace its callers together.
pub struct DynamicRowsQuery;

impl DynamicRowsQuery {
    /// Canonical method identity, unchanged across native-codec adoption.
    pub const ID: u64 = u64::from_le_bytes(*b"ICYDBDR1");
    /// Public IC query name.
    pub const NAME: &'static str = "query_one_entity_dynamic_rows";
    /// Unit request envelope; native traversal needs four visits and three layers.
    pub const REQUEST_LIMITS: Limits = Limits {
        bytes: 11,
        allocation: 11,
        nodes: 4,
        depth: 3,
    };
    /// Frame bytes plus two native value-size charges; the payload remains one u32.
    pub const RESPONSE_LIMITS: Limits = Limits {
        bytes: 16,
        allocation: 80, // 16 frame bytes plus two u32 charges (2 × 8 × 4).
        nodes: 4,
        depth: 3,
    };
}

#[cfg(test)]
mod tests {
    use super::DynamicRowsQuery as Contract;
    use ic_wire::{Error, native};

    #[test]
    fn dynamic_rows_vectors_bind_method_direction_and_integer_width() {
        let request = native::encode_request(Contract::ID, &(), Contract::REQUEST_LIMITS).unwrap();
        assert_eq!(request.as_slice(), b"IWICYDBDR1\x00");
        assert_eq!(
            native::decode_request::<()>(
                Contract::ID,
                request.as_slice(),
                Contract::REQUEST_LIMITS
            ),
            Ok(())
        );

        let empty =
            native::encode_response(Contract::ID, &0_u32, Contract::RESPONSE_LIMITS).unwrap();
        assert_eq!(empty.as_slice(), b"IWICYDBDR1\x01\x00");
        assert_eq!(
            native::decode_response::<u32>(
                Contract::ID,
                empty.as_slice(),
                Contract::RESPONSE_LIMITS
            ),
            Ok(0)
        );

        let maximum =
            native::encode_response(Contract::ID, &u32::MAX, Contract::RESPONSE_LIMITS).unwrap();
        assert_eq!(maximum.as_slice(), b"IWICYDBDR1\x01\xff\xff\xff\xff\x0f");
        assert_eq!(
            native::decode_response::<u32>(
                Contract::ID,
                maximum.as_slice(),
                Contract::RESPONSE_LIMITS
            ),
            Ok(u32::MAX)
        );
        assert_eq!(
            native::decode_request::<()>(Contract::ID, empty.as_slice(), Contract::REQUEST_LIMITS),
            Err(Error::Limit)
        );
        assert_eq!(
            native::decode_response::<u32>(
                Contract::ID,
                request.as_slice(),
                Contract::RESPONSE_LIMITS
            ),
            Err(Error::Identity)
        );
    }

    #[test]
    fn dynamic_rows_replies_reject_truncation_trailing_and_noncanonical_bytes() {
        let reply =
            native::encode_response(Contract::ID, &1_u32, Contract::RESPONSE_LIMITS).unwrap();
        for length in 0..reply.as_slice().len() {
            assert!(
                native::decode_response::<u32>(
                    Contract::ID,
                    &reply[..length],
                    Contract::RESPONSE_LIMITS
                )
                .is_err()
            );
        }
        let mut trailing = reply.as_slice().to_vec();
        trailing.push(0);
        assert_eq!(
            native::decode_response::<u32>(Contract::ID, &trailing, Contract::RESPONSE_LIMITS),
            Err(Error::Trailing)
        );
        assert_eq!(
            native::decode_response::<u32>(
                Contract::ID,
                b"IWICYDBDR1\x01\x81\x00",
                Contract::RESPONSE_LIMITS
            ),
            Err(Error::Malformed)
        );
    }
}
