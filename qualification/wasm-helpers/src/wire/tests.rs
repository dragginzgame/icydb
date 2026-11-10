use super::DynamicRowsQuery as Contract;
use ic_wire::{Error, native};

#[test]
fn dynamic_rows_vectors_bind_method_direction_and_integer_width() {
    let request = native::encode_request(Contract::ID, &(), Contract::REQUEST_LIMITS).unwrap();
    assert_eq!(request.as_slice(), b"IWICYDBDR1\x00");
    assert_eq!(
        native::decode_request::<()>(Contract::ID, request.as_slice(), Contract::REQUEST_LIMITS),
        Ok(())
    );

    let empty = native::encode_response(Contract::ID, &0_u32, Contract::RESPONSE_LIMITS).unwrap();
    assert_eq!(empty.as_slice(), b"IWICYDBDR1\x01\x00");
    assert_eq!(
        native::decode_response::<u32>(Contract::ID, empty.as_slice(), Contract::RESPONSE_LIMITS),
        Ok(0)
    );

    let maximum =
        native::encode_response(Contract::ID, &u32::MAX, Contract::RESPONSE_LIMITS).unwrap();
    assert_eq!(maximum.as_slice(), b"IWICYDBDR1\x01\xff\xff\xff\xff\x0f");
    assert_eq!(
        native::decode_response::<u32>(Contract::ID, maximum.as_slice(), Contract::RESPONSE_LIMITS),
        Ok(u32::MAX)
    );
    assert_eq!(
        native::decode_request::<()>(Contract::ID, empty.as_slice(), Contract::REQUEST_LIMITS),
        Err(Error::Limit)
    );
    assert_eq!(
        native::decode_response::<u32>(Contract::ID, request.as_slice(), Contract::RESPONSE_LIMITS),
        Err(Error::Identity)
    );
}

#[test]
fn dynamic_rows_replies_reject_truncation_trailing_and_noncanonical_bytes() {
    let reply = native::encode_response(Contract::ID, &1_u32, Contract::RESPONSE_LIMITS).unwrap();
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
