//! Wire-only actor for an isolated bounded native key/count contract.

use ic_wire::Limits;
use icydb_testing_codec_query::MAX_FRAME_BYTES;

#[cfg(any(target_arch = "wasm32", test))]
use ic_wire::native;
#[cfg(any(target_arch = "wasm32", test))]
use icydb_testing_codec_query::{QueryCodecError, row_count};

/// Current bounded key/count contract.
pub struct CountRows;

impl CountRows {
    /// Canonical key/count method identity.
    pub const ID: u64 = u64::from_le_bytes(*b"ICYDBCQ1");
    /// Fixed frame allowance plus two native value-size charges for one u64.
    pub const REQUEST_LIMITS: Limits = Limits {
        bytes: MAX_FRAME_BYTES,
        allocation: MAX_FRAME_BYTES + 128, // Two native u64 charges: 2 × 8 × 8.
        nodes: 4,
        depth: 3,
    };
    /// Fixed frame allowance plus two native value-size charges for one u32.
    pub const RESPONSE_LIMITS: Limits = Limits {
        bytes: MAX_FRAME_BYTES,
        allocation: MAX_FRAME_BYTES + 64, // Two native u32 charges: 2 × 8 × 4.
        nodes: 4,
        depth: 3,
    };
}

#[cfg(target_arch = "wasm32")]
#[expect(
    unsafe_code,
    reason = "the raw synchronous query ABI requires an explicit IC export name"
)]
#[unsafe(export_name = "canister_query count_rows")]
extern "C" fn count_rows() {
    icydb_testing_codec_query::serve_query(reply);
}

#[cfg(any(target_arch = "wasm32", test))]
fn reply(request: &[u8]) -> Result<Vec<u8>, QueryCodecError> {
    let key = native::decode_request::<u64>(CountRows::ID, request, CountRows::REQUEST_LIMITS)
        .map_err(|_| QueryCodecError::InvalidRequest)?;
    native::encode_response(CountRows::ID, &row_count(key), CountRows::RESPONSE_LIMITS)
        .map_err(|_| QueryCodecError::Encoding)
}

#[cfg(test)]
mod tests {
    use super::{CountRows, reply};
    use ic_wire::native;

    #[test]
    fn wire_control_preserves_counts_and_rejects_invalid_frames() {
        for (key, count, payload) in [
            (0_u64, 1_u32, vec![0]),
            (7, 2, vec![7]),
            (
                u64::MAX,
                1,
                vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 1],
            ),
        ] {
            let request =
                native::encode_request(CountRows::ID, &key, CountRows::REQUEST_LIMITS).unwrap();
            let mut expected = b"IWICYDBCQ1\x00".to_vec();
            expected.extend_from_slice(&payload);
            assert_eq!(request, expected);
            let response = reply(&request).unwrap();
            let mut expected = b"IWICYDBCQ1\x01".to_vec();
            expected.push(u8::try_from(count).unwrap());
            assert_eq!(response, expected);
            assert_eq!(
                native::decode_response::<u32>(
                    CountRows::ID,
                    &response,
                    CountRows::RESPONSE_LIMITS
                ),
                Ok(count)
            );
        }
        let request =
            native::encode_request(CountRows::ID, &7_u64, CountRows::REQUEST_LIMITS).unwrap();
        let response = reply(&request).unwrap();
        assert_eq!(
            native::decode_response::<u32>(CountRows::ID, &response, CountRows::RESPONSE_LIMITS),
            Ok(2)
        );
        for length in 0..request.len() {
            assert!(reply(&request[..length]).is_err());
        }
        let mut trailing = request;
        trailing.push(0);
        assert!(reply(&trailing).is_err());
        assert!(reply(&[0; 65]).is_err());
    }
}
