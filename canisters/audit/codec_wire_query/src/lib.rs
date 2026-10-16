//! Wire-only actor for the isolated key/count codec comparison.

use ic_wire::{Limits, Method};
use icydb_testing_codec_query::MAX_FRAME_BYTES;

#[cfg(any(target_arch = "wasm32", test))]
use icydb_testing_codec_query::{QueryCodecError, row_count};

/// Current contract shared with the comparison's matching Rust caller.
pub struct CountRows;

impl Method for CountRows {
    type Request = u64;
    type Response = u32;
    const ID: u64 = u64::from_le_bytes(*b"ICYDBCQ1");
    const NAME: &'static str = "count_rows";
    const REQUEST_LIMITS: Limits = Limits {
        bytes: MAX_FRAME_BYTES,
        allocation: 21,
        nodes: 1,
        depth: 1,
    };
    const RESPONSE_LIMITS: Limits = Limits {
        bytes: MAX_FRAME_BYTES,
        allocation: 16,
        nodes: 1,
        depth: 1,
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
    let key = ic_wire::decode_request::<CountRows>(request)
        .map_err(|_| QueryCodecError::InvalidRequest)?;
    ic_wire::encode_response::<CountRows>(&row_count(key))
        .map(ic_wire::ResponseBytes::into_bytes)
        .map_err(|_| QueryCodecError::Encoding)
}

#[cfg(test)]
mod tests {
    use super::{CountRows, reply};

    #[test]
    fn wire_control_preserves_counts_and_rejects_invalid_frames() {
        let request = ic_wire::encode_request::<CountRows>(&7)
            .unwrap()
            .into_bytes();
        let response = reply(&request).unwrap();
        assert_eq!(ic_wire::decode_response::<CountRows>(&response), Ok(2));
        for length in 0..request.len() {
            assert!(reply(&request[..length]).is_err());
        }
        let mut trailing = request;
        trailing.push(0);
        assert!(reply(&trailing).is_err());
        assert!(reply(&[0; 65]).is_err());
    }
}
