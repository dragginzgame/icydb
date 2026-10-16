//! Candid-only control for the isolated key/count codec comparison.

#[cfg(any(target_arch = "wasm32", test))]
use icydb_testing_codec_query::{MAX_FRAME_BYTES, QueryCodecError, row_count};

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
    if request.len() > MAX_FRAME_BYTES {
        return Err(QueryCodecError::InvalidRequest);
    }
    let mut config = candid::DecoderConfig::new();
    config
        .set_decoding_quota(10_000)
        .set_skipping_quota(100)
        .set_max_type_len(16)
        .set_max_header_len(MAX_FRAME_BYTES);
    let key: u64 = candid::utils::decode_one_with_config(request, &config)
        .map_err(|_| QueryCodecError::InvalidRequest)?;
    if candid::encode_one(key).map_err(|_| QueryCodecError::Encoding)? != request {
        return Err(QueryCodecError::InvalidRequest);
    }
    candid::encode_one(row_count(key)).map_err(|_| QueryCodecError::Encoding)
}

#[cfg(test)]
mod tests {
    use super::reply;

    #[test]
    fn candid_control_preserves_counts_and_rejects_noncanonical_frames() {
        let request = candid::encode_one(7_u64).unwrap();
        let response = reply(&request).unwrap();
        assert_eq!(candid::decode_one::<u32>(&response).unwrap(), 2);
        for length in 0..request.len() {
            assert!(reply(&request[..length]).is_err());
        }
        let mut trailing = request;
        trailing.push(0);
        assert!(reply(&trailing).is_err());
        assert!(reply(&[0; 65]).is_err());
    }
}
