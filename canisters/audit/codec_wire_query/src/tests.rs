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
            native::decode_response::<u32>(CountRows::ID, &response, CountRows::RESPONSE_LIMITS),
            Ok(count)
        );
    }
    let request = native::encode_request(CountRows::ID, &7_u64, CountRows::REQUEST_LIMITS).unwrap();
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
