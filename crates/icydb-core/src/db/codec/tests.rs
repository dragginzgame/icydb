use super::*;
use crate::error::{ErrorClass, ErrorOrigin};
use icydb_diagnostic_code::DiagnosticFactTag;

#[test]
fn row_encoder_checks_size_before_writing_payload() {
    for payload_len in [
        MAX_ROW_BYTES as usize - ROW_ENVELOPE_HEADER_LEN + 1,
        MAX_ROW_BYTES as usize,
        usize::MAX,
    ] {
        let mut called = false;
        let result = serialize_row_payload(RowLayoutVersion::INITIAL, payload_len, |_| {
            called = true;
            Ok(())
        });
        let error = result.unwrap_err();
        assert_eq!(error.class(), ErrorClass::Unsupported);
        assert_eq!(error.origin(), ErrorOrigin::Serialize);
        if let Some(total_len) = payload_len.checked_add(ROW_ENVELOPE_HEADER_LEN) {
            assert_eq!(
                error.diagnostic_facts(),
                vec![
                    (DiagnosticFactTag::ActualLength, total_len as u64),
                    (DiagnosticFactTag::Limit, u64::from(MAX_ROW_BYTES)),
                ]
            );
        } else {
            // No representable exact total exists; never fabricate one.
            assert!(error.diagnostic_facts().is_empty());
        }
        assert!(!called);
    }
}

#[test]
fn row_encoder_accepts_exact_envelope_budget() {
    let payload_len = MAX_ROW_BYTES as usize - ROW_ENVELOPE_HEADER_LEN;
    let encoded = serialize_row_payload(RowLayoutVersion::INITIAL, payload_len, |out| {
        out.resize(out.len() + payload_len, 0xab);
        Ok(())
    })
    .expect("an exact-budget row should encode");
    assert_eq!(encoded.len(), MAX_ROW_BYTES as usize);
    let decoded = decode_row_payload_bytes(&encoded).expect("the envelope should decode");
    assert_eq!(decoded.layout_version(), RowLayoutVersion::INITIAL);
    let payload = decoded.into_payload();
    assert_eq!(
        payload.as_ptr(),
        encoded[ROW_ENVELOPE_HEADER_LEN..].as_ptr()
    );
    assert_eq!(payload.len(), payload_len);
    assert!(payload.iter().all(|byte| *byte == 0xab));
}

#[test]
fn row_encoder_rejects_incomplete_or_failed_payloads() {
    for written in [0, 2] {
        let error = serialize_row_payload(RowLayoutVersion::INITIAL, 1, |out| {
            out.resize(out.len() + written, 0);
            Ok(())
        })
        .unwrap_err();
        assert_eq!(error.class(), ErrorClass::Internal);
        assert_eq!(error.origin(), ErrorOrigin::Serialize);
    }
    assert!(
        serialize_row_payload(RowLayoutVersion::INITIAL, 1, |out| {
            out.push(0);
            Err(InternalError::persisted_row_encode_internal())
        })
        .is_err()
    );
}
