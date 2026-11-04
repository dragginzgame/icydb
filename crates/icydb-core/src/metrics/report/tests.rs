//! Report-owned admission preserves the existing five-field frontend contract.

use super::InstructionMetrics;
use ic_metrics::{MeasurementAggregate, persistence::restore_aggregate};
use icydb_schema::wire::{decode_one, encode_one};

#[test]
fn report_fields_preserve_admission_and_exact_caps_without_recorder_deserialization() {
    for samples in [0, 1, 2, u64::MAX] {
        for total in [
            0,
            1,
            2,
            u128::from(u64::MAX),
            u128::from(u64::MAX) * 2,
            u128::MAX - 1,
            u128::MAX,
        ] {
            for maximum in [None, Some(0), Some(1), Some(u64::MAX)] {
                for (sample_overflow, total_overflow) in
                    [(false, false), (true, false), (false, true), (true, true)]
                {
                    let bytes =
                        encode_one((samples, total, maximum, sample_overflow, total_overflow))
                            .unwrap();
                    let expected =
                        restore_aggregate(samples, total, maximum, sample_overflow, total_overflow);
                    let decoded = decode_one::<InstructionMetrics>(&bytes);
                    assert_eq!(decoded.is_ok(), expected.is_ok());
                    if let Ok(recorded) = expected {
                        let report = decoded.unwrap();
                        assert_eq!(report, InstructionMetrics::from(recorded));
                        assert_eq!(report.samples(), samples);
                        assert_eq!(report.total(), total);
                        assert_eq!(report.maximum(), maximum);
                        assert_eq!(report.samples_overflowed(), sample_overflow);
                        assert_eq!(report.total_overflowed(), total_overflow);
                        assert_eq!(encode_one(&report).unwrap(), bytes);
                    }
                }
            }
        }
    }
}

#[test]
fn report_preserves_recorded_fields_bytes_and_truncation_refusal() {
    for fields in [
        (0, 0, None, false, false),
        (1, 0, Some(0), false, false),
        (u64::MAX, 0, Some(0), false, false),
        (u64::MAX, u128::MAX, Some(u64::MAX), true, true),
    ] {
        let canonical =
            restore_aggregate(fields.0, fields.1, fields.2, fields.3, fields.4).unwrap();
        let report = InstructionMetrics::from(canonical);
        let bytes = encode_one(&report).unwrap();
        assert_eq!(bytes, encode_one(fields).unwrap());
        assert_eq!(decode_one::<InstructionMetrics>(&bytes).unwrap(), report);
        for end in 0..bytes.len() {
            assert!(decode_one::<InstructionMetrics>(&bytes[..end]).is_err());
        }
    }
    let mut recorded = MeasurementAggregate::EMPTY;
    for value in [0, 1, u64::MAX] {
        recorded.record(value);
    }
    let report = InstructionMetrics::from(recorded);
    assert_eq!(report.samples(), 3);
    assert_eq!(report.total(), u128::from(u64::MAX) + 1);
    assert_eq!(report.maximum(), Some(u64::MAX));
}
