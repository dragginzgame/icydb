//! Module: types::tests_temporal_boundary
//! Responsibility: module-local ownership and contracts for types::tests_temporal_boundary.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::types::{Date, Duration, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct TemporalRow {
    ts: Timestamp,
    date: Date,
    dur: Duration,
}

#[test]
fn temporal_json_boundary_roundtrip() {
    let row = TemporalRow {
        ts: Timestamp::parse_rfc3339("2025-01-01T12:00:00Z").expect("timestamp should parse"),
        date: Date::new_checked(2025, 1, 1).expect("date should construct"),
        dur: Duration::from_secs(5),
    };

    let json = serde_json::to_string(&row).expect("temporal struct should serialize");

    // Ensure correct API boundary shapes
    assert!(
        json.contains("2025-01-01"),
        "Date should serialize as ISO string"
    );

    assert!(
        json.contains("T12:00:00"),
        "Timestamp should serialize as RFC3339"
    );

    assert!(
        json.contains("5000"),
        "Duration should serialize as integer milliseconds"
    );

    let decoded: TemporalRow =
        serde_json::from_str(&json).expect("temporal struct should deserialize");

    assert_eq!(decoded, row, "temporal JSON roundtrip must preserve values");
}
