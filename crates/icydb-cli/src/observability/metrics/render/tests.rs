use super::{average, entity_row, render_metrics_report};
use icydb::metrics::{EntityMetrics, MetricsReport};

#[test]
fn metrics_report_displays_producer_overflow_quality() {
    let wire = icydb_schema::wire::encode_one(MetricsReport::default()).unwrap();
    let report: MetricsReport = icydb_schema::wire::decode_one(&wire).unwrap();
    let rendered = render_metrics_report(&report);
    assert_eq!(
        rendered
            .matches("sample overflow=false, total overflow=false")
            .count(),
        2
    );
    assert!(!rendered.contains("+ marks a clipped count or instruction total"));
    assert!(rendered.contains("None"));
}

fn decoded_entity(
    hits: u64,
    total: u128,
    mean: Option<u128>,
    samples_saturated: bool,
    total_saturated: bool,
) -> EntityMetrics {
    #[derive(serde::Serialize)]
    struct ReportEntity {
        path: &'static str,
        instructions: ReportInstructions,
    }
    #[derive(serde::Serialize)]
    struct ReportInstructions {
        samples: u64,
        total: u128,
        maximum: Option<u64>,
        samples_overflowed: bool,
        total_overflowed: bool,
    }
    let maximum = (hits != 0).then_some(u64::try_from(total).unwrap_or(u64::MAX));
    let instructions = ReportInstructions {
        samples: hits,
        total,
        maximum,
        samples_overflowed: samples_saturated,
        total_overflowed: total_saturated,
    };
    let encoded = icydb_schema::wire::encode_one(ReportEntity {
        path: "app.Entity",
        instructions,
    })
    .expect("encode report entity");
    let decoded: EntityMetrics =
        icydb_schema::wire::decode_one(&encoded).expect("decode report entity");
    assert_eq!(average(decoded.instructions()), mean);
    decoded
}

#[test]
fn metrics_entity_row_keeps_fields_and_rounds_average_down() {
    assert_eq!(
        entity_row(&decoded_entity(3, 10, Some(3), false, false)),
        ["app.Entity", "3", "10", "3", "10"].map(str::to_string),
    );
}

#[test]
fn metrics_average_distinguishes_empty_from_measured_zero() {
    assert_eq!(
        entity_row(&decoded_entity(0, 0, None, false, false))[3],
        "unavailable"
    );
    assert_eq!(
        entity_row(&decoded_entity(3, 0, Some(0), false, false))[3],
        "0"
    );
}

#[test]
fn metrics_average_preserves_exact_caps_and_producer_quality() {
    assert_eq!(
        entity_row(&decoded_entity(u64::MAX, 0, Some(0), false, false))[3],
        "0"
    );
    for (total, total_saturated) in [(0, false), (u128::MAX, true)] {
        let row = entity_row(&decoded_entity(
            u64::MAX,
            total,
            None,
            true,
            total_saturated,
        ));
        assert_eq!(row[1], format!("{}+", u64::MAX));
        assert_eq!(
            row[2],
            if total_saturated {
                format!("{total}+")
            } else {
                total.to_string()
            }
        );
        assert_eq!(row[3], "unavailable");
    }
    assert_eq!(
        entity_row(&decoded_entity(
            1,
            u128::from(u64::MAX),
            Some(u128::from(u64::MAX)),
            false,
            false
        ))[3],
        u64::MAX.to_string()
    );
}

#[test]
fn metrics_average_preserves_totals_above_the_individual_sample_limit() {
    let total = u128::from(u64::MAX) * 2;
    assert_eq!(
        entity_row(&decoded_entity(
            2,
            total,
            Some(u128::from(u64::MAX)),
            false,
            false
        )),
        [
            "app.Entity".to_string(),
            "2".to_string(),
            total.to_string(),
            u64::MAX.to_string(),
            u64::MAX.to_string(),
        ]
    );
}
