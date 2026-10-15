//! Module: metrics report rendering.
//! Responsibility: render canonical journal debt and one execution-cost window.
//! Does not own: endpoint calls, Candid decoding, or endpoint publication.

use icydb::metrics::{EntityMetrics, MetricsReport};

use crate::table::{ColumnAlign, append_indented_table};

type MetricsEntityRow = [String; 5];

const HEADERS: [&str; 5] = ["entity", "hits", "instructions", "average", "max"];
const ALIGNMENTS: [ColumnAlign; 5] = [
    ColumnAlign::Left,
    ColumnAlign::Right,
    ColumnAlign::Right,
    ColumnAlign::Right,
    ColumnAlign::Right,
];

pub(super) fn render_metrics_report(report: &MetricsReport) -> String {
    let debt = report.journal_debt();
    let convergence = report.convergence();
    let appended = convergence.appended();
    let retired = convergence.retired();
    let mut output = format!(
        "IcyDB metrics\n  window: {}..{} ({} ms)\n  heap-local window ID: {}\n  entities: {} of {} (bounded path prefix, sorted by cost)\n\njournal\n  canonical debt: {} batches, {} records, {} encoded batch bytes\n  window appended: {} batches, {} records, {} encoded batch bytes\n  window retired: {} batches, {} records, {} encoded batch bytes\n  movement overflowed: {}\n  fold: {} samples, {} total instructions, {} max instructions (sample overflow={}, total overflow={})\n  startup recovery: {} samples, {} total instructions, {} max instructions (sample overflow={}, total overflow={})\n  instruction spans may nest; totals are not additive\n\nentities\n",
        report.window_start_ms(),
        report.window_end_ms(),
        report
            .window_end_ms()
            .saturating_sub(report.window_start_ms()),
        report
            .window_id()
            .map_or_else(|| "unavailable".to_string(), |id| id.to_string()),
        report.entities().len(),
        report.total_entities(),
        debt.batch_count(),
        debt.record_count(),
        debt.encoded_batch_bytes(),
        appended.batch_count(),
        appended.record_count(),
        appended.encoded_batch_bytes(),
        retired.batch_count(),
        retired.record_count(),
        retired.encoded_batch_bytes(),
        convergence.overflowed(),
        convergence.journal_fold().samples(),
        convergence.journal_fold().instructions_total(),
        convergence.journal_fold().instructions_max(),
        convergence.journal_fold().samples_saturated(),
        convergence.journal_fold().total_saturated(),
        report.schema_lifecycle().startup_recovery().samples(),
        report
            .schema_lifecycle()
            .startup_recovery()
            .instructions_total(),
        report
            .schema_lifecycle()
            .startup_recovery()
            .instructions_max(),
        report
            .schema_lifecycle()
            .startup_recovery()
            .samples_saturated(),
        report
            .schema_lifecycle()
            .startup_recovery()
            .total_saturated(),
    );
    if report.entities().is_empty() {
        output.push_str("  None\n");
        return output;
    }

    if report
        .entities()
        .iter()
        .any(|entity| entity.samples_saturated() || entity.total_saturated())
    {
        output.push_str("  + marks a clipped count or instruction total\n");
    }
    let rows = report.entities().iter().map(entity_row).collect::<Vec<_>>();
    append_indented_table(&mut output, "  ", &HEADERS, &rows, &ALIGNMENTS);
    output
}

fn entity_row(entity: &EntityMetrics) -> MetricsEntityRow {
    // The producer preserves actual overflow quality; raw cap values cannot
    // distinguish clipped observations from exact caps and cannot rebuild it.
    let average = entity
        .instructions_mean()
        .map_or_else(|| "unavailable".to_string(), |value| value.to_string());
    [
        entity.path().to_string(),
        if entity.samples_saturated() {
            format!("{}+", entity.hits())
        } else {
            entity.hits().to_string()
        },
        if entity.total_saturated() {
            format!("{}+", entity.instructions_total())
        } else {
            entity.instructions_total().to_string()
        },
        average,
        entity.instructions_max().to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::{entity_row, render_metrics_report};
    use icydb::metrics::{EntityMetrics, MetricsReport};

    #[test]
    fn metrics_report_displays_producer_overflow_quality() {
        let wire = candid::encode_one(MetricsReport::default()).unwrap();
        let report: MetricsReport = candid::decode_one(&wire).unwrap();
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
        // Use complete producer-projected state through the real Candid decoder;
        // the renderer consumes the admitted mean instead of redoing arithmetic.
        #[derive(candid::CandidType)]
        struct ReportEntity {
            path: &'static str,
            hits: u64,
            instructions_total: u128,
            instructions_max: u64,
            samples_saturated: bool,
            total_saturated: bool,
            instructions_mean: Option<u128>,
        }
        let encoded = candid::encode_one(ReportEntity {
            path: "app.Entity",
            hits,
            instructions_total: total,
            instructions_max: u64::try_from(total).unwrap_or(u64::MAX),
            samples_saturated,
            total_saturated,
            instructions_mean: mean,
        })
        .expect("encode report entity");
        candid::decode_one(&encoded).expect("decode report entity")
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
}
