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
        "IcyDB metrics\n  window: {}..{} ({} ms)\n  heap-local window ID: {}\n  entities: {} of {} (bounded path prefix, sorted by cost)\n\njournal\n  canonical debt: {} batches, {} records, {} encoded batch bytes\n  window appended: {} batches, {} records, {} encoded batch bytes\n  window retired: {} batches, {} records, {} encoded batch bytes\n  movement overflowed: {}\n  fold: {} samples, {} total instructions, {} max instructions\n  startup recovery: {} samples, {} total instructions, {} max instructions\n  instruction spans may nest; totals are not additive\n\nentities\n",
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
        report.schema_lifecycle().startup_recovery().samples(),
        report
            .schema_lifecycle()
            .startup_recovery()
            .instructions_total(),
        report
            .schema_lifecycle()
            .startup_recovery()
            .instructions_max(),
    );
    if report.entities().is_empty() {
        output.push_str("  None\n");
        return output;
    }

    let rows = report.entities().iter().map(entity_row).collect::<Vec<_>>();
    append_indented_table(&mut output, "  ", &HEADERS, &rows, &ALIGNMENTS);
    output
}

fn entity_row(entity: &EntityMetrics) -> MetricsEntityRow {
    // Counts and totals saturate independently; unavailable means are not zero.
    let average = match ic_metrics::checked_mean(entity.hits(), entity.instructions_total()) {
        Ok(Some(value)) => value.to_string(),
        Ok(None) | Err(_) => "unavailable".to_string(),
    };
    [
        entity.path().to_string(),
        entity.hits().to_string(),
        entity.instructions_total().to_string(),
        average,
        entity.instructions_max().to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::entity_row;
    use icydb::metrics::EntityMetrics;

    fn decoded_entity(hits: u64, total: u128) -> EntityMetrics {
        // Exercise the endpoint's wide Candid field without JSON's narrower
        // default number representation becoming a test-only truncation boundary.
        #[derive(candid::CandidType)]
        struct ReportEntity {
            path: &'static str,
            hits: u64,
            instructions_total: u128,
            instructions_max: u64,
        }
        let encoded = candid::encode_one(ReportEntity {
            path: "app.Entity",
            hits,
            instructions_total: total,
            instructions_max: u64::try_from(total).unwrap_or(u64::MAX),
        })
        .expect("encode report entity");
        candid::decode_one(&encoded).expect("decode report entity")
    }

    #[test]
    fn metrics_entity_row_keeps_fields_and_rounds_average_down() {
        assert_eq!(
            entity_row(&decoded_entity(3, 10)),
            ["app.Entity", "3", "10", "3", "10"].map(str::to_string),
        );
    }

    #[test]
    fn metrics_average_distinguishes_empty_from_measured_zero() {
        assert_eq!(entity_row(&decoded_entity(0, 0))[3], "unavailable");
        assert_eq!(entity_row(&decoded_entity(3, 0))[3], "0");
    }

    #[test]
    fn metrics_average_rejects_inconsistent_or_saturated_report_fields() {
        for (hits, total) in [(0, 1), (1, u128::MAX), (u64::MAX, 0), (u64::MAX, u128::MAX)] {
            assert_eq!(entity_row(&decoded_entity(hits, total))[3], "unavailable");
        }
        assert_eq!(
            entity_row(&decoded_entity(1, u128::from(u64::MAX)))[3],
            u64::MAX.to_string()
        );
    }

    #[test]
    fn metrics_average_preserves_totals_above_the_individual_sample_limit() {
        let total = u128::from(u64::MAX) * 2;
        assert_eq!(
            entity_row(&decoded_entity(2, total)),
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
