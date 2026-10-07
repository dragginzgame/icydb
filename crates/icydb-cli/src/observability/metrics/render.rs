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

    fn decoded_entity(hits: u64, total: u64) -> EntityMetrics {
        serde_json::from_value(serde_json::json!({
            "path": "app.Entity",
            "hits": hits,
            "instructions_total": total,
            "instructions_max": total,
        }))
        .expect("decode report entity")
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
        for (hits, total) in [(0, 1), (1, u64::MAX), (u64::MAX, 0), (u64::MAX, u64::MAX)] {
            assert_eq!(entity_row(&decoded_entity(hits, total))[3], "unavailable");
        }
        assert_eq!(
            entity_row(&decoded_entity(1, u64::MAX - 1))[3],
            (u64::MAX - 1).to_string()
        );
    }
}
