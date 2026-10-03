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
    let average = if entity.hits() == 0 {
        0
    } else {
        entity.instructions_total() / entity.hits()
    };
    [
        entity.path().to_string(),
        entity.hits().to_string(),
        entity.instructions_total().to_string(),
        average.to_string(),
        entity.instructions_max().to_string(),
    ]
}
