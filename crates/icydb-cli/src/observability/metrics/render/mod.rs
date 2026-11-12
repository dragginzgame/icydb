//! Module: metrics report rendering.
//! Responsibility: render canonical journal debt and one execution-cost window.
//! Does not own: endpoint calls, Wire decoding, or endpoint publication.

use icydb::metrics::{EntityMetrics, InstructionMetrics, MetricsReport};

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
        convergence.journal_fold().total(),
        convergence.journal_fold().maximum().unwrap_or(0),
        convergence.journal_fold().samples_overflowed(),
        convergence.journal_fold().total_overflowed(),
        report.schema_lifecycle().startup_recovery().samples(),
        report.schema_lifecycle().startup_recovery().total(),
        report
            .schema_lifecycle()
            .startup_recovery()
            .maximum()
            .unwrap_or(0),
        report
            .schema_lifecycle()
            .startup_recovery()
            .samples_overflowed(),
        report
            .schema_lifecycle()
            .startup_recovery()
            .total_overflowed(),
    );
    if report.entities().is_empty() {
        output.push_str("  None\n");
        return output;
    }

    if report.entities().iter().any(|entity| {
        entity.instructions().samples_overflowed() || entity.instructions().total_overflowed()
    }) {
        output.push_str("  + marks a clipped count or instruction total\n");
    }
    let rows = report.entities().iter().map(entity_row).collect::<Vec<_>>();
    append_indented_table(&mut output, "  ", &HEADERS, &rows, &ALIGNMENTS);
    output
}

fn entity_row(entity: &EntityMetrics) -> MetricsEntityRow {
    // The producer preserves actual overflow quality; raw cap values cannot
    // distinguish clipped observations from exact caps and cannot rebuild it.
    let average = average(entity.instructions())
        .map_or_else(|| "unavailable".to_string(), |value| value.to_string());
    [
        entity.path().to_string(),
        if entity.instructions().samples_overflowed() {
            format!("{}+", entity.instructions().samples())
        } else {
            entity.instructions().samples().to_string()
        },
        if entity.instructions().total_overflowed() {
            format!("{}+", entity.instructions().total())
        } else {
            entity.instructions().total().to_string()
        },
        average,
        entity.instructions().maximum().unwrap_or(0).to_string(),
    ]
}

// Rendering owns floor arithmetic. Clipped source quality is not an exact mean.
const fn average(instructions: &InstructionMetrics) -> Option<u128> {
    if instructions.samples_overflowed() || instructions.total_overflowed() {
        return None;
    }
    instructions
        .total()
        .checked_div(instructions.samples() as u128)
}

#[cfg(test)]
mod tests;
