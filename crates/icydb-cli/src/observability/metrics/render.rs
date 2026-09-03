//! Module: metrics report rendering.
//! Responsibility: render one entity execution-cost report.
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
    let mut output = format!(
        "IcyDB metrics\n  window: {}..{} ({} ms)\n  entities: {}\n\nentities\n",
        report.window_start_ms(),
        report.window_end_ms(),
        report
            .window_end_ms()
            .saturating_sub(report.window_start_ms()),
        report.entities().len(),
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
