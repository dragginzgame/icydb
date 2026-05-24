//! Module: schema-check report rendering.
//! Responsibility: render analyzed schema-check summaries as human-readable tables.
//! Does not own: schema drift analysis, canister calls, or CLI dispatch.
//! Boundary: formats summary rows without interpreting schema semantics.

use crate::table::{ColumnAlign, append_indented_table};

use super::analysis::SchemaCheckSummary;

pub(super) fn render_schema_check_report_from_summary(summary: &SchemaCheckSummary) -> String {
    let mut output = String::new();

    output.push_str("IcyDB schema check\n");
    output.push_str(
        format!(
            "  status: {}\n  entities: {}\n  accepted-only fields: {}\n  DDL-owned indexes: {}\n  mismatches: {}\n\n",
            summary.status,
            summary.entities,
            summary.accepted_only_fields,
            summary.accepted_ddl_indexes,
            summary.mismatches,
        )
        .as_str(),
    );
    append_schema_check_entity_table(&mut output, summary.entity_rows.as_slice());
    output.push('\n');
    append_schema_check_detail_table(&mut output, "accepted drift", summary.drift_rows.as_slice());
    output.push('\n');
    append_schema_check_detail_table(&mut output, "mismatches", summary.mismatch_rows.as_slice());
    output.push('\n');
    append_schema_check_recommendations(&mut output, summary.recommendations.as_slice());

    output
}

fn append_schema_check_recommendations(output: &mut String, recommendations: &[String]) {
    output.push_str("recommendations\n");
    for recommendation in recommendations {
        output.push_str("  ");
        output.push_str(recommendation);
        output.push('\n');
    }
}

fn append_schema_check_entity_table(output: &mut String, rows: &[[String; 8]]) {
    append_schema_check_table(
        output,
        "entities",
        &[
            "entity",
            "status",
            "gen fields",
            "acc fields",
            "gen indexes",
            "acc indexes",
            "acc-only fields",
            "mismatches",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
        ],
    );
}

fn append_schema_check_detail_table(output: &mut String, title: &str, rows: &[[String; 4]]) {
    append_schema_check_table(
        output,
        title,
        &["entity", "kind", "generated", "accepted"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_check_table<const N: usize>(
    output: &mut String,
    title: &str,
    headers: &[&str; N],
    rows: &[[String; N]],
    alignments: &[ColumnAlign; N],
) {
    output.push_str(title);
    output.push('\n');
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(output, "  ", headers, rows, alignments);
}
