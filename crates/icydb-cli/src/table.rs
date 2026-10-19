//! Module: CLI table rendering.
//! Responsibility: apply shared CLI section, indentation and line ending conventions.
//! Does not own: width calculation, terminal escaping or report contents.
//! Boundary: report cells -> plain CLI presentation.

use ic_host_text::table::{Options, render};

pub(crate) use ic_host_text::table::Alignment as ColumnAlign;

pub(crate) fn append_indented_table<const N: usize>(
    output: &mut String,
    indent: &str,
    headers: &[&str; N],
    rows: &[[String; N]],
    alignments: &[ColumnAlign; N],
) {
    let table = render(
        headers,
        rows,
        alignments,
        Options {
            column_gap: 3,
            header_separator: true,
        },
    );
    for line in table.lines() {
        output.push_str(indent);
        output.push_str(line);
        output.push('\n');
    }
}

// Reports share title and empty-state presentation; Host still owns table cells.
pub(crate) fn append_report_section<const N: usize>(
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
