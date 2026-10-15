//! Module: CLI table rendering.
//! Responsibility: apply CLI indentation and line endings to Host-rendered tables.
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
