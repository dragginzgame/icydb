//! Module: CLI table rendering.
//! Responsibility: render simple aligned text tables for CLI reports.
//! Does not own: report content, command execution, or observability-specific formatting.
//! Boundary: exposes alignment choices and indented table appending to report modules.

const COLUMN_GAP: &str = "   ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnAlign {
    Left,
    Right,
}

pub(crate) fn append_indented_table<const N: usize>(
    output: &mut String,
    indent: &str,
    headers: &[&str; N],
    rows: &[[String; N]],
    alignments: &[ColumnAlign; N],
) {
    let widths = table_widths(headers, rows);

    append_indented_table_row(output, indent, headers, &widths, alignments);
    append_indented_line(output, indent, render_separator(&widths).as_str());
    for row in rows {
        append_indented_table_row(output, indent, row, &widths, alignments);
    }
}

fn append_indented_table_row<const N: usize>(
    output: &mut String,
    indent: &str,
    row: &[impl AsRef<str>; N],
    widths: &[usize; N],
    alignments: &[ColumnAlign; N],
) {
    append_indented_line(
        output,
        indent,
        render_table_row(row, widths, alignments).as_str(),
    );
}

fn append_indented_line(output: &mut String, indent: &str, line: &str) {
    output.push_str(indent);
    output.push_str(line);
    output.push('\n');
}

#[must_use]
fn table_widths<const N: usize>(headers: &[&str; N], rows: &[[String; N]]) -> [usize; N] {
    let mut widths = headers.map(str::chars).map(Iterator::count);

    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }

    widths
}

#[must_use]
fn render_table_row<const N: usize>(
    row: &[impl AsRef<str>; N],
    widths: &[usize; N],
    alignments: &[ColumnAlign; N],
) -> String {
    widths
        .iter()
        .zip(alignments)
        .enumerate()
        .map(|(index, (width, alignment))| {
            let value = row[index].as_ref();
            match alignment {
                ColumnAlign::Left => format!("{value:<width$}"),
                ColumnAlign::Right => format!("{value:>width$}"),
            }
        })
        .collect::<Vec<_>>()
        .join(COLUMN_GAP)
        .trim_end()
        .to_string()
}

#[must_use]
fn render_separator<const N: usize>(widths: &[usize; N]) -> String {
    widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>()
        .join(COLUMN_GAP)
}
