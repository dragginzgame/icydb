const COLUMN_GAP: &str = "   ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnAlign {
    Left,
    Right,
}

/// Render a whitespace-aligned table with an underlined header row.
#[must_use]
pub(crate) fn render_table<const N: usize>(
    headers: &[&str; N],
    rows: &[[String; N]],
    alignments: &[ColumnAlign; N],
) -> String {
    let widths = table_widths(headers, rows);
    let mut lines = Vec::with_capacity(rows.len() + 2);

    lines.push(render_table_row(headers, &widths, alignments));
    lines.push(render_separator(&widths));
    lines.extend(
        rows.iter()
            .map(|row| render_table_row(row, &widths, alignments)),
    );

    lines.join("\n")
}

pub(crate) fn append_indented_table<const N: usize>(
    output: &mut String,
    indent: &str,
    headers: &[&str; N],
    rows: &[[String; N]],
    alignments: &[ColumnAlign; N],
) {
    let table = render_table(headers, rows, alignments);

    for line in table.lines() {
        output.push_str(indent);
        output.push_str(line);
        output.push('\n');
    }
}

#[must_use]
pub(crate) fn table_widths<const N: usize>(
    headers: &[&str; N],
    rows: &[[String; N]],
) -> [usize; N] {
    let mut widths = headers.map(str::chars).map(Iterator::count);

    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }

    widths
}

#[must_use]
pub(crate) fn render_table_row<const N: usize>(
    row: &[impl AsRef<str>],
    widths: &[usize; N],
    alignments: &[ColumnAlign; N],
) -> String {
    widths
        .iter()
        .zip(alignments)
        .enumerate()
        .map(|(index, (width, alignment))| {
            let value = row.get(index).map_or("", AsRef::as_ref);
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
pub(crate) fn render_separator<const N: usize>(widths: &[usize; N]) -> String {
    widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>()
        .join(COLUMN_GAP)
}
