use std::time::Instant;

use icydb::{
    Error,
    db::sql::{SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput, render_grouped_lines},
};
use serde_json::Value;

use crate::shell::perf::{
    ShellLocalRenderAttribution, ShellPerfAttribution, find_result_payload, parse_perf_result,
    render_executor_residual_suffix, render_perf_suffix, render_pure_covering_suffix,
    render_shell_render_suffix,
};

pub(crate) fn render_shell_text_from_dfx_json(input: &str) -> Result<String, String> {
    let envelope: Value = serde_json::from_str(input).map_err(|err| err.to_string())?;
    let payload = find_result_payload(&envelope)
        .ok_or_else(|| "find Ok/Err result payload in dfx json envelope".to_string())?;

    if let Some(ok) = payload.get("Ok") {
        let (result, attribution) = parse_perf_result(ok)?;
        let render_start = Instant::now();
        let rendered = render_shell_text(result, Some(attribution), None);
        let render_attribution = ShellLocalRenderAttribution {
            render_micros: render_start.elapsed().as_micros(),
        };

        return Ok(append_shell_render_suffix(
            rendered,
            Some(&render_attribution),
        ));
    }

    let err: Error = serde_json::from_value(
        payload
            .get("Err")
            .cloned()
            .ok_or_else(|| "Err payload missing".to_string())?,
    )
    .map_err(|err| err.to_string())?;

    Ok(format!("ERROR: {err}"))
}

fn render_shell_text(
    result: SqlQueryResult,
    attribution: Option<ShellPerfAttribution>,
    render_attribution: Option<ShellLocalRenderAttribution>,
) -> String {
    match result {
        SqlQueryResult::Projection(rows) => {
            render_projection_shell_text(rows, attribution, render_attribution)
        }
        SqlQueryResult::Grouped(rows) => {
            render_grouped_shell_text(rows, attribution, render_attribution)
        }
        other => other.render_text(),
    }
}

pub(crate) fn render_projection_shell_text(
    mut rows: SqlQueryRowsOutput,
    attribution: Option<ShellPerfAttribution>,
    render_attribution: Option<ShellLocalRenderAttribution>,
) -> String {
    uppercase_null_cells(rows.rows.as_mut_slice());

    let mut lines =
        icydb::db::sql::render_projection_lines(rows.entity.as_str(), &rows.as_projection_rows());
    append_perf_suffix(
        lines.as_mut_slice(),
        attribution.as_ref(),
        render_attribution.as_ref(),
    );

    lines.join("\n")
}

pub(crate) fn render_grouped_shell_text(
    mut rows: SqlGroupedRowsOutput,
    attribution: Option<ShellPerfAttribution>,
    render_attribution: Option<ShellLocalRenderAttribution>,
) -> String {
    uppercase_null_cells(rows.rows.as_mut_slice());

    let mut lines = render_grouped_lines(&rows);
    append_perf_suffix(
        lines.as_mut_slice(),
        attribution.as_ref(),
        render_attribution.as_ref(),
    );

    lines.join("\n")
}

// Keep successful command output visually isolated so the next prompt or shell
// continuation appears after one blank separator line.
pub(crate) fn finalize_successful_command_output(rendered: &str) -> String {
    let mut finalized = String::with_capacity(rendered.len().saturating_add(2));
    finalized.push_str(rendered);
    finalized.push('\n');
    finalized.push('\n');

    finalized
}

fn uppercase_null_cells(rows: &mut [Vec<String>]) {
    for row in rows {
        for cell in row {
            if cell == "null" {
                *cell = "NULL".to_string();
            }
        }
    }
}

fn append_perf_suffix(
    lines: &mut [String],
    attribution: Option<&ShellPerfAttribution>,
    render_attribution: Option<&ShellLocalRenderAttribution>,
) {
    let Some(last) = lines.last_mut() else {
        return;
    };
    let perf_suffix = render_perf_suffix(attribution);
    let pure_covering_suffix = render_pure_covering_suffix(attribution);
    let executor_residual_suffix = render_executor_residual_suffix(attribution);
    let render_suffix = render_shell_render_suffix(render_attribution);
    if perf_suffix.is_none()
        && pure_covering_suffix.is_none()
        && executor_residual_suffix.is_none()
        && render_suffix.is_none()
    {
        return;
    }

    let mut suffixes = Vec::new();
    if let Some(perf_suffix) = perf_suffix {
        suffixes.push(perf_suffix);
    }
    if let Some(pure_covering_suffix) = pure_covering_suffix {
        suffixes.push(pure_covering_suffix);
    }
    if let Some(executor_residual_suffix) = executor_residual_suffix {
        suffixes.push(executor_residual_suffix);
    }
    if let Some(render_suffix) = render_suffix {
        suffixes.push(render_suffix);
    }

    *last = format!("{last} {}", suffixes.join(" "));
}

pub(crate) fn append_shell_render_suffix(
    rendered: String,
    render_attribution: Option<&ShellLocalRenderAttribution>,
) -> String {
    let Some(render_suffix) = render_shell_render_suffix(render_attribution) else {
        return rendered;
    };
    let mut lines = rendered.lines().map(str::to_string).collect::<Vec<_>>();
    let Some(last) = lines.last_mut() else {
        return rendered;
    };
    *last = format!("{last} {render_suffix}");

    lines.join("\n")
}
