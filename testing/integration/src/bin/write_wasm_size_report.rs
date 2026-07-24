use std::{
    env, fs,
    io::Read,
    path::{Path, PathBuf},
};

use serde::Serialize;
use sha2::{Digest, Sha256};

const GENERATED_EXPORTS: &[&str] = &[
    "icydb_query",
    "icydb_ddl",
    "icydb_update",
    "icydb_integrity",
    "icydb_fixtures_reset",
    "icydb_fixtures_load",
    "icydb_metrics",
    "icydb_metrics_extended",
    "icydb_metrics_reset",
    "icydb_snapshot",
    "icydb_schema",
    "icydb_schema_check",
];

#[derive(Debug)]
struct Args {
    canister: String,
    profile: String,
    sql_variant: String,
    did: PathBuf,
    raw_wasm: PathBuf,
    raw_gz: PathBuf,
    raw_gz_emitted: PathBuf,
    shrunk_wasm: PathBuf,
    shrunk_gz: PathBuf,
    raw_info: PathBuf,
    shrunk_info: PathBuf,
    report_json: PathBuf,
    summary_md: PathBuf,
}

#[derive(Serialize)]
struct SizeReport {
    canister: String,
    profile: String,
    sql_variant: String,
    artifacts: Artifacts,
    analysis: Analysis,
    build: Build,
    deltas: Deltas,
}

#[derive(Serialize)]
struct Artifacts {
    did: Option<FileMeta>,
    candid_export: &'static str,
    icp_built_wasm: FileMeta,
    icp_built_wasm_gz_deterministic: FileMeta,
    icp_built_wasm_gz_emitted: Option<FileMeta>,
    icp_shrunk_wasm: FileMeta,
    icp_shrunk_wasm_gz: FileMeta,
}

#[derive(Clone, Serialize)]
struct FileMeta {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Serialize)]
struct Analysis {
    icp_built: WasmInfo,
    icp_shrunk: WasmInfo,
}

#[derive(Clone, Serialize)]
struct WasmInfo {
    function_count: Option<u64>,
    callback_count: Option<u64>,
    data_section_count: Option<u64>,
    data_section_bytes: Option<u64>,
    exported_method_count: usize,
    exported_methods: Vec<String>,
}

#[derive(Serialize)]
struct Build {
    generated_endpoint_surface: GeneratedEndpointSurface,
    custom_exports: Vec<String>,
}

#[derive(Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "matches the JSON report schema"
)]
struct GeneratedEndpointSurface {
    sql_readonly: bool,
    sql_ddl: bool,
    sql_update: bool,
    sql_integrity: bool,
    sql_fixtures: bool,
    metrics: bool,
    metrics_extended: bool,
    snapshot: bool,
    schema: bool,
}

#[derive(Serialize)]
struct Deltas {
    shrink_wasm_bytes: i64,
    shrink_wasm_gz_bytes: i64,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = parse_args(env::args().skip(1))?;

    let raw_wasm = file_meta(&args.raw_wasm)?;
    let raw_gz = file_meta(&args.raw_gz)?;
    let raw_gz_emitted = optional_file_meta(&args.raw_gz_emitted)?;
    let shrunk_wasm = file_meta(&args.shrunk_wasm)?;
    let shrunk_gz = file_meta(&args.shrunk_gz)?;
    let did = optional_file_meta(&args.did)?;
    let raw_info = parse_info(&args.raw_info)?;
    let shrunk_info = parse_info(&args.shrunk_info)?;

    let candid_export = if did.is_some() {
        "available"
    } else {
        "unavailable"
    };
    let build = endpoint_surface(&shrunk_info);
    let report = SizeReport {
        canister: args.canister,
        profile: args.profile,
        sql_variant: args.sql_variant,
        artifacts: Artifacts {
            did,
            candid_export,
            icp_built_wasm: raw_wasm.clone(),
            icp_built_wasm_gz_deterministic: raw_gz.clone(),
            icp_built_wasm_gz_emitted: raw_gz_emitted,
            icp_shrunk_wasm: shrunk_wasm.clone(),
            icp_shrunk_wasm_gz: shrunk_gz.clone(),
        },
        analysis: Analysis {
            icp_built: raw_info,
            icp_shrunk: shrunk_info,
        },
        build,
        deltas: Deltas {
            shrink_wasm_bytes: delta_bytes(&raw_wasm, &shrunk_wasm)?,
            shrink_wasm_gz_bytes: delta_bytes(&raw_gz, &shrunk_gz)?,
        },
    };

    let json = serde_json::to_string_pretty(&report)
        .map_err(|err| format!("failed to serialize size report JSON: {err}"))?;
    fs::write(&args.report_json, format!("{json}\n")).map_err(|err| {
        format!(
            "failed to write report JSON {}: {err}",
            args.report_json.display()
        )
    })?;

    let summary = render_summary(&report, &args.report_json);
    fs::write(&args.summary_md, &summary).map_err(|err| {
        format!(
            "failed to write summary markdown {}: {err}",
            args.summary_md.display()
        )
    })?;

    if let Ok(step_summary) = env::var("GITHUB_STEP_SUMMARY") {
        append_step_summary(Path::new(&step_summary), &summary)?;
    }

    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut parsed = ParsedArgs::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--canister" => parsed.canister = Some(required_value(&arg, &mut args)?),
            "--profile" => parsed.profile = Some(required_value(&arg, &mut args)?),
            "--sql-variant" => parsed.sql_variant = Some(required_value(&arg, &mut args)?),
            "--did" => parsed.did = Some(required_path(&arg, &mut args)?),
            "--raw-wasm" => parsed.raw_wasm = Some(required_path(&arg, &mut args)?),
            "--raw-gz" => parsed.raw_gz = Some(required_path(&arg, &mut args)?),
            "--raw-gz-emitted" => parsed.raw_gz_emitted = Some(required_path(&arg, &mut args)?),
            "--shrunk-wasm" => parsed.shrunk_wasm = Some(required_path(&arg, &mut args)?),
            "--shrunk-gz" => parsed.shrunk_gz = Some(required_path(&arg, &mut args)?),
            "--raw-info" => parsed.raw_info = Some(required_path(&arg, &mut args)?),
            "--shrunk-info" => parsed.shrunk_info = Some(required_path(&arg, &mut args)?),
            "--report-json" => parsed.report_json = Some(required_path(&arg, &mut args)?),
            "--summary-md" => parsed.summary_md = Some(required_path(&arg, &mut args)?),
            "--help" | "-h" => return Err(usage()),
            value => return Err(format!("unknown option '{value}'\n{}", usage())),
        }
    }

    parsed.finish()
}

#[derive(Default)]
struct ParsedArgs {
    canister: Option<String>,
    profile: Option<String>,
    sql_variant: Option<String>,
    did: Option<PathBuf>,
    raw_wasm: Option<PathBuf>,
    raw_gz: Option<PathBuf>,
    raw_gz_emitted: Option<PathBuf>,
    shrunk_wasm: Option<PathBuf>,
    shrunk_gz: Option<PathBuf>,
    raw_info: Option<PathBuf>,
    shrunk_info: Option<PathBuf>,
    report_json: Option<PathBuf>,
    summary_md: Option<PathBuf>,
}

impl ParsedArgs {
    fn finish(self) -> Result<Args, String> {
        Ok(Args {
            canister: require_arg(self.canister, "--canister")?,
            profile: require_arg(self.profile, "--profile")?,
            sql_variant: require_arg(self.sql_variant, "--sql-variant")?,
            did: require_arg(self.did, "--did")?,
            raw_wasm: require_arg(self.raw_wasm, "--raw-wasm")?,
            raw_gz: require_arg(self.raw_gz, "--raw-gz")?,
            raw_gz_emitted: require_arg(self.raw_gz_emitted, "--raw-gz-emitted")?,
            shrunk_wasm: require_arg(self.shrunk_wasm, "--shrunk-wasm")?,
            shrunk_gz: require_arg(self.shrunk_gz, "--shrunk-gz")?,
            raw_info: require_arg(self.raw_info, "--raw-info")?,
            shrunk_info: require_arg(self.shrunk_info, "--shrunk-info")?,
            report_json: require_arg(self.report_json, "--report-json")?,
            summary_md: require_arg(self.summary_md, "--summary-md")?,
        })
    }
}

fn required_value(flag: &str, args: &mut impl Iterator<Item = String>) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value\n{}", usage()))
}

fn required_path(flag: &str, args: &mut impl Iterator<Item = String>) -> Result<PathBuf, String> {
    required_value(flag, args).map(PathBuf::from)
}

fn require_arg<T>(value: Option<T>, flag: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("missing required argument {flag}\n{}", usage()))
}

fn usage() -> String {
    "usage: write_wasm_size_report --canister name --profile profile --sql-variant sql-on|sql-off --did path --raw-wasm path --raw-gz path --raw-gz-emitted path --shrunk-wasm path --shrunk-gz path --raw-info path --shrunk-info path --report-json path --summary-md path".to_string()
}

fn file_meta(path: &Path) -> Result<FileMeta, String> {
    let bytes = path
        .metadata()
        .map_err(|err| format!("failed to stat {}: {err}", path.display()))?
        .len();
    Ok(FileMeta {
        path: path.display().to_string(),
        bytes,
        sha256: sha256_hex(path)?,
    })
}

fn optional_file_meta(path: &Path) -> Result<Option<FileMeta>, String> {
    if path.exists() {
        file_meta(path).map(Some)
    } else {
        Ok(None)
    }
}

fn sha256_hex(path: &Path) -> Result<String, String> {
    let mut file =
        fs::File::open(path).map_err(|err| format!("failed to open {}: {err}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];

    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(encode_hex_lower(&hasher.finalize()))
}

fn encode_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn parse_info(path: &Path) -> Result<WasmInfo, String> {
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    let exported_methods = parse_exported_methods(&text);
    Ok(WasmInfo {
        function_count: int_field(&text, "Number of functions:"),
        callback_count: int_field(&text, "Number of callbacks:"),
        data_section_count: int_field(&text, "Number of data sections:"),
        data_section_bytes: int_field(&text, "Size of data sections:"),
        exported_method_count: exported_methods.len(),
        exported_methods,
    })
}

fn int_field(text: &str, prefix: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.trim().strip_prefix(prefix))
        .and_then(|rest| {
            rest.split_whitespace()
                .find_map(|word| word.parse::<u64>().ok())
        })
}

fn parse_exported_methods(text: &str) -> Vec<String> {
    let mut exports = Vec::new();
    let mut in_block = false;

    for line in text.lines() {
        let trimmed = line.trim();
        if in_block {
            if trimmed == "]" {
                break;
            }
            if let Some(export) = parse_export_line(trimmed) {
                exports.push(export);
            }
        } else if let Some(rest) = trimmed.strip_prefix("Exported methods:") {
            in_block = true;
            if let Some(export) = parse_export_line(rest.trim()) {
                exports.push(export);
            }
        }
    }

    exports
}

fn parse_export_line(line: &str) -> Option<String> {
    let line = line.trim_end_matches(',').trim();
    if line == "[]" || line == "[" || line == "]" {
        return None;
    }
    line.strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .map(ToOwned::to_owned)
}

fn endpoint_surface(info: &WasmInfo) -> Build {
    let names = info
        .exported_methods
        .iter()
        .map(|export| export_name(export))
        .collect::<Vec<_>>();
    let generated_endpoint_surface = GeneratedEndpointSurface {
        sql_readonly: names.contains(&"icydb_query"),
        sql_ddl: names.contains(&"icydb_ddl"),
        sql_update: names.contains(&"icydb_update"),
        sql_integrity: names.contains(&"icydb_integrity"),
        sql_fixtures: names.contains(&"icydb_fixtures_reset")
            || names.contains(&"icydb_fixtures_load"),
        metrics: names.contains(&"icydb_metrics"),
        metrics_extended: names.contains(&"icydb_metrics_extended"),
        snapshot: names.contains(&"icydb_snapshot"),
        schema: names.contains(&"icydb_schema") || names.contains(&"icydb_schema_check"),
    };
    let custom_exports = names
        .into_iter()
        .filter(|name| !GENERATED_EXPORTS.contains(name) && *name != "get_candid_pointer")
        .map(ToOwned::to_owned)
        .collect();

    Build {
        generated_endpoint_surface,
        custom_exports,
    }
}

fn export_name(export: &str) -> &str {
    if let Some(rest) = export.strip_prefix("canister_query ") {
        return rest.split_whitespace().next().unwrap_or(rest);
    }
    if let Some(rest) = export.strip_prefix("canister_update ") {
        return rest.split_whitespace().next().unwrap_or(rest);
    }
    export.split_whitespace().next().unwrap_or(export)
}

fn delta_bytes(before: &FileMeta, after: &FileMeta) -> Result<i64, String> {
    let before = i64::try_from(before.bytes)
        .map_err(|_| format!("file too large to diff: {}", before.path))?;
    let after = i64::try_from(after.bytes)
        .map_err(|_| format!("file too large to diff: {}", after.path))?;
    Ok(before - after)
}

fn render_summary(report: &SizeReport, report_path: &Path) -> String {
    let artifacts = &report.artifacts;
    let mut lines = vec![
        format!(
            "## Wasm Size Report: `{}` ({}, {})",
            report.canister, report.profile, report.sql_variant
        ),
        String::new(),
        "| Artifact | Bytes |".to_string(),
        "| --- | ---: |".to_string(),
        format!("| icp-built `.wasm` | {} |", artifacts.icp_built_wasm.bytes),
        format!(
            "| icp-built deterministic `.wasm.gz` | {} |",
            artifacts.icp_built_wasm_gz_deterministic.bytes
        ),
    ];

    if let Some(emitted) = &artifacts.icp_built_wasm_gz_emitted {
        lines.push(format!("| icp-emitted `.wasm.gz` | {} |", emitted.bytes));
    }

    lines.extend([
        format!("| candid export | {} |", artifacts.candid_export),
        format!(
            "| icp-shrunk `.wasm` (canonical) | {} |",
            artifacts.icp_shrunk_wasm.bytes
        ),
        format!(
            "| icp-shrunk `.wasm.gz` (canonical) | {} |",
            artifacts.icp_shrunk_wasm_gz.bytes
        ),
        format!(
            "| Shrink delta `.wasm` | {} |",
            report.deltas.shrink_wasm_bytes
        ),
        format!(
            "| Shrink delta `.wasm.gz` | {} |",
            report.deltas.shrink_wasm_gz_bytes
        ),
        String::new(),
        format!("SQL variant: `{}`", report.sql_variant),
        String::new(),
        "Generated endpoint surface:".to_string(),
        String::new(),
        "| Option | Enabled |".to_string(),
        "| --- | --- |".to_string(),
    ]);

    let surface = &report.build.generated_endpoint_surface;
    let surface_rows = [
        ("sql_readonly", surface.sql_readonly),
        ("sql_ddl", surface.sql_ddl),
        ("sql_update", surface.sql_update),
        ("sql_integrity", surface.sql_integrity),
        ("sql_fixtures", surface.sql_fixtures),
        ("metrics", surface.metrics),
        ("metrics_extended", surface.metrics_extended),
        ("snapshot", surface.snapshot),
        ("schema", surface.schema),
    ];
    for (option, enabled) in surface_rows {
        lines.push(format!(
            "| `{option}` | {} |",
            if enabled { "yes" } else { "no" }
        ));
    }

    let custom_exports = if report.build.custom_exports.is_empty() {
        "none".to_string()
    } else {
        report
            .build
            .custom_exports
            .iter()
            .map(|export| format!("`{export}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    lines.extend([
        String::new(),
        format!("Custom exports: {custom_exports}"),
        String::new(),
        format!(
            "Exports (shrunk): {}",
            report.analysis.icp_shrunk.exported_method_count
        ),
        String::new(),
        format!("JSON report: `{}`", report_path.display()),
    ]);

    format!("{}\n", lines.join("\n"))
}

fn append_step_summary(path: &Path, summary: &str) -> Result<(), String> {
    use std::io::Write;

    let mut file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .map_err(|err| format!("failed to open step summary {}: {err}", path.display()))?;
    file.write_all(summary.as_bytes())
        .map_err(|err| format!("failed to write step summary {}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{WasmInfo, endpoint_surface};

    fn wasm_info(exported_methods: &[&str]) -> WasmInfo {
        WasmInfo {
            function_count: None,
            callback_count: None,
            data_section_count: None,
            data_section_bytes: None,
            exported_method_count: exported_methods.len(),
            exported_methods: exported_methods
                .iter()
                .map(|export| (*export).to_string())
                .collect(),
        }
    }

    #[test]
    fn endpoint_surface_reports_absent_generated_sql_update_endpoint() {
        let build = endpoint_surface(&wasm_info(&[
            "canister_query icydb_query",
            "canister_update icydb_ddl",
            "canister_update icydb_fixtures_reset",
            "canister_update icydb_fixtures_load",
        ]));

        assert!(build.generated_endpoint_surface.sql_readonly);
        assert!(build.generated_endpoint_surface.sql_ddl);
        assert!(build.generated_endpoint_surface.sql_fixtures);
        assert!(!build.generated_endpoint_surface.sql_update);
        assert!(!build.generated_endpoint_surface.sql_integrity);
        assert!(build.custom_exports.is_empty());
    }

    #[test]
    fn endpoint_surface_reports_generated_sql_update_endpoint() {
        let build = endpoint_surface(&wasm_info(&[
            "canister_query icydb_query",
            "canister_update icydb_update",
            "canister_update icydb_integrity",
        ]));

        assert!(build.generated_endpoint_surface.sql_update);
        assert!(build.generated_endpoint_surface.sql_integrity);
        assert!(build.custom_exports.is_empty());
    }
}
