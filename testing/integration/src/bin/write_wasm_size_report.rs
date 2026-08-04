use std::{
    env, fs,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};

use icydb_testing_integration::{
    canister_artifact::MAINTAINED_CANISTER_POLICIES,
    wasm_measurement::{
        WASM_LINE_BUDGETS, WASM_MEASUREMENT_COMPARISONS, WASM_MEASUREMENT_PROFILE_ID,
        WASM_MEASUREMENT_PROFILE_VERSION, WASM_MEASUREMENT_SUBJECTS, WASM_PATCH_BUDGETS,
        WasmComparison, WasmLineBudget, WasmPatchBudget, validate_wasm_measurement_contract,
    },
};
use serde::Serialize;
use sha2::{Digest, Sha256};

const SIZE_REPORT_FORMAT_VERSION: u32 = 2;

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
    analysis_shrunk_wasm: PathBuf,
    analysis_shrunk_gz: PathBuf,
    raw_info: PathBuf,
    analysis_shrunk_info: PathBuf,
    report_json: PathBuf,
    summary_md: PathBuf,
    ic_wasm_bin: PathBuf,
    wasm_opt_bin: PathBuf,
}

#[derive(Serialize)]
struct SizeReport {
    format_version: u32,
    measurement_profile: MeasurementProfile,
    provenance: Provenance,
    tools: Tools,
    pipeline: Pipeline,
    canister: String,
    profile: String,
    sql_variant: String,
    artifacts: Artifacts,
    analysis: Analysis,
    build: Build,
    deltas: Deltas,
}

#[derive(Serialize)]
struct MeasurementProfile {
    version: u32,
    identity: &'static str,
    comparisons: &'static [WasmComparison],
    patch_budgets: &'static [WasmPatchBudget],
    line_budgets: &'static [WasmLineBudget],
}

#[derive(Serialize)]
struct Provenance {
    source_revision: String,
    source_tree: String,
    source_dirty: bool,
    lockfile_sha256: String,
    workspace_root: String,
    cargo_target_dir: String,
    rust_toolchain: String,
}

#[derive(Serialize)]
struct Tools {
    ic_wasm_version: String,
    ic_wasm_sha256: String,
    wasm_opt_version: String,
    wasm_opt_sha256: String,
}

#[derive(Serialize)]
struct Pipeline {
    compiler_emitted_stage: &'static str,
    post_link_transform: &'static str,
    final_deployable_stage: &'static str,
    candid_metadata: &'static str,
    build_profile: &'static str,
    no_default_features: bool,
    path_remapping: &'static str,
}

#[derive(Serialize)]
struct Artifacts {
    did: Option<FileMeta>,
    candid_export: &'static str,
    icp_built_wasm: FileMeta,
    icp_built_wasm_gz_deterministic: FileMeta,
    icp_built_wasm_gz_emitted: Option<FileMeta>,
    analysis_shrunk_wasm: FileMeta,
    analysis_shrunk_wasm_gz: FileMeta,
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
    analysis_shrunk: WasmInfo,
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
    exact_features: Vec<String>,
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
    analysis_shrink_wasm_bytes: i64,
    analysis_shrink_wasm_gz_bytes: i64,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = parse_args(env::args().skip(1))?;
    validate_wasm_measurement_contract().map_err(str::to_string)?;
    if !WASM_MEASUREMENT_SUBJECTS.contains(&args.canister.as_str()) {
        return Err(format!(
            "canister '{}' is outside the 0.220 Wasm measurement contract",
            args.canister
        ));
    }

    let workspace_root = workspace_root()?;
    let provenance = capture_provenance(&workspace_root)?;
    let tools = capture_tools(&workspace_root, &args.ic_wasm_bin, &args.wasm_opt_bin)?;

    let raw_wasm = file_meta(&args.raw_wasm)?;
    let raw_gz = file_meta(&args.raw_gz)?;
    let raw_gz_emitted = optional_file_meta(&args.raw_gz_emitted)?;
    let analysis_shrunk_wasm = file_meta(&args.analysis_shrunk_wasm)?;
    let analysis_shrunk_gz = file_meta(&args.analysis_shrunk_gz)?;
    let did = optional_file_meta(&args.did)?;
    let raw_info = parse_info(&args.raw_info)?;
    let analysis_shrunk_info = parse_info(&args.analysis_shrunk_info)?;

    let candid_export = if did.is_some() {
        "available"
    } else {
        "unavailable"
    };
    let build = endpoint_surface(&args.canister, &args.sql_variant, &raw_info)?;
    let report = SizeReport {
        format_version: SIZE_REPORT_FORMAT_VERSION,
        measurement_profile: MeasurementProfile {
            version: WASM_MEASUREMENT_PROFILE_VERSION,
            identity: WASM_MEASUREMENT_PROFILE_ID,
            comparisons: WASM_MEASUREMENT_COMPARISONS,
            patch_budgets: WASM_PATCH_BUDGETS,
            line_budgets: WASM_LINE_BUDGETS,
        },
        provenance,
        tools,
        pipeline: Pipeline {
            compiler_emitted_stage: "icp_built_wasm",
            post_link_transform: "identity",
            final_deployable_stage: "icp_built_wasm",
            candid_metadata: "enabled",
            build_profile: "production",
            no_default_features: true,
            path_remapping: "workspace=/w;cargo-registry=/c;rust-library=/r",
        },
        canister: args.canister,
        profile: args.profile,
        sql_variant: args.sql_variant,
        artifacts: Artifacts {
            did,
            candid_export,
            icp_built_wasm: raw_wasm.clone(),
            icp_built_wasm_gz_deterministic: raw_gz.clone(),
            icp_built_wasm_gz_emitted: raw_gz_emitted,
            analysis_shrunk_wasm: analysis_shrunk_wasm.clone(),
            analysis_shrunk_wasm_gz: analysis_shrunk_gz.clone(),
        },
        analysis: Analysis {
            icp_built: raw_info,
            analysis_shrunk: analysis_shrunk_info,
        },
        build,
        deltas: Deltas {
            analysis_shrink_wasm_bytes: delta_bytes(&raw_wasm, &analysis_shrunk_wasm)?,
            analysis_shrink_wasm_gz_bytes: delta_bytes(&raw_gz, &analysis_shrunk_gz)?,
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
            "--analysis-shrunk-wasm" => {
                parsed.analysis_shrunk_wasm = Some(required_path(&arg, &mut args)?);
            }
            "--analysis-shrunk-gz" => {
                parsed.analysis_shrunk_gz = Some(required_path(&arg, &mut args)?);
            }
            "--raw-info" => parsed.raw_info = Some(required_path(&arg, &mut args)?),
            "--analysis-shrunk-info" => {
                parsed.analysis_shrunk_info = Some(required_path(&arg, &mut args)?);
            }
            "--report-json" => parsed.report_json = Some(required_path(&arg, &mut args)?),
            "--summary-md" => parsed.summary_md = Some(required_path(&arg, &mut args)?),
            "--ic-wasm-bin" => parsed.ic_wasm_bin = Some(required_path(&arg, &mut args)?),
            "--wasm-opt-bin" => parsed.wasm_opt_bin = Some(required_path(&arg, &mut args)?),
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
    analysis_shrunk_wasm: Option<PathBuf>,
    analysis_shrunk_gz: Option<PathBuf>,
    raw_info: Option<PathBuf>,
    analysis_shrunk_info: Option<PathBuf>,
    report_json: Option<PathBuf>,
    summary_md: Option<PathBuf>,
    ic_wasm_bin: Option<PathBuf>,
    wasm_opt_bin: Option<PathBuf>,
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
            analysis_shrunk_wasm: require_arg(self.analysis_shrunk_wasm, "--analysis-shrunk-wasm")?,
            analysis_shrunk_gz: require_arg(self.analysis_shrunk_gz, "--analysis-shrunk-gz")?,
            raw_info: require_arg(self.raw_info, "--raw-info")?,
            analysis_shrunk_info: require_arg(self.analysis_shrunk_info, "--analysis-shrunk-info")?,
            report_json: require_arg(self.report_json, "--report-json")?,
            summary_md: require_arg(self.summary_md, "--summary-md")?,
            ic_wasm_bin: require_arg(self.ic_wasm_bin, "--ic-wasm-bin")?,
            wasm_opt_bin: require_arg(self.wasm_opt_bin, "--wasm-opt-bin")?,
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
    "usage: write_wasm_size_report --canister name --profile profile --sql-variant sql-on|sql-off --did path --raw-wasm path --raw-gz path --raw-gz-emitted path --analysis-shrunk-wasm path --analysis-shrunk-gz path --raw-info path --analysis-shrunk-info path --report-json path --summary-md path --ic-wasm-bin path --wasm-opt-bin path".to_string()
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "integration crate should live two levels below workspace root".to_string())
}

fn capture_provenance(workspace_root: &Path) -> Result<Provenance, String> {
    let lockfile = workspace_root.join("Cargo.lock");
    let cargo_target_dir = env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| workspace_root.join("target"), PathBuf::from);

    Ok(Provenance {
        source_revision: command_text(workspace_root, Path::new("git"), &["rev-parse", "HEAD"])?,
        source_tree: command_text(
            workspace_root,
            Path::new("git"),
            &["rev-parse", "HEAD^{tree}"],
        )?,
        source_dirty: !command_text(
            workspace_root,
            Path::new("git"),
            &["status", "--porcelain=v1", "--untracked-files=normal"],
        )?
        .is_empty(),
        lockfile_sha256: sha256_hex(&lockfile)?,
        workspace_root: workspace_root.display().to_string(),
        cargo_target_dir: cargo_target_dir.display().to_string(),
        rust_toolchain: command_text(workspace_root, Path::new("rustc"), &["-vV"])?,
    })
}

fn capture_tools(
    workspace_root: &Path,
    ic_wasm_bin: &Path,
    wasm_opt_bin: &Path,
) -> Result<Tools, String> {
    if !ic_wasm_bin.is_file() {
        return Err(format!(
            "ic-wasm binary is missing: {}",
            ic_wasm_bin.display()
        ));
    }

    if !wasm_opt_bin.is_file() {
        return Err(format!(
            "wasm-opt binary is missing: {}",
            wasm_opt_bin.display()
        ));
    }

    Ok(Tools {
        ic_wasm_version: command_text(workspace_root, ic_wasm_bin, &["--version"])?,
        ic_wasm_sha256: sha256_hex(ic_wasm_bin)?,
        wasm_opt_version: command_text(workspace_root, wasm_opt_bin, &["--version"])?,
        wasm_opt_sha256: sha256_hex(wasm_opt_bin)?,
    })
}

fn command_text(current_dir: &Path, program: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .current_dir(current_dir)
        .args(args)
        .output()
        .map_err(|error| format!("failed to run {}: {error}", program.display()))?;
    if !output.status.success() {
        return Err(format!(
            "{} exited with status {}",
            program.display(),
            output.status
        ));
    }
    String::from_utf8(output.stdout)
        .map(|text| text.trim().to_string())
        .map_err(|error| format!("{} emitted non-UTF-8 output: {error}", program.display()))
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

fn endpoint_surface(canister: &str, sql_variant: &str, info: &WasmInfo) -> Result<Build, String> {
    let policy = MAINTAINED_CANISTER_POLICIES
        .iter()
        .find(|policy| policy.canister == canister)
        .ok_or_else(|| format!("no maintained canister policy exists for '{canister}'"))?;
    let exact_features = policy
        .production_features
        .iter()
        .copied()
        .filter(|feature| *feature == "candid-export" || sql_variant == "sql-on")
        .map(str::to_string)
        .collect();
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

    Ok(Build {
        exact_features,
        generated_endpoint_surface,
        custom_exports,
    })
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
            "| analysis-only shrunk `.wasm` | {} |",
            artifacts.analysis_shrunk_wasm.bytes
        ),
        format!(
            "| analysis-only shrunk `.wasm.gz` | {} |",
            artifacts.analysis_shrunk_wasm_gz.bytes
        ),
        format!(
            "| Analysis shrink delta `.wasm` | {} |",
            report.deltas.analysis_shrink_wasm_bytes
        ),
        format!(
            "| Analysis shrink delta `.wasm.gz` | {} |",
            report.deltas.analysis_shrink_wasm_gz_bytes
        ),
        String::new(),
        format!(
            "Measurement profile: `{}` (v{})",
            report.measurement_profile.identity, report.measurement_profile.version
        ),
        String::new(),
        format!("Source revision: `{}`", report.provenance.source_revision),
        String::new(),
        format!("Source dirty: `{}`", report.provenance.source_dirty),
        String::new(),
        format!(
            "Exact features: `{}`",
            report.build.exact_features.join(",")
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
            "Exports (final deployable): {}",
            report.analysis.icp_built.exported_method_count
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
        let build = endpoint_surface(
            "sql",
            "sql-on",
            &wasm_info(&[
                "canister_query icydb_query",
                "canister_update icydb_ddl",
                "canister_update icydb_fixtures_reset",
                "canister_update icydb_fixtures_load",
            ]),
        )
        .expect("maintained SQL policy should resolve");

        assert!(build.generated_endpoint_surface.sql_readonly);
        assert!(build.generated_endpoint_surface.sql_ddl);
        assert!(build.generated_endpoint_surface.sql_fixtures);
        assert!(!build.generated_endpoint_surface.sql_update);
        assert!(!build.generated_endpoint_surface.sql_integrity);
        assert!(build.custom_exports.is_empty());
    }

    #[test]
    fn endpoint_surface_reports_generated_sql_update_endpoint() {
        let build = endpoint_surface(
            "sql",
            "sql-on",
            &wasm_info(&[
                "canister_query icydb_query",
                "canister_update icydb_update",
                "canister_update icydb_integrity",
            ]),
        )
        .expect("maintained SQL policy should resolve");

        assert!(build.generated_endpoint_surface.sql_update);
        assert!(build.generated_endpoint_surface.sql_integrity);
        assert!(build.custom_exports.is_empty());
    }

    #[test]
    fn production_feature_identity_is_exact_and_variant_sensitive() {
        let sql_on = endpoint_surface("sql_perf", "sql-on", &wasm_info(&[]))
            .expect("maintained SQL perf policy should resolve");
        assert_eq!(
            sql_on.exact_features,
            ["candid-export", "diagnostics", "sql"]
        );

        let sql_off = endpoint_surface("sql_perf", "sql-off", &wasm_info(&[]))
            .expect("maintained SQL perf policy should resolve");
        assert_eq!(sql_off.exact_features, ["candid-export"]);
    }
}
