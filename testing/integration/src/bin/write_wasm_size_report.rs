//! Module: write_wasm_size_report
//! Responsibility: validate and render Wasm evidence.
//! Does not own: policy or builds.
//! Boundary: writes resolved report format v1.

use std::{
    env, fs,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};

use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterSqlMode,
    CanisterWasmProfile, ResolvedCanisterBuildConfiguration,
    resolve_fixture_canister_build_configuration,
    wasm_measurement::{
        MINIMUM_POST_LINK_RAW_REDUCTION_BASIS_POINTS, WASM_LINE_BUDGETS,
        WASM_MEASUREMENT_COMPARISONS, WASM_MEASUREMENT_PROFILE_ID,
        WASM_MEASUREMENT_PROFILE_VERSION, WASM_MEASUREMENT_SUBJECTS, WasmComparison,
        WasmLineBudget, validate_wasm_measurement_contract,
    },
    wasm_optimizer::{
        POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, WASM_OPT_OUTPUT_FEATURES, WASM_OPT_SHA256,
        WASM_OPT_VERSION,
    },
};
use serde::Serialize;
use sha2::{Digest, Sha256};

const SIZE_REPORT_FORMAT_VERSION: u32 = 1;

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
    build_options: CanisterBuildOptions,
    did: PathBuf,
    compiler_wasm: PathBuf,
    final_wasm: PathBuf,
    final_gz: PathBuf,
    compiler_info: PathBuf,
    final_info: PathBuf,
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
    compiler_emitted_wasm: FileMeta,
    final_deployable_wasm: FileMeta,
    final_deployable_wasm_gz: FileMeta,
}

#[derive(Clone, Serialize)]
struct FileMeta {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Serialize)]
struct Analysis {
    compiler_emitted: WasmInfo,
    final_deployable: WasmInfo,
    enabled_wasm_features: Vec<String>,
}

#[derive(Clone, Serialize)]
struct WasmInfo {
    function_count: Option<u64>,
    defined_function_count: u64,
    code_section_bytes: u64,
    call_indirect_count: u64,
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
    post_link_wasm_bytes: i64,
    post_link_reduction_basis_points: u16,
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
            "canister '{}' is outside the current Wasm measurement contract",
            args.canister
        ));
    }
    let resolved =
        resolve_fixture_canister_build_configuration(&args.canister, args.build_options)?;

    let workspace_root = workspace_root()?;
    let provenance = capture_provenance(&workspace_root)?;
    let tools = capture_tools(&workspace_root, &args.ic_wasm_bin, &args.wasm_opt_bin)?;
    if tools.wasm_opt_version != WASM_OPT_VERSION || tools.wasm_opt_sha256 != WASM_OPT_SHA256 {
        return Err(format!(
            "size report optimizer does not match the deployable pipeline: version='{}', sha256='{}'",
            tools.wasm_opt_version, tools.wasm_opt_sha256
        ));
    }

    let compiler_wasm = file_meta(&args.compiler_wasm)?;
    let final_wasm = file_meta(&args.final_wasm)?;
    let final_gz = file_meta(&args.final_gz)?;
    let did = optional_file_meta(&args.did)?;
    if did.is_some() != resolved.candid_export() {
        return Err(format!(
            "Candid artifact availability for '{}' does not match its resolved build configuration",
            args.canister
        ));
    }
    let compiler_info = parse_info(&args.compiler_info, &args.compiler_wasm, &args.wasm_opt_bin)?;
    let final_info = parse_info(&args.final_info, &args.final_wasm, &args.wasm_opt_bin)?;
    let enabled_wasm_features =
        validate_final_wasm_features(&workspace_root, &args.wasm_opt_bin, &args.final_wasm)?;

    let candid_export = if did.is_some() {
        "available"
    } else {
        "unavailable"
    };
    let (build, post_link_reduction_basis_points) = validate_post_link_contract(
        &args,
        &compiler_wasm,
        &final_wasm,
        &compiler_info,
        &final_info,
        &resolved,
    )?;
    let report = SizeReport {
        format_version: SIZE_REPORT_FORMAT_VERSION,
        measurement_profile: MeasurementProfile {
            version: WASM_MEASUREMENT_PROFILE_VERSION,
            identity: WASM_MEASUREMENT_PROFILE_ID,
            comparisons: WASM_MEASUREMENT_COMPARISONS,
            line_budgets: WASM_LINE_BUDGETS,
        },
        provenance,
        tools,
        pipeline: pipeline(&resolved),
        canister: args.canister,
        profile: resolved.profile().as_str().to_string(),
        sql_variant: resolved.sql_mode().report_variant().to_string(),
        artifacts: Artifacts {
            did,
            candid_export,
            compiler_emitted_wasm: compiler_wasm.clone(),
            final_deployable_wasm: final_wasm.clone(),
            final_deployable_wasm_gz: final_gz,
        },
        analysis: Analysis {
            compiler_emitted: compiler_info,
            final_deployable: final_info,
            enabled_wasm_features,
        },
        build,
        deltas: Deltas {
            post_link_wasm_bytes: delta_bytes(&compiler_wasm, &final_wasm)?,
            post_link_reduction_basis_points,
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
            "--profile" => {
                parsed.profile = Some(CanisterWasmProfile::parse(&required_value(
                    &arg, &mut args,
                )?)?);
            }
            "--build-profile" => {
                parsed.build_profile = Some(CanisterBuildProfile::parse(&required_value(
                    &arg, &mut args,
                )?)?);
            }
            "--sql-mode" => {
                parsed.sql_mode = Some(CanisterSqlMode::parse(&required_value(&arg, &mut args)?)?);
            }
            "--candid-export" => {
                parsed.candid_export = Some(CanisterCandidExportMode::parse(&required_value(
                    &arg, &mut args,
                )?)?);
            }
            "--did" => parsed.did = Some(required_path(&arg, &mut args)?),
            "--compiler-wasm" => parsed.compiler_wasm = Some(required_path(&arg, &mut args)?),
            "--final-wasm" => parsed.final_wasm = Some(required_path(&arg, &mut args)?),
            "--final-gz" => parsed.final_gz = Some(required_path(&arg, &mut args)?),
            "--compiler-info" => parsed.compiler_info = Some(required_path(&arg, &mut args)?),
            "--final-info" => parsed.final_info = Some(required_path(&arg, &mut args)?),
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
    profile: Option<CanisterWasmProfile>,
    build_profile: Option<CanisterBuildProfile>,
    sql_mode: Option<CanisterSqlMode>,
    candid_export: Option<CanisterCandidExportMode>,
    did: Option<PathBuf>,
    compiler_wasm: Option<PathBuf>,
    final_wasm: Option<PathBuf>,
    final_gz: Option<PathBuf>,
    compiler_info: Option<PathBuf>,
    final_info: Option<PathBuf>,
    report_json: Option<PathBuf>,
    summary_md: Option<PathBuf>,
    ic_wasm_bin: Option<PathBuf>,
    wasm_opt_bin: Option<PathBuf>,
}

impl ParsedArgs {
    fn finish(self) -> Result<Args, String> {
        Ok(Args {
            canister: require_arg(self.canister, "--canister")?,
            build_options: CanisterBuildOptions {
                profile: require_arg(self.profile, "--profile")?,
                build_profile: require_arg(self.build_profile, "--build-profile")?,
                sql_mode: require_arg(self.sql_mode, "--sql-mode")?,
                candid_export: require_arg(self.candid_export, "--candid-export")?,
            },
            did: require_arg(self.did, "--did")?,
            compiler_wasm: require_arg(self.compiler_wasm, "--compiler-wasm")?,
            final_wasm: require_arg(self.final_wasm, "--final-wasm")?,
            final_gz: require_arg(self.final_gz, "--final-gz")?,
            compiler_info: require_arg(self.compiler_info, "--compiler-info")?,
            final_info: require_arg(self.final_info, "--final-info")?,
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
    "usage: write_wasm_size_report --canister name --build-profile local|production --profile debug|release|wasm-release --sql-mode on|off --candid-export auto|on|off --did path --compiler-wasm path --final-wasm path --final-gz path --compiler-info path --final-info path --report-json path --summary-md path --ic-wasm-bin path --wasm-opt-bin path".to_string()
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

fn validate_final_wasm_features(
    workspace_root: &Path,
    wasm_opt_bin: &Path,
    final_wasm: &Path,
) -> Result<Vec<String>, String> {
    let output = Command::new(wasm_opt_bin)
        .current_dir(workspace_root)
        .arg(final_wasm)
        .args(&WASM_OPT_FLAGS[1..])
        .arg("--print-features")
        .output()
        .map_err(|error| format!("failed to validate final Wasm features: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "final Wasm feature validation failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim_end()
        ));
    }
    let mut features = String::from_utf8(output.stdout)
        .map_err(|error| format!("wasm-opt emitted non-UTF-8 feature output: {error}"))?
        .lines()
        .filter(|line| line.starts_with("--enable-"))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    features.sort();
    let expected = WASM_OPT_OUTPUT_FEATURES
        .iter()
        .map(|feature| (*feature).to_owned())
        .collect::<Vec<_>>();
    if features != expected {
        return Err(format!(
            "final Wasm feature set drifted: expected {expected:?}, observed {features:?}"
        ));
    }
    Ok(features)
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

fn parse_info(path: &Path, wasm_path: &Path, wasm_opt_bin: &Path) -> Result<WasmInfo, String> {
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    let exported_methods = parse_exported_methods(&text);
    let (defined_function_count, code_section_bytes) = wasm_code_structure(wasm_path)?;
    let call_indirect_count = wasm_call_indirect_count(wasm_path, wasm_opt_bin)?;
    Ok(WasmInfo {
        function_count: int_field(&text, "Number of functions:"),
        defined_function_count,
        code_section_bytes,
        call_indirect_count,
        callback_count: int_field(&text, "Number of callbacks:"),
        data_section_count: int_field(&text, "Number of data sections:"),
        data_section_bytes: int_field(&text, "Size of data sections:"),
        exported_method_count: exported_methods.len(),
        exported_methods,
    })
}

fn wasm_code_structure(path: &Path) -> Result<(u64, u64), String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("failed to read Wasm '{}': {error}", path.display()))?;
    if !bytes.starts_with(b"\0asm\x01\0\0\0") {
        return Err(format!("invalid Wasm header: '{}'", path.display()));
    }

    let mut position = 8_usize;
    let mut defined_functions = 0_u64;
    let mut code_section_bytes = 0_u64;
    while position < bytes.len() {
        let section = bytes[position];
        position = position.saturating_add(1);
        let payload_len = usize::try_from(read_u32_leb(&bytes, &mut position)?)
            .map_err(|_| format!("Wasm section is too large: '{}'", path.display()))?;
        let payload_end = position
            .checked_add(payload_len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| format!("truncated Wasm section: '{}'", path.display()))?;
        if section == 3 {
            let mut payload_position = position;
            defined_functions = u64::from(read_u32_leb(&bytes, &mut payload_position)?);
        } else if section == 10 {
            code_section_bytes = u64::try_from(payload_len)
                .map_err(|_| format!("Wasm code section is too large: '{}'", path.display()))?;
        }
        position = payload_end;
    }

    Ok((defined_functions, code_section_bytes))
}

fn read_u32_leb(bytes: &[u8], position: &mut usize) -> Result<u32, String> {
    let mut value = 0_u32;
    for shift in (0..35).step_by(7) {
        let byte = *bytes
            .get(*position)
            .ok_or_else(|| "truncated unsigned LEB128 value".to_string())?;
        *position = position.saturating_add(1);
        let payload = u32::from(byte & 0x7f);
        if shift == 28 && payload > 0x0f {
            return Err("unsigned LEB128 value exceeds u32".to_string());
        }
        value |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("unsigned LEB128 value exceeds five bytes".to_string())
}

fn wasm_call_indirect_count(path: &Path, wasm_opt_bin: &Path) -> Result<u64, String> {
    let output = Command::new(wasm_opt_bin)
        .arg(path)
        .args(["--metrics", "--all-features"])
        .output()
        .map_err(|error| {
            format!(
                "failed to run '{}' for Wasm metrics: {error}",
                wasm_opt_bin.display()
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "Wasm metrics failed for '{}': {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let metrics = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    metric_field(&metrics, "CallIndirect")
        .ok_or_else(|| format!("Wasm metrics omitted CallIndirect for '{}'", path.display()))
}

fn metric_field(text: &str, label: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (name, value) = line.trim().split_once(':')?;
        if name.trim() != label {
            return None;
        }
        value.split_whitespace().next()?.parse::<u64>().ok()
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

fn endpoint_surface(resolved: &ResolvedCanisterBuildConfiguration, info: &WasmInfo) -> Build {
    let exact_features = resolved
        .features()
        .iter()
        .map(|feature| (*feature).to_string())
        .collect();
    let names = exported_method_names(info);
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
        exact_features,
        generated_endpoint_surface,
        custom_exports,
    }
}

const fn pipeline(resolved: &ResolvedCanisterBuildConfiguration) -> Pipeline {
    Pipeline {
        compiler_emitted_stage: "cargo_wasm",
        post_link_transform: POST_LINK_PIPELINE_IDENTITY,
        final_deployable_stage: "binaryen_oz_wasm",
        candid_metadata: if resolved.candid_export() {
            "enabled"
        } else {
            "disabled"
        },
        build_profile: resolved.build_profile().as_str(),
        no_default_features: resolved.no_default_features(),
        path_remapping: if resolved.path_trimming() {
            "workspace=/w;cargo-registry=/c;rust-library=/r"
        } else {
            "disabled"
        },
    }
}

fn validate_post_link_contract(
    args: &Args,
    compiler_wasm: &FileMeta,
    final_wasm: &FileMeta,
    compiler_info: &WasmInfo,
    final_info: &WasmInfo,
    resolved: &ResolvedCanisterBuildConfiguration,
) -> Result<(Build, u16), String> {
    let compiler_exports = exported_method_names(compiler_info);
    let final_exports = exported_method_names(final_info);
    if compiler_exports != final_exports {
        return Err(format!(
            "post-link export drift for {}: compiler={compiler_exports:?}, final={final_exports:?}",
            args.canister
        ));
    }
    let build = endpoint_surface(resolved, final_info);
    let reduction = reduction_basis_points(compiler_wasm, final_wasm)?;
    if reduction < MINIMUM_POST_LINK_RAW_REDUCTION_BASIS_POINTS {
        return Err(format!(
            "post-link raw-Wasm budget failed for {}: observed={}bp, required={}bp",
            args.canister, reduction, MINIMUM_POST_LINK_RAW_REDUCTION_BASIS_POINTS
        ));
    }
    Ok((build, reduction))
}

fn exported_method_names(info: &WasmInfo) -> Vec<&str> {
    info.exported_methods
        .iter()
        .map(|export| export_name(export))
        .collect()
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
    Ok(after - before)
}

fn reduction_basis_points(before: &FileMeta, after: &FileMeta) -> Result<u16, String> {
    if before.bytes == 0 || after.bytes > before.bytes {
        return Err(format!(
            "post-link artifact did not reduce compiler output: before={}, after={}",
            before.bytes, after.bytes
        ));
    }
    let reduction = before.bytes - after.bytes;
    let basis_points = reduction
        .checked_mul(10_000)
        .ok_or_else(|| "post-link reduction basis-point arithmetic overflowed".to_string())?
        / before.bytes;
    u16::try_from(basis_points)
        .map_err(|_| format!("post-link reduction basis points exceed u16: {basis_points}"))
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
        format!(
            "| compiler-emitted `.wasm` | {} |",
            artifacts.compiler_emitted_wasm.bytes
        ),
        format!(
            "| final deployable `.wasm` | {} |",
            artifacts.final_deployable_wasm.bytes
        ),
        format!(
            "| final deployable deterministic `.wasm.gz` | {} |",
            artifacts.final_deployable_wasm_gz.bytes
        ),
        format!("| candid export | {} |", artifacts.candid_export),
        format!(
            "| Post-link delta `.wasm` | {} |",
            report.deltas.post_link_wasm_bytes
        ),
        format!(
            "| Post-link reduction | {} basis points |",
            report.deltas.post_link_reduction_basis_points
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
    ];

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
            report.analysis.final_deployable.exported_method_count
        ),
    ]);
    lines.extend(render_final_structure_summary(report));
    lines.extend([
        String::new(),
        format!("JSON report: `{}`", report_path.display()),
    ]);

    format!("{}\n", lines.join("\n"))
}

fn render_final_structure_summary(report: &SizeReport) -> [String; 6] {
    let final_wasm = &report.analysis.final_deployable;
    [
        String::new(),
        format!(
            "Defined functions (final deployable): {}",
            final_wasm.defined_function_count
        ),
        String::new(),
        format!(
            "Code section (final deployable): {} bytes",
            final_wasm.code_section_bytes
        ),
        String::new(),
        format!(
            "`call_indirect` (final deployable): {}",
            final_wasm.call_indirect_count
        ),
    ]
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
    use icydb_testing_integration::{
        CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterSqlMode,
        CanisterWasmProfile, ResolvedCanisterBuildConfiguration,
        resolve_fixture_canister_build_configuration,
    };

    use super::{WasmInfo, endpoint_surface, parse_args, pipeline};

    fn resolved(
        canister: &str,
        profile: CanisterWasmProfile,
        sql_mode: CanisterSqlMode,
        candid_export: CanisterCandidExportMode,
    ) -> ResolvedCanisterBuildConfiguration {
        resolve_fixture_canister_build_configuration(
            canister,
            CanisterBuildOptions {
                profile,
                sql_mode,
                candid_export,
                build_profile: CanisterBuildProfile::Production,
            },
        )
        .expect("maintained canister build configuration should resolve")
    }

    fn wasm_info(exported_methods: &[&str]) -> WasmInfo {
        WasmInfo {
            function_count: None,
            defined_function_count: 0,
            code_section_bytes: 0,
            call_indirect_count: 0,
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
        let resolved = resolved(
            "sql",
            CanisterWasmProfile::WasmRelease,
            CanisterSqlMode::Enabled,
            CanisterCandidExportMode::Enabled,
        );
        let build = endpoint_surface(
            &resolved,
            &wasm_info(&[
                "canister_query icydb_query",
                "canister_update icydb_ddl",
                "canister_update icydb_fixtures_reset",
                "canister_update icydb_fixtures_load",
            ]),
        );

        assert!(build.generated_endpoint_surface.sql_readonly);
        assert!(build.generated_endpoint_surface.sql_ddl);
        assert!(build.generated_endpoint_surface.sql_fixtures);
        assert!(!build.generated_endpoint_surface.sql_update);
        assert!(!build.generated_endpoint_surface.sql_integrity);
        assert!(build.custom_exports.is_empty());
    }

    #[test]
    fn endpoint_surface_reports_generated_sql_update_endpoint() {
        let resolved = resolved(
            "sql",
            CanisterWasmProfile::WasmRelease,
            CanisterSqlMode::Enabled,
            CanisterCandidExportMode::Enabled,
        );
        let build = endpoint_surface(
            &resolved,
            &wasm_info(&[
                "canister_query icydb_query",
                "canister_update icydb_update",
                "canister_update icydb_integrity",
            ]),
        );

        assert!(build.generated_endpoint_surface.sql_update);
        assert!(build.generated_endpoint_surface.sql_integrity);
        assert!(build.custom_exports.is_empty());
    }

    #[test]
    fn production_feature_identity_is_exact_and_variant_sensitive() {
        let sql_on = endpoint_surface(
            &resolved(
                "sql_perf",
                CanisterWasmProfile::WasmRelease,
                CanisterSqlMode::Enabled,
                CanisterCandidExportMode::Enabled,
            ),
            &wasm_info(&[]),
        );
        assert_eq!(
            sql_on.exact_features,
            ["candid-export", "diagnostics", "sql"]
        );

        let sql_off = endpoint_surface(
            &resolved(
                "sql_perf",
                CanisterWasmProfile::WasmRelease,
                CanisterSqlMode::Disabled,
                CanisterCandidExportMode::Enabled,
            ),
            &wasm_info(&[]),
        );
        assert_eq!(sql_off.exact_features, ["candid-export"]);
    }

    #[test]
    fn pipeline_identity_comes_from_resolved_build_configuration() {
        let debug = resolved(
            "sql_perf",
            CanisterWasmProfile::Debug,
            CanisterSqlMode::Disabled,
            CanisterCandidExportMode::Disabled,
        );
        let pipeline = pipeline(&debug);

        assert_eq!(pipeline.candid_metadata, "disabled");
        assert_eq!(pipeline.build_profile, "production");
        assert!(pipeline.no_default_features);
        assert_eq!(pipeline.path_remapping, "disabled");
    }

    #[test]
    fn report_arguments_reject_untyped_build_labels() {
        assert!(parse_args(["--profile", "profile-ish"].map(str::to_string)).is_err());
        assert!(parse_args(["--sql-mode", "maybe"].map(str::to_string)).is_err());
    }
}
