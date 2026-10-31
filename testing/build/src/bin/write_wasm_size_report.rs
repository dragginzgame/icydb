//! Module: write_wasm_size_report
//! Responsibility: acquire retained canister artifacts, validate and render evidence.
//! Does not own: Cargo execution, post-link transforms or measurement policy.
//! Boundary: writes report format v1 from the retained build's observations.

use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use clap::Parser;
use ic_build::artifacts::CargoBuildFacts;
use ic_host_artifacts::{
    artifact::{Sha256Digest, encode_gzip},
    wasm::{ExportKind, InspectionLimits, WasmFacts, inspect},
};
use ic_host_fs::read::hash_file;
use ic_host_process::tool::{
    AdmittedTool, CompletedExecution, OutputLimit, OutputLimits, capture_command,
    format_failure_evidence,
};
use ic_host_tools::interface::exported_methods;
use ic_metrics::checked_scaled_ratio;
use icydb::__macro::RpcMethod;
use icydb_testing_build::{
    BuiltCanisterArtifacts, CanisterBuildOptions, CanisterBuildProfile, CanisterSqlMode,
    CanisterWasmProfile, ResolvedCanisterBuildConfiguration, build_canister_with_options,
    canister_artifact::read_wasm_artifact,
    resolve_fixture_canister_build_configuration,
    wasm_measurement::{
        MINIMUM_POST_LINK_RAW_REDUCTION_BASIS_POINTS, WASM_LINE_BUDGETS,
        WASM_MEASUREMENT_COMPARISONS, WASM_MEASUREMENT_PROFILE_ID,
        WASM_MEASUREMENT_PROFILE_VERSION, WasmComparison, WasmLineBudget,
        validate_wasm_measurement_contract, wasm_measurement_subjects,
    },
    wasm_optimizer::{
        POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, WASM_OPT_OUTPUT_FEATURES,
        admit_wasm_optimizer, run_wasm_optimizer,
    },
};
use serde::Serialize;

const SIZE_REPORT_FORMAT_VERSION: u32 = 1;

// These commands emit version/provenance or structural text, never Wasm bytes.
// Match the existing optimizer capture envelope; this is an operational bound.
const REPORT_OUTPUT_LIMITS: OutputLimits = OutputLimits {
    stdout: OutputLimit::Terminate(1024 * 1024),
    stderr: OutputLimit::Terminate(1024 * 1024),
    timeout: Some(Duration::from_secs(600)),
};

#[derive(Debug, Parser)]
#[command(about = "Acquire retained canister artifacts and write Wasm size evidence")]
struct Args {
    #[arg(long)]
    canister: String,
    #[arg(long, value_parser = CanisterWasmProfile::parse)]
    profile: CanisterWasmProfile,
    #[arg(long, value_parser = CanisterBuildProfile::parse)]
    build_profile: CanisterBuildProfile,
    #[arg(long, value_parser = CanisterSqlMode::parse)]
    sql_mode: CanisterSqlMode,
    #[arg(long)]
    compiler_wasm: PathBuf,
    #[arg(long)]
    final_wasm: PathBuf,
    #[arg(long)]
    final_gz: PathBuf,
    #[arg(long)]
    compiler_info: PathBuf,
    #[arg(long)]
    final_info: PathBuf,
    #[arg(long)]
    report_json: PathBuf,
    #[arg(long)]
    summary_md: PathBuf,
    #[arg(long)]
    ic_wasm_bin: PathBuf,
    #[arg(long)]
    wasm_opt_bin: PathBuf,
}

impl Args {
    const fn build_options(&self) -> CanisterBuildOptions {
        CanisterBuildOptions {
            profile: self.profile,
            build_profile: self.build_profile,
            sql_mode: self.sql_mode,
        }
    }
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
    build_profile: &'static str,
    no_default_features: bool,
    path_remapping: &'static str,
}

#[derive(Serialize)]
struct Artifacts {
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
    #[serde(skip)]
    method_names: Vec<String>,
}

#[derive(Serialize)]
struct Build {
    target: String,
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
    let args = Args::parse();
    validate_wasm_measurement_contract().map_err(str::to_string)?;
    if !wasm_measurement_subjects().any(|name| name == args.canister) {
        return Err(format!(
            "canister '{}' is outside the current Wasm measurement contract",
            args.canister
        ));
    }
    let resolved =
        resolve_fixture_canister_build_configuration(&args.canister, args.build_options())?;

    let workspace_root = workspace_root()?;
    let optimizer = admit_wasm_optimizer(&args.wasm_opt_bin, &workspace_root)?;
    let tools = capture_tools(&workspace_root, &args.ic_wasm_bin, &optimizer)?;
    // Retain both cache owners until evidence is copied and rendered. The facts
    // belong to this acquisition, including reuse; reporting never probes rustc.
    let artifacts = build_canister_with_options(&args.canister, args.build_options())?;
    let facts = artifacts.cargo().facts();
    let provenance = capture_provenance(&workspace_root, facts)?;
    write_report_artifacts(&args, &artifacts, &workspace_root)?;

    let compiler_wasm = file_meta(&args.compiler_wasm)?;
    let final_wasm = file_meta(&args.final_wasm)?;
    let final_gz = file_meta(&args.final_gz)?;
    let compiler_info = parse_info(&args.compiler_info, &args.compiler_wasm, &optimizer)?;
    let final_info = parse_info(&args.final_info, &args.final_wasm, &optimizer)?;
    let enabled_wasm_features =
        validate_final_wasm_features(&workspace_root, &optimizer, &args.final_wasm)?;

    let (build, post_link_reduction_basis_points) = validate_post_link_contract(
        &args,
        &compiler_wasm,
        &final_wasm,
        &compiler_info,
        &final_info,
        facts,
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
        pipeline: pipeline(&resolved, facts),
        canister: args.canister,
        profile: facts
            .selection
            .profile
            .clone()
            .unwrap_or_else(|| "debug".to_string()),
        sql_variant: resolved.sql_mode().report_variant().to_string(),
        artifacts: Artifacts {
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

// Copy only paths borrowed from the live retained owner. Inspection and gzip
// are report outputs, not a second build recipe or an ICP staging contract.
fn write_report_artifacts(
    args: &Args,
    artifacts: &BuiltCanisterArtifacts,
    workspace_root: &Path,
) -> Result<(), String> {
    for (source, destination) in [
        (artifacts.compiler_emitted(), &args.compiler_wasm),
        (artifacts.as_ref(), &args.final_wasm),
    ] {
        fs::copy(source, destination)
            .map_err(|err| format!("copy retained artifact to {}: {err}", destination.display()))?;
    }
    let final_bytes = read_wasm_artifact(&args.final_wasm).map_err(|err| err.to_string())?;
    let gzip = fs::File::create(&args.final_gz)
        .map_err(|err| format!("create {}: {err}", args.final_gz.display()))?;
    encode_gzip(&final_bytes, gzip, 9, u64::MAX)
        .map_err(|err| format!("compress {}: {err}", args.final_gz.display()))?;
    for (wasm, output) in [
        (&args.compiler_wasm, &args.compiler_info),
        (&args.final_wasm, &args.final_info),
    ] {
        let info = command_text(
            workspace_root,
            &args.ic_wasm_bin,
            &[wasm.as_os_str().to_owned(), OsString::from("info")],
        )?;
        fs::write(output, format!("{info}\n"))
            .map_err(|err| format!("write {}: {err}", output.display()))?;
    }

    Ok(())
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "build crate should live two levels below workspace root".to_string())
}

fn capture_provenance(
    workspace_root: &Path,
    facts: &CargoBuildFacts,
) -> Result<Provenance, String> {
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
        // Neither environment values nor raw probe bytes are report-safe.
        // Build fingerprints stdout and stderr; wrappers can log private inputs.
        rust_toolchain: format!("sha256:{}", Sha256Digest::compute(&facts.rustc_identity)),
    })
}

fn capture_tools(
    workspace_root: &Path,
    ic_wasm_bin: &Path,
    optimizer: &AdmittedTool,
) -> Result<Tools, String> {
    if !ic_wasm_bin.is_file() {
        return Err(format!(
            "ic-wasm binary is missing: {}",
            ic_wasm_bin.display()
        ));
    }

    Ok(Tools {
        ic_wasm_version: command_text(workspace_root, ic_wasm_bin, &["--version"])?,
        ic_wasm_sha256: sha256_hex(ic_wasm_bin)?,
        wasm_opt_version: optimizer.version_identity().to_owned(),
        wasm_opt_sha256: optimizer.identity().sha256.to_string(),
    })
}

fn command_text(
    current_dir: &Path,
    program: &Path,
    args: &[impl AsRef<OsStr>],
) -> Result<String, String> {
    let output = capture_command(
        Command::new(program).current_dir(current_dir).args(args),
        REPORT_OUTPUT_LIMITS,
    )
    .and_then(CompletedExecution::require_success)
    .map_err(|error| format_failure_evidence(&format!("run {}", program.display()), &error))?;
    String::from_utf8(output.stdout)
        .map(|text| text.trim().to_string())
        .map_err(|error| format!("{} emitted non-UTF-8 output: {error}", program.display()))
}

fn validate_final_wasm_features(
    workspace_root: &Path,
    optimizer: &AdmittedTool,
    final_wasm: &Path,
) -> Result<Vec<String>, String> {
    let arguments = std::iter::once(final_wasm.as_os_str().to_owned())
        .chain(WASM_OPT_FLAGS[1..].iter().map(OsString::from))
        .chain([OsString::from("--print-features")])
        .collect::<Vec<_>>();
    let output = run_wasm_optimizer(
        optimizer,
        &arguments,
        workspace_root,
        "final Wasm feature validation",
    )?;
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
    // The bytes and digest describe the same complete stream, rather than a
    // separate pathname stat followed by a second open for hashing.
    let identity = hash_file(path, u64::MAX)
        .map_err(|error| format!("failed to identify {}: {error}", path.display()))?;
    Ok(FileMeta {
        path: path.display().to_string(),
        bytes: identity.bytes,
        sha256: identity.sha256.to_string(),
    })
}

fn sha256_hex(path: &Path) -> Result<String, String> {
    // Preserve the report's whole-stream policy; it has no per-artifact byte cap.
    let identity = hash_file(path, u64::MAX)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    Ok(identity.sha256.to_string())
}

fn parse_info(path: &Path, wasm_path: &Path, optimizer: &AdmittedTool) -> Result<WasmInfo, String> {
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    let bytes = read_wasm_artifact(wasm_path)
        .map_err(|error| format!("failed to read Wasm '{}': {error}", wasm_path.display()))?;
    let call_indirect_count = wasm_call_indirect_count(wasm_path, optimizer)?;
    wasm_structure(&bytes, &text, call_indirect_count)
        .map_err(|error| format!("failed to inspect Wasm '{}': {error}", wasm_path.display()))
}

fn wasm_structure(bytes: &[u8], text: &str, call_indirect_count: u64) -> Result<WasmInfo, String> {
    // Each structural entry consumes input bytes. Natural input-size ceilings
    // preserve whole-artifact reporting without adding a consumer budget knob.
    let facts = inspect(
        bytes,
        InspectionLimits {
            module_bytes: bytes.len(),
            sections: bytes.len(),
            exports: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
            custom_sections: bytes.len(),
        },
    )
    .map_err(|error| error.to_string())?;
    report_wasm_facts(&facts, text, call_indirect_count)
}

// Project canonical facts into the local report schema. Keep tool-only metrics
// separate: data payload bytes exclude the encoded data-section framing.
fn report_wasm_facts(
    facts: &WasmFacts<'_>,
    text: &str,
    call_indirect_count: u64,
) -> Result<WasmInfo, String> {
    let code_section_bytes = u64::try_from(facts.code_section_bytes)
        .map_err(|_| "Wasm code section is too large".to_string())?;
    let exports = facts
        .exports
        .iter()
        .filter(|(_, export)| export.kind == ExportKind::Function)
        .map(|(name, _)| (*name).to_owned())
        .collect::<Vec<_>>();
    Ok(WasmInfo {
        function_count: Some(
            u64::from(facts.defined_functions) + u64::from(facts.imported_functions),
        ),
        defined_function_count: u64::from(facts.defined_functions),
        code_section_bytes,
        call_indirect_count,
        callback_count: int_field(text, "Number of callbacks:"),
        data_section_count: Some(u64::from(facts.data_segments)),
        data_section_bytes: int_field(text, "Size of data sections:"),
        exported_method_count: exports.len(),
        exported_methods: exports,
        method_names: exported_methods(facts)
            .map(|method| method.name.to_owned())
            .collect(),
    })
}

fn wasm_call_indirect_count(path: &Path, optimizer: &AdmittedTool) -> Result<u64, String> {
    let arguments = [
        path.as_os_str().to_owned(),
        OsString::from("--metrics"),
        OsString::from("--all-features"),
    ];
    let current_dir = env::current_dir()
        .map_err(|error| format!("failed to resolve metrics working directory: {error}"))?;
    let output = run_wasm_optimizer(
        optimizer,
        &arguments,
        &current_dir,
        &format!("Wasm metrics for '{}'", path.display()),
    )?;
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

fn endpoint_surface(facts: &CargoBuildFacts, info: &WasmInfo) -> Build {
    let exact_features = facts.selection.features.features.iter().cloned().collect();
    let names = info
        .method_names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let generated_endpoint_surface = GeneratedEndpointSurface {
        sql_readonly: names.contains(&"icydb_query"),
        sql_ddl: names.contains(&"icydb_ddl"),
        sql_update: names.contains(&"icydb_update"),
        sql_integrity: names.contains(&"icydb_integrity"),
        sql_fixtures: names.contains(&"icydb_fixtures_reset")
            || names.contains(&"icydb_fixtures_load"),
        metrics: names.contains(&"icydb_metrics"),
        snapshot: names.contains(&"icydb_snapshot"),
        schema: names.iter().any(|name| {
            matches!(
                RpcMethod::from_name(name),
                Some(RpcMethod::Schema | RpcMethod::SchemaMigrate | RpcMethod::SchemaMigration)
            )
        }),
    };
    let custom_exports = names
        .into_iter()
        .filter(|name| RpcMethod::from_name(name).is_none())
        .map(ToOwned::to_owned)
        .collect();

    Build {
        target: facts.target.clone(),
        exact_features,
        generated_endpoint_surface,
        custom_exports,
    }
}

const fn pipeline(
    resolved: &ResolvedCanisterBuildConfiguration,
    facts: &CargoBuildFacts,
) -> Pipeline {
    Pipeline {
        compiler_emitted_stage: "cargo_wasm",
        post_link_transform: POST_LINK_PIPELINE_IDENTITY,
        final_deployable_stage: "binaryen_oz_wasm",
        build_profile: resolved.build_profile().as_str(),
        no_default_features: !facts.selection.features.default_features,
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
    facts: &CargoBuildFacts,
) -> Result<(Build, u16), String> {
    let compiler_exports = &compiler_info.exported_methods;
    let final_exports = &final_info.exported_methods;
    if compiler_exports != final_exports {
        return Err(format!(
            "post-link export drift for {}: compiler={compiler_exports:?}, final={final_exports:?}",
            args.canister
        ));
    }
    let build = endpoint_surface(facts, final_info);
    let reduction = reduction_basis_points(compiler_wasm, final_wasm)?;
    if reduction < MINIMUM_POST_LINK_RAW_REDUCTION_BASIS_POINTS {
        return Err(format!(
            "post-link raw-Wasm budget failed for {}: observed={}bp, required={}bp",
            args.canister, reduction, MINIMUM_POST_LINK_RAW_REDUCTION_BASIS_POINTS
        ));
    }
    Ok((build, reduction))
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
    let basis_points = checked_scaled_ratio(u128::from(reduction), before.bytes, 10_000)
        .map_err(|err| format!("post-link reduction basis-point arithmetic failed: {err}"))?;
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

///
/// TESTS
///
#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env, fs};

    use clap::Parser;

    use ic_build::{
        artifacts::CargoBuildFacts,
        cargo::selection::{CargoFeatureSelection, CargoSelection},
    };

    use icydb_testing_build::{
        CanisterBuildOptions, CanisterBuildProfile, CanisterSqlMode, CanisterWasmProfile,
        ResolvedCanisterBuildConfiguration, resolve_fixture_canister_build_configuration,
    };

    use super::{
        Args, FileMeta, WasmInfo, command_text, endpoint_surface, file_meta, pipeline,
        reduction_basis_points, validate_final_wasm_features, wasm_call_indirect_count,
        wasm_structure,
    };

    #[test]
    fn reduction_keeps_floor_rounding_and_handles_full_width_artifact_sizes() {
        let artifact = |bytes| FileMeta {
            path: "fixture.wasm".into(),
            bytes,
            sha256: String::new(),
        };
        assert_eq!(
            reduction_basis_points(&artifact(3), &artifact(2)).unwrap(),
            3_333
        );
        assert_eq!(
            reduction_basis_points(&artifact(10), &artifact(10)).unwrap(),
            0
        );
        assert_eq!(
            reduction_basis_points(&artifact(u64::MAX), &artifact(0)).unwrap(),
            10_000
        );
        assert_eq!(
            reduction_basis_points(&artifact(u64::MAX), &artifact(u64::MAX - 1)).unwrap(),
            0
        );
        assert!(reduction_basis_points(&artifact(0), &artifact(0)).is_err());
        assert!(reduction_basis_points(&artifact(10), &artifact(11)).is_err());
    }

    #[test]
    fn report_capture_keeps_native_arguments_utf8_and_refuses_failed_or_oversized_output() {
        let current_dir = env::current_dir().unwrap();
        let shell = std::path::Path::new("sh");
        assert_eq!(
            command_text(
                &current_dir,
                shell,
                &["-c", "printf '  %s  \\n' \"$1\"", "report", "spaced value"]
            )
            .unwrap(),
            "spaced value"
        );
        for script in [
            "printf '\\377'",
            "printf 'partial output'; printf 'producer failure' >&2; exit 7",
            "i=0; while [ \"$i\" -lt 1025 ]; do printf '%01024d' 0; i=$((i + 1)); done",
            "i=0; while [ \"$i\" -lt 1025 ]; do printf '%01024d' 0 >&2; i=$((i + 1)); done",
        ] {
            assert!(command_text(&current_dir, shell, &["-c", script]).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn report_capture_keeps_feature_and_metrics_parsing_for_spaced_artifact_paths() {
        use ic_host_process::{
            testing::write_executable_script,
            tool::{AdmittedTool, ExecutionContext, VersionSpec},
        };

        let root =
            ic_host_fs::testing::create_fixture_directory(&env::temp_dir(), "icydb report capture")
                .unwrap();
        let tool = root.join("fixture tool");
        let wasm = root.join("selected artifact.wasm");
        fs::write(&wasm, b"\0asm\x01\0\0\0").unwrap();
        let features = crate::WASM_OPT_OUTPUT_FEATURES.join("\n");
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then echo fixture; exit 0; fi\n[ -f \"$1\" ] || exit 9\ncase \"$2\" in\n--metrics) [ \"$3\" = --all-features ] || exit 6; printf 'CallIndirect : 7\\n' >&2 ;;\n--enable-bulk-memory) [ \"$6\" = --print-features ] || exit 5; printf '%s\\n' '{features}' ;;\n*) exit 8 ;;\nesac\n"
        );
        // Keep writable executable descriptors out of parallel parent forks.
        write_executable_script(&tool, script.as_bytes()).unwrap();
        let selected_tool = fs::canonicalize(&tool).unwrap();
        let optimizer = AdmittedTool::admit_version(
            &VersionSpec {
                executable: &selected_tool,
                executable_bytes: u64::MAX,
                version_arguments: &["--version".into()],
            },
            "fixture",
            &ExecutionContext {
                current_dir: &root,
                environment: &[],
            },
            crate::REPORT_OUTPUT_LIMITS,
        )
        .unwrap();
        let tools = super::capture_tools(&root, &tool, &optimizer).unwrap();
        assert_eq!(tools.wasm_opt_version, optimizer.version_identity());
        assert_eq!(
            tools.wasm_opt_sha256,
            optimizer.identity().sha256.to_string()
        );
        assert_eq!(
            validate_final_wasm_features(&root, &optimizer, &wasm).unwrap(),
            crate::WASM_OPT_OUTPUT_FEATURES
        );
        assert_eq!(wasm_call_indirect_count(&wasm, &optimizer).unwrap(), 7);
        // Queries must retain admission authority even if the pathname is reused.
        // Keep otherwise-valid output: failure must come from executable drift,
        // rather than a malformed feature/metric response masking execution.
        let mut changed_tool = fs::read(&tool).unwrap();
        changed_tool.extend_from_slice(b"\n# changed executable identity\n");
        write_executable_script(&tool, &changed_tool).unwrap();
        assert!(validate_final_wasm_features(&root, &optimizer, &wasm).is_err());
        assert!(wasm_call_indirect_count(&wasm, &optimizer).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn wasm_structure_reports_defined_functions_and_complete_code_payload_bytes() {
        let mut bytes = b"\0asm\x01\0\0\0".to_vec();
        let info = wasm_structure(&bytes, "", 0).unwrap();
        assert_eq!(
            (info.defined_function_count, info.code_section_bytes),
            (0, 0)
        );
        assert_eq!(info.function_count, Some(0));

        // Imported functions do not count as defined functions.
        bytes.extend([1, 4, 1, 0x60, 0, 0, 2, 7, 1, 1, b'm', 1, b'f', 0, 0]);
        let info = wasm_structure(&bytes, "Number of functions: 999", 0).unwrap();
        assert_eq!(
            (info.defined_function_count, info.code_section_bytes),
            (0, 0)
        );
        assert_eq!(info.function_count, Some(1));

        // Two defined functions: one body crosses the one-byte LEB128 boundary.
        // The code payload includes its vector count and both body lengths,
        // while excluding the section ID and the section-length prefix.
        bytes.extend([3, 3, 2, 0, 0, 10, 0x88, 0x01, 2, 0x82, 0x01, 0]);
        bytes.extend(std::iter::repeat_n(0x01, 128));
        bytes.extend([0x0b, 2, 0, 0x0b, 0, 2, 1, b'x']);
        let info = wasm_structure(&bytes, "", 7).unwrap();
        assert_eq!(
            (info.defined_function_count, info.code_section_bytes),
            (2, 136)
        );
        assert_eq!(info.function_count, Some(3));
        assert_eq!(info.call_indirect_count, 7);
    }

    #[test]
    fn wasm_structure_rejects_malformed_framing_and_inconsistent_function_counts() {
        for tail in [
            &[3, 2, 1, 0][..],
            &[3, 2, 1, 0, 10, 1, 0][..],
            &[10, 1, 1][..],
            &[1, 0x80, 0x80, 0x80, 0x80, 0x10][..],
        ] {
            let bytes = [b"\0asm\x01\0\0\0".as_slice(), tail].concat();
            assert!(wasm_structure(&bytes, "", 0).is_err());
        }
    }

    #[test]
    fn artifact_identity_keeps_report_fields_and_hashes_complete_files() {
        let root = ic_host_fs::testing::create_fixture_directory(
            &env::temp_dir(),
            "icydb-report-artifact",
        )
        .unwrap();
        let path = root.join("artifact.wasm");
        let streamed_bytes = vec![0x80; 16 * 1024 + 1];
        for (bytes, digest) in [
            (
                b"".as_slice(),
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                b"abc".as_slice(),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                streamed_bytes.as_slice(),
                "06d3b72e7e774a68bba5660dc7225971c8069732da200495a9a5869d074cd8a8",
            ),
        ] {
            fs::write(&path, bytes).expect("report artifact should be writable");
            let report =
                serde_json::to_value(file_meta(&path).expect("artifact should be readable"))
                    .expect("artifact metadata should serialize");
            assert_eq!(
                report,
                serde_json::json!({
                    "path": path.display().to_string(),
                    "bytes": bytes.len(),
                    "sha256": digest,
                })
            );
        }
        fs::remove_dir_all(root).expect("report fixture should be removable");
        assert!(file_meta(&path).is_err());
    }

    fn resolved(
        canister: &str,
        profile: CanisterWasmProfile,
        sql_mode: CanisterSqlMode,
    ) -> ResolvedCanisterBuildConfiguration {
        resolve_fixture_canister_build_configuration(
            canister,
            CanisterBuildOptions {
                profile,
                sql_mode,
                build_profile: CanisterBuildProfile::Production,
            },
        )
        .expect("maintained canister build configuration should resolve")
    }

    fn build_facts(
        canister: &str,
        profile: CanisterWasmProfile,
        sql_mode: CanisterSqlMode,
    ) -> CargoBuildFacts {
        let config = resolved(canister, profile, sql_mode);
        CargoBuildFacts {
            selection: CargoSelection {
                profile: Some(profile.as_str().to_string()),
                features: CargoFeatureSelection {
                    features: config
                        .features()
                        .iter()
                        .map(|name| (*name).to_owned())
                        .collect(),
                    default_features: !config.no_default_features(),
                    all_features: false,
                },
                locked: true,
                offline: false,
            },
            target: "wasm32-unknown-unknown".to_string(),
            environment: BTreeMap::default(),
            cargo_identity: b"retained cargo\n".to_vec(),
            rustc_identity: b"retained compiler\n".to_vec(),
        }
    }

    #[test]
    fn reporting_uses_retained_observations_without_exposing_the_build_environment() {
        let mut facts = build_facts(
            "sql_perf",
            CanisterWasmProfile::Debug,
            CanisterSqlMode::Disabled,
        );
        facts
            .selection
            .features
            .features
            .insert("observed-feature".to_string());
        facts.selection.features.default_features = true;
        facts
            .environment
            .insert("SECRET".into(), "private-build-input".into());
        let workspace = super::workspace_root().unwrap();
        let provenance = super::capture_provenance(&workspace, &facts).unwrap();
        assert_eq!(
            provenance.rust_toolchain,
            "sha256:ce5ce60b780e1e91986400e36ac372c4f2ecf49a6c5eaf6acc1041f3b9dda81a"
        );
        let json = serde_json::to_value(&provenance).unwrap();
        assert!(!json.to_string().contains("private-build-input"));
        let build = endpoint_surface(&facts, &wasm_info(&[]));
        assert_eq!(build.exact_features, ["observed-feature"]);
        assert_eq!(build.target, "wasm32-unknown-unknown");
        let config = resolved(
            "sql_perf",
            CanisterWasmProfile::Debug,
            CanisterSqlMode::Disabled,
        );
        assert!(!pipeline(&config, &facts).no_default_features);
        facts.rustc_identity = vec![0xff];
        let other = super::capture_provenance(&workspace, &facts).unwrap();
        assert_ne!(other.rust_toolchain, provenance.rust_toolchain);
    }

    fn wasm_info(exported_methods: &[&str]) -> WasmInfo {
        use ic_host_artifacts::wasm::{Export, ExportKind, InspectionLimits, inspect};

        let mut facts = inspect(
            b"\0asm\x01\0\0\0",
            InspectionLimits {
                module_bytes: 8,
                sections: 0,
                exports: 0,
                custom_sections: 0,
            },
        )
        .unwrap();
        for name in exported_methods {
            facts.exports.insert(
                name,
                Export {
                    kind: ExportKind::Function,
                    index: 0,
                },
            );
        }
        // A namespace-looking non-function export must not become an endpoint.
        facts.exports.insert(
            "canister_query icydb_schema",
            Export {
                kind: ExportKind::Memory,
                index: 0,
            },
        );
        super::report_wasm_facts(&facts, "Exported methods: [\"wrong\"]", 0).unwrap()
    }

    #[test]
    fn endpoint_inventory_preserves_exact_names_and_ignores_non_methods() {
        let info = wasm_info(&[
            "canister_composite_query icydb_query",
            "canister_query spaced name (with parentheses)",
            "canister_update quoted\"name",
            "canister_init",
            "ordinary_helper",
        ]);
        let build = endpoint_surface(
            &build_facts(
                "sql",
                CanisterWasmProfile::WasmRelease,
                CanisterSqlMode::Enabled,
            ),
            &info,
        );
        assert!(build.generated_endpoint_surface.sql_readonly);
        assert!(!build.generated_endpoint_surface.schema);
        assert_eq!(
            build.custom_exports,
            ["spaced name (with parentheses)", "quoted\"name"]
        );
        assert_eq!(info.exported_method_count, 5);
        assert!(info.exported_methods.contains(&"canister_init".to_string()));
        assert!(
            info.exported_methods
                .contains(&"ordinary_helper".to_string())
        );
        let json = serde_json::to_value(&info).unwrap();
        assert!(json.get("method_names").is_none());
        assert_eq!(
            json["exported_methods"],
            serde_json::json!(info.exported_methods)
        );
    }

    #[test]
    fn post_link_contract_preserves_modes_lifecycle_exports_and_complete_names() {
        let mut arguments = [
            "--canister",
            "sql",
            "--profile",
            "wasm-release",
            "--build-profile",
            "production",
            "--sql-mode",
            "on",
        ]
        .map(str::to_string)
        .to_vec();
        for flag in [
            "--compiler-wasm",
            "--final-wasm",
            "--final-gz",
            "--compiler-info",
            "--final-info",
            "--report-json",
            "--summary-md",
            "--ic-wasm-bin",
            "--wasm-opt-bin",
        ] {
            arguments.extend([flag.to_string(), "unused".to_string()]);
        }
        let args = Args::try_parse_from(
            std::iter::once("write_wasm_size_report".to_string()).chain(arguments),
        )
        .unwrap();
        let before = super::FileMeta {
            path: "before".to_string(),
            bytes: 100,
            sha256: String::new(),
        };
        let after = super::FileMeta {
            path: "after".to_string(),
            bytes: 1,
            sha256: String::new(),
        };
        let resolved = build_facts(
            "sql",
            CanisterWasmProfile::WasmRelease,
            CanisterSqlMode::Enabled,
        );
        let compiler = wasm_info(&[
            "canister_query spaced name",
            "canister_init",
            "ordinary_helper",
        ]);
        assert!(
            super::validate_post_link_contract(
                &args, &before, &after, &compiler, &compiler, &resolved
            )
            .is_ok()
        );
        for changed in [
            [
                "canister_update spaced name",
                "canister_init",
                "ordinary_helper",
            ],
            [
                "canister_query spaced other",
                "canister_init",
                "ordinary_helper",
            ],
            [
                "canister_query spaced name",
                "canister_pre_upgrade",
                "ordinary_helper",
            ],
            [
                "canister_query spaced name",
                "canister_init",
                "other_helper",
            ],
        ] {
            assert!(
                super::validate_post_link_contract(
                    &args,
                    &before,
                    &after,
                    &compiler,
                    &wasm_info(&changed),
                    &resolved
                )
                .is_err()
            );
        }
    }

    #[test]
    fn data_payload_metric_keeps_its_meaning_separate_from_encoded_section_bytes() {
        // One passive segment containing three bytes; encoded payload is six bytes.
        let bytes = b"\0asm\x01\0\0\0\x0b\x06\x01\x01\x03abc";
        let info = wasm_structure(
            bytes,
            "Number of data sections: 999\nSize of data sections: 3 bytes\nNumber of callbacks: 2",
            0,
        )
        .unwrap();
        assert_eq!(info.data_section_count, Some(1));
        assert_eq!(info.data_section_bytes, Some(3));
        assert_eq!(info.callback_count, Some(2));
    }

    #[test]
    fn endpoint_surface_reports_absent_generated_sql_update_endpoint() {
        let resolved = build_facts(
            "sql",
            CanisterWasmProfile::WasmRelease,
            CanisterSqlMode::Enabled,
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
        let resolved = build_facts(
            "sql",
            CanisterWasmProfile::WasmRelease,
            CanisterSqlMode::Enabled,
        );
        let build = endpoint_surface(
            &resolved,
            &wasm_info(&[
                "canister_query icydb_query",
                "canister_update icydb_update",
                "canister_update icydb_integrity",
                "canister_update icydb_schema_migrate",
                "canister_query icydb_schema_migration",
                "canister_query custom_query",
            ]),
        );

        assert!(build.generated_endpoint_surface.sql_update);
        assert!(build.generated_endpoint_surface.sql_integrity);
        assert!(build.generated_endpoint_surface.schema);
        assert_eq!(build.custom_exports, ["custom_query"]);
    }

    #[test]
    fn production_feature_identity_is_exact_and_variant_sensitive() {
        let sql_on = endpoint_surface(
            &build_facts(
                "sql_perf",
                CanisterWasmProfile::WasmRelease,
                CanisterSqlMode::Enabled,
            ),
            &wasm_info(&[]),
        );
        assert_eq!(sql_on.exact_features, ["sql"]);

        let sql_off = endpoint_surface(
            &build_facts(
                "sql_perf",
                CanisterWasmProfile::WasmRelease,
                CanisterSqlMode::Disabled,
            ),
            &wasm_info(&[]),
        );
        assert!(sql_off.exact_features.is_empty());
    }

    #[test]
    fn pipeline_identity_comes_from_resolved_build_configuration() {
        let debug = resolved(
            "sql_perf",
            CanisterWasmProfile::Debug,
            CanisterSqlMode::Disabled,
        );
        let pipeline = pipeline(
            &debug,
            &build_facts(
                "sql_perf",
                CanisterWasmProfile::Debug,
                CanisterSqlMode::Disabled,
            ),
        );

        assert_eq!(pipeline.build_profile, "production");
        assert!(pipeline.no_default_features);
        assert_eq!(pipeline.path_remapping, "disabled");
    }

    #[test]
    fn report_arguments_reject_untyped_build_labels() {
        assert!(
            Args::try_parse_from(["write_wasm_size_report", "--profile", "profile-ish"]).is_err()
        );
        assert!(Args::try_parse_from(["write_wasm_size_report", "--sql-mode", "maybe"]).is_err());
    }
}
