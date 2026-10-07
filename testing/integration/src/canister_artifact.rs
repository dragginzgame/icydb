//! Module: integration::canister_artifact
//! Responsibility: actor policy and inspection.
//! Does not own: builds or runtime.
//! Boundary: checks artifacts against policy.

use std::{collections::BTreeSet, env, ffi::OsString, fs, path::Path, time::Duration};

use crate::wasm_optimizer::format_tool_failure;
use candid::{
    CandidType,
    pretty::candid::compile,
    types::{FuncMode, Function, Type, TypeInner, internal::TypeContainer},
};
use ic_host_artifacts::wasm::{ExportKind, InspectionLimits, inspect};
use ic_host_fs::read::hash_file;
use ic_host_process::tool::{
    AdmittedTool, ExecutionContext, OutputLimits, ToolSpec, resolve_executable,
};
use ic_host_tools::candid::{ExtractionError, extract};
use icydb::{
    Error,
    db::{SchemaMigrationCommand, SchemaMigrationStatusPage, SchemaMigrationStatusRequest},
};

/// Query/update mode encoded by both an IC Wasm export and Candid service.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CanisterMethodMode {
    /// Ordinary replicated update method.
    Update,
    /// Non-composite query method.
    Query,
    /// Composite query method.
    CompositeQuery,
}

/// One method observed in a built canister artifact.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CanisterMethod {
    /// Fixed public method name.
    pub name: String,
    /// IC execution mode.
    pub mode: CanisterMethodMode,
}

impl CanisterMethod {
    fn new(name: impl Into<String>, mode: CanisterMethodMode) -> Self {
        Self {
            name: name.into(),
            mode,
        }
    }
}

/// One frozen expected method without allocating policy state.
pub type ExpectedCanisterMethod = (&'static str, CanisterMethodMode);

/// Frozen production/local policy for one maintained generated actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintainedCanisterPolicy {
    /// Integration-harness canister name.
    pub canister: &'static str,
    /// Cargo package producing the actor Wasm.
    pub package: &'static str,
    /// Exact production feature set, in deterministic lexical order.
    pub production_features: &'static [&'static str],
    /// Exact local/test feature set, in deterministic lexical order.
    pub local_test_features: &'static [&'static str],
    /// IcyDB-prefixed methods exported by the maintained production build.
    pub production_icydb_methods: &'static [ExpectedCanisterMethod],
    /// IcyDB-prefixed methods exported by the maintained local/test build.
    pub local_test_icydb_methods: &'static [ExpectedCanisterMethod],
}

const NO_METHODS: &[ExpectedCanisterMethod] = &[];
const METRICS_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
];
const SQL_PERF_LOCAL_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_fixtures_load", CanisterMethodMode::Update),
    ("icydb_fixtures_reset", CanisterMethodMode::Update),
];
const TEST_SQL_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_fixtures_load", CanisterMethodMode::Update),
    ("icydb_fixtures_reset", CanisterMethodMode::Update),
    ("icydb_integrity", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_query", CanisterMethodMode::Query),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
    ("icydb_update", CanisterMethodMode::Update),
];
const TEST_SQL_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_integrity", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
    ("icydb_update", CanisterMethodMode::Update),
];
const TEST_SQL_BOUNDED_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_fixtures_load", CanisterMethodMode::Update),
    ("icydb_fixtures_reset", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_query", CanisterMethodMode::Query),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
    ("icydb_update", CanisterMethodMode::Update),
];
const TEST_SQL_BOUNDED_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
    ("icydb_update", CanisterMethodMode::Update),
];
const TEST_SQL_GUARD_METHODS: &[ExpectedCanisterMethod] =
    &[("icydb_query", CanisterMethodMode::Query)];
const TEST_READ_AUTHORITY_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_query", CanisterMethodMode::Query),
    ("icydb_schema", CanisterMethodMode::Query),
];
const TEST_SCHEMA_METHODS: &[ExpectedCanisterMethod] =
    &[("icydb_schema", CanisterMethodMode::Query)];
const RPG_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
];
const RPG_LOCAL_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_fixtures_load", CanisterMethodMode::Update),
    ("icydb_fixtures_reset", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_query", CanisterMethodMode::Query),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
];

/// Frozen current build and export policy for maintained and evidence actors.
pub const MAINTAINED_CANISTER_POLICIES: &[MaintainedCanisterPolicy] = &[
    MaintainedCanisterPolicy {
        canister: "default_empty",
        package: "canister_audit_default_empty",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "sql"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "default_empty_metrics",
        package: "canister_audit_default_empty_metrics",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "sql"],
        production_icydb_methods: METRICS_METHODS,
        local_test_icydb_methods: METRICS_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "group_path_sql_query",
        package: "canister_audit_group_path_sql_query",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "sql"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "nested_relation_none",
        package: "canister_audit_nested_relation_none",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "nested_relation_direct",
        package: "canister_audit_nested_relation_direct",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "nested_relation_shallow",
        package: "canister_audit_nested_relation_shallow",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "nested_relation_repeated",
        package: "canister_audit_nested_relation_repeated",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_dynamic_query",
        package: "canister_audit_one_entity_dynamic_query",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_reachable_operations",
        package: "canister_audit_one_entity_reachable_operations",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_sql_query",
        package: "canister_audit_one_entity_sql_query",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "sql"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_typed_query",
        package: "canister_audit_one_entity_typed_query",
        production_features: &["candid-export", "u256-audit"],
        local_test_features: &["candid-export", "lifecycle-audit", "u256-audit"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "request_future_scale",
        package: "canister_audit_request_future_scale",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql_perf",
        package: "canister_audit_sql_perf",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "sql", "test-admin-api"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: SQL_PERF_LOCAL_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "ten_entity_typed_query",
        package: "canister_audit_ten_entity_typed_query",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "ten_entity_reachable_operations",
        package: "canister_audit_ten_entity_reachable_operations",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql",
        package: "canister_test_sql",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "local-sql-query", "test-admin-api"],
        production_icydb_methods: TEST_SQL_PRODUCTION_METHODS,
        local_test_icydb_methods: TEST_SQL_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql_bounded",
        package: "canister_test_sql_bounded",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "local-sql-query", "test-admin-api"],
        production_icydb_methods: TEST_SQL_BOUNDED_PRODUCTION_METHODS,
        local_test_icydb_methods: TEST_SQL_BOUNDED_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "lifecycle_participant",
        package: "canister_test_lifecycle_participant",
        production_features: &["candid-export"],
        local_test_features: &["candid-export", "population-seed"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "read_authority",
        package: "canister_test_read_authority",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "guarded-reads", "sql"],
        production_icydb_methods: TEST_READ_AUTHORITY_METHODS,
        local_test_icydb_methods: TEST_READ_AUTHORITY_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "schema_guard",
        package: "canister_test_schema_guard",
        production_features: &["candid-export"],
        local_test_features: &["candid-export", "guarded-schema"],
        production_icydb_methods: TEST_SCHEMA_METHODS,
        local_test_icydb_methods: TEST_SCHEMA_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "schema_public",
        package: "canister_test_schema_public",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: TEST_SCHEMA_METHODS,
        local_test_icydb_methods: TEST_SCHEMA_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql_guard",
        package: "canister_test_sql_guard",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "guarded-sql-query", "sql"],
        production_icydb_methods: TEST_SQL_GUARD_METHODS,
        local_test_icydb_methods: TEST_SQL_GUARD_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "startup_timer",
        package: "canister_test_startup_timer",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "demo_rpg",
        package: "canister_demo_rpg",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "local-sql-query", "test-admin-api"],
        production_icydb_methods: RPG_PRODUCTION_METHODS,
        local_test_icydb_methods: RPG_LOCAL_METHODS,
    },
];

/// Candid and raw-Wasm method manifests for one built actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanisterArtifactManifest {
    /// Generated Candid carried by the exact inspected Wasm artifact.
    pub candid: String,
    /// Methods registered in generated Candid.
    pub candid_methods: BTreeSet<CanisterMethod>,
    /// IC query/update exports present in raw Wasm, including reserved runtime
    /// self-call entrypoints that are intentionally absent from Candid.
    pub wasm_methods: BTreeSet<CanisterMethod>,
}

impl CanisterArtifactManifest {
    /// Return only methods in IcyDB's maintained public namespace.
    #[must_use]
    pub fn icydb_methods(&self) -> BTreeSet<CanisterMethod> {
        self.wasm_methods
            .iter()
            .filter(|method| method.name.starts_with("icydb_"))
            .cloned()
            .collect()
    }
}

/// Inspect one Candid-exporting canister and require Candid/application-Wasm
/// agreement while retaining reserved CDK runtime exports in the raw manifest.
///
/// # Errors
///
/// Returns an error for unreadable or malformed Wasm, unavailable Candid
/// extraction, malformed Candid service declarations, or a method/mode drift
/// between the two artifacts.
pub fn inspect_canister_artifacts(wasm_path: &Path) -> Result<CanisterArtifactManifest, String> {
    let wasm = fs::read(wasm_path)
        .map_err(|error| format!("failed to read {}: {error}", wasm_path.display()))?;
    let wasm_methods = inspect_wasm_methods(&wasm)?;

    let candid = extract_canister_candid(wasm_path)?;
    let candid_methods = inspect_candid_methods(&candid)?;
    let application_wasm_methods = candid_visible_wasm_methods(&wasm_methods);
    if candid_methods != application_wasm_methods {
        let runtime_methods = wasm_methods
            .difference(&application_wasm_methods)
            .cloned()
            .collect::<BTreeSet<_>>();
        return Err(format!(
            "Candid/Wasm method drift for {}: Candid {candid_methods:?}, application Wasm {application_wasm_methods:?}, reserved runtime Wasm {runtime_methods:?}",
            wasm_path.display()
        ));
    }

    Ok(CanisterArtifactManifest {
        candid,
        candid_methods,
        wasm_methods,
    })
}

// The setup catalog owns version selection. Capture the installed executable's
// identity once, then let shared admission/extraction detect changes during use.
// This is a local installed-tool observation, not an upstream binary digest pin.
pub(crate) fn extract_canister_candid(wasm_path: &Path) -> Result<String, String> {
    let current_dir = env::current_dir().map_err(|error| error.to_string())?;
    let environment = env::vars_os().collect::<Vec<_>>();
    let context = ExecutionContext {
        current_dir: &current_dir,
        environment: &environment,
    };
    let search = env::var_os("PATH")
        .map(|path| env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();
    let executable = resolve_executable(Path::new("candid-extractor"), &current_dir, &search)
        .map_err(|error| format!("resolve Candid extractor: {error}"))?;
    let version = include_str!("../../../ci/icydb-tools.env")
        .lines()
        .find_map(|line| line.strip_prefix("export ICYDB_CANDID_EXTRACTOR_VERSION="))
        .ok_or_else(|| "Candid extractor version is absent from the tool catalog".to_string())?;
    let limits = OutputLimits {
        stdout_bytes: 1024 * 1024,
        stderr_bytes: 1024 * 1024,
        timeout: Duration::from_secs(600),
    };
    let tool = AdmittedTool::admit(
        &ToolSpec {
            executable: &executable,
            sha256: hash_file(&executable, u64::MAX)
                .map_err(|error| error.to_string())?
                .sha256,
            executable_bytes: u64::MAX,
            version_arguments: &[OsString::from("--version")],
            version_identity: &format!("candid-extractor {version}"),
        },
        &context,
        limits,
    )
    .map_err(|error| format_tool_failure("Candid extractor admission", &error))?;
    let source =
        fs::canonicalize(wasm_path).map_err(|error| format!("resolve Candid source: {error}"))?;
    let extracted =
        extract(&tool, &source, &context, u64::MAX, limits).map_err(|error| match error {
            ExtractionError::Tool(error) => format_tool_failure("Candid extraction", &error),
            error => format!("Candid extraction: {error}"),
        })?;
    // Keep the exact existing artifact/manifest text, rather than adopting
    // Canic's whitespace normalization as an IcyDB format change.
    String::from_utf8(extracted.evidence.stdout)
        .map_err(|error| format!("Candid extractor returned non-UTF-8 output: {error}"))
}

/// Read IC method exports directly from a raw Wasm module.
///
/// # Errors
///
/// Returns an error for invalid framing, overflowing lengths, malformed UTF-8,
/// duplicate IC method exports, or a truncated export section.
pub fn inspect_wasm_methods(wasm: &[u8]) -> Result<BTreeSet<CanisterMethod>, String> {
    // Natural byte-derived ceilings preserve whole-module inspection. Shared
    // facts own framing; IcyDB owns the method namespace, modes and CDK policy.
    let facts = inspect(
        wasm,
        InspectionLimits {
            module_bytes: wasm.len(),
            sections: wasm.len(),
            exports: u32::try_from(wasm.len()).unwrap_or(u32::MAX),
            custom_sections: wasm.len(),
        },
    )
    .map_err(|error| format!("failed to inspect canister Wasm: {error}"))?;
    Ok(facts
        .exports
        .into_iter()
        .filter(|(_, export)| export.kind == ExportKind::Function)
        .filter_map(|(name, _)| method_from_wasm_export(name))
        .collect())
}

/// Read method names and modes from one generated Candid service.
///
/// # Errors
///
/// Returns an error for a missing or unterminated service, malformed method
/// declaration, unsupported mode suffix, or duplicate method/mode pair.
pub fn inspect_candid_methods(candid: &str) -> Result<BTreeSet<CanisterMethod>, String> {
    let candid = strip_candid_line_comments(candid);
    let service_offset = candid
        .rfind("service :")
        .ok_or_else(|| "Candid contract has no service declaration".to_string())?;
    let service = &candid[service_offset..];
    let open = service
        .find('{')
        .ok_or_else(|| "Candid service has no opening brace".to_string())?;

    let mut methods = BTreeSet::new();
    let mut depth = 1_u32;
    let mut statement = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for character in service[open + 1..].chars() {
        if quoted {
            statement.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                quoted = false;
            }
            continue;
        }

        match character {
            '"' => {
                quoted = true;
                statement.push(character);
            }
            '{' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| "Candid service nesting overflow".to_string())?;
                statement.push(character);
            }
            '}' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| "Candid service nesting underflow".to_string())?;
                if depth == 0 {
                    if !statement.trim().is_empty() {
                        let method = parse_candid_method(statement.trim())?;
                        if !methods.insert(method.clone()) {
                            return Err(format!("duplicate Candid method {method:?}"));
                        }
                    }
                    return Ok(methods);
                }
                statement.push(character);
            }
            ';' if depth == 1 => {
                let method = parse_candid_method(statement.trim())?;
                if !methods.insert(method.clone()) {
                    return Err(format!("duplicate Candid method {method:?}"));
                }
                statement.clear();
            }
            _ => statement.push(character),
        }
    }

    Err("unterminated Candid service declaration".to_string())
}

/// Render the normative 0.218 migration endpoint ABI from public Rust DTOs.
#[must_use]
pub fn render_schema_migration_endpoint_abi() -> String {
    let mut container = TypeContainer::new();
    let methods = vec![
        endpoint_method::<SchemaMigrationCommand, Result<SchemaMigrationStatusPage, Error>>(
            &mut container,
            "icydb_schema_migrate",
            CanisterMethodMode::Update,
        ),
        endpoint_method::<SchemaMigrationStatusRequest, Result<SchemaMigrationStatusPage, Error>>(
            &mut container,
            "icydb_schema_migration",
            CanisterMethodMode::Query,
        ),
    ];
    let actor: Type = TypeInner::Service(methods).into();
    format!("{}\n", compile(&container.env, &Some(actor)))
}

fn endpoint_method<A: CandidType + 'static, R: CandidType>(
    container: &mut TypeContainer,
    name: &str,
    mode: CanisterMethodMode,
) -> (String, Type) {
    let args = if std::any::TypeId::of::<A>() == std::any::TypeId::of::<()>() {
        Vec::new()
    } else {
        vec![container.add::<A>()]
    };
    let mode = match mode {
        CanisterMethodMode::Update => Vec::new(),
        CanisterMethodMode::Query => vec![FuncMode::Query],
        CanisterMethodMode::CompositeQuery => vec![FuncMode::CompositeQuery],
    };
    let function = Function {
        modes: mode,
        args,
        rets: vec![container.add::<R>()],
    };
    (name.to_string(), TypeInner::Func(function).into())
}

fn method_from_wasm_export(name: &str) -> Option<CanisterMethod> {
    [
        ("canister_update ", CanisterMethodMode::Update),
        ("canister_query ", CanisterMethodMode::Query),
        (
            "canister_composite_query ",
            CanisterMethodMode::CompositeQuery,
        ),
    ]
    .into_iter()
    .find_map(|(prefix, mode)| {
        name.strip_prefix(prefix)
            .map(|method| CanisterMethod::new(method, mode))
    })
}

fn candid_visible_wasm_methods(methods: &BTreeSet<CanisterMethod>) -> BTreeSet<CanisterMethod> {
    methods
        .iter()
        .filter(|method| {
            method.name != "<ic-cdk internal> timer_executor"
                || method.mode != CanisterMethodMode::Update
        })
        .cloned()
        .collect()
}

fn parse_candid_method(statement: &str) -> Result<CanisterMethod, String> {
    let (name, signature) = statement
        .split_once(':')
        .ok_or_else(|| format!("malformed Candid method declaration '{statement}'"))?;
    let name = name.trim().trim_matches('"');
    if name.is_empty() {
        return Err("Candid method name is empty".to_string());
    }
    let signature = signature.trim_end();
    let mode = if signature.ends_with(" composite_query") {
        CanisterMethodMode::CompositeQuery
    } else if signature.ends_with(" query") {
        CanisterMethodMode::Query
    } else if signature.ends_with(')') {
        CanisterMethodMode::Update
    } else {
        return Err(format!(
            "unsupported Candid method mode in declaration '{statement}'"
        ));
    };

    Ok(CanisterMethod::new(name, mode))
}

fn strip_candid_line_comments(candid: &str) -> String {
    let mut output = String::with_capacity(candid.len());
    for line in candid.lines() {
        let mut quoted = false;
        let mut escaped = false;
        let mut chars = line.char_indices().peekable();
        let mut end = line.len();
        while let Some((index, character)) = chars.next() {
            if quoted {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    quoted = false;
                }
                continue;
            }
            if character == '"' {
                quoted = true;
                continue;
            }
            if character == '/' && chars.peek().is_some_and(|(_, next)| *next == '/') {
                end = index;
                break;
            }
        }
        output.push_str(&line[..end]);
        output.push('\n');
    }
    output
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs};

    use super::{
        CanisterMethod, CanisterMethodMode, MAINTAINED_CANISTER_POLICIES,
        candid_visible_wasm_methods, inspect_candid_methods, inspect_canister_artifacts,
        inspect_wasm_methods, render_schema_migration_endpoint_abi,
    };

    #[test]
    fn candid_inspection_preserves_names_and_modes_through_nested_types() {
        let candid = r"
            type Nested = record { value : text; callback : func () -> () };
            service : {
                // Comments and nested records do not split declarations.
                read : (record { nested : Nested }) -> (variant { Ok; Err : text }) query;
                write : (text) -> (record { nested : record { value : nat64 } });
                composed : () -> (Nested) composite_query
            }
        ";

        let observed = inspect_candid_methods(candid).expect("Candid should inspect");
        let expected = BTreeSet::from([
            CanisterMethod::new("composed", CanisterMethodMode::CompositeQuery),
            CanisterMethod::new("read", CanisterMethodMode::Query),
            CanisterMethod::new("write", CanisterMethodMode::Update),
        ]);
        assert_eq!(observed, expected);
    }

    #[test]
    fn raw_wasm_inspection_reads_only_ic_function_exports() {
        let wasm = wasm_with_exports(&[
            ("canister_composite_query composed", 0),
            ("canister_query read", 0),
            ("canister_update write", 0),
            ("canister_update <ic-cdk internal> timer_executor", 0),
            ("get_candid_pointer", 0),
            ("canister_query not_a_function", 3),
        ]);

        let observed = inspect_wasm_methods(&wasm).expect("Wasm should inspect");
        let expected = BTreeSet::from([
            CanisterMethod::new("composed", CanisterMethodMode::CompositeQuery),
            CanisterMethod::new(
                "<ic-cdk internal> timer_executor",
                CanisterMethodMode::Update,
            ),
            CanisterMethod::new("read", CanisterMethodMode::Query),
            CanisterMethod::new("write", CanisterMethodMode::Update),
        ]);
        assert_eq!(observed, expected);
    }

    #[test]
    fn raw_wasm_inspection_rejects_duplicate_exports_and_malformed_sections() {
        let duplicate =
            wasm_with_exports(&[("canister_query read", 0), ("canister_query read", 0)]);
        assert!(inspect_wasm_methods(&duplicate).is_err());
        for tail in [
            &[7, 2, 1][..],
            &[7, 0x80, 0x80, 0x80, 0x80, 0x10][..],
            &[7, 2, 0, 0][..],
        ] {
            let wasm = [b"\0asm\x01\0\0\0".as_slice(), tail].concat();
            assert!(inspect_wasm_methods(&wasm).is_err());
        }
    }

    #[test]
    fn candid_agreement_excludes_only_reserved_cdk_runtime_exports() {
        let methods = BTreeSet::from([
            CanisterMethod::new(
                "<ic-cdk internal> timer_executor",
                CanisterMethodMode::Update,
            ),
            CanisterMethod::new("<ic-cdk internal> unexpected", CanisterMethodMode::Update),
            CanisterMethod::new("read", CanisterMethodMode::Query),
            CanisterMethod::new("write", CanisterMethodMode::Update),
        ]);

        assert_eq!(
            candid_visible_wasm_methods(&methods),
            BTreeSet::from([
                CanisterMethod::new("<ic-cdk internal> unexpected", CanisterMethodMode::Update,),
                CanisterMethod::new("read", CanisterMethodMode::Query),
                CanisterMethod::new("write", CanisterMethodMode::Update),
            ]),
        );
    }

    #[test]
    fn maintained_policy_identity_is_unique_and_policy_is_deterministic() {
        let names = MAINTAINED_CANISTER_POLICIES
            .iter()
            .map(|policy| policy.canister)
            .collect::<BTreeSet<_>>();
        let packages = MAINTAINED_CANISTER_POLICIES
            .iter()
            .map(|policy| policy.package)
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), MAINTAINED_CANISTER_POLICIES.len());
        assert_eq!(packages.len(), MAINTAINED_CANISTER_POLICIES.len());
        for policy in MAINTAINED_CANISTER_POLICIES {
            assert!(policy.production_features.is_sorted());
            assert!(policy.local_test_features.is_sorted());
            assert!(policy.local_test_icydb_methods.is_sorted());
            assert!(policy.production_icydb_methods.is_sorted());
        }
    }

    #[test]
    fn schema_migration_endpoint_abi_matches_golden() {
        assert_eq!(
            render_schema_migration_endpoint_abi(),
            include_str!("contracts/0.218/schema-migration-endpoints.did")
        );
    }

    #[test]
    fn shared_candid_and_publication_preserve_artifacts_and_failure_boundaries() {
        let root = std::env::temp_dir().join(format!("icydb host staging {}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let input = root.join("input.wasm");
        let directory = root.join("missing parent").join("staged");
        let text = "service : { icydb_schema : () -> () query; };  \n\n";
        let bytes = candid_exporting_wasm(text);
        fs::write(&input, &bytes).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&input, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let manifest = inspect_canister_artifacts(&input).unwrap();
        assert_eq!(manifest.candid, format!("{text}\n"));
        assert_eq!(
            manifest.icydb_methods(),
            BTreeSet::from([CanisterMethod::new(
                "icydb_schema",
                CanisterMethodMode::Query
            ),])
        );
        let (wasm, did) =
            crate::stage_canister_artifact_paths(&input, &input, &directory, "probe", true)
                .unwrap();
        let did = did.unwrap();
        assert_eq!(fs::read(&wasm).unwrap(), bytes);
        assert_eq!(
            fs::read(directory.join("probe.compiler.wasm")).unwrap(),
            bytes
        );
        assert_eq!(fs::read(&did).unwrap(), manifest.candid.as_bytes());
        assert_eq!(fs::read(&input).unwrap(), bytes);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&wasm).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert!(crate::publish_artifact_copy(&root, &wasm).is_err());
        assert_eq!(fs::read(&wasm).unwrap(), bytes);
        fs::write(&input, b"\0asm\x01\0\0\0").unwrap();
        assert!(
            crate::stage_canister_artifact_paths(&input, &input, &directory, "probe", true)
                .is_err()
        );
        assert_eq!(fs::read(&did).unwrap(), manifest.candid.as_bytes());
        let (wasm, absent) =
            crate::stage_canister_artifact_paths(&input, &input, &directory, "probe", false)
                .unwrap();
        assert_eq!(fs::read(wasm).unwrap(), b"\0asm\x01\0\0\0");
        assert!(absent.is_none() && !did.exists());
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 2);
        fs::remove_dir_all(root).unwrap();
    }

    // Real extractor input: an exported memory contains the NUL-terminated DID,
    // a pointer function returns its address, and the IC query is a separate body.
    fn candid_exporting_wasm(text: &str) -> Vec<u8> {
        let mut wasm = b"\0asm\x01\0\0\0".to_vec();
        for (id, payload) in [
            (1, vec![2, 0x60, 0, 1, 0x7f, 0x60, 0, 0]),
            (3, vec![2, 0, 1]),
            (5, vec![1, 0, 1]),
        ] {
            wasm.extend([id, u8::try_from(payload.len()).unwrap()]);
            wasm.extend(payload);
        }
        let mut exports = vec![3];
        for (name, kind, index) in [
            ("get_candid_pointer", 0, 0),
            ("canister_query icydb_schema", 0, 1),
            ("memory", 2, 0),
        ] {
            push_u32_leb(&mut exports, u32::try_from(name.len()).unwrap());
            exports.extend(name.as_bytes());
            exports.extend([kind, index]);
        }
        wasm.push(7);
        push_u32_leb(&mut wasm, u32::try_from(exports.len()).unwrap());
        wasm.extend(exports);
        wasm.extend([10, 9, 2, 4, 0, 0x41, 0, 0x0b, 2, 0, 0x0b]);
        let mut data = vec![1, 0, 0x41, 0, 0x0b];
        push_u32_leb(&mut data, u32::try_from(text.len() + 1).unwrap());
        data.extend(text.as_bytes());
        data.push(0);
        wasm.push(11);
        push_u32_leb(&mut wasm, u32::try_from(data.len()).unwrap());
        wasm.extend(data);
        wasm
    }

    fn wasm_with_exports(exports: &[(&str, u8)]) -> Vec<u8> {
        let mut section = Vec::new();
        push_u32_leb(
            &mut section,
            u32::try_from(exports.len()).expect("fixture count fits"),
        );
        for (index, (name, kind)) in exports.iter().enumerate() {
            push_u32_leb(
                &mut section,
                u32::try_from(name.len()).expect("fixture name length fits"),
            );
            section.extend_from_slice(name.as_bytes());
            section.push(*kind);
            push_u32_leb(
                &mut section,
                u32::try_from(index).expect("fixture index fits"),
            );
        }

        let mut wasm = b"\0asm\x01\0\0\0".to_vec();
        wasm.push(7);
        push_u32_leb(
            &mut wasm,
            u32::try_from(section.len()).expect("fixture section length fits"),
        );
        wasm.extend_from_slice(&section);
        wasm
    }

    fn push_u32_leb(output: &mut Vec<u8>, mut value: u32) {
        loop {
            let mut byte = u8::try_from(value & 0x7f).expect("seven bits fit u8");
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                return;
            }
        }
    }
}
